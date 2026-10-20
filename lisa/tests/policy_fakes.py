"""Shared doubles for Lisa evaluation service tests."""

from __future__ import annotations

from lisa.services.evaluations import EvaluationService
from lisa.services.sessions import SessionService


class FakeDecisionsClient:
    def __init__(self, choices, confidences=None):
        self.choices = choices
        self.confidences = confidences
        self.request = None

    def post(self, _url, *, json):
        self.request = json
        answers = []
        for index, question in enumerate(json["questions"]):
            answer = {
                "type": "choice",
                "name": question["name"],
                "choice": self.choices[index],
            }
            if self.confidences is not None:
                answer["confidence"] = self.confidences[index]
            answers.append(answer)
        return FakeResponse({"answers": answers})


class FakeResponse:
    def __init__(self, body):
        self.body = body

    def raise_for_status(self):
        pass

    def json(self):
        return self.body


class FakePolicyStore:
    def __init__(self, rules):
        self.rules = rules

    def approved_rules(self, target_agent_name):
        return self.rules


class FakeSessionClient:
    def __init__(self, session_detail):
        self.session_detail = session_detail
        self.get_calls = []
        self.posts = []

    def get(self, path):
        self.get_calls.append(path)
        return self.session_detail

    def post(self, path, payload, *, include_status=False):
        self.posts.append((path, payload))
        return ({}, 201) if include_status else {}


class FakeTriager:
    def __init__(self, selected):
        self.selected = selected
        self.calls = []
        self.model = "test-decisions-model"

    def triage(self, trajectory, rules):
        self.calls.append((trajectory, rules))
        selected_ids = {item.get("id") for item in self.selected}
        return {
            "selected_policies": self.selected,
            "policy_decisions": [
                {
                    "id": rule.id,
                    "name": rule.name,
                    "scope": rule.scope,
                    "decision": (
                        "suspected_violation"
                        if rule.id in selected_ids
                        else "no_violation_found"
                    ),
                }
                for rule in rules
            ],
        }


def make_evaluation_service(**kwargs):
    client = kwargs.pop("client")
    build_events = kwargs.pop("build_events", None)
    triager = kwargs.pop("policy_triage", FakeTriager([]))
    sessions = SessionService(client, build_events) if build_events else SessionService(client)
    return EvaluationService(
        client=client,
        sessions=sessions,
        policy_triage=triager,
        **kwargs,
    )
