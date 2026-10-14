"""Compose workspace services independently of an agent backend or protocol."""

from dataclasses import dataclass

from .context import WorkspaceContext
from .evaluation.failures import FailureStore
from .evaluation.service import EvaluationService
from .notifications import SlackNotifier
from .policies import PolicyStore
from .thelake_client import TheLakeClient


@dataclass
class WorkspaceServices:
    context: WorkspaceContext
    client: TheLakeClient
    policies: PolicyStore
    failures: FailureStore
    notifier: SlackNotifier
    evaluations: EvaluationService


def build_services(context: WorkspaceContext) -> WorkspaceServices:
    client = TheLakeClient(context)
    policies = PolicyStore(context)
    failures = FailureStore(context, client)
    notifier = SlackNotifier(context)
    evaluations = EvaluationService(context, client, policies, failures, notifier)
    return WorkspaceServices(context, client, policies, failures, notifier, evaluations)
