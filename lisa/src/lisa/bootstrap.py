"""Compose workspace services independently of an agent backend or protocol."""

from dataclasses import dataclass
from threading import Lock

from .context import WorkspaceContext
from .evaluation.policy_triage import OpenAIPolicyTriager
from .repositories.conversations import FileConversationRepository
from .repositories.evaluation_artifacts import EvaluationArtifactRepository
from .repositories.failures import FailureRepository
from .repositories.lisa_database import LisaDatabaseReader
from .repositories.notifications import NotificationReceiptRepository
from .repositories.policies import InMemoryPolicyRepository, PostgresPolicyRepository
from .repositories.slack_events import SlackEventRepository
from .repositories.work_items import PostgresWorkItemStore
from .runtime import confirmed_policy_title, run_turn
from .services.conversations import ConversationService, ScheduledQAService
from .services.database_inspection import DatabaseInspectionService
from .services.evaluations import EvaluationService
from .services.failures import FailureService
from .services.notifications import SlackNotifier
from .services.policies import PolicyService
from .services.sessions import SessionService
from .services.slack_events import SlackEventService, SlackEventWorker
from .slack import SlackClient
from .thelake_client import TheLakeClient
from .views.evaluations import render_evaluation_context

_memory_policy_repositories: dict[tuple[str, str], InMemoryPolicyRepository] = {}
_memory_policy_repositories_lock = Lock()


@dataclass
class WorkspaceServices:
    context: WorkspaceContext
    client: TheLakeClient
    conversation_repository: FileConversationRepository
    conversations: ConversationService
    policy_service: PolicyService
    failures: FailureService
    notifier: SlackNotifier
    evaluations: EvaluationService
    sessions: SessionService
    database_inspection: DatabaseInspectionService
    slack_event_service: SlackEventService
    slack_worker: SlackEventWorker
    scheduled_qa: ScheduledQAService


def build_services(context: WorkspaceContext) -> WorkspaceServices:
    client = TheLakeClient(context)
    conversation_repository = FileConversationRepository(context.data_dir)
    conversations = ConversationService(conversation_repository)
    slack_event_service = SlackEventService(
        conversation_repository,
        SlackEventRepository(conversation_repository),
    )
    slack_worker = SlackEventWorker(
        slack_event_service,
        SlackClient(context.slack_token),
        run_turn,
        confirmed_policy_title,
    )
    scheduled_qa = ScheduledQAService(conversation_repository, conversations)
    if context.database_url:
        policy_repository = PostgresPolicyRepository(
            context.database_url, context.workspace_id()
        )
    else:
        # The database-free development runtime still shares policy state
        # between HTTP/MCP service graphs within this process.
        key = (str(context.data_dir), context.workspace)
        with _memory_policy_repositories_lock:
            policy_repository = _memory_policy_repositories.setdefault(
                key, InMemoryPolicyRepository()
            )
    policy_service = PolicyService(context, repository=policy_repository)
    failures = FailureService(context, client, repository=FailureRepository(context))
    notifier = SlackNotifier(
        context,
        SlackClient(context.slack_token),
        repository=NotificationReceiptRepository(context),
    )
    work_items = (
        PostgresWorkItemStore(context.database_url, context.workspace_id())
        if context.database_url
        else None
    )
    database_reader = (
        LisaDatabaseReader(context.database_read_url, context.workspace_id())
        if context.database_read_url
        else None
    )
    sessions = SessionService(client)
    database_inspection = DatabaseInspectionService(database_reader)
    evaluations = EvaluationService(
        context,
        client,
        policy_service,
        failures,
        notifier,
        sessions=sessions,
        policy_triage=OpenAIPolicyTriager(),
        work_items=work_items,
        artifact_repository=EvaluationArtifactRepository(context),
        render_context=render_evaluation_context,
    )
    return WorkspaceServices(
        context=context,
        client=client,
        conversation_repository=conversation_repository,
        conversations=conversations,
        policy_service=policy_service,
        failures=failures,
        notifier=notifier,
        evaluations=evaluations,
        sessions=sessions,
        database_inspection=database_inspection,
        slack_event_service=slack_event_service,
        slack_worker=slack_worker,
        scheduled_qa=scheduled_qa,
    )
