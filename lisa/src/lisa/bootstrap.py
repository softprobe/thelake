"""Compose workspace services independently of an agent backend or protocol."""

from dataclasses import dataclass
from threading import Lock

from .context import WorkspaceContext
from .evaluation.policy_triage import OpenAIPolicyTriager
from .repositories.activities import PostgresActivityStore
from .repositories.conversations import FileConversationRepository
from .repositories.evaluation_artifacts import EvaluationArtifactRepository
from .repositories.failures import FailureRepository
from .repositories.lisa_database import LisaDatabaseReader
from .repositories.notifications import NotificationReceiptRepository
from .repositories.policies import InMemoryPolicyRepository, PostgresPolicyRepository
from .repositories.slack_events import SlackEventRepository
from .repositories.session_traces import SessionTraceStore
from .repositories.session_evaluation_queue import SessionEvaluationQueue
from .runtime import run_turn
from .services.conversations import ConversationService
from .services.database_inspection import DatabaseInspectionService
from .services.evaluations import EvaluationService
from .services.failures import FailureService
from .services.notifications import SlackNotifier
from .services.policies import PolicyService
from .services.sessions import SessionService
from .services.session_evaluations import SessionEvaluationWorker
from .services.slack_events import SlackEventService, SlackEventWorker
from .slack import SlackClient
from .views.evaluations import render_evaluation_context

_memory_policy_repositories: dict[tuple[str, str], InMemoryPolicyRepository] = {}
_memory_policy_repositories_lock = Lock()


@dataclass
class WorkspaceServices:
    context: WorkspaceContext
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
    session_traces: SessionTraceStore
    session_evaluation_queue: SessionEvaluationQueue | None = None
    session_evaluation_worker: SessionEvaluationWorker | None = None
    activity_store: PostgresActivityStore | None = None


def build_services(context: WorkspaceContext) -> WorkspaceServices:
    conversation_repository = FileConversationRepository(context.data_dir)
    activities = (
        PostgresActivityStore(context.database_url, context.workspace_id())
        if context.database_url
        else None
    )
    conversations = ConversationService(
        conversation_repository,
        activity_store=activities,
        owner_user_ids=context.owner_user_ids,
    )
    slack_event_service = SlackEventService(
        conversation_repository,
        SlackEventRepository(conversation_repository),
        activity_store=activities,
        owner_user_ids=context.owner_user_ids,
    )
    slack_worker = SlackEventWorker(
        slack_event_service,
        SlackClient(context.slack_token),
        run_turn,
    )
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
    failures = FailureService(context, repository=FailureRepository(context))
    notifier = SlackNotifier(
        context,
        SlackClient(context.slack_token),
        repository=NotificationReceiptRepository(context),
    )
    database_reader = (
        LisaDatabaseReader(context.database_read_url, context.workspace_id())
        if context.database_read_url
        else None
    )
    session_traces = SessionTraceStore(context.agent_workspace_root)
    sessions = SessionService(session_traces)
    database_inspection = DatabaseInspectionService(database_reader)
    evaluations = EvaluationService(
        context,
        policy_service,
        failures,
        notifier,
        sessions=sessions,
        policy_triage=OpenAIPolicyTriager(),
        activities=activities,
        artifact_repository=EvaluationArtifactRepository(context),
        render_context=render_evaluation_context,
    )
    queue = SessionEvaluationQueue(context.database_url) if context.database_url else None
    services = WorkspaceServices(
        context=context,
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
        session_traces=session_traces,
        session_evaluation_queue=queue,
        activity_store=activities,
    )
    if queue is not None:
        services.session_evaluation_worker = SessionEvaluationWorker(
            queue, session_traces, services
        )
    return services
