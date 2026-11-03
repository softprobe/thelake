use super::encode::tail_message;
use super::logql::parse_logql;
use super::params::LokiTailParams;
use crate::api::AppState;
use crate::compat::backends::logs::{LogDirection, LogsQueryBackend};
use crate::compat::workspace::CompatWorkspaceContext;
use axum::extract::ws::{Message, WebSocket};
use futures::{SinkExt, StreamExt};
use std::time::Duration;

const DEFAULT_TAIL_LOOKBACK_NS: i64 = 3_600_000_000_000;
const TAIL_POLL_INTERVAL: Duration = Duration::from_secs(1);

pub async fn run(
    state: AppState,
    ctx: CompatWorkspaceContext,
    mut socket: WebSocket,
    params: LokiTailParams,
) {
    let mut request = match parse_logql(&params.query) {
        Ok(request) => request,
        Err(_) => {
            let _ = socket.close().await;
            return;
        }
    };
    let backend = match super::handlers::backend_for(&state, &ctx).await {
        Ok(backend) => backend,
        Err(_) => {
            let _ = socket.close().await;
            return;
        }
    };

    let (mut sender, mut receiver) = socket.split();
    tokio::spawn(async move {
        while let Some(Ok(message)) = receiver.next().await {
            if matches!(message, Message::Close(_)) {
                break;
            }
        }
    });

    let delay = Duration::from_secs(params.delay_for_secs);
    let mut cursor_ns = params.start_ns.unwrap_or_else(|| {
        chrono::Utc::now()
            .timestamp_nanos_opt()
            .unwrap_or(0)
            .saturating_sub(DEFAULT_TAIL_LOOKBACK_NS)
    });

    loop {
        if ctx.remaining().is_zero() {
            break;
        }
        let now_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        request.start_ns = Some(cursor_ns);
        request.end_ns = Some(now_ns.saturating_add(1));
        request.limit = params.limit;
        request.direction = LogDirection::Forward;

        let hits = backend.query_range(&ctx, request.clone()).await;
        match hits {
            Ok(hits) if !hits.is_empty() => {
                if let Some(last_ts) = hits.iter().map(|hit| hit.timestamp_ns).max() {
                    cursor_ns = last_ts.saturating_add(1);
                }
                let payload = tail_message(&hits);
                let Ok(text) = serde_json::to_string(&payload) else {
                    break;
                };
                if sender.send(Message::Text(text.into())).await.is_err() {
                    break;
                }
            }
            Ok(_) => {}
            Err(_) => break,
        }

        tokio::time::sleep(delay + TAIL_POLL_INTERVAL).await;
    }

    let _ = sender.close().await;
}

#[cfg(test)]
mod tests {
    #[test]
    fn tail_poll_does_not_take_a_workspace_lifecycle_lock() {
        let source = include_str!("tail.rs");
        let production = source.split("#[cfg(test)]").next().expect("production");
        assert!(!production.contains("with_active_workspace_access"));
        assert!(production.contains("backend.query_range(&ctx, request.clone())"));
    }
}
