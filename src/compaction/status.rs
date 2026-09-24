//! Maintenance action status and pass summaries.

#[derive(Debug, Clone)]
pub struct MaintenanceSummary {
    pub tables: Vec<TableMaintenanceResult>,
}

#[derive(Debug, Clone)]
pub struct TableMaintenanceResult {
    pub table: String,
    pub metadata: MetadataMaintenanceResult,
    pub compaction: ActionResult,
    pub rewrite_manifests: ActionResult,
    pub remove_orphan_files: ActionResult,
}

#[derive(Debug, Clone)]
pub struct MetadataMaintenanceResult {
    pub expired_snapshots: usize,
    pub skipped: bool,
}

#[derive(Debug, Clone)]
pub struct ActionResult {
    pub status: ActionStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActionStatus {
    Completed,
    Skipped,
    /// Attempted and failed (ops metrics: status=error).
    Failed,
    Unsupported,
}

/// Ops metric status for orphan cleanup: `None` = do not emit (disabled / no-op).
pub fn orphan_metric_status(enabled: bool, status: ActionStatus) -> Option<&'static str> {
    if !enabled {
        return None;
    }
    match status {
        ActionStatus::Completed => Some("ok"),
        ActionStatus::Failed | ActionStatus::Unsupported => Some("error"),
        ActionStatus::Skipped => None,
    }
}

/// Ops metric status for snapshot expire: `None` = do not emit (disabled).
pub fn snapshot_metric_status(enabled: bool, skipped: bool) -> Option<&'static str> {
    if !enabled {
        return None;
    }
    Some(if skipped { "error" } else { "ok" })
}

/// Pass-level ok: false when any attempted compaction is Failed or Unsupported.
pub fn pass_compaction_ok(statuses: &[ActionStatus]) -> bool {
    !statuses
        .iter()
        .any(|s| matches!(s, ActionStatus::Failed | ActionStatus::Unsupported))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn orphan_metric_status_emit_rules() {
        assert_eq!(orphan_metric_status(false, ActionStatus::Completed), None);
        assert_eq!(orphan_metric_status(false, ActionStatus::Failed), None);
        assert_eq!(
            orphan_metric_status(true, ActionStatus::Completed),
            Some("ok")
        );
        assert_eq!(
            orphan_metric_status(true, ActionStatus::Failed),
            Some("error")
        );
        assert_eq!(
            orphan_metric_status(true, ActionStatus::Unsupported),
            Some("error")
        );
        assert_eq!(orphan_metric_status(true, ActionStatus::Skipped), None);
    }

    #[test]
    fn snapshot_metric_status_emit_rules() {
        assert_eq!(snapshot_metric_status(false, true), None);
        assert_eq!(snapshot_metric_status(false, false), None);
        assert_eq!(snapshot_metric_status(true, false), Some("ok"));
        assert_eq!(snapshot_metric_status(true, true), Some("error"));
    }

    #[test]
    fn pass_compaction_ok_rules() {
        assert!(pass_compaction_ok(&[
            ActionStatus::Completed,
            ActionStatus::Skipped
        ]));
        assert!(!pass_compaction_ok(&[
            ActionStatus::Completed,
            ActionStatus::Failed
        ]));
        assert!(!pass_compaction_ok(&[ActionStatus::Unsupported]));
    }
}
