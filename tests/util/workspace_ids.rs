//! Canonical workspace UUID fixtures for auth and isolation contracts.
//!
//! Keep string values identical to `tests/util/workspace_ids.env` (shell/compose).
//! Each test binary may use a subset of these IDs; unused constants are expected.
#![allow(dead_code)]

/// Shared workspace UUID for single-tenant compatibility auth contracts.
pub const COMPAT_WORKSPACE_ID: &str = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";

/// Isolation fixture workspace A (matches Grafana tenant-a mapping).
pub const COMPAT_WORKSPACE_A: &str = "cccccccc-cccc-cccc-cccc-cccccccccccc";

/// Isolation fixture workspace B (matches Grafana tenant-b mapping).
pub const COMPAT_WORKSPACE_B: &str = "dddddddd-dddd-dddd-dddd-dddddddddddd";

/// Distinct UUID used only as a mismatched `X-Scope-OrgID` / spoof query value.
pub const COMPAT_OTHER_WORKSPACE_ID: &str = "ffffffff-ffff-ffff-ffff-ffffffffffff";
