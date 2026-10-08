//! Shared control-plane wiring (auth resolver and admin engine).

pub mod admin;

use crate::authn;
pub use admin::AdminEngine;

#[derive(Clone)]
pub struct ControlPlaneRuntime {
    pub resolver: authn::Resolver,
}
