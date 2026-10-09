---
name: deploy-thelake
description: Prepare or deploy the public TheLake runtime using its documented release packaging and deployment configuration.
---

# Deploy TheLake

Start with `README.md`'s **Publish Docker image** section and
`docs/reference/config.md`. This public repository documents release image
packaging and publishing, but does not provide production Kubernetes, Helm, or
cloud rollout manifests. Do not invent a production target or use private
Softprobe cloud procedures in this public project.

## Choose the requested operation

- **Build a local release artifact:** use `make build-release`; inspect the
  resulting `dist/` contents and the current Dockerfile before packaging.
- **Publish a versioned image:** use the official GitHub Release workflow when
  requested. Publishing is an external release action; do not trigger it as a
  side effect of a build or test request.
- **Deploy to a customer-selected platform:** inspect its manifests and
  deployment instructions first. If the platform, environment, or target
  release is missing, ask for that information before changing live resources.
  Keep secrets in the platform's secret manager.

## Runtime configuration

The runtime requires a PostgreSQL DuckLake catalog and a durable `data_path`
(local filesystem, S3, or GCS). Follow `docs/reference/config.md` for object
store credential variables, auth, workspace provisioning, and evaluator/Slack
settings. Keep credentials out of YAML, diffs, command history, and output.

Before calling a rollout complete, follow the selected platform's documented
procedure to verify the deployed image tag, `/health`, `/ready`, expected
workspace binding, and a safe ingest/query smoke test. If those target details
or safe test credentials are unavailable, report rollout verification as
unverified instead of implying success. Do not use `make test-deploy` as a
deploy command; it sends test telemetry to an already-running endpoint.
