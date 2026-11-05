---
name: deploy-thelake-staging
description: >-
  Ships a merged thelake main commit to the staging GKE namespace. Use when
  deploying thelake to staging, cutting a staging prerelease, bumping
  softprobe/thelake in gcp/staging, or running the sp-llm staging e2e against
  the deployed staging service.
---

# Deploy thelake to staging

Staging is namespace `staging` on cluster `sp-k8s-1-autopilot-cluster` in project `cs-poc-sasxbttlzroculpau4u6e2l`, region `us-central1`. The image pin lives in `gcp/staging/sp-llm/thelake.yaml` in deployment-k8s. Production is out of scope.

Cluster login is human-only. If `kubectl` has no context, stop and ask for `gcloud container clusters get-credentials sp-k8s-1-autopilot-cluster --region us-central1 --project cs-poc-sasxbttlzroculpau4u6e2l`.

## Release

1. Confirm the change is on `softprobe/thelake` `main`.
2. Read the newest tag with `gh release list --repo softprobe/thelake --limit 5`. The next staging tag is the next `v0.1.33-rc.N` prerelease. Do not move `:latest`.
3. From a clean `main` checkout, create the release with `gh release create <tag> --repo softprobe/thelake --target main --prerelease`.
4. Wait until the workflow `Build and Push TheLake Docker Image` for that tag succeeds. A prerelease skips npm and passes `TAG_LATEST=0`.
5. Confirm the tag exists before changing the manifest. `https://hub.docker.com/v2/repositories/softprobe/thelake/tags/<tag>`

## Manifest

1. Change only the `image:` line in `gcp/staging/sp-llm/thelake.yaml` to `softprobe/thelake:<tag>`.
2. From `deployment-k8s`, run `kubectl diff -k gcp/staging/`. If the diff touches Secret data, apply only the Deployment.
3. Wait with `kubectl -n staging rollout status deploy/thelake`.
4. Confirm the running image equals the tag.

Do not point staging at schema `thelake` or prefix `shared`.

## Staging e2e

Use the `sp-llm` branch `origin/staging`. Do not run `make e2e`. That target starts a local thelake and posts `e2e-admin-token`.

1. Port-forward with `kubectl -n staging port-forward svc/thelake 18091:8080`.
2. From that checkout, run `THELAKE_URL=http://127.0.0.1:18091 THELAKE_TOKEN=staging-thelake-smoke-token python3 scripts/e2e_phase1.py`.
3. The auth mock maps any validate call to workspace `4ccbdde6-0f8b-48f0-8af0-04f67f73fade`.
