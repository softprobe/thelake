FROM node:22-slim AS lisa-frontend

WORKDIR /ui
COPY packages/thelake-explorer/package.json packages/thelake-explorer/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY packages/thelake-explorer/vite.lisa.config.ts ./vite.lisa.config.ts
COPY packages/thelake-explorer/src ./src
COPY packages/thelake-explorer/lisa ./lisa
RUN npm run build:lisa-frontend

FROM python:3.12-slim AS lisa-runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    OPENHANDS_SUPPRESS_BANNER=1 \
    OH_PERSISTENCE_DIR=/var/lib/lisa/openhands

WORKDIR /app
COPY lisa/pyproject.toml ./pyproject.toml
# Runtime package only. Never COPY lisa/benchmark or lisa/datasets into the image.
COPY lisa/src ./src
COPY lisa/skills ./skills
COPY lisa/scripts/provision_lisa.py ./provision_lisa.py
COPY --from=lisa-frontend /ui/lisa-dist ./static
RUN pip install --no-cache-dir '.[openhands]'

RUN useradd --system --uid 10002 --create-home lisa && \
    mkdir -p /var/lib/lisa && chown -R lisa:lisa /var/lib/lisa /app
USER 10002:10002

EXPOSE 8082
CMD ["uvicorn", "lisa.app:app", "--host", "0.0.0.0", "--port", "8082"]
