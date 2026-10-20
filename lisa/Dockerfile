FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    OPENHANDS_SUPPRESS_BANNER=1 \
    OH_PERSISTENCE_DIR=/var/lib/lisa/openhands

WORKDIR /app
COPY lisa/pyproject.toml ./pyproject.toml
# Runtime package only. Never COPY lisa/benchmark or lisa/datasets into the image.
COPY lisa/src ./src
COPY lisa/skills ./skills
RUN pip install --no-cache-dir '.[openhands]'

RUN useradd --system --uid 10002 --create-home lisa && \
    mkdir -p /var/lib/lisa && chown -R lisa:lisa /var/lib/lisa /app
USER 10002:10002

EXPOSE 8082
CMD ["uvicorn", "lisa.app:app", "--host", "0.0.0.0", "--port", "8082"]
