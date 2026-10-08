FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    USE_GEMINI_MODEL=1 \
    GEMINI_MODEL_NAME=gemini-2.5-flash

WORKDIR /app
COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-cache-dir .

USER 10001:10001
EXPOSE 8081
CMD ["uvicorn", "thelake_evaluation_runner.api:app", "--host", "0.0.0.0", "--port", "8081"]
