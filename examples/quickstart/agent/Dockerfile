FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
RUN pip install --no-cache-dir 'softprobe[openai]>=0.1,<0.2'
COPY refund_agent.py ./refund_agent.py
ENTRYPOINT ["python", "/app/refund_agent.py"]
