FROM python:3.11-slim
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
RUN apt-get update && apt-get install -y --no-install-recommends gcc libev-dev && rm -rf /var/lib/apt/lists/*
COPY pyproject.toml .
COPY fraudstream_agents ./fraudstream_agents
RUN pip install --no-cache-dir . && pip cache purge
RUN useradd -m app && mkdir -p /data && chown app /data
USER app
EXPOSE 8000
HEALTHCHECK CMD python -c "import urllib.request;urllib.request.urlopen('http://localhost:8000/health')" || exit 1
CMD ["uvicorn", "fraudstream_agents.api:app", "--host", "0.0.0.0", "--port", "8000"]
