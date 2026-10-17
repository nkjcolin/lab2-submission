FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update \
    && apt-get install --yes --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY analysis/requirements.txt /tmp/analysis-requirements.txt
RUN pip install --no-cache-dir --requirement /tmp/analysis-requirements.txt

COPY analysis/main.py analysis/worker.py analysis/cloud_runner.py /app/analysis/
COPY backend/job_store.py backend/load_results.py /app/backend/

CMD ["python", "-m", "analysis.worker"]

