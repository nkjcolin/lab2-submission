FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    RESULT_DIR=/data/results \
    DB_PATH=/data/state/users.db

WORKDIR /app

COPY backend/requirements.txt /tmp/backend-requirements.txt
RUN pip install --no-cache-dir --requirement /tmp/backend-requirements.txt

COPY backend/main.py backend/cloud_jobs.py backend/job_store.py backend/load_results.py backend/s3_results.py /app/backend/

EXPOSE 8000

CMD ["uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8000"]
