FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /srv

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY migrations ./migrations
COPY verify ./verify
COPY tests ./tests

EXPOSE 8000

# Default command runs one API instance; compose overrides per-service.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
