# LEO radiation-monitoring service - Python 3.13 runtime.
FROM python:3.13-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    APP_HOST=0.0.0.0 \
    APP_PORT=8080 \
    DB_PATH=/data/radiation.db

WORKDIR /srv

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY tests ./tests
COPY scripts ./scripts

RUN mkdir -p /data && useradd -r -u 10001 appuser \
    && chown -R appuser:appuser /srv /data
USER appuser

VOLUME ["/data"]
EXPOSE 8080

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=5 \
    CMD ["python", "scripts/healthcheck.py"]

CMD ["python", "-m", "app.main"]
