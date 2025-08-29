FROM python:3.11-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential freetds-dev freetds-bin ca-certificates && \
    rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY loader.py /app/loader.py
RUN pip install --no-cache-dir pandas "sqlalchemy>=2.0" "pymssql==2.2.11" tqdm
CMD ["python", "/app/loader.py"]
