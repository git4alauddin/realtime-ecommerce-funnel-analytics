FROM apache/airflow:2.10.2-python3.12

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
WORKDIR /opt/project
COPY pyproject.toml README.md /opt/project/
COPY src /opt/project/src
RUN pip install --no-cache-dir psycopg2-binary>=2.9,<3 \
    && pip install --no-cache-dir "/opt/project[spark,postgres]"
