# Uber Data Analysis — Airflow + PySpark ETL

This repository is an example Airflow + PySpark ETL project that demonstrates running analytical Spark jobs from Airflow and persisting outputs to an S3-compatible store (MinIO).

Overview
- `docker-compose.yml` — Orchestrates Airflow, Spark master/worker, MinIO and helper services.
- `Dockerfile.airf-base` — Airflow image (this project installs Python 3.10 and PySpark here).
- `Dockerfile.spark-base` — Spark image base used by Spark services.
- `dags/etl.py` — Airflow DAG that submits the PySpark job.
- `spark_jobs/uber_solution.py` — PySpark application that computes 10 analytical "questions" and writes results to MinIO.
- `shared/` — Shared helpers: `utils/`, `settings.py`, data, and logs.
- `scripts/setup-minio.sh` — helper script to configure MinIO (create alias, bucket, policies).

What I implemented (functional)
-------------------------------
This project was inspired by the Medium article "Uber Data Analysis Project in PySpark" (https://medium.com/towards-data-engineering/uber-data-analysis-project-in-pyspark-e105a445fc3e) and adapted to run under a local Airflow + Spark + MinIO stack. Key functional work completed in this repository:

- Data ingestion: download and read the provided CSV dataset into a Spark DataFrame (see `shared/utils/commonUtils.py` and `shared/utils/sparkUtils.py`).
- Data cleaning and standardization: column renaming, filling null dates, and generating a timestamp column `timeLocalTS` (implemented in `spark_jobs/uber_solution.py::standardize`).
- Ten analytical questions implemented as Spark transformations/actions (q1..q10) that compute the requested metrics and write results back to MinIO under `question1/`..`question10/` folders.
- Airflow orchestration: an Airflow DAG (`dags/etl.py`) that downloads the data, submits the Spark job via `SparkSubmitOperator`, and performs cleanup.
- MinIO integration: results are written to MinIO (S3-compatible). A helper `scripts/setup-minio.sh` configures the bucket and policies.
- Containerization: Dockerfiles for Airflow and Spark (`Dockerfile.airf-base`, `Dockerfile.spark-base`) and a `docker-compose.yml` to run the full stack locally.
- Logging: added structured logging via the existing `get_logger` helper so each question and key steps log progress and debug info.
- Reliability fixes: removed fragile runtime-copying of Python, installed Python 3.10 in the Airflow image, and documented the requirement that driver and worker Python minor versions match. Also added recommendations to expose `AIRFLOW_CONN_SPARK_DEFAULT` and to set `PYSPARK_DRIVER_PYTHON` / `PYSPARK_PYTHON` in compose.

Notes on provenance: the algorithmic approach for the questions is adapted from the Medium article linked above but integrated into an Airflow+Spark+MinIO workflow, containerized for local development and testing.

Prerequisites
- Docker & Docker Compose (v2+).
- At least ~8GB RAM available to the Docker engine (Spark + Airflow require resources).
- macOS + zsh (example commands assume zsh, but Linux works too).

Environment & configuration
- Copy `.env` (or edit it) to configure the environment variables used by compose. Key values:
  - `SPARK_CONN` — Spark URI (example: `spark://spark-master:7077`).
  - `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY` — MinIO credentials.
  - `BUCKET_NAME` — bucket used to write question outputs.

Build (optional)
You can build the project images locally if you prefer not to use remote images.

Build Airflow image (uses Python 3.10):
```bash
docker build -t local/airflow-sparklib:py3.10 -f Dockerfile.airf-base .
```

Build Spark image (only if you changed `Dockerfile.spark-base`):
```bash
docker build -t local/spark-base:custom -f Dockerfile.spark-base .
```

Start the stack
```bash
# from project root
docker compose up -d
docker compose logs -f
```

Verify services
- Airflow UI — check the port mapped in `docker-compose.yml` (commonly http://localhost:8080).
- Spark Master UI — port depends on the Spark image (inspect `docker-compose.yml`).
- MinIO — http://localhost:9000 (credentials from `.env`).

Important runtime notes

- Python minor version compatibility: PySpark requires the driver (Airflow submitter) and Spark workers to use the same Python *minor* version (for example, both 3.10). This repo configures Airflow to use Python 3.10 in `Dockerfile.airf-base`. Ensure your Spark worker image also exposes Python 3.10. Alternatively set matching interpreter paths via environment variables in `docker-compose.yml`:

```yaml
# in Airflow service
environment:
  PYSPARK_DRIVER_PYTHON: /usr/bin/python3.10

# in spark-master and spark-worker
environment:
  PYSPARK_PYTHON: /usr/bin/python3.10
```

- Java: The Airflow image installs OpenJDK 17; make sure `JAVA_HOME` in DAG/operator or images points to the same path. Prefer not to hard-code `JAVA_HOME` in the DAG unless necessary.

- Airflow Spark connection: Airflow reads connections via env vars named `AIRFLOW_CONN_<CONN_ID>`. For a connection id of `spark_default` set `AIRFLOW_CONN_SPARK_DEFAULT` in compose (mapped to the value of `SPARK_CONN` in `.env`).

Running and testing

- Trigger the DAG from the Airflow UI or CLI. Example CLI trigger (from host):
```bash
# trigger the full DAG
docker compose exec airflow-scheduler airflow dags trigger uber-data-analysis

# trigger specific question(s) using dag_run.conf to pass "question_id" (comma-separated list)
docker compose exec airflow-scheduler airflow dags trigger -c '{"question_id":"7"}' uber-data-analysis
```

- The Spark application will write outputs to the configured MinIO bucket under `question1/`, `question2/`, ... `question10/`.

Logging & debugging
- Airflow task logs show SparkSubmit stdout/stderr. The PySpark job logs include messages from `shared.utils.sparkUtils.get_logger` which the app uses.
- If you encounter the error "Python in worker has different version...", confirm Python versions inside containers:
```bash
docker compose exec airflow-scheduler python3.10 --version
docker compose exec spark-worker python3 --version
```

If they differ, either adjust images to use the same minor version or set the `PYSPARK_*` environment variables as shown above.

Project notes & tips
- Keep the driver in client mode only if the Airflow worker/container has a matching environment (Python & Java). For cluster deploy mode, ensure the application and dependencies are available to the cluster.
- The repo includes `shared/utils/minioUtils.py` and `shared/utils/sparkUtils.py` to centralize S3/MinIO and Spark session creation; these are good places to adapt credentials, region, or storage paths.

Contributing
- Make changes on a feature branch and open a PR. Keep Dockerfile and compose edits minimal and documented.
