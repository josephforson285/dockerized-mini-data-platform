# The base image already ships pandas 3.0.5, boto3, psycopg and PyYAML at the
# same versions as the host venv, so there is nothing to pip install and no
# constraint solving to go wrong.
FROM apache/airflow:3.3.1 AS runtime

ENV PYTHONPATH=/opt/airflow \
    PYTHONDONTWRITEBYTECODE=1

# A named volume inherits the ownership of its mountpoint in the image. Without
# this the directory is created root-owned and the airflow user cannot write.
# Numeric ids: 0 = root, 50000 = the image's airflow user.
USER 0
RUN mkdir -p /opt/airflow/auth \
    && chown airflow:root /opt/airflow/auth \
    && chmod 0775 /opt/airflow/auth
USER 50000

COPY --chown=airflow:root mini_platform /opt/airflow/mini_platform
COPY --chown=airflow:root config/pipeline.yml /opt/airflow/config/pipeline.yml

# Fail the build rather than the first DAG run if the contract is unparseable.
RUN python -c "from mini_platform.settings import load; load()"


# Test stage: pytest and the suites, kept out of the runtime image that ships.
FROM runtime AS test

COPY --chown=airflow:root tests /opt/airflow/tests
COPY --chown=airflow:root dags /opt/airflow/dags

RUN pip install --no-cache-dir pytest==9.1.1
