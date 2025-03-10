FROM apache/airflow:latest-python3.9
COPY requirements.txt /requirements.txt

# Print the contents of requirements.txt for debugging
RUN cat /requirements.txt

# Run pip install with verbose output for debugging
RUN pip3 install --no-cache-dir -r /requirements.txt -v

# RUN pip install --no-cache-dir --user -r /requirements.txt

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

USER ROOT
USER root
RUN apt-get update -qq && apt-get install vim unzip -qqq

ARG CLOUD_SDK_VERSION=425.0.0
ENV GCLOUD_HOME=/home/google-cloud-sdk

ENV PATH="${GCLOUD_HOME}/bin/:${PATH}"

RUN DOWNLOAD_URL="https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz" \
    && TMP_DIR="$(mktemp -d)" \
    && curl -fL "${DOWNLOAD_URL}" --output "${TMP_DIR}/google-cloud-sdk.tar.gz" \
    && mkdir -p "${GCLOUD_HOME}" \
    && tar xzf "${TMP_DIR}/google-cloud-sdk.tar.gz" -C "${GCLOUD_HOME}" --strip-components=1 \
    && "${GCLOUD_HOME}/install.sh" \
       --bash-completion=false \
       --path-update=false \
       --usage-reporting=false \
       --quiet \
    && rm -rf "${TMP_DIR}" \
    && gcloud --version

USER airflow

COPY requirements.txt .
COPY stock_list.csv /opt/airflow/stock_list.csv
COPY ./code/spark-jobs/raw_to_staging.py /opt/airflow/spark-jobs/raw_to_staging.py
COPY ./code/scripts/transform_and_copy_to_landing_info.py /opt/airflow/spark-jobs/transform_and_copy_to_landing_info.py
COPY ./code/scripts/transform_and_copy_to_landing_sustainability.py /opt/airflow/spark-jobs/transform_and_copy_to_landing_sustainability.py
COPY ./code/scripts/transform_job_tabular_entitities.py /opt/airflow/spark-jobs/transform_job_tabular_entitities.py


RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir pandas sqlalchemy psycopg2-binary


SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]


WORKDIR $AIRFLOW_HOME

USER $AIRFLOW_UID
