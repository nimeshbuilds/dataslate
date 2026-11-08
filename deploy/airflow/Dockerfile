FROM apache/airflow:3.4.0-python3.12@sha256:818f94affc5bd7ce0be0cc04f2ace09aa8382e65190a73a08881229ff32700cb

# Build only from build_context.py's allowlisted context. DAG code remains operator-reviewed.
COPY --chown=airflow:root deploy/airflow/constraints-3.4.0-python3.12.txt /opt/dataslate-profile/constraints.txt
COPY --chown=airflow:root sdk/python /opt/dataslate-build/sdk
COPY --chown=airflow:root integrations/airflow /opt/dataslate-build/provider
RUN python -m pip install --no-cache-dir --constraint /opt/dataslate-profile/constraints.txt \
      'apache-airflow==3.4.0' 'apache-airflow-providers-fab==3.10.2' \
      'apache-airflow-providers-git==1.0.1' 'psycopg[binary]==3.3.6' 'setuptools==84.2.1' \
    && python -m pip install --no-cache-dir --no-deps --no-build-isolation \
      /opt/dataslate-build/sdk /opt/dataslate-build/provider \
    && python -m pip check
COPY --chown=airflow:root deploy/airflow/runtime_check.py deploy/airflow/launch.py /opt/dataslate-profile/
RUN python /opt/dataslate-profile/runtime_check.py --imports-only
ENV PYTHONDONTWRITEBYTECODE=1
ENTRYPOINT ["/usr/bin/dumb-init", "--", "python", "/opt/dataslate-profile/launch.py"]
CMD ["airflow", "version"]
