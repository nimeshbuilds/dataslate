# Multiarchitecture upstream index; qualify the built DataSlate image and bind
# its resulting digest. Maven network access occurs only during this build.
FROM apache/spark:4.1.3@sha256:bf9d035a7c32a8ca46aa58d6348182ffd7d2dff6409206ecfbb3915ff1fef211 AS dependencies
USER 0
COPY download_jars.py jars.lock.json /tmp/dataslate-build/
RUN python3 /tmp/dataslate-build/download_jars.py --directory /opt/dataslate-jars

FROM apache/spark:4.1.3@sha256:bf9d035a7c32a8ca46aa58d6348182ffd7d2dff6409206ecfbb3915ff1fef211
USER 0
COPY --chmod=0644 --from=dependencies /opt/dataslate-jars/*.jar /opt/spark/jars/
COPY dataslate_ingestion.py /opt/dataslate/python/dataslate_ingestion.py
COPY jars.lock.json /opt/dataslate/jars.lock.json
ENV PYTHONPATH=/opt/dataslate/python
USER 185:185
