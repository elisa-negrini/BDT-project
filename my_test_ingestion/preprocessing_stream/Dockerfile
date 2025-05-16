FROM apache/flink:1.17

USER root

RUN pip install --no-cache-dir apache-flink==1.17 pandas numpy

COPY preprocess_flink.py /opt/flink/jobs/

ENTRYPOINT ["flink", "run", "/opt/flink/jobs/preprocess_flink.py"]
# da controllare