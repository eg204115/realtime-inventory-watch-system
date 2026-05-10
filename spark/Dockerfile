FROM apache/spark:3.5.7-scala2.12-java17-python3-ubuntu

USER root

RUN python3 -m pip install --no-cache-dir \
    psycopg2-binary==2.9.9

USER spark