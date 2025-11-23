FROM bde2020/spark-submit:3.1.1-hadoop3.2

USER root

RUN apk update && \
    apk add --no-cache \
      python3 \
      py3-pip \
      postgresql-dev \
      gcc \
      musl-dev \
      python3-dev \
      krb5-dev && \
    ln -sf python3 /usr/bin/python

WORKDIR /opt/hadoop/jobs

# Install dependencies at container startup
ENTRYPOINT ["./entrypoint.sh"]