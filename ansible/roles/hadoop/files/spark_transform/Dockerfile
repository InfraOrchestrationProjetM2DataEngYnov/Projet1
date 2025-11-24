FROM bde2020/spark-submit:3.1.1-hadoop3.2

USER root

# Installation des dépendances système
RUN apk update && \
    apk add --no-cache \
      python3 \
      py3-pip \
      postgresql-dev \
      gcc \
      musl-dev \
      python3-dev \
      krb5-dev \
      bash && \
    ln -sf python3 /usr/bin/python

WORKDIR /opt/hadoop/jobs

# COPIER l'entrypoint dans l'image
COPY entrypoint.sh /opt/hadoop/jobs/entrypoint.sh

# Le rendre exécutable
RUN chmod +x /opt/hadoop/jobs/entrypoint.sh

# Définir l'entrypoint
ENTRYPOINT ["/opt/hadoop/jobs/entrypoint.sh"]