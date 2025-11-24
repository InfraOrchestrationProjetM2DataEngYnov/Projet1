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
      bash dos2unix && \
    ln -sf python3 /usr/bin/python

WORKDIR /opt/hadoop/jobs

# Copier l'entrypoint dans l'image
COPY entrypoint.sh /opt/hadoop/jobs/entrypoint.sh

# Convertir automatiquement CRLF → LF et rendre exécutable
RUN dos2unix /opt/hadoop/jobs/entrypoint.sh && \
    chmod +x /opt/hadoop/jobs/entrypoint.sh

# Définir l'entrypoint
ENTRYPOINT ["/opt/hadoop/jobs/entrypoint.sh"]
