FROM bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8

USER root
COPY setup-hdfs.sh /setup-hdfs.sh
RUN chmod +x /setup-hdfs.sh

# Ne pas écraser ENTRYPOINT, juste CMD
CMD ["/setup-hdfs.sh"]
