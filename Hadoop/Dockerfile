FROM bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8

WORKDIR /app
COPY . .

RUN pip install kafka-python requests psycopg2-binary pytz hdfs
CMD ["python", "pull_Hdfs.py"]

