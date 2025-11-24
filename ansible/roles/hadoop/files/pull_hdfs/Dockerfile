FROM python:3.12-slim
WORKDIR /app
COPY . .
RUN pip install kafka-python requests psycopg2-binary pytz hdfs
CMD ["python", "pull_Hdfs.py"]
