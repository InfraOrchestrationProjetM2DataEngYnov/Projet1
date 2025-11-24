FROM python:3.12-slim
WORKDIR /app
COPY . .
RUN pip install kafka-python requests
CMD ["python", "main.py"]
