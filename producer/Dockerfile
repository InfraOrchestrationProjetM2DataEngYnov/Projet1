FROM python:3.12-slim
WORKDIR /app
COPY . .
RUN pip install kafka-python requests python-dotenv
CMD ["python", "main.py"]
