FROM python:3.11-slim

WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the consumer script
COPY consumer.py .
COPY consumer_protobuf.py .

# Run the consumer
CMD ["python", "consumer_protobuf.py"]