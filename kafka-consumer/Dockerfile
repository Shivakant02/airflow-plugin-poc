FROM python:3.8-slim

WORKDIR /app

# Install required packages
RUN pip install --no-cache-dir \
    kafka-python==2.0.2 \
    avro-python3==1.10.2 \
    psycopg2-binary==2.9.7

# Copy consumer script
COPY consumer.py .

# Run the consumer
CMD ["python", "consumer.py"]
