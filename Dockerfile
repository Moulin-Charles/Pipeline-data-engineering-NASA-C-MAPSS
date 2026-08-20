FROM python:3.14
WORKDIR /Scripts_Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY consumer.py .
COPY producer.py .