FROM python:3.10-slim

RUN apt-get update && apt-get install -y ffmpeg git && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY transcribe_many.py .
COPY audio/ .

# 👇 Αυτό είναι το "default executable" του container
ENTRYPOINT ["python", "transcribe_many.py"]
