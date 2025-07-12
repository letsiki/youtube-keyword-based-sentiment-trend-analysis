FROM python:3.10-slim

# Εγκατάσταση ffmpeg και git (για whisper dependencies)
RUN apt-get update && apt-get install -y ffmpeg git && rm -rf /var/lib/apt/lists/*

# Δημιουργία working directory
WORKDIR /app

# Αντιγραφή dependencies και εγκατάσταση
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Αντιγραφή του script απομαγνητοφώνησης
COPY transcribe.py .

# Το container περιμένει να του δώσεις το αρχείο κατά την εκτέλεση (μέσω bind mount)
# Παράδειγμα εκτέλεσης: docker run -v $(pwd)/audio.mp3:/app/audio.mp3 whisper-service

CMD ["python", "transcribe.py", "audio.mp3"]
