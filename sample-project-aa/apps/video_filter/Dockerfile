FROM python:3.12-slim

# Install ffmpeg
RUN apt-get update && \
    apt-get install -y ffmpeg && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Install yt-dlp
RUN pip install --no-cache-dir yt-dlp

# Create a working directory
WORKDIR /app

# Copy your Python script into the container
COPY filter_videos.py .

# Set the entrypoint
ENTRYPOINT ["python", "filter_videos.py"]
