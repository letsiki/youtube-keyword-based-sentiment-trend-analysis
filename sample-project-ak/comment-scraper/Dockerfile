# Use official slim Python image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy only requirements first (better caching)
COPY requirements.txt ./

# Install system dependencies required by Playwright
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    gnupg \
    ca-certificates \
    fonts-liberation \
    libnss3 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    libasound2 \
    libxshmfence1 \
    libgbm1 \
    libgtk-3-0 \
    && rm -rf /var/lib/apt/lists/*

# Install Python packages
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Install Playwright and its browser binaries
RUN playwright install --with-deps

# Copy the rest of the project files
COPY . .

# Set default command
CMD ["python", "comment-scraper.py"]

