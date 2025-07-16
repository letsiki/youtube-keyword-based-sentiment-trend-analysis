FROM python:3.11.11-slim-bookworm

WORKDIR /app

ENV FIREFOX_BIN=/usr/bin/firefox-esr

RUN apt-get update && \
    apt-get install -y firefox-esr

COPY requirements.txt ./

RUN pip install -r requirements.txt

COPY scrape.py ./

ENTRYPOINT ["python", "scrape.py"]
