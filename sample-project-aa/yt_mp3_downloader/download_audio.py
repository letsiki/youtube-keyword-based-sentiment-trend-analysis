import yt_dlp
from time import perf_counter
import argparse
import os
import pickle

parser = argparse.ArgumentParser()
# parser.add_argument("--debug", action="store_true")
parser.add_argument("--urls", type=str, nargs="+")
# parser.add_argument("--port", type=int, default=8000)

args = parser.parse_args()

# url = "https://www.youtube.com/watch?list=PLpTHjAucqq9A2IDIsR8Ftbg7U6bKCMrcr"

# remove prefix 'v' from video id's
urls = [url[1:] for url in args.urls]


ydl_opts = {
    "format": "251",
    "outtmpl": "/app/downloads/160/%(id)s.%(ext)s",
    "ignoreerrors": True,
    "postprocessors": [
        {
            "key": "FFmpegExtractAudio",
            "preferredcodec": "mp3",
            "preferredquality": "160",
        }
    ],
}


with yt_dlp.YoutubeDL(ydl_opts) as ydl:
    start = perf_counter()
    for url in ["https://www.youtube.com/watch?v=" + u for u in urls]:
        ydl.download(
            url,
        )
        br160_time = round(perf_counter() - start, 2)
# ydl.extract_info(url, download=False)
# print(ydl._playlist_urls)


print(f"processed 52 songs 160kbps audios in {br160_time} seconds.")

os.makedirs("/airflow/xcom", exist_ok=True)
with open("/airflow/xcom/return.pkl", "wb") as f:
    pickle.dump(list(urls), f)
