import yt_dlp
from time import perf_counter
import argparse
import os
import pickle
import pathlib
import json
from urllib.parse import quote, urlparse, parse_qs

parser = argparse.ArgumentParser()
parser.add_argument("--urls", type=str, nargs="*", default=[])
args = parser.parse_args()

urls = [url[1:] for url in args.urls]
filtered_urls = []

string_date = os.environ.get("LOGICAL_DATE", None)
print(f"looking for videos in {string_date}")

ydl_opts = {
    "quiet": True,
    "skip_download": True,
    "http_headers": {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    },
}


def process_url(u):
    full_url = "https://www.youtube.com/watch?v=" + u
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            # First: quick probe for upload_date
            info = ydl.extract_info(
                full_url, download=False, process=False
            )
            if not info or info.get("upload_date") != string_date:
                return None

            # Second: full metadata check
            info = ydl.extract_info(full_url, download=False)
            # duration = info.get("duration")
            # language = info.get("language")

            # # Accept if duration is missing or <= 1500
            # if duration is not None and duration > 1500:
            #     return None

            # # Accept if language is missing or explicitly 'en'
            # if language is not None and language != "en":
            #     return None

            return full_url
    except Exception as e:
        print(f"Failed: {u} with {e}")
    return None


start = perf_counter()
results = []
for url in urls:
    r = process_url(url)
    if r:
        results.append(r)

filtered_urls = results

video_ids = [
    "v" + parse_qs(urlparse(url).query).get("v", [None])[0]
    for url in filtered_urls
]

br160_time = round(perf_counter() - start, 2)
nr_files_extracted = len(video_ids)

print(f"data filtered, keeping {nr_files_extracted} urls")

os.makedirs("/airflow/xcom", exist_ok=True)
with open("/airflow/xcom/return.pkl", "wb") as f:
    pickle.dump(video_ids, f)
