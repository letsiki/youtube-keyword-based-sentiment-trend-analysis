import yt_dlp
from time import perf_counter
import argparse
import os
import pickle
import pathlib
import json
from urllib.parse import quote, urlparse, parse_qs
from concurrent.futures import ThreadPoolExecutor


parser = argparse.ArgumentParser()
# parser.add_argument("--debug", action="store_true")
parser.add_argument("--urls", type=str, nargs="*", default=[])
# parser.add_argument("--port", type=int, default=8000)

args = parser.parse_args()

# url = "https://www.youtube.com/watch?list=PLpTHjAucqq9A2IDIsR8Ftbg7U6bKCMrcr"

# remove prefix 'v' from video id's
urls = [url[1:] for url in args.urls]

filtered_urls = []

ydl_opts = {"quiet": True, "skip_download": True}

string_date = os.environ.get("LOGICAL_DATE")
print(f"looking for videos in {string_date}")


def process_url(u):
    full_url = "https://www.youtube.com/watch?v=" + u
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(full_url, download=False)
        if (
            info
            and info.get("upload_date") == string_date
            and info.get("duration")
            and info["duration"] <= 1500
            and info.get("language") == "en"
        ):
            return full_url
    except Exception as e:
        print(f"Failed: {u} with {e}")
    return None


start = perf_counter()
with ThreadPoolExecutor(max_workers=9) as executor:
    results = list(executor.map(process_url, urls))

filtered_urls = [r for r in results if r]

# extract video_id from url and prefix it with v
video_ids = [
    "v" + parse_qs(urlparse(url).query).get("v", [None])[0]
    for url in filtered_urls
]

br160_time = round(perf_counter() - start, 2)

# change to len of list
nr_files_extracted = len(video_ids)


print(f"data filtered, keeping {nr_files_extracted} urls")

os.makedirs("/airflow/xcom", exist_ok=True)
with open("/airflow/xcom/return.pkl", "wb") as f:
    pickle.dump(video_ids, f)
