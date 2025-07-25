import yt_dlp
from time import perf_counter
import argparse
import os
import pickle
import pathlib
import json


def clean_unicode(s):
    if isinstance(s, str):
        return s.encode("utf-8", "replace").decode("utf-8")
    return s


parser = argparse.ArgumentParser()
# parser.add_argument("--debug", action="store_true")
parser.add_argument("--urls", type=str, nargs="*", default=[])
# parser.add_argument("--port", type=int, default=8000)

args = parser.parse_args()

# url = "https://www.youtube.com/watch?list=PLpTHjAucqq9A2IDIsR8Ftbg7U6bKCMrcr"

# remove prefix 'v' from video id's
urls = [url[1:] for url in args.urls]


ydl_opts = {
    "quiet": True,
    "format": "251/bestaudio",
    "outtmpl": "/app/mp3/%(id)s.%(ext)s",
    "http_headers": {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    },
    "ignoreerrors": True,
    "postprocessors": [
        {
            "key": "FFmpegExtractAudio",
            "preferredcodec": "mp3",
            "preferredquality": "160",
        }
    ],
}

string_date = os.environ.get("LOGICAL_DATE")
print(f"looking for videos in {string_date}")
with yt_dlp.YoutubeDL(ydl_opts) as ydl:
    start = perf_counter()
    for url in ["https://www.youtube.com/watch?v=" + u for u in urls]:
        info_dict = ydl.extract_info(url, download=False)
        if info_dict:
            video_date = info_dict.get("upload_date", None)
            print(f"video date is {video_date}")
            if (
                # video_date == string_date
                info_dict.get("duration")
                and info_dict.get("duration") <= 3600
                # and info_dict.get("language")
                # and info_dict["language"] == "en"
            ):
                ydl.download(url)
                metadata = {
                    "title": info_dict.get("title", None),
                    "uploader": info_dict.get("uploader", None),
                    "upload_date": info_dict.get("upload_date", None),
                    "description": info_dict.get("description", None),
                    "view_count": info_dict.get("view_count", None),
                }

                safe_metadata = {
                    k: clean_unicode(v) for k, v in metadata.items()
                }

                with open(
                    f"/app/json/video/{info_dict["id"]}.json", "w"
                ) as json_file:
                    json.dump(
                        safe_metadata, json_file, ensure_ascii=False
                    )
    br160_time = round(perf_counter() - start, 2)
nr_files_extracted = len(list(pathlib.Path("/app/mp3").glob("*.mp3")))


print(
    f"processed {nr_files_extracted} songs 160kbps audios in {br160_time} seconds."
)

# os.makedirs("/airflow/xcom", exist_ok=True)
# with open("/airflow/xcom/return.pkl", "wb") as f:
#     pickle.dump(nr_files_extracted, f)
