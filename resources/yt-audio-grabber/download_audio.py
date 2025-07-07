import yt_dlp
from time import perf_counter
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--debug", action="store_true")
# parser.add_argument("--port", type=int, default=8000)

args = parser.parse_args()

url = "https://www.youtube.com/watch?list=PLpTHjAucqq9A2IDIsR8Ftbg7U6bKCMrcr"


ydl_opts = {
    "format": "251",
    "outtmpl": "/app/downloads/160/%(title)s.%(ext)s",
    "ignoreerrors": True,
    "playliststart": 1,
    "postprocessors": [
        {
            "key": "FFmpegExtractAudio",
            "preferredcodec": "mp3",
            "preferredquality": "160",
        }
    ],
}

if args.debug:
    ydl_opts["playlistend"] = 5


with yt_dlp.YoutubeDL(ydl_opts) as ydl:
    start = perf_counter()
    ydl.download(
        [url],
    )
    br160_time = round(perf_counter() - start, 2)
ydl.extract_info(url, download=False)
print(ydl._playlist_urls)


print(f"processed 52 songs 160kbps audios in {br160_time} seconds.")
