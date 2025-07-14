import os
from urllib.parse import quote, urlparse, parse_qs
import time
import json
import argparse

from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

start = time.perf_counter()

# Config
QUERY_KEYWORDS = ["CNN Israel", "BBC Israel"]
N_RESULTS_PER_QUERY = 3
HEADLESS = True
FIREFOX_BINARY = os.getenv(
    "FIREFOX_BIN", "/snap/firefox/current/usr/lib/firefox/firefox"
)
SEARCH_URL_TEMPLATE = "https://www.youtube.com/results?search_query={}&sp=EgIQAw%253D%253D"
WAIT_TIME = 15

arg_parser = argparse.ArgumentParser()
arg_parser.add_argument("--debug", action="store_true")
args = arg_parser.parse_args()


def scrape():
    all_video_ids = set()

    # Firefox options
    options = Options()
    if HEADLESS:
        options.add_argument("--headless")
    options.binary_location = FIREFOX_BINARY

    # Start driver
    driver = webdriver.Firefox(options=options)

    try:
        search_urls = [
            SEARCH_URL_TEMPLATE.format(quote(q)) for q in QUERY_KEYWORDS
        ]

        for search_url in search_urls:
            driver.get(search_url)
            wait = WebDriverWait(driver, WAIT_TIME)
            wait.until(
                EC.presence_of_element_located(
                    (
                        By.CSS_SELECTOR,
                        "a.yt-lockup-metadata-view-model-wiz__title",
                    )
                )
            )
            anchors = driver.find_elements(
                By.CSS_SELECTOR,
                "a.yt-lockup-metadata-view-model-wiz__title",
            )
            playlist_urls = [
                a.get_attribute("href")
                for a in anchors[:N_RESULTS_PER_QUERY]
                if a.get_attribute("href")
            ]

            if args.debug:
                playlist_urls = playlist_urls[-1:]

            for i, playlist_url in enumerate(playlist_urls):
                parsed = urlparse(playlist_url)
                playlist_id = parse_qs(parsed.query).get(
                    "list", [None]
                )[0]
                if not playlist_id:
                    logger.warning("No playlist ID found in URL")
                    continue

                external_playlist_url = f"https://www.youtube.com/playlist?list={playlist_id}"
                print(f"Opening playlist: {external_playlist_url}")
                driver.get(external_playlist_url)

                # For the first iteration only
                if i == 0:
                    # Accept cookies (if any)
                    try:
                        WebDriverWait(driver, 3).until(
                            EC.element_to_be_clickable(
                                (
                                    By.XPATH,
                                    "//button[.//span[text()='Accept all']]",
                                )
                            )
                        ).click()
                        driver.get(external_playlist_url)
                    except:
                        pass

                wait.until(
                    EC.presence_of_element_located(
                        (
                            By.CSS_SELECTOR,
                            "#contents.ytd-playlist-video-list-renderer",
                        )
                    )
                )
                body = driver.find_element(By.TAG_NAME, "body")

                last_count = -1
                last_scroll_height = -1

                while True:
                    body.send_keys(Keys.END)

                    # Wait up to 4 seconds for new videos to load
                    try:
                        WebDriverWait(driver, 4).until(
                            lambda d: len(
                                d.find_elements(
                                    By.CSS_SELECTOR,
                                    "ytd-playlist-video-renderer",
                                )
                            )
                            > last_count
                        )
                    except TimeoutException:
                        pass  # No new items loaded in 4 seconds

                    current_count = len(
                        driver.find_elements(
                            By.CSS_SELECTOR,
                            "ytd-playlist-video-renderer",
                        )
                    )
                    current_scroll_height = driver.execute_script(
                        "return document.documentElement.scrollHeight"
                    )

                    if (
                        current_count == last_count
                        and current_scroll_height == last_scroll_height
                    ):
                        break

                    last_count = current_count
                    last_scroll_height = current_scroll_height

                # Extract video URLs
                video_links = driver.find_elements(
                    By.CSS_SELECTOR, "a.ytd-playlist-video-renderer"
                )
                urls = [
                    a.get_attribute("href")
                    for a in video_links
                    if a.get_attribute("href")
                ]
                print(f"Found {len(urls)} video URLs.")

                for link in video_links:
                    href = link.get_attribute("href")
                    if href:
                        video_id = parse_qs(urlparse(href).query).get(
                            "v", [None]
                        )[0]
                        if video_id:
                            all_video_ids.add(video_id)

                print(
                    f"Collected {len(all_video_ids)} unique video IDs so far."
                )
    finally:
        driver.quit()

    if args.debug:
        all_video_ids = list(all_video_ids)[:16]

    all_video_ids = set(["v" + video_id for video_id in all_video_ids])
    os.makedirs("/airflow/xcom", exist_ok=True)
    with open("/airflow/xcom/return.pkl", "wb") as f:
        import pickle

        pickle.dump(list(all_video_ids), f)

    print(
        f"Scraping took {round((time.perf_counter() - start), 2)} seconds"
    )


if __name__ == "__main__":
    scrape()
