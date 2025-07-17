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
from selenium.common.exceptions import ElementClickInterceptedException


# Config
QUERY_KEYWORDS = ["CNN Israel", "BBC Israel"]
N_RESULTS_PER_QUERY = 3
HEADLESS = True
FIREFOX_BINARY = os.getenv(
    "FIREFOX_BIN", "/snap/firefox/current/usr/lib/firefox/firefox"
)
SEARCH_URL_TEMPLATE = "https://www.youtube.com/results?search_query={}"
WAIT_TIME = 15
MAX_VIDEOS_PER_QUERY = 2000


def scrape():

    start = time.perf_counter()

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("--debug", action="store_true")
    # arg_parser.add_argument("--head", action="store_true")
    args = arg_parser.parse_args()

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

            # accept cookies
            try:
                accept_btn = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable(
                        (
                            By.CSS_SELECTOR,
                            "button[aria-label*='Accept'][aria-disabled='false']",
                        )
                    )
                )
                accept_btn.click()
            except TimeoutException:
                print("No cookie consent button found — skipping.")
            time.sleep(3)
            try:
                wait.until(
                    EC.element_to_be_clickable(
                        (By.CLASS_NAME, "ytChipShapeButtonReset")
                    )
                )
                driver.find_element(
                    By.CLASS_NAME, "ytChipShapeButtonReset"
                ).click()
            except TimeoutException:
                print(
                    f"No filter button for query {search_url} — skipping click."
                )

            # scroll load videos
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
                                By.ID,
                                "video-title",
                            )
                        )
                        > last_count
                    )
                except TimeoutException:
                    pass  # No new items loaded in 4 seconds

                current_count = len(
                    driver.find_elements(
                        By.ID,
                        "video-title",
                    )
                )
                if (
                    current_count >= MAX_VIDEOS_PER_QUERY
                    if not args.debug
                    else 10
                ):
                    break
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

            # Find all matching video links
            a_tags = driver.find_elements(By.ID, "video-title")

            # Extract hrefs
            urls = [link.get_attribute("href") for link in a_tags]

            # get video id from url
            video_ids = [
                parse_qs(urlparse(url).query).get("v", [None])[0]
                for url in urls
            ]
            print(f"Received {len(set(video_ids))}")
            all_video_ids.update(video_ids)
            print(f"Received {len(all_video_ids)} so far")

        print(f"Received {len(all_video_ids)} in total")
    finally:
        driver.quit()

    all_video_ids = set(
        [
            "v" + video_id
            for video_id in all_video_ids
            if isinstance(video_id, str)
        ]
    )
    os.makedirs("/airflow/xcom", exist_ok=True)
    with open("/airflow/xcom/return.pkl", "wb") as f:
        import pickle

        pickle.dump(list(all_video_ids), f)

    print(
        f"Scraping took {round((time.perf_counter() - start), 2)} seconds"
    )


if __name__ == "__main__":
    scrape()
