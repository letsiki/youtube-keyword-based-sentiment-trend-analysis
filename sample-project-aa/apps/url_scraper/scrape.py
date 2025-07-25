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
QUERY_KEYWORDS = ["Israel", "Palestine"]
# N_RESULTS_PER_QUERY = 3
HEADLESS = True
FIREFOX_BINARY = os.getenv(
    "FIREFOX_BIN", "/snap/firefox/current/usr/lib/firefox/firefox"
)
SEARCH_URL_TEMPLATE = "https://www.youtube.com/results?search_query={}"
WAIT_TIME = 15
MAX_VIDEOS_PER_QUERY = 500

debug_videos = [
    "vJzz0NOFetVM",
    "v6LfdSnic1JI",
    "vwn3o4FnRqGw",
    "vbdwkLWNoj7s",
    "vLoyYF796OGo",
    "vp28R2fUb_zE",
    "v1X_KdkoGxSs",
    "vy2A-CnYyHV8",
    "v4_2gXLj3o10",
    "veYNGpcaUwBI",
    "vgfl5o6vLeto",
    "vSf19E1ZZAGQ",
    "vV_GU4TVq2d8",
    "vm19F4IHTVGc",
    "vbKbJtp2iqgI",
    "vtN9dNGgc44Q",
    "vYNauolfUVWg",
    "vVzlY_Wkj8dc",
    "v_skGtOM_XT0",
    "vCnltF8o2iLo",
    "vBAujeFizTi4",
    "vfJjhmZJpntY",
    "vyy3ATmzyeIY",
    "vDnFO6e1JQpE",
    "vPQrD3EnKeaQ",
    "vzE8GCX1w3ys",
    "vN_MO03yBxfA",
    "v4J9npr3fdk4",
    "vJPJmj2Y8jrY",
    "vyq3hGbW7D5M",
    "v3Dh41x-vcrU",
    "vXZDFvjsLxWM",
    "vE_bzbu68U24",
    "vNI6o2ytLaCQ",
    "vysdhMsaG17A",
    "vCjxH_aiBfBs",
    "vgFa4MwSDg-k",
    "v5iRky7_LBr0",
    "v9IjGK4kHZME",
    "vwpKitfORhsM",
    "v6O6BZ5bWa5g",
    "vwldcbB7lwxQ",
    "vbce-IkH3wBE",
    "v-RscwolhIYo",
    "vF1H1SA0F_wY",
    "vPD0ZqGe591I",
    "vORDSpvg-ms4",
    "vx0LpF9aPZJ0",
    "v8qsaUU3ha90",
    "vCmEWZ8Pl7Iw",
    "vd2yMpk59_Bo",
    "vhzywF0xnjtQ",
    "vj2FVbCZCABQ",
    "vEVnL9wT90FM",
    "vNHoFXArEbG4",
    "vDtYkRM_pbXE",
    "v_B_5ZSjP0OE",
    "vi6MPJX4pZN8",
    "vvEs8KnY9UHA",
    "v8D2yAFZbekY",
    "vM7z2sUaRGlE",
    "vYtKDm979cTY",
    "vk9M0HKH9DUc",
    "vWPRtfsJULVs",
    "vTZZiW1lDq_E",
    "v_Jj8vne0ca0",
    "vVOtpyF74oGI",
    "viqq3rZh6mnE",
    "v-hv3y4QM-jM",
    "vdbqPsRSSzFY",
    "v6jI_kBxLp2A",
    "vZ2hDyzQK1FU",
][:4]


def scrape():

    start = time.perf_counter()

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("--debug", action="store_true")
    arg_parser.add_argument("--kw", nargs="+", default=[])
    # arg_parser.add_argument("--head", action="store_true")
    args = arg_parser.parse_args()

    if args.debug:

        all_video_ids = debug_videos
        os.makedirs("/airflow/xcom", exist_ok=True)
        with open("/airflow/xcom/return.pkl", "wb") as f:
            import pickle

            pickle.dump(list(all_video_ids), f)
            return
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
            SEARCH_URL_TEMPLATE.format(quote(q)) for q in args.kw
        ]

        for search_url in search_urls:
            print(f"Looking for videos in {search_url}")
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
                    WebDriverWait(driver, 8).until(
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
                    current_count
                    >= MAX_VIDEOS_PER_QUERY
                    # if not args.debug
                    # else 10
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
