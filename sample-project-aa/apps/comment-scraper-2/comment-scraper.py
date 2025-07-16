import time
import pandas as pd
import argparse
import os
from playwright.sync_api import sync_playwright, TimeoutError
from http.cookiejar import MozillaCookieJar

youtube_video_template = "https://www.youtube.com/watch?v="


#!!!!!!!If you don't need this function comment it out!!!!!!!
def load_cookies_from_file(filepath: str, domain: str):
    cj = MozillaCookieJar()
    cj.load(filepath, ignore_discard=True, ignore_expires=True)

    cookies = []
    for c in cj:
        if domain in c.domain:
            cookies.append(
                {
                    "name": c.name,
                    "value": c.value,
                    "domain": c.domain,
                    "path": c.path,
                    "expires": int(c.expires) if c.expires else -1,
                    "httpOnly": c._rest.get("HttpOnly", False),
                    "secure": c.secure,
                    "sameSite": "Lax",
                }
            )
    return cookies


def scrape_youtube_comments(video_id, max_comments=90):
    video_url = youtube_video_template + video_id[1:]
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            # ---------- If you don't use cookies comment them out----------
            # Create context manually
            context = browser.new_context()

            # Load and set cookies if needed
            cookies = load_cookies_from_file(
                "cookies.txt", domain=".youtube.com"
            )
            context.add_cookies(cookies)
            # --------------------------------------------------------------

            print(f"Navigating to {video_url}...")
            page = browser.new_page()
            page.goto(video_url, timeout=60000)

            print("Waiting for video page to load...")
            page.wait_for_selector("ytd-watch-flexy", timeout=20000)

            print("Scrolling to load comments...")
            for _ in range(10):
                page.mouse.wheel(0, 7000)
                time.sleep(3)

            print("Waiting for comments to load...")
            try:
                page.wait_for_selector(
                    "ytd-comment-thread-renderer", timeout=30000
                )
            except TimeoutError:
                print("Timeout: Comments section did not load.")
                browser.close()
                return None

            collected_comments = []
            comment_elements = page.query_selector_all(
                "ytd-comment-thread-renderer"
            )
            for comment in comment_elements:
                if len(collected_comments) >= max_comments:
                    break

                author = comment.query_selector("#author-text span")
                text = comment.query_selector("#content-text")
                timestamp = comment.query_selector(
                    "#published-time-text"
                )

                if author and text and timestamp:
                    collected_comments.append(
                        {
                            "video_id": video_url.split("v=")[-1].split(
                                "&"
                            )[
                                0
                            ],  # Extract video ID more robustly
                            "author": author.inner_text().strip(),
                            "comment": text.inner_text().strip(),
                            "published_at": timestamp.inner_text().strip(),
                        }
                    )

            print(f"Collected {len(collected_comments)} comments.")
            browser.close()

            df = pd.DataFrame(collected_comments)
            return df

    except Exception as e:
        print(f"An error occurred: {e}")
        return None


def main():
    parser = argparse.ArgumentParser(
        description="Scrape YouTube comments from multiple videos."
    )
    parser.add_argument(
        "video_urls",
        nargs="+",
        help="List of YouTube video URLs to scrape",
    )
    args = parser.parse_args()

    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)

    for url in args.video_urls:
        print(f"Scraping comments for: {url}")
        df = scrape_youtube_comments(url)
        if df is not None and not df.empty:
            video_id = url.split("v=")[-1].split("&")[0]
            filename = os.path.join(output_dir, f"{video_id[1:]}.json")
            df.to_json(filename, orient="records", indent=2)
            print(f"Saved comments to {filename}")
        else:
            print(f"No comments scraped for {url}")


if __name__ == "__main__":
    main()
