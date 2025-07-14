import time
import pandas as pd
#from sqlalchemy import create_engine
from playwright.sync_api import sync_playwright, TimeoutError
from http.cookiejar import MozillaCookieJar
# PostgreSQL database connection URL
#DATABASE_URL = "postgresql://ytscraper:scrapemall@localhost:5432/youtube_comments_db"

#---------------------Just for one URL-----------------------------

#!!!!!!!If you don't need this function comment it out!!!!!!!
def load_cookies_from_file(filepath: str, domain: str):
    # Read cookies from Netscape file
    cj = MozillaCookieJar()
    cj.load(filepath, ignore_discard=True, ignore_expires=True)

    cookies = []
    for c in cj:
        # We filter per domain 
        if domain in c.domain:
            cookies.append({
                "name": c.name,
                "value": c.value,
                "domain": c.domain,
                "path": c.path,
                "expires": int(c.expires) if c.expires else -1,
                "httpOnly": c._rest.get("HttpOnly", False),
                "secure": c.secure,
                "sameSite": "Lax"  # ή "None"/"Strict" ανάλογα
            })
    return cookies

def scrape_youtube_comments(video_url, max_comments=90): # Adjust according to your needs. Be careful if you exceed 90 you may have to adjust the for loop on line 48  
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)  # Set to True to run in the background
            
            #---------- If you don't use cookies comment them out---------- 
            # Create context manually
            context = browser.new_context()

            # We load the cookies
            cookies = load_cookies_from_file("cookies.txt", domain=".youtube.com")
            context.add_cookies(cookies)
            #--------------------------------------------------------------

            print(f"Navigating to {video_url}...")
            page = browser.new_page()
            page.goto(video_url, timeout=60000)

            # Handle YouTube pop-ups
            # try:
            #     read_more_button = page.locator("button:has-text('<text on the button>')") # Write the name of the button you want to click
                
            #     if read_more_button.is_visible():
            #         read_more_button.scroll_into_view_if_needed()
            #         read_more_button.click()
            #         print("Clicked 'Read more'!")
            #         time.sleep(1)
            # except Exception as e:
            #     print("No 'Read more' button found or error occurred:", e)

            # try:
            #     reject_all_button = page.locator("button:has-text('<text on the button>')") # Write the name of the button you want to click

            #     if reject_all_button.is_visible():
            #         reject_all_button.scroll_into_view_if_needed()
            #         reject_all_button.click()
            #         print("Clicked 'Reject all'!")
            #     else:
            #         print("'Reject all' button not found.")
            # except Exception as e:
            #     print("No 'Reject all' button found or error occurred:", e)

            # Wait for video page to load
            print("Waiting for video page to load...")
            page.wait_for_selector("ytd-watch-flexy", timeout=20000)

            # Scroll down to load more comments
            print("Scrolling to load comments...")
            for _ in range(10):  
                page.mouse.wheel(0, 7000)
                time.sleep(3)  # Wait for content to load

            # Wait for comments section
            print("Waiting for comments to load...")
            try:
                page.wait_for_selector("ytd-comment-thread-renderer", timeout=30000)  # Increased timeout
            except TimeoutError:
                print("Timeout: Comments section did not load.")
                browser.close()
                return None
                        
            # Extract comments
            collected_comments = []
            comment_elements = page.query_selector_all("ytd-comment-thread-renderer")
            for comment in comment_elements:
                    
                if len(collected_comments) >= max_comments:
                    break  # Stop once we reach the desired number of comments

                author = comment.query_selector("#author-text span")
                text = comment.query_selector("#content-text")
                timestamp = comment.query_selector("#published-time-text")

                print("Appending comments")
                if author and text and timestamp:
                    collected_comments.append({
                        "video_id": video_url.split("v=")[-1],  # Extract video ID
                        "author": author.inner_text().strip(),
                        "comment": text.inner_text().strip(),
                        "published_at": timestamp.inner_text().strip()
                    })
            
            print(f"Collected {len(collected_comments)} comments.")

            browser.close()

            # Convert to DataFrame
            df = pd.DataFrame(collected_comments)
            return df

    except Exception as e:
        print(f"An error occurred: {e}")
        return None


# def save_to_postgresql(df):
#     """Save the DataFrame to a PostgreSQL database."""
#     try:
#         engine = create_engine(DATABASE_URL)
#         with engine.connect() as conn:
#             df.to_sql("comments", con=conn, if_exists="replace", index=True, index_label='id')
#         print("Data successfully inserted into PostgreSQL!")
#     except Exception as e:
#         print(f"Database insertion failed: {e}")


# Main
def main():
    video_url = "https://www.youtube.com/watch?v=BBe9vCH0T7o&list=PLAeu18HndGgBR-QLw8b8Wzp0gLiVfCS7n&index=6"  # Replace with actual video URL
    comments_df = scrape_youtube_comments(video_url)
    # It outputs one json file
    comments_df.to_json("output.json", orient="records", indent=2)

    # if comments_df is not None and not comments_df.empty:
    #     save_to_postgresql(comments_df)
    # else:
    #     print("No comments to insert.")

if __name__ == '__main__':
    main()
