import concurrent

import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import logging
import random
from sys import platform
from typing import Dict
import time
from bs4 import BeautifulSoup
import json
import os
from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
import logging


INPUT_FILE_PATH = "generated/fk_product_links.csv"
OUTPUT_FILE_PATH="generated/json_fk_products_scraped_data.json"

os.makedirs('logs', exist_ok=True)
os.makedirs('generated', exist_ok=True)
logging.basicConfig(filename='logs/scraping_products.log', level=logging.DEBUG)




def extract_product_details(div_element):
    result = {}

    # Extracting Name
    try:
        name_tag = div_element.select_one('span.B_NuCI')
        result['Name'] = name_tag.get_text().strip() if name_tag else ''
    except Exception as e:
        print(f"Exception extracting Name: {e}")
        result['Name'] = ''

    # Extracting Price
    try:
        price_tag = div_element.select_one('div._30jeq3._16Jk6d')
        result['Price'] = price_tag.get_text().strip() if price_tag else ''
    except Exception as e:
        print(f"Exception extracting Price: {e}")
        result['Price'] = ''

    # Extracting Original Price
    try:
        original_price_tag = div_element.select_one('div._3I9_wc._2p6lqe')
        result['Original Price'] = original_price_tag.get_text().strip() if original_price_tag else ''
    except Exception as e:
        print(f"Exception extracting Original Price: {e}")
        result['Original Price'] = ''

    # Extracting Discount
    try:
        discount_tag = div_element.select_one('div._3Ay6Sb._31Dcoz.pZkvcx span')
        print("discount_tag",discount_tag)
        result['Discount'] = discount_tag.get_text().strip() if discount_tag else ''
    except Exception as e:
        print(f"Exception extracting Discount: {e}")
        result['Discount'] = ''
    if discount_tag is None:
        try:
            discount_tag = div_element.select_one('div._3Ay6Sb')
            print("discount_tag", discount_tag)
            result['Discount'] = discount_tag.get_text().strip() if discount_tag else ''
        except Exception as e:
            print(f"Exception extracting Discount: {e}")
            result['Discount'] = ''

    # Extracting Rating
    try:
        rating_tag = div_element.select_one('div._3LWZlK._3uSWvT')
        print("rating_tag",rating_tag)
        result['Rating'] = rating_tag.get_text().strip() if rating_tag else ''
    except Exception as e:
        print(f"Exception extracting Rating: {e}")
        result['Rating'] = ''
    if(rating_tag is None):
        try:
            rating_tag = div_element.select_one('div._3LWZlK')
            print("rating_tag", rating_tag)
            result['Rating'] = rating_tag.get_text().strip() if rating_tag else ''
        except Exception as e:
            print(f"Exception extracting Rating: {e}")
            result['Rating'] = ''

    # Extracting Ratings Count and Reviews Count
    try:
        ratings_reviews_tag = div_element.select_one('span._2_R_DZ')
        if ratings_reviews_tag:

            ratings_reviews_text = ratings_reviews_tag.get_text().strip()
            print("ratings_reviews_tag", ratings_reviews_text)

            if 'and' in ratings_reviews_text:
                ratings, reviews = ratings_reviews_text.split('and')
            else:
                ratings, reviews = ratings_reviews_text.split('&')
            # ratings, reviews = ratings_reviews_text.split('and')
            result['Ratings Count'] = ratings.strip().split(' ')[0].strip()
            result['Reviews Count'] = reviews.strip().split(' ')[0].strip()
            print("ratings", ratings.strip().split(' ')[0].strip())
            print("reviews", reviews.strip().split(' ')[0].strip())
    except Exception as e:
        print(f"Exception extracting Ratings Count and Reviews Count: {e}")
        result['Ratings Count'] = ''
        result['Reviews Count'] = ''

    return result


def scrape_product_page(url: str) -> Dict[str, str]:
    # url = 'https://www.flipkart.com/p/p/p?pid=KPBGUKFQ9ZUG6XEG'
    # url = 'https://www.flipkart.com/p/p/p?pid=SUIGS9QB4FZ47YDM'
    try:
        print("Scraping URL:", url)
        user_agent = [
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.54 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36",
        ]

        options = ChromeOptions()
        options.add_argument(f"user-agent={random.choices(user_agent)}")
        options.add_argument("--headless")
        options.add_argument("--enable-javascript")
        options.add_argument("--no-sandbox")
        options.add_experimental_option("prefs", {"download_restrictions": 3})

        if platform == "linux" or platform == "linux2":
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--remote-debugging-port=9222")

        driver = webdriver.Chrome(options=options)

        driver.get(url)

        try:
            WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
        except Exception as e:
            print(f"Error while waiting for content to load-1: {e}")

        try:
            buttons = driver.find_elements(By.CSS_SELECTOR, "button._1JIkBw")
            for button in buttons:
                span = button.find_element(By.TAG_NAME, "span")
                if "offers" in span.text.strip():
                    button.click()
                    break
        except Exception as e:
            print(f"More offers button could not clicked:\n{e}")

        try:
            buttons = driver.find_elements(By.CSS_SELECTOR, "button._2KpZ6l._1AJtlD")
            for button in buttons:
                if button.text.strip() == "View all features":
                    button.click()
                    break
        except Exception as e:
            print(f"View all features button could not clicked:\n{e}")

        try:
            buttons = driver.find_elements(By.CSS_SELECTOR, "button._2KpZ6l._1FH0tX")
            for button in buttons:
                if button.text.strip() == "Read More":
                    button.click()
                    break
        except Exception as e:
            print(f"Read More button could not clicked:\n{e}")

        # Getting main page content
        try:
            target_class = "_2c7YLP.UtUXW0._6t1WkM._3HqJxg"
            container_html = driver.execute_script(
                f"return document.querySelector('div.{target_class}').outerHTML;"
            )
            soup = BeautifulSoup(container_html, "html.parser")
            for script in soup(["script", "style"]):
                script.extract()
        except Exception as e:
            print(f"SOUP object not created:\n{e}")
            return False

        result = dict()

        # Name and Price
        try:
            div_element = soup.select_one("div.aMaAEs")
            if div_element:
                extracted_data = extract_product_details(div_element)
                result.update(extracted_data)
            else:
                print("Div element not found")
                result.update({"Name": ""})
        except Exception as e:
            print(f"Exception while processing div element: {e}")
            result.update({"Name": ""})
        try:
            # Extracting Description
            description_div = soup.select_one("div._1mXcCf")
            if description_div:
                description_text = description_div.getText(separator="\n").strip()
                result['Description'] = description_text
            else:
                result['Description'] = ''

            # Extracting Highlights
            highlights_div = soup.select_one("div._2cM9lP")
            if highlights_div:
                highlights = []
                for li in highlights_div.select("li._21Ahn-"):
                    highlights.append(li.get_text().strip())
                result['Highlights'] = ', '.join(highlights)
            else:
                result['Highlights'] = ''

        except Exception as e:
            result.update({"Description": '', "Highlights": ''})

        # Images
        product_links = []
        product_divs = soup.find_all('img', class_="q6DClP")

        try:
            for product_div in product_divs:
                src = product_div.get('src')
                if src:
                    product_links.append(src)
        except Exception as e:
            print(f"Error: {e}")

        result.update({"Thumbnail-Images": product_links})

        # FLIPKART HAS A CDN THAT TAKES RESOLUTION AS A PATH PARAMETER, SO WE CAN CHANGE THE RESOLUTION OF THE IMAGE
        new_resolution = '832/832'
        modified_links = [link.replace('/image/128/128/', f'/image/{new_resolution}/') for link in product_links]
        result.update({"Original-Images": modified_links})
        result.update({"Url":url})

        # just to make sure it wont think this is a bot
        time.sleep(random.uniform(1, 3))

        driver.quit()
        return result
    except Exception as e:
        logging.error(f"Error processing {url}: {str(e)}")
        return False


def main():
    df = pd.read_csv(INPUT_FILE_PATH)
    #shuffle df rows
    # df = df.sample(frac=1).reset_index(drop=True)
    scraped_data = []

    # Using ThreadPoolExecutor to create a pool of threads
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_url = {executor.submit(scrape_product_page, url): url for url in df['URL']}
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                data = future.result()
                if data:
                    scraped_data.append(data)
                print(f"Completed: {url}")
            except Exception as exc:
                logging.error(f'{url} generated an exception: {exc}')

    # Saving scraped data
    # output_file_path = os.path.join(os.getcwd(), "scraped_data.json")
    with open(OUTPUT_FILE_PATH, 'w',encoding='utf-8') as json_file:
        json.dump(scraped_data, json_file, indent=2)
    print(f"Scraped data saved to {OUTPUT_FILE_PATH}")

if __name__ == "__main__":
    main()