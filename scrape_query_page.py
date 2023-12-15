import random
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import csv

def get_product_links(base_url, total_pages):
    product_links = []

    user_agents = [
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
    options.add_argument(f"user-agent={random.choice(user_agents)}")
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("referer=https://www.flipkart.com/")
    options.add_argument('sec-ch-ua="Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"')
    options.add_argument("sec-ch-ua-mobile=?0")
    options.add_argument('sec-ch-ua-platform="macOS"')

    driver = webdriver.Chrome(options=options)

    for page in range(1, total_pages + 1):
        url = f"{base_url}?page={page}"
        print(url)
        driver.get(url)

        try:
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        except Exception as e:
            print(f"Error while waiting for content to load: {e}")
            continue

        # Simulating scrolling to load more content waiting for content to load
        html = driver.find_element(By.TAG_NAME, 'html')
        for _ in range(3):
            html.send_keys(Keys.END)
            time.sleep(2) 

        soup = BeautifulSoup(driver.page_source, 'html.parser')

        # Finding product divs and then extracting links from them
        product_divs = soup.find_all('div', class_="_1xHGtK _373qXS")
        for div in product_divs:
            print("Divvv",div)
            a_tag = div.find('a', class_="_2UzuFa")
            if a_tag and 'href' in a_tag.attrs:
                full_link = f"https://www.flipkart.com{a_tag['href']}"
                if full_link not in product_links:
                    product_links.append(full_link)

        print(f"Page {page} done. Total links so far: {len(product_links)}")

        time.sleep(random.uniform(1, 3))  # Random sleep to mimic human behavior

    driver.quit()
    with open('product_links.csv', 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['URL'])  # Writing header
        for link in product_links:
            writer.writerow([link])

    print(f"Total Product Links Collected: {len(product_links)}")
    print("Data has been written to product_links.csv")
    return product_links

# Base Url and total pages to scrape, total pages would be around 25 to 30 based on product query, check this in website first, on how many pages are there

# base_url = 'https://www.flipkart.com/q/womens-ethnic-wear'  
# base_url = 'https://www.flipkart.com/q/ethnic-dress-for-women'
# base_url = 'https://www.flipkart.com/womens-kurtas-kurtis/ethnic-dress~type/pr?sid=clo%2Ccfv%2Ccib%2Crkt'  
# base_url = 'https://www.flipkart.com/q/ethnic-gowns'  
# base_url = 'https://www.flipkart.com/womens-clothing/ethnic-wear/ethnic-sets/pr?sid=2oq%2Cc1r%2C3pj%2Cu62'  
base_url = 'https://www.flipkart.com/womens-clothing/ethnic-wear/ethnic-sets/pr?sid=2oq%2Cc1r%2C3pj%2Cu62'  
total_pages = 25

product_links = get_product_links(base_url, total_pages)
print(f"Total Product Links Collected: {len(product_links)}")
