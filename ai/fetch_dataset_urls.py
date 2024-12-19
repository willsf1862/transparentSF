import os
import json
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Directory to save dataset URLs
data_directory = "data"
if not os.path.exists(data_directory):
    os.makedirs(data_directory)

# Base URL to browse datasets
base_url = "https://data.sfgov.org/browse?limitTo=datasets"

def get_driver():
    """Initialize and return a Selenium WebDriver."""
    chrome_options = Options()
    # chrome_options.add_argument("--headless")  # Runs Chrome in headless mode.
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    # Adjust the version of ChromeDriver to match your Chrome browser if needed
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver

def scrape_dataset_urls():
    """Scrape all dataset URLs from the browse page, handling pagination."""
    driver = get_driver()
    dataset_urls = []
    file_path = os.path.join(data_directory, "dataset_urls.json")

    try:
        driver.get(base_url)
        while True:
            # Wait for dataset links to load
            try:
                WebDriverWait(driver, 15).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'a.browse2-result-name-link'))
                )
            except TimeoutException:
                print("Timeout waiting for dataset links to load.")
                break

            # Collect dataset URLs on the page
            dataset_elements = driver.find_elements(By.CSS_SELECTOR, 'a.browse2-result-name-link')
            if not dataset_elements:
                print("No datasets found on the page.")
                break

            for element in dataset_elements:
                href = element.get_attribute('href')
                if href and href not in dataset_urls:
                    dataset_urls.append(href)
                    print(f"Found dataset URL: {href}")

            # Save progress periodically
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(dataset_urls, f, ensure_ascii=False, indent=4)

            # Check for the "Next" button
            try:
                next_button = driver.find_element(By.CSS_SELECTOR, 'a.nextLink:not(.disabled)')
                driver.execute_script("arguments[0].scrollIntoView(true);", next_button)
                time.sleep(1)  # Wait for scrolling
                next_button.click()
                time.sleep(3)  # Wait for the next page to load
            except NoSuchElementException:
                print("No more pages.")
                break
    finally:
        driver.quit()

    # Final deduplication and save
    dataset_urls = list(dict.fromkeys(dataset_urls))
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(dataset_urls, f, ensure_ascii=False, indent=4)
    print(f"Saved {len(dataset_urls)} dataset URLs to {file_path}")


if __name__ == "__main__":
    scrape_dataset_urls()
