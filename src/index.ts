import os
import asyncio
from playwright.async_api import async_playwright
import aiohttp
from aiofile import AIOFile, Writer
import json
import re
from datetime import datetime
from typing import List, Dict, Any
import { upsertProductToFrappe } from './frappe.ts'

# Load environment variables
from dotenv import load_dotenv
load_dotenv()
load_dotenv(dotenv_path='.env.local', override=True)

# Constants
PAGE_LOAD_DELAY_SECONDS = 7
PRODUCT_LOG_DELAY_MILLISECONDS = 20

# Initialize global variables
database_mode = False
upload_images_mode = False
headless_mode = True
start_time = datetime.now()

async def main():
    global database_mode, upload_images_mode, headless_mode

    # Parse command-line arguments
    args = os.sys.argv[1:]
    categorised_urls = await handle_arguments(args)

    # Establish MySQL if being used
    if database_mode:
        await establish_mysql()

    # Establish Playwright browser
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless_mode)
        page = await browser.new_page()

        # Select store location
        await select_store_by_location_name(page)

        # Main Loop - Scrape through each page URL
        await scrape_all_page_urls(page, categorised_urls)

        # Program End and Cleanup
        await browser.close()
        print(f"\nAll Pages Completed = Total Time Elapsed {get_time_elapsed_since(start_time)} \n")

async def handle_arguments(args: List[str]) -> List[Dict[str, Any]]:
    global database_mode, upload_images_mode, headless_mode

    categorised_urls = load_urls_file()

    for arg in args:
        if arg == "db":
            database_mode = True
        elif arg == "images":
            upload_images_mode = True
        elif arg == "headless":
            headless_mode = True
        elif arg == "headed":
            headless_mode = False
        elif ".co.nz" in arg:
            categorised_urls = [parse_and_categorise_url(arg)]
        elif arg == "reverse":
            categorised_urls.reverse()

    return categorised_urls

async def establish_mysql():
    # Implement MySQL connection logic
    pass

async def select_store_by_location_name(page):
    location_name = os.getenv("STORE_NAME", "")
    if not location_name:
        return

    print("Selecting Store Location..")
    await page.goto("https://www.woolworths.co.nz/bookatimeslot", wait_until='domcontentloaded')
    await asyncio.sleep(2)
    await page.click("fieldset div div p button")
    await page.type("form-suburb-autocomplete form-input input", location_name)
    await asyncio.sleep(1.5)
    await page.keyboard.press("ArrowDown")
    await asyncio.sleep(0.3)
    await page.keyboard.press("Enter")
    await asyncio.sleep(1)
    await page.click("text=Save and Continue Shopping")
    await asyncio.sleep(2)

async def scrape_all_page_urls(page, categorised_urls):
    print(f"{len(categorised_urls)} pages to be scraped with {PAGE_LOAD_DELAY_SECONDS}s delay between scrapes")
    for i, categorised_url in enumerate(categorised_urls):
        url = categorised_url["url"]
        short_url = url.replace("https://", "")
        print(f"\n[{i + 1}/{len(categorised_urls)}] {short_url}")

        try:
            await page.goto(url)
            for _ in range(5):
                await asyncio.sleep(0.5 + 1.0 * asyncio.random())
                await page.keyboard.press("PageDown")

            await page.set_default_timeout(15000)
            await page.wait_for_selector("product-price h3")

            html = await page.inner_html("product-grid")
            product_entries = parse_html_to_products(html)

            print(f"{len(product_entries)} product entries found")

            per_page_log_stats = await process_found_product_entries(categorised_url, product_entries)

            if database_mode:
                print(f"MySQL: {per_page_log_stats['newProducts']} new products, "
                      f"{per_page_log_stats['priceChanged']} updated prices, "
                      f"{per_page_log_stats['infoUpdated']} updated info, "
                      f"{per_page_log_stats['alreadyUpToDate']} already up-to-date")

            await asyncio.sleep(PAGE_LOAD_DELAY_SECONDS)

        except Exception as e:
            print(f"Error: {e}")

async def process_found_product_entries(categorised_url, product_entries):
    per_page_log_stats = {"newProducts": 0, "priceChanged": 0, "infoUpdated": 0, "alreadyUpToDate": 0}
    for product_entry in product_entries:
        product = parse_product_entry(product_entry, categorised_url["categories"])
        if database_mode and product:
            response = await upsertProductToFrappe(product)
            if response == "NewProduct":
                per_page_log_stats["newProducts"] += 1
            elif response == "PriceChanged":
                per_page_log_stats["priceChanged"] += 1
            elif response == "InfoChanged":
                per_page_log_stats["infoUpdated"] += 1
            elif response == "AlreadyUpToDate":
                per_page_log_stats["alreadyUpToDate"] += 1
            if upload_images_mode:
                await upload_image_rest_api(product)
        else:
            print_product_row(product)

        await asyncio.sleep(PRODUCT_LOG_DELAY_MILLISECONDS / 1000)
    return per_page_log_stats

def load_urls_file(file_path="src/urls.txt"):
    with open(file_path, "r") as file:
        lines = file.readlines()
    categorised_urls = [parse_and_categorise_url(line.strip()) for line in lines if line.strip()]
    return categorised_urls

def parse_and_categorise_url(line):
    # Implement URL parsing and categorisation logic
    pass

def parse_html_to_products(html):
    # Implement HTML parsing to extract products
    pass

def parse_product_entry(entry, categories):
    # Implement product entry parsing
    pass

def print_product_row(product):
    # Implement product row printing
    pass

async def upsert_product_to_mysql(product):
    # Implement MySQL upsert logic
    pass

async def upload_image_rest_api(product):
    # Implement image upload logic
    pass

def get_time_elapsed_since(start_time):
    return str(datetime.now() - start_time)

if __name__ == "__main__":
    asyncio.run(main())
