import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time
from urllib.parse import quote
import json
import aiohttp
import asyncio


semaphore = asyncio.Semaphore(10)

def get_cookies_local(email: str, password: str) -> dict:
    """getting cookies"""

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    driver.get('https://eggheads.solutions/fe3/login')

    time.sleep(2)
    try:
        email_field = driver.find_element(By.NAME, 'email')
        password_field = driver.find_element(By.NAME, 'password')
        email_field.send_keys(email)
        password_field.send_keys(password)

        login_button = driver.find_element(By.CLASS_NAME, 'authorization-button')
        login_button.click()

        time.sleep(3)

        cookies = driver.get_cookies()
        driver.quit()

        cookies = {cookie['name']: cookie['value'] for cookie in cookies}

    except Exception as e:
        print(f"Ошибка: {e}")
        driver.quit()

    return cookies

def get_cookies(email: str, password: str) -> dict:
    """getting cookies"""

    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Run without GUI
    chrome_options.add_argument("--no-sandbox")  # Required for Docker security
    chrome_options.add_argument("--disable-dev-shm-usage")  # Avoids /dev/shm issues
    chrome_options.add_argument("--disable-gpu")  # Optional, but helps stability
    chrome_options.add_argument("--window-size=1920,1080")  # Set a reasonable window size

    try:
        # Connect to the remote Selenium container
        driver = webdriver.Remote(
            command_executor="http://selenium:4444/wd/hub",  # Service name from docker-compose
            options=chrome_options
        )

        driver.get('https://eggheads.solutions/fe3/login')

        time.sleep(2)

        email_field = driver.find_element(By.NAME, 'email')
        password_field = driver.find_element(By.NAME, 'password')
        email_field.send_keys(email)
        password_field.send_keys(password)

        login_button = driver.find_element(By.CLASS_NAME, 'authorization-button')
        login_button.click()

        time.sleep(3)

        cookies = driver.get_cookies()
        driver.quit()

        cookies = {cookie['name']: cookie['value'] for cookie in cookies}

    except Exception as e:
        print(f"Ошибка: {e}")
        if 'driver' in locals():
            driver.quit()
        cookies = {}

    print('cookie done')

    return cookies

def get_session():

    session = requests.Session()

    return session

def get_l1_l2(check_dt: str, today: str, session: requests.Session, cookies: dict):

    url = f'https://eggheads.solutions/analytics/wbCategoryTree/getParentTree/{check_dt}.json?dns-cache={today}_09-1'
    response = session.get(url, cookies=cookies)

    if response.status_code == 200:
        data = response.json()
    else:
        print(response.text)
        print(url)

    return data

def get_l3(check_dt: str, l2_id: int, today: str, session: requests.Session, cookies: dict):

    url = f'https://eggheads.solutions/analytics/wbCategoryTree/getTreeItems/{check_dt}/{l2_id}.json?dns-cache={today}_09-1'
    response = session.get(url, cookies=cookies)
    if response.status_code == 200:
        data = response.json()
    else:
        print(response.text)
        print(url)

    return data

def get_info_30_days(check_dt: str, l3_id: int, today: str, session: requests.Session, cookies: dict):

    session.headers.update({
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (compatible; DataCollector/1.0)"
    })

    # Формируем JSON query
    query_params = {
        "start": 0,
        "length": 0,
        "orderBy": "ordersSum",
        "orderDirection": "desc",
        "checkDate": check_dt,
        "periodDays": 30,
        "trendType": "day",
        "filters": {
            "showFavoritesOnly": {"value": False}
        }
    }

    encoded_query = quote(json.dumps(query_params))
    # Generate dynamic dns-cache
    response = session.post(f'https://eggheads.solutions/analytics/wbCategory/buildCache/{l3_id}',cookies=cookies)

    url = f"https://eggheads.solutions/analytics/wbCategory/getBrandsList/{l3_id}.json?query={encoded_query}&dns-cache={today}_09-1"
    response = session.get(url, cookies=cookies)

    if response.status_code == 200:
        data = response.json()
        return data['totals']
    else:
        print(response.text)
        print(url)
        return data['totals']

async def get_info_30_async_days(check_dt: str, l3_id: int, today: str, session: aiohttp.ClientSession, cookies: dict):
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (compatible; DataCollector/1.0)"
    }

    query_params = {
        "start": 0,
        "length": 0,
        "checkDate": check_dt,
        "periodDays": 30,
    }
    encoded_query = quote(json.dumps(query_params))

    async with semaphore:  # ограничиваем параллельность
        # POST — buildCache
        async with session.post(
            f'https://eggheads.solutions/analytics/wbCategory/buildCache/{l3_id}',
            cookies=cookies,
            headers=headers
        ) as resp:
            await resp.text()

        url = f"https://eggheads.solutions/analytics/wbCategory/getBrandsList/{l3_id}.json?query={encoded_query}&dns-cache={today}_09-1"

        for attempt in range(5):  # до 5 попыток при 429
            async with session.get(url, cookies=cookies, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get("totals", [])
                elif response.status == 429:
                    wait_time = 2 ** attempt  # экспоненциальный backoff
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    text = await response.text()
                    print(f"Error {response.status}: {text}")
                    return []
        return []

def get_info_subjects(check_dt: str, today: str, session: requests.Session, cookies: dict):
    session.headers.update({
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (compatible; DataCollector/1.0)"
    })

    # Формируем JSON query
    query_params = {
        "start": 0,
        "length": 7000,
        "orderBy": "totalItems",
        "orderDirection": "desc",
        "checkDate": check_dt,
        "filters": {
            "showFavoritesOnly": {"value": False}
        }
    }

    encoded_query = quote(json.dumps(query_params))

    url = f"https://eggheads.solutions/analytics/wbSubjects/rating?query={encoded_query}&dns-cache={today}_09-1"
    response = session.get(url, cookies=cookies)

    if response.status_code == 200:
        data = response.json()
        return data
    else:
        print(response.text)
        print(url)
        return data

async def get_async_season_ratio(subject_id: int, today: str, session: aiohttp.ClientSession, cookies: dict):

    session.headers.update({
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (compatible; DataCollector/1.0)"
    })

    async with semaphore:
        url = f"https://eggheads.solutions/clientAnalytics/subject/seasonRatio/{subject_id}?dns-cache={today}_09-1"
        for attempt in range(5):
            async with session.get(url, cookies=cookies) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get("coefficients", [])
                elif response.status == 429:
                    wait_time = 2 ** attempt
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    text = await response.text()
                    print(f"Error {response.status}: {text}")

                    return []

        return []

async def get_async_info_subjects_30_days(check_dt: str, subject_id: int, today: str, session: aiohttp.ClientSession, cookies: dict):
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (compatible; DataCollector/1.0)"
    }

    # Формируем JSON query based on the provided URL
    query_params = {
        "start": 0,
        "length": 0,
        "checkDate": check_dt,
        "periodDays": "30",
        "trendType": "day",
        "filters": {
            "showFavoritesOnly": {"value": False},
            "showMyProducts": {"value": False},
            "showNewProducts": {"value": False}
        }
    }

    encoded_query = quote(json.dumps(query_params))
    url = f"https://eggheads.solutions/analytics/wbSubject/getProductsList/{subject_id}.json?query={encoded_query}&dns-cache={today}_09-1"

    async with semaphore:  # Ограничиваем параллельность

        async with session.post(
            f'https://eggheads.solutions/analytics/wbSubject/buildCache/{subject_id}',
            cookies=cookies,
            headers=headers
        ) as resp:
            await resp.text()

        for attempt in range(5):  # До 5 попыток при 429
            async with session.get(url, cookies=cookies, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get("totals", [])
                elif response.status == 429:
                    wait_time = 2 ** attempt  # Экспоненциальный backoff
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    text = await response.text()
                    print(f"Error {response.status}: {text}")
                    print(f"URL: {url}")
                    return []
        return []