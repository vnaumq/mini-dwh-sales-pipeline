import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time

def get_cookies(email: str, password: str) -> dict:
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

        cookies_dict = {cookie['name']: cookie['value'] for cookie in cookies}

    except Exception as e:
        print(f"Ошибка: {e}")
        driver.quit()

    return cookies_dict

def get_session():
    session = requests.Session()

    return session

def get_l1_l2(yesterday_str: str, today_str: str, session: requests.Session, cookies_dict: dict):

    url = f'https://eggheads.solutions/analytics/wbCategoryTree/getParentTree/{yesterday_str}.json?dns-cache={today_str}_09-1'
    response = session.get(url, cookies=cookies_dict)
    data = response.json()

    return data

def get_l3(yesterday_str: str, l2_id: int, today_str: str, session: requests.Session, cookies_dict: dict):

    url = f'https://eggheads.solutions/analytics/wbCategoryTree/getTreeItems/{yesterday_str}/{l2_id}.json?dns-cache={today_str}_23-1'
    response = session.get(url, cookies=cookies_dict)
    data = response.json()

    return data