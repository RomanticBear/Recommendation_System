import pandas as pd
import csv
import os
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Selenium 옵션 설정
options = Options()
options.add_argument("--headless")  # 브라우저 창 없이 실행
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--enable-unsafe-swiftshader")
options.add_argument("--ignore-gpu-blocklist")
options.add_argument("--disable-software-rasterizer")

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

CHECKPOINT_FILE = "processed_isbns.txt"

# 이미 크롤링한 ISBN 불러오기
def load_processed_isbns():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            return set(f.read().splitlines())
    return set()

# 크롤링한 ISBN 저장하기
def save_processed_isbn(isbn):
    with open(CHECKPOINT_FILE, "a", encoding="utf-8") as f:
        f.write(isbn + "\n")

# ISBN 목록 읽기 (NA 값 제거)
def read_isbn_list(filename):
    try:
        data = pd.read_csv(filename, low_memory=False, dtype={"ISBN_THIRTEEN_NO": str})
        isbn_list = data['ISBN_THIRTEEN_NO'].dropna().astype(str).tolist()
        unique_isbn_list = list(dict.fromkeys(isbn_list))  # 순서 유지하면서 중복 제거
        return unique_isbn_list
    except Exception as e:
        print(f"[ERROR] ISBN CSV 파일 읽기 실패: {e}")
        return []

# ISBN으로 도서 조회 및 URL 반환
def get_book_page_url(book_isbn):
    # 연습용
    # search_url = f"https://search.kyobobook.co.kr/web/search?vPstrKeyWord={book_isbn}&orderClick=LAG"

    try:
        response = requests.get(search_url, headers=headers, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"[ERROR] 검색 페이지 요청 실패: {e}")
        return None

    soup = BeautifulSoup(response.text, "html.parser")
    book_detail_link = soup.find("a", class_="prod_link")
    return book_detail_link["href"] if book_detail_link else None

# 도서 정보 크롤링
def get_book_info(book_url, driver):
    if not book_url:
        return None

    try:
        driver.get(book_url)
        wait = WebDriverWait(driver, 10)
        wait.until(EC.presence_of_element_located((By.CLASS_NAME, "basic_info")))
    except Exception as e:
        print(f"[ERROR] 도서 정보 페이지 로딩 실패: {e}")
        return None

    soup = BeautifulSoup(driver.page_source, "html.parser")

    # 제목
    title_tag = soup.find("span", class_="prod_title")
    title = title_tag.text.strip() if title_tag else "N/A"

    # 이미지 URL
    img_div = soup.find("div", class_="blur_img_wrap portrait")
    img_url = img_div.find("img").get("src") if img_div and img_div.find("img") else "N/A"

    # 카테고리
    category_tags = soup.find_all(class_="intro_category_link")
    categories = [tag.get_text(strip=True) for tag in category_tags] if category_tags else []
    category = list(set([item for category in categories for item in category.split("/")])) if categories else ["N/A"]

    # 도서 소개
    intro_tag = soup.find("div", class_="intro_bottom")
    intro_list = intro_tag.find_all("div", class_="info_text") if intro_tag else []
    intro = ''.join([intro.text.strip() for intro in intro_list]) if intro_list else "N/A"

    # 목차
    book_contents_div = soup.find("div", class_="book_contents")
    book_contents_li = book_contents_div.find("li", class_="book_contents_item") if book_contents_div else None
    book_contents = book_contents_li.text.strip() if book_contents_li else "N/A"

    # 출판사 서평
    book_publish_review_div = soup.find("div", class_="book_publish_review")
    book_publish_review_p = book_publish_review_div.find("p", class_="info_text") if book_publish_review_div else None
    book_publish_review = book_publish_review_p.text.strip() if book_publish_review_p else "N/A"

    # 쪽수
    page_num = "N/A"
    basic_info_div = soup.find("div", class_="basic_info")
    if basic_info_div:
        for tr in basic_info_div.find_all("tr"):
            th = tr.find("th")
            if th and th.text.strip() == "쪽수":
                td = tr.find("td")
                page_num = td.text.strip()[:-1] if td else "N/A"
                break

    print(f"[INFO] 크롤링 완료: {title}")

    return {
        "Title": title,
        "Image": img_url,
        "Category": ", ".join(category),
        "Intro": intro,
        "Contents": book_contents,
        "Publisher Review": book_publish_review,
        "Page": page_num
    }

# CSV 저장 함수
def save_to_csv(data, filename="crawling_data/books.csv"):
    try:
        file_exists = os.path.isfile(filename)
        with open(filename, mode="a", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=["Title", "Image", "Category", "Intro", "Contents", "Publisher Review", "Page"])
            if not file_exists:
                writer.writeheader()
            writer.writerows(data)
        print(f"[INFO] CSV 파일 업데이트 완료 (총 {len(data)}권 저장됨)")
    except Exception as e:
        print(f"[ERROR] CSV 파일 저장 실패: {e}")

# main 함수
def main():
    base_folder = "book_data"
    processed_isbns = load_processed_isbns()

    for year in os.listdir(base_folder):

        if year == "2019":
            print(f"[INFO] {year} 폴더 건너뜀")
            continue

        year_path = os.path.join(base_folder, year)
        if os.path.isdir(year_path):
            print(f"\n📂 {year} 폴더")
            csv_files = [f for f in os.listdir(year_path) if f.endswith(".csv")]
            for file in csv_files:
                print(f"\n👻 {file} 크롤링 시작")
                file_path = os.path.join(year_path, file)
                isbn_list = read_isbn_list(file_path)
                driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
                book_data = []

                for idx, isbn in enumerate(isbn_list, start=1):
                    if not isbn or isbn in processed_isbns:
                        continue

                    print(f"[INFO] {idx}/{len(isbn_list)} ISBN 처리 중: {isbn}")
                    book_url = get_book_page_url(isbn)
                    if book_url:
                        book_info = get_book_info(book_url, driver)
                        if book_info:
                            book_data.append(book_info)
                            save_processed_isbn(isbn)

                    if idx % 100 == 0:
                        save_to_csv(book_data)
                        book_data = []

                driver.quit()
                save_to_csv(book_data)

if __name__ == "__main__":
    main()