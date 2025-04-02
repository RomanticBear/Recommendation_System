import requests
import pandas as pd
from bs4 import BeautifulSoup
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from fake_useragent import UserAgent

ua = UserAgent()
headers = {"User-Agent": ua.random}

# 카테고리 필터 조건
EXCLUDE_KEYWORDS = ["유아", "4-6세", "4-7세", "어린이"]
REQUIRED_KEYWORDS = ["소설"]


def safe_request(url, retries=3, delay=2):
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            return response
        except:
            time.sleep(delay)
    return None


def read_csv_subset(filename):
    df = pd.read_csv(filename, usecols=["ISBN_THIRTEEN_NO"])
    df['ISBN_THIRTEEN_NO'] = pd.to_numeric(df['ISBN_THIRTEEN_NO'], errors='coerce').astype('Int64')
    return df.dropna().drop_duplicates()


def get_all_saved_isbns(output_dir):
    saved_isbns = set()
    for file in os.listdir(output_dir):
        if file.endswith(".csv") and file.startswith("top_rent_books_chunk_"):
            try:
                df = pd.read_csv(os.path.join(output_dir, file), usecols=["ISBN"])
                saved_isbns.update(df["ISBN"].dropna().astype(str))
            except Exception as e:
                print(f"[ERROR] {file} 읽기 실패: {e}")
    return saved_isbns


def get_book_page_url(book_isbn):
    search_url = f"https://www.yes24.com/product/search?domain=BOOK&query={book_isbn}"
    response = safe_request(search_url)
    if not response:
        return None
    soup = BeautifulSoup(response.text, "html.parser")
    book_link = soup.select_one("div.info_name > a")
    return book_link["href"] if book_link else None


def is_valid_category(category_str):
    for word in EXCLUDE_KEYWORDS:
        if word in category_str:
            return False
    return any(req in category_str for req in REQUIRED_KEYWORDS)


def get_book_info(book_url, book_isbn):
    if not book_url:
        return None

    response = safe_request("https://www.yes24.com" + book_url)
    if not response:
        return None
    soup = BeautifulSoup(response.text, "html.parser")

    category_set = set()
    for dl in soup.select("dl.yesAlertDl"):
        dt = dl.select_one("dt")
        if dt and dt.get_text(strip=True) == "카테고리 분류":
            for link in dl.select("dd a"):
                text = link.get_text(strip=True)
                if text:
                    category_set.add(text)
    category_str = ", ".join(sorted(category_set))

    if not is_valid_category(category_str):
        print(f"[SKIP] ISBN {book_isbn}: 카테고리 조건 불만족 - {category_str}")
        return None

    title = soup.select_one("div.gd_titArea > h2")
    title = title.text.strip() if title else "N/A"

    author_raw = soup.select_one("span.gd_auth")
    author_text = author_raw.text.strip() if author_raw else "N/A"
    roles = ['저', '저.', '그림', '그림.', '역', '역.', '글', '글.', '편', '편.']
    author = []
    for part in re.split(r"/", author_text):
        for name in part.split(","):
            name = name.strip()
            for role in roles:
                if name.endswith(role):
                    name = name[:-len(role)].strip()
            if name:
                author.append(name)
    author = ", ".join(author)

    publisher = soup.select_one("span.gd_pub > a")
    publisher = publisher.text.strip() if publisher else "N/A"

    published_date = soup.select_one("span.gd_date")
    published_date = published_date.text.split('년')[0].strip() if published_date else "N/A"

    image = soup.select_one("span.gd_img img")
    image_url = image["src"] if image else "N/A"

    page = ""
    page_table = soup.select_one("#infoset_specific table")
    if page_table:
        for row in page_table.select("tr"):
            th = row.select_one("th")
            if th and "쪽수" in th.text:
                td = row.select_one("td")
                page = td.text.split("쪽")[0].strip() if td else ""
                break

    introduce = soup.select_one("#infoset_introduce textarea.txtContentText")
    index = soup.select_one("#infoset_toc textarea.txtContentText")
    pub_review = soup.select_one("#infoset_pubReivew textarea.txtContentText")

    return {
        "ISBN": book_isbn,
        "TITLE": title,
        "AUTHOR": author,
        "PUBLISHER": publisher,
        "PUBLISHED_DATE": published_date,
        "PAGE": page,
        "BOOK_IMAGE": image_url,
        "CATEGORY": category_str,
        "INTRO": introduce.text.strip() if introduce else "N/A",
        "CONTENTS": index.text.strip() if index else "N/A",
        "PUBLISHER_REVIEW": pub_review.text.strip() if pub_review else "N/A"
    }


def chunked_iterable(data, chunk_size):
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]


def save_data(data, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    pd.DataFrame(data).to_csv(path, index=False, encoding="utf-8")
    print(f"[SAVE] {path} 저장 완료.")


def main():
    input_path = "top_rent_books/top_rent_books.csv"
    output_base_dir = "book_data_crawled/top_rent"
    os.makedirs(output_base_dir, exist_ok=True)

    # 전체 ISBN 목록
    isbn_df = read_csv_subset(input_path)
    isbn_list = isbn_df["ISBN_THIRTEEN_NO"].astype(str).tolist()

    # 저장된 ISBN 제외
    saved_isbns = get_all_saved_isbns(output_base_dir)
    isbn_list = [isbn for isbn in isbn_list if isbn not in saved_isbns]

    print(f"[INFO] 총 {len(isbn_df)}개 중 {len(saved_isbns)}개는 이미 수집됨 → {len(isbn_list)}개 대상")

    def process_isbn(isbn):
        book_url = get_book_page_url(isbn)
        if not book_url:
            return None
        return get_book_info(book_url, isbn)

    chunk_size = 10000
    for i, chunk in enumerate(chunked_iterable(isbn_list, chunk_size), start=1):
        output_chunk_path = f"{output_base_dir}/top_rent_books_chunk_{i:03}.csv"
        temp_save_path = f"{output_base_dir}/temp_chunk_{i:03}.csv"

        if os.path.exists(output_chunk_path):
            print(f"[SKIP] {output_chunk_path} 이미 존재 - 건너뜀")
            continue

        print(f"\n[CHUNK {i}] 처리 시작 - {len(chunk)}건")
        chunk_results = []

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(process_isbn, isbn): isbn for isbn in chunk}
            for idx, future in enumerate(as_completed(futures), 1):
                result = future.result()
                if result:
                    chunk_results.append(result)

                # ✅ 100개 단위 임시 저장
                if idx % 100 == 0:
                    save_data(chunk_results, temp_save_path)
                    print(f"[TEMP SAVE] {idx}개 완료")

        save_data(chunk_results, output_chunk_path)
        if os.path.exists(temp_save_path):
            os.remove(temp_save_path)

    print("\n[SUCCESS] 전체 크롤링 완료!")


if __name__ == "__main__":
    main()
