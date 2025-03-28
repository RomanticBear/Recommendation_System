import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from fake_useragent import UserAgent

ua = UserAgent()
HEADERS = {'User-Agent': ua.random}
BASE_LIST_URL = "https://www.yes24.com/product/category/steadyseller?pageNumber={}&pageSize=24&categoryNumber={}"
BASE_URL = "https://www.yes24.com"

category_list = ["001001019", "001001047"]
page_range = range(1, 43)
OUTPUT_CSV = 'yes24_steadyseller_books_full.csv'

def get_book_links(category, page):
    url = BASE_LIST_URL.format(page, category)
    try:
        res = requests.get(url, headers=HEADERS, timeout=5)
        soup = BeautifulSoup(res.text, 'html.parser')
        book_items = soup.select('#yesBestList li')
        links = [
            BASE_URL + item.select_one("a.gd_name")['href']
            for item in book_items if item.select_one("a.gd_name")
        ]
        return links
    except Exception as e:
        print(f"[!] 리스트 페이지 에러: {url} | {e}")
        return []

def parse_book_detail(book_url):
    try:
        res = requests.get(book_url, headers=HEADERS, timeout=5)
        soup = BeautifulSoup(res.text, 'html.parser')

        get_text = lambda sel: soup.select_one(sel).get_text(strip=True) if soup.select_one(sel) else None
        get_attr = lambda sel, attr: soup.select_one(sel)[attr] if soup.select_one(sel) and soup.select_one(sel).has_attr(attr) else None

        # 제목
        title = get_text('#yDetailTopWrap > div.topColRgt > div.gd_infoTop > div > h2')

        # ISBN13 정확하게 찾기
        isbn = None
        spec_table = soup.select('#infoset_specific table tr')
        for row in spec_table:
            th = row.select_one('th')
            td = row.select_one('td')
            if th and 'ISBN13' in th.get_text():
                isbn = td.get_text(strip=True)
                break

        # 페이지 수만 추출
        page_raw = get_text('#infoset_specific > div.infoSetCont_wrap > div > table > tbody > tr:nth-child(2) > td')
        page = page_raw.split("쪽")[0].strip() if page_raw and '쪽' in page_raw else None

        # 저자
        author = None
        author_tag = soup.select_one('span.gd_auth a')
        if author_tag:
            author = author_tag.get_text(strip=True)

        # 출판사
        publisher = None
        pub_tag = soup.select_one('span.gd_pub a')
        if pub_tag:
            publisher = pub_tag.get_text(strip=True)

        # 발행 연도만 추출
        pub_date_raw = get_text('#infoset_specific > div.infoSetCont_wrap > div > table > tbody > tr:nth-child(1) > td')
        published_date = pub_date_raw.split('년')[0].strip() if pub_date_raw and '년' in pub_date_raw else None

        # 이미지 URL
        image_url = None
        img_tag = soup.select_one('#yDetailTopWrap img')
        if img_tag and img_tag.has_attr('src'):
            image_url = img_tag['src']

        # 책 소개
        intro = None
        intro_area = soup.select_one('#infoset_introduce .infoWrap_txt')
        if intro_area:
            intro = intro_area.get_text(separator='\n', strip=True)

        # 출판사 리뷰
        publisher_review = None
        pubrev_area = soup.select_one('#infoset_pubReivew .infoWrap_txt')
        if pubrev_area:
            publisher_review = pubrev_area.get_text(separator='\n', strip=True)

        # 목차
        book_index = None
        toc_area = soup.select_one('#infoset_toc .infoWrap_txt')
        if toc_area:
            book_index = toc_area.get_text(separator='\n', strip=True)

        # 출력 로그
        print(f"[크롤링 완료] 제목: {title} | ISBN: {isbn} | 저자: {author} | 출판사: {publisher} | URL: {book_url}")

        return {
            'title': title,
            'isbn': isbn,
            'page': page,
            'author': author,
            'publisher': publisher,
            'published_date': published_date,
            'image_url': image_url,
            'publisher_review': publisher_review,
            'intro': intro,
            'book_index': book_index,
            'url': book_url
        }

    except Exception as e:
        print(f"[!] 상세 페이지 에러: {book_url} | {e}")
        return None

def crawl_all():
    # 기존 데이터 로딩
    if os.path.exists(OUTPUT_CSV):
        existing_df = pd.read_csv(OUTPUT_CSV)
        crawled_urls = set(existing_df['url'].dropna().tolist())
        print(f"[✓] 기존에 크롤링된 도서 수: {len(crawled_urls)}")
    else:
        existing_df = pd.DataFrame()
        crawled_urls = set()

    # 도서 URL 수집
    all_links = set()
    for category in category_list:
        for page in page_range:
            links = get_book_links(category, page)
            all_links.update(links)
            time.sleep(0.2)

    print(f"[+] 전체 수집된 도서 링크 수: {len(all_links)}")

    # 중복 제거
    new_links = list(all_links - crawled_urls)
    print(f"[⏳] 새로 크롤링할 도서 수: {len(new_links)}")

    # 순차 크롤링
    results = []
    for url in new_links:
        result = parse_book_detail(url)
        if result:
            results.append(result)
        time.sleep(0.1)

    if not results:
        print("[!] 크롤링된 데이터가 없습니다. 종료합니다.")
        return

    new_df = pd.DataFrame(results)

    # 병합 및 저장
    full_df = pd.concat([existing_df, new_df], ignore_index=True)
    full_df.drop_duplicates(subset='url', inplace=True)
    full_df.to_csv(OUTPUT_CSV, index=False, encoding='utf-8-sig')
    print(f"[✓] 최종 저장 완료: {len(full_df)}권")

if __name__ == "__main__":
    crawl_all()
