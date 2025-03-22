import mariadb
import pandas as pd
import numpy as np

# 데이터베이스 연결 정보
DB_HOST = "43.203.172.46"
DB_PORT = 3306
DB_USER = "booknest"
DB_PASSWORD = "b203B203"
DB_NAME = "booknest"
#-*- coding: utf-8 -*-#

# CSV 파일 로드 (인코딩 확인)
csv_file = "test_data.csv"  # 실제 CSV 파일 경로로 변경
df = pd.read_csv(csv_file, encoding='utf-8')  # 또는 CSV 파일의 실제 인코딩 지정

# NaN 값을 None으로 변환
df = df.replace({np.nan: None})

try:
    # MariaDB 연결 (charset 인자 제거)
    conn = mariadb.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )
    cursor = conn.cursor()
    cursor.execute("SET NAMES utf8mb4")  # 연결 후 문자셋 설정

    # 중복 방지를 위한 딕셔너리
    author_dict = {}
    category_dict = {}

    # AUTHOR & CATEGORY 테이블 중복 방지 후 삽입
    cursor.execute("SELECT id, name FROM author")
    for author_id, name in cursor.fetchall():
        author_dict[name] = author_id

    cursor.execute("SELECT id, name FROM category")
    for category_id, name in cursor.fetchall():
        category_dict[name] = category_id

    for _, row in df.iterrows():
        title = row["Title"] or ""
        published_date = None  # 또는 row["PublishedDate"] 등
        isbn = None            # 또는 row["ISBN"] 등
        publisher = row["Publisher"] or ""
        pages = row["Page"] or 0
        image_url = row["Image"] or ""
        intro = row["Intro"] or ""
        contents = row["Contents"] or ""
        publisher_review = str(row["Review"]) if row["Review"] is not None else ""

        cursor.execute("""
            INSERT INTO book (title, published_date, isbn, publisher, pages, image_url, intro, book_index, publisher_review, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())
        """, (title, published_date, isbn, publisher, pages, image_url, intro, contents, publisher_review))

        book_id = cursor.lastrowid

        # # AUTHOR 데이터 처리
        # if row["AUTHR_NM"]:
        #     authors = row["AUTHR_NM"].split(",")
        #     for author in authors:
        #         author = author.strip()
        #         if author not in author_dict:
        #             cursor.execute("INSERT INTO author (name) VALUES (?)", (author,))
        #             author_dict[author] = cursor.lastrowid
        #         author_id = author_dict[author]
        #         cursor.execute("INSERT INTO book_author (book_id, author_id) VALUES (?, ?)", (book_id, author_id))

        # # CATEGORY 데이터 처리
        # if row["CATEGORY"]:
        #     categories = row["CATEGORY"].split(",")
        #     for category in categories:
        #         category = category.strip()
        #         if category not in category_dict:
        #             cursor.execute("INSERT INTO category (name) VALUES (?)", (category,))
        #             category_dict[category] = cursor.lastrowid
        #         category_id = category_dict[category]
        #         cursor.execute("INSERT INTO book_category (book_id, category_id) VALUES (?, ?)", (book_id, category_id))

    conn.commit()
    print("데이터 삽입 완료!")

except mariadb.Error as e:
    print(f"Database Error: {e}")
    conn.rollback()

finally:
    if conn:
        cursor.close()
        conn.close()
