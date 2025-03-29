import mariadb
import pandas as pd
import numpy as np

# ------------------ Config ------------------
DB_CONFIG = {
    "host": "43.203.172.46",
    "port": 3306,
    "user": "booknest",
    "password": "b203B203",
    "database": "booknest"
}
CSV_FILE = "bookdata_crawled/page_1/NL_BO_BOOK_PUB_202402-1_crawled.csv"
ENCODING = 'utf-8'

# ------------------ DB Helper ------------------
def connect_db():
    conn = mariadb.connect(**DB_CONFIG)
    conn.cursor().execute("SET NAMES utf8mb4")
    return conn

def load_existing_dict(cursor, table):
    cursor.execute(f"SELECT id, name FROM {table}")
    return {name: id for id, name in cursor.fetchall()}

def insert_and_get_id(cursor, table, name, cache_dict):
    if name not in cache_dict:
        cursor.execute(f"INSERT INTO {table} (name) VALUES (?)", (name,))
        cache_dict[name] = cursor.lastrowid
    return cache_dict[name]

def insert_book(cursor, row):
    cursor.execute("""
        INSERT INTO book (title, published_date, isbn, publisher, pages, image_url, intro, book_index, publisher_review, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())
    """, (
        row["TITLE"] or "",
        row["PUBLISHED_DATE"],
        row["ISBN"] or None,
        row["PUBLISHER"] or "",
        row["PAGE"] or 0,
        row["BOOK_IMAGE"] or "",
        row["INTRO"] or "",
        row["CONTENTS"] or "",
        str(row["PUBLISHER_REVIEW"]) if row["PUBLISHER_REVIEW"] is not None else ""
    ))
    return cursor.lastrowid

def process_many_to_many(cursor, book_id, items, table, bridge_table, cache_dict):
    for item in items:
        name = item.strip()
        if name:
            item_id = insert_and_get_id(cursor, table, name, cache_dict)
            cursor.execute(f"INSERT INTO {bridge_table} (book_id, {table}_id) VALUES (?, ?)", (book_id, item_id))

# ------------------ Main ------------------
def main():
    df = pd.read_csv(CSV_FILE, encoding=ENCODING)
    df = df[df["STATUS"] == "SUCCESS"].replace({np.nan: None})
    print(f"Processing {len(df)} rows")

    try:
        with connect_db() as conn:
            cursor = conn.cursor()

            author_dict = load_existing_dict(cursor, "author")
            category_dict = load_existing_dict(cursor, "category")
            tag_dict = load_existing_dict(cursor, "tag")

            for _, row in df.iterrows():
                book_id = insert_book(cursor, row)

                if row["AUTHOR"]:
                    authors = row["AUTHOR"].split(",")
                    process_many_to_many(cursor, book_id, authors, "author", "book_author", author_dict)

                if row["CATEGORY"]:
                    categories = row["CATEGORY"].split(",")
                    process_many_to_many(cursor, book_id, categories, "category", "book_category", category_dict)

                if row.get("TAG"):
                    tags = row["TAG"].split(",")
                    process_many_to_many(cursor, book_id, tags, "tag", "book_tag", tag_dict)

            conn.commit()
            print("✅ 데이터 삽입 완료!")

    except mariadb.Error as e:
        print(f"❌ Database Error: {e}")
        conn.rollback()

if __name__ == "__main__":
    main()
