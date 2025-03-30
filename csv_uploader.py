# -*- coding: utf-8 -*-
import mariadb
import pandas as pd
import numpy as np
import os

# ------------------ Config ------------------
DB_CONFIG = {
    "host": "43.203.172.46",
    "port": 3306,
    "user": "booknest",
    "password": "b203B203",
    "database": "booknest"
}
CSV_FILE = "book_data_merged/2020/NL_BO_BOOK_PUB_202012_merged.csv"
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
    param_names = [
        "title", "published_date", "isbn", "publisher",
        "pages", "image_url", "intro", "book_index", "publisher_review"
    ]

    try:
        params = (
            row["TITLE"] or "",
            row["PUBLISHED_DATE"],
            str(row["ISBN"]) if pd.notna(row["ISBN"]) else None,
            row["PUBLISHER"] or "",
            int(str(row["PAGE"]).replace(",", "")) if pd.notna(row["PAGE"]) else 0,
            row["BOOK_IMAGE"] or "",
            row["INTRO"] or "",
            row["CONTENTS"] or "",
            str(row["PUBLISHER_REVIEW"]) if pd.notna(row["PUBLISHER_REVIEW"]) else ""
        )

        cursor.execute("""
            INSERT INTO book (
                title, published_date, isbn, publisher,
                pages, image_url, intro, book_index,
                publisher_review, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())
        """, params)

        return cursor.lastrowid

    except Exception as e:
        print("❌ 에러 발생! 해당 row 정보 ↓↓↓")
        print("row dict:", row.to_dict())
        print("\n📦 파라미터 디버깅:")
        for name, val in zip(param_names, params if 'params' in locals() else []):
            val_str = val if val is not None else "None"
            print(f"  {name:<17}: {val_str!r} (len: {len(val_str) if isinstance(val_str, str) else 'n/a'})")
        raise

def process_many_to_many(cursor, book_id, items, table, bridge_table, cache_dict):
    for item in items:
        name = item.strip()
        if name:
            item_id = insert_and_get_id(cursor, table, name, cache_dict)
            cursor.execute(f"INSERT INTO {bridge_table} (book_id, {table}_id) VALUES (?, ?)", (book_id, item_id))

# ------------------ Main ------------------
def main():
    df = pd.read_csv(CSV_FILE, encoding=ENCODING)
    df = df.replace({np.nan: None})
    print(f"Processing {len(df)} rows")

    conn = None
    try:
        conn = connect_db()
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
        if conn:
            try:
                conn.rollback()
            except:
                print("⚠️ rollback 실패 (커넥션이 이미 닫혔을 수 있음)")

    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()