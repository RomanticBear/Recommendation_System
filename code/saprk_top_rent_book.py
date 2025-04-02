from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
import os

# 1. Spark 세션 생성
spark = SparkSession.builder \
    .appName("BookDataProcessing") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

# 2. 경로 설정
book_data_root = "raw_book_data"
rental_data_root = "raw_book_rental_data"

# 3. 소장 도서 데이터 로드
exclude_dirs = ['2019', 'steady_seller']
valid_book_dirs = [
    os.path.join(book_data_root, d)
    for d in os.listdir(book_data_root)
    if os.path.isdir(os.path.join(book_data_root, d)) and d not in exclude_dirs
]

book_df_list = []
for folder in valid_book_dirs:
    csv_path = os.path.join(folder, "*.csv")
    df = spark.read.option("header", "true").csv(csv_path)
    book_df_list.append(df)

book_df = book_df_list[0]
for df in book_df_list[1:]:
    book_df = book_df.unionByName(df)

print("✅ Book data loaded with columns:", book_df.columns)

# 4. 대여 도서 데이터 로드
valid_rental_dirs = [
    os.path.join(rental_data_root, d)
    for d in os.listdir(rental_data_root)
    if os.path.isdir(os.path.join(rental_data_root, d))
]

rental_df_list = []
for folder in valid_rental_dirs:
    csv_path = os.path.join(folder, "*.csv")
    df = spark.read.option("header", "true").csv(csv_path)

    if "SEQ_NO" in df.columns:
        rental_df_list.append(df)
    else:
        print(f"⚠️ Skipped: {csv_path} - SEQ_NO column not found")

if not rental_df_list:
    raise ValueError("❌ No valid rental data loaded. Check directory or file structure.")

rental_df = rental_df_list[0]
for df in rental_df_list[1:]:
    rental_df = rental_df.unionByName(df)

print("✅ Rental data loaded with columns:", rental_df.columns)

# 5. JOIN: CTRL_NO == BOOK_KEY_NO
joined_df = book_df.join(
    rental_df,
    book_df["CTRL_NO"] == rental_df["BOOK_KEY_NO"],
    how="inner"
)

# 6. ISBN_THIRTEEN_NO 기준 대여 수 집계
isbn_count_df = joined_df.groupBy("ISBN_THIRTEEN_NO") \
    .agg(count("*").alias("loan_count"))

# 7. 상위 2,000,000개 ISBN만 추출
top_isbn_df = isbn_count_df.orderBy(col("loan_count").desc()).limit(2000000)

# 8. 도서 정보 붙이기 (중복 제거 후 LEFT JOIN)
book_info_df = book_df.dropDuplicates(["ISBN_THIRTEEN_NO"])
book_with_count_df = top_isbn_df.join(
    book_info_df,
    on="ISBN_THIRTEEN_NO",
    how="left"
)

# 9. loan_count 기준 정렬 → 단일 파일로 저장
output_path = "output/top_rent_books_fullinfo_singlefile"
book_with_count_df \
    .orderBy(col("loan_count").desc()) \
    .coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(output_path)

print("✅ 완료! 도서 정보 + 대여 수가 포함된 단일 CSV 파일 생성 →", output_path)
