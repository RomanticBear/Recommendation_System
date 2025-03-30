import pandas as pd
import os

# 파일 경로 설정
input_file_1 = 'book_data_crawled/2020/NL_BO_BOOK_PUB_202012-1.csv'
input_file_2 = 'book_data_taged/2020/NL_BO_BOOK_PUB_202012-1_taged.csv'
output_dir = 'book_data_merged/2020'
os.makedirs(output_dir, exist_ok=True)
output_file = os.path.join(output_dir, 'NL_BO_BOOK_PUB_202012_merged.csv')

# CSV 파일 읽기
df_main = pd.read_csv(input_file_1)
df_tag = pd.read_csv(input_file_2)

# ISBN과 isbn 컬럼을 모두 문자열로 변환
df_main['ISBN'] = df_main['ISBN'].astype(str)
df_tag['ISBN'] = df_tag['ISBN'].astype(str)

# 병합 (왼쪽 기준 조인)
df_merged = pd.merge(df_main, df_tag, how='left', left_on='ISBN', right_on='ISBN')

# 태그가 없는 경우 공백으로 채움
df_merged['TAG'] = df_merged['TAG'].fillna('')

# 불필요한 isbn 컬럼 제거 (원본 ISBN 유지)
df_merged.drop(columns=['STATUS'], inplace=True)

# 저장
df_merged.to_csv(output_file, index=False, encoding='utf-8-sig')

print(f"병합 완료. 결과 파일 저장 위치: {output_file}")
