# -*- coding: utf-8 -*-

import os
import time
import pandas as pd
from queue import Queue
from threading import Thread, Lock
from dotenv import load_dotenv
from groq import Groq

start_time = time.time()

# API 키 로딩
load_dotenv()
api_keys = [
    os.getenv("API_KEY_1"),
    os.getenv("API_KEY_2"),
    os.getenv("API_KEY_3"),
    os.getenv("API_KEY_4"),
]

clients = [Groq(api_key=k) for k in api_keys]

model_list = [
    "deepseek-r1-distill-llama-70b",
    "deepseek-r1-distill-qwen-32b",
    "gemma2-9b-it",
    "llama-3.1-8b-instant",
    "llama-3.2-11b-vision-preview",
    "llama-3.2-1b-preview",
    "llama-3.2-3b-preview",
    "llama-3.2-90b-vision-preview",
    "llama-3.3-70b-specdec",
    "llama-3.3-70b-versatile",
    "llama3-70b-8192",
    "llama3-8b-8192",
    "mistral-saba-24b",
    "qwen-2.5-32b",
    "qwen-2.5-coder-32b",
    "qwen-qwq-32b"
]

# 태그 목록
raw_tag_list = """판타지
SF
미스터리
스릴러
공포
로맨스
역사소설
모험
코미디
드라마
감동적인
잔잔한
긴장감 있는
우울한
유머러스한
어두운
밝고 긍정적
철학적
역동적인
몽환적인
역설적인
잔혹한
황홀한
불안감을 조성하는
사회 비판적
불평등
젠더 문제
전쟁과 평화
운명과 자유의지
도덕적 딜레마
혁명과 저항
환경과 생태
의료 및 바이오테크
외교와 정치 음모
권력과 부패
노동과 계급투쟁
이민과 정체성
문화 충돌
무정부주의
전통과 혁신
인공지능과 정보화
성장 이야기
트라우마와 치유
우정과 동료애
종교적·영적 요소
복수와 정의
정체성 탐색
내면 탐구
기억과 시간
이중성
금기와 반항
생명과 죽음
욕망과 타락
재난·서바이벌
의식의 흐름
미완의 이야기
실험적 문체
형식 파괴적
독특한 서술 방식
디스토피아
유토피아
포스트 아포칼립스
사이버펑크
다중 우주
시간 여행
초현실적 공간
미지의 영역 탐험
이세계
신화적 세계
가상 현실
기후 변화 세계"""

tag_candidates = [t.strip() for t in raw_tag_list.strip().splitlines() if t.strip()]

def generate_prompt(review_text):
    prompt = """
    당신은 문학 서평 전문가입니다. 아래 서평을 읽고, 제공된 태그 목록 중 이 책에 어울리는 태그를 3개에서 5개까지 골라주세요. 무조건 골라야합니다.

    조건:
    - 설명 없이 태그만 한 줄로 출력
    - 쉼표(,)로 구분된 태그를 출력
    - 태그는 반드시 목록 중에서 선택
    - 반드시 3개 이상 출력할 것
    """
    return f"{prompt}\n\n서평:\n{review_text}\n\n태그 목록:\n{raw_tag_list}"

def extract_valid_tags(text):
    raw_tags = [tag.strip() for tag in text.split(",") if tag.strip()]
    return [tag for tag in raw_tags if tag in tag_candidates]

results = {}
token_cooldown = {}
all_combos = [(m, i) for m in model_list for i in range(len(clients))]
combo_index = 0
combo_lock = Lock()
processed_isbns_lock = Lock()
task_queue = Queue()

tagged_file = "revise_book_test_tagged.csv"
if os.path.exists(tagged_file):
    existing_df = pd.read_csv(tagged_file)
    processed_isbns = set(existing_df["isbn"].dropna().astype(str).tolist())
else:
    existing_df = pd.DataFrame(columns=["isbn", "AI_태그"])
    processed_isbns = set()

max_retries = 5

def model_worker():
    global combo_index
    while True:
        item = task_queue.get()
        if item is None:
            break

        idx, isbn, title, review_text, retry_count = item

        with processed_isbns_lock:
            if isbn in processed_isbns:
                task_queue.task_done()
                continue

        if len(review_text.strip()) <= 100:
            tag_str = ""
            print(f"ℹ️ [{idx+1}] {title} → 리뷰 짧음, 태그 생략")
            new_row = pd.DataFrame([{"isbn": isbn, "AI_태그": tag_str}])
            write_header = not os.path.exists(tagged_file) or os.path.getsize(tagged_file) == 0
            new_row.to_csv(tagged_file, mode="a", header=write_header, index=False, encoding="utf-8-sig")
            with processed_isbns_lock:
                processed_isbns.add(isbn)
            print(f"📌 저장된 ISBN: {isbn} / 태그: (생략)")
            task_queue.task_done()
            continue

        with combo_lock:
            combo = all_combos[combo_index % len(all_combos)]
            combo_index += 1

        model, client_idx = combo
        client = clients[client_idx]
        cooldown_key = (model, client_idx)

        now = time.time()
        last_used = token_cooldown.get(cooldown_key, 0)
        wait_time = max(0, 4 - (now - last_used))
        if wait_time > 0:
            time.sleep(wait_time)

        prompt = generate_prompt(review_text)
        try:
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": "문학 전문가로서 한국어로 답하세요."},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.2,
                max_completion_tokens=300,
                top_p=1,
                stream=False,
                timeout=10
            )
            token_cooldown[cooldown_key] = time.time()
            content = response.choices[0].message.content.strip()
            tags = extract_valid_tags(content)

            if len(tags) < 3:
                print(f"⚠️ [{idx+1}] {title} → 태그 부족 → 재시도 {retry_count+1}/{max_retries}")
                if retry_count + 1 < max_retries:
                    task_queue.put((idx, isbn, title, review_text, retry_count+1))
                else:
                    tag_str = "오류"
                    new_row = pd.DataFrame([{"isbn": isbn, "AI_태그": tag_str}])
                    write_header = not os.path.exists(tagged_file) or os.path.getsize(tagged_file) == 0
                    new_row.to_csv(tagged_file, mode="a", header=write_header, index=False, encoding="utf-8-sig")
                    with processed_isbns_lock:
                        processed_isbns.add(isbn)
                    print(f"📌 저장된 ISBN: {isbn} / 태그: 오류")
            else:
                tag_str = ", ".join(tags)
                print(f"✅ [{idx+1}] {title} → {tags} ({model}, API_KEY_{client_idx+1})")
                new_row = pd.DataFrame([{"isbn": isbn, "AI_태그": tag_str}])
                write_header = not os.path.exists(tagged_file) or os.path.getsize(tagged_file) == 0
                new_row.to_csv(tagged_file, mode="a", header=write_header, index=False, encoding="utf-8-sig")
                with processed_isbns_lock:
                    processed_isbns.add(isbn)
                print(f"📌 저장된 ISBN: {isbn} / 태그: {tag_str}")

        except Exception as e:
            print(f"❌ [{idx+1}] {title} 에러: {e} (API_KEY_{client_idx+1})")
            if retry_count + 1 < max_retries:
                task_queue.put((idx, isbn, title, review_text, retry_count+1))

        task_queue.task_done()

# CSV 로드 및 컬럼 소문자화
df = pd.read_csv("yes24_steadyseller.csv", on_bad_lines='skip', encoding="utf-8")
df.columns = [col.lower() for col in df.columns]

# 태스크 큐에 작업 넣기
for idx, row in df.iterrows():
    isbn = str(row.get("isbn", "")).strip()
    if not isbn or isbn.lower() == "nan" or isbn in processed_isbns:
        continue
    title = row.get("title", "")
    review = str(row.get("publisher_review", "")).strip()
    task_queue.put((idx, isbn, title, review, 0))

# 쓰레드 실행
threads = []
num_threads = len(model_list) * len(clients) // 2
for _ in range(num_threads):
    t = Thread(target=model_worker)
    t.start()
    threads.append(t)

# 작업 대기
task_queue.join()
for _ in threads:
    task_queue.put(None)
for t in threads:
    t.join()

end_time = time.time()
print(f"\n✅ 서평 태그 추출 완료 → '{tagged_file}'에 저장됨!")
print(f"⏱️ 전체 소요 시간: {end_time - start_time:.2f}초")


'''

문제 상황

# 문제 1 : 서평 데이터 없어도 기본 프롬프트로 태깅 진행

>> 해결 방법 서평 데이터 길이 10이하면 공백 저장하고 넘어감
>> 공백, N/A 등등 포괄적으로 처리 

# 문제 2 : 하나에 대해서 여러 스레드가 처리 Ex) 9788959062249 
processed_isbns는 실행 시작 시에만 한 번 만들어짐.
태그 추출 결과(book_test_tagged.csv)를 저장할 때, 중복 ISBN 체크를 하지 않음.
특히 재시도 로직이 있고, 태그가 부족하거나 오류가 발생하면 다시 task_queue에 넣는데, 이 과정에서 이전 결과와 함께 중복 저장될 수 있음.

# 처음 생각
1. 결과 저장 전 processed_isbns에 ISBN 추가
문제점 -> 실패한 ISBN도 '처리된 것'으로 간주 → 그래서 재시도도 안 되고, 결과도 없는 상태로 누락

2(최종종). 상황별로 processed_isbns.add(isbn)의 위치를 분리
-> 생략 케이스(서평 데이터x) + 태그가 3개 이상일 때 + 재시도 끝 "오류" 저장
'''