import pandas as pd
import numpy as np
from lightfm import LightFM
from lightfm.data import Dataset

# ================================
# 1. 사용자 로그 데이터 불러오기
# ================================
log_df = pd.read_csv("Test_user_logs.csv")

# 로그 행동별 가중치 설정
action_weights = {
    "review_rating_5": 1.0,
    "review_rating_4": 0.9,
    "review_rating_3": 0.0,
    "review_rating_2": -0.3,
    "review_rating_1": -0.5,
    "review_only": 0.0,
    "rating_5": 1.0,
    "rating_4": 0.8,
    "rating_3": 0.0,
    "rating_2": -0.3,
    "rating_1": -0.5,
    "like": 0.9,
    "add_to_library": 0.8,
    "wishlist": 0.65,
    "click_detail": 0.4,
    "tag_click": 0.4,
    "external_library": 0.7,
    "search_click_detail": 0.6,
    "today_book_click": 0.8,
}

# 문자열로 들어온 JSON-like 값을 파싱
def parse_action_value(val):
    try:
        return eval(val) if isinstance(val, str) else val
    except:
        return val

log_df["action_value"] = log_df["action_value"].apply(parse_action_value)

# 점수 계산 함수
def map_score(row):
    key = row["action_type"]
    if key == "review_rating":
        return action_weights.get(f"review_rating_{row['action_value']['rating']}", 0)
    elif key == "rating":
        return action_weights.get(f"rating_{row['action_value']}", 0)
    else:
        return action_weights.get(key, 0)

log_df["score"] = log_df.apply(map_score, axis=1)
log_df = log_df[log_df["score"] != 0]  # 중립 제거

# ================================
# 2. 도서 태그 데이터 불러오기
# ================================
book_df = pd.read_csv("Test_book_tags.csv")
book_df["tag_list"] = book_df["TAG"].apply(lambda x: [tag.strip() for tag in x.split(",")])

# ================================
# 3. Dataset 정의 및 등록
# ================================
dataset = Dataset()
dataset.fit(
    users=log_df["user_id"].unique(),
    items=book_df["ISBN"].unique(),
    item_features=set(tag for tags in book_df["tag_list"] for tag in tags)
)

# ================================
# 4. 상호작용 + 가중치 행렬 생성
# ================================
interactions_source = list(zip(log_df["user_id"], log_df["isbn"], log_df["score"]))
interactions, weights_matrix = dataset.build_interactions(interactions_source)

# ================================
# 5. 아이템 피처 구성
# ================================
item_features_source = [(row.ISBN, row.tag_list) for row in book_df.itertuples()]
item_features = dataset.build_item_features(item_features_source)

# ================================
# 6. 모델 학습
# ================================
model = LightFM(loss="logistic", no_components=32, learning_rate=0.05)  # WARP 대신 안정적인 logistic
model.fit(
    interactions,
    sample_weight=weights_matrix,
    item_features=item_features,
    epochs=10,
    num_threads=4,
    verbose=True
)

# ================================
# 7. 추천 함수 정의
# ================================
def recommend_books(model, user_id, user_mapping, item_mapping, item_features, top_n=5):
    user_index = user_mapping[user_id]
    n_items = len(item_mapping) # 추천할 후보 -> 모든 책, 총 개수 확인
    scores = model.predict(user_index, np.arange(n_items), item_features=item_features)
    top_items = np.argsort(-scores)[:top_n]
    reverse_item_mapping = {v: k for k, v in item_mapping.items()}
    return [reverse_item_mapping[i] for i in top_items]

# ================================
# 8. 추천 사용 예시
# ================================
user_mapping, item_mapping = dataset.mapping()[0], dataset.mapping()[2]
user_id = 'u1'  # 추천 대상 유저 ID
recommended_isbns = recommend_books(model, user_id, user_mapping, item_mapping, item_features)

print(f"📚 사용자 {user_id}에게 추천하는 책 ISBN:")
for isbn in recommended_isbns:
    print(" -", isbn)

# 도서 정보 출력
print("\n📘 도서 상세 정보:")
print(book_df[book_df["ISBN"].isin(recommended_isbns)][["ISBN", "TAG"]])
