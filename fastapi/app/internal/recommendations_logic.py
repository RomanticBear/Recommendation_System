from sqlalchemy import text


def get_today_recommendations(db):
    """
    오늘의 책 추천 로직 구현 예시
    """
    # result = db.execute(text("SELECT * FROM user")).mappings().fetchone()
    # result_dict = dict(result) if result is not None else None
    result = db.execute(text("SELECT * FROM user")).mappings().fetchall()
    result_dict = [dict(row) for row in result]
    return [{"db_status": "ok", "result": result_dict}]


def get_library_recommendations():
    """
    도서 대출 기반 추천 로직 구현
    """
    pass

def get_keyword_recommendations():
    """
    키워드 추천 로직 구현
    """
    pass

def get_bestseller_recommendations():
    """
    베스트셀러 추천 로직 구현
    """
    pass

def get_local_recommendations():
    """
    내 지역에서 많이 본 책 추천 로직 구현
    """
    pass

def get_age_gender_recommendations():
    """
    내 연령대 성별이 많이 본 책 추천 로직 구현
    """
    pass

def get_trending_author():
    """
    화제의 작가 추천 로직 구현
    """
    pass

def get_critic_recommendations():
    """
    평론가의 추천 로직 구현
    """
    pass

def get_rated_author_related_recommendations():
    """
    최근 평점 준 작가와 연관된 책 추천 로직 구현
    """
    pass

def get_rated_keyword_related_recommendations():
    """
    최근 평점 준 키워드와 연관된 책 추천 로직 구현
    """
    pass

def get_user_keyword_recommendations(user_id: int, db):
    """
    내가 많이 본 키워드 상위 5개 반환
    """
    query = text("""
        SELECT t.name, COUNT(*) AS count
        FROM Nest n
        JOIN BookTag bt ON n.book_id = bt.book_id
        JOIN Tag t ON bt.tag_id = t.id
        WHERE n.user_id = :user_id
        GROUP BY t.name
        ORDER BY count DESC
        LIMIT 5
    """)
    result = db.execute(query, {"user_id": user_id}).mappings().fetchall()
    return [{"tag": row["name"], "count": row["count"]} for row in result]

def get_user_category_recommendations(user_id: int, db):
    """
    내가 많이 본 카테고리 상위 5개 반환
    """
    query = text("""
        SELECT c.name, COUNT(*) AS count
        FROM Nest n
        JOIN BookCategory bc ON n.book_id = bc.book_id
        JOIN Category c ON bc.category_id = c.id
        WHERE n.user_id = :user_id
        GROUP BY c.name
        ORDER BY count DESC
        LIMIT 5
    """)
    result = db.execute(query, {"user_id": user_id}).mappings().fetchall()
    return [{"category": row["name"], "count": row["count"]} for row in result]

def get_architype_recommendations():
    """
    내 아키타입을 위한 책 추천 로직 구현
    """
    pass
