# API 라우터 (엔드포인트 정의)

from fastapi import APIRouter, Depends
from app.dependencies import get_db
from app.internal import recommendations_logic as rec
from sqlalchemy import text


router = APIRouter(prefix="/recommend", tags=["Recommendations"])


@router.get("/today")
def recommend_today_book(db=Depends(get_db)):
    """
    오늘의 책 추천 API
    """
    try:
        # 간단한 쿼리 실행하여 DB 연결 상태 확인
        result = rec.get_today_recommendations(db)
        return {"db_status": "ok", "result": result}
    except Exception as e:
        return {"db_status": "error", "detail": str(e)}


@router.get("/library")
def recommend_library():
    """
    도서 대출 기반 추천 API
    """
    print("호출됨")
    return rec.get_loan_based_recommendations()


@router.get("/keyword")
def recommend_keyword():
    """
    키워드 추천 API
    """
    return rec.get_keyword_recommendations()

@router.get("/bestseller")
def recommend_bestseller():
    """
    베스트셀러 추천 API
    """
    return rec.get_bestseller_recommendations()

@router.get("/local")
def recommend_local():
    """
    내 지역에서 많이 본 책 추천 API
    """
    return rec.get_local_recommendations()

@router.get("/age-gender")
def recommend_age_gender():
    """
    내 연령대 성별이 많이 본 책 추천 API
    """
    return rec.get_age_gender_recommendations()

@router.get("/trending-author")
def recommend_trending_author():
    """
    화제의 작가 추천 API
    """
    return rec.get_trending_author()

@router.get("/critic")
def recommend_critic():
    """
    평론가의 추천 API
    """
    return rec.get_critic_recommendations()

@router.get("/recent-author")
def recommend_rated_author_related():
    """
    최근 평점 준 작가와 연관된 책 추천 API
    """
    return rec.get_rated_author_related_recommendations()

@router.get("/recent-keyword")
def recommend_rated_keyword_related():
    """
    최근 평점 준 키워드와 연관된 책 추천 API
    """
    return rec.get_rated_keyword_related_recommendations()

@router.get("/user-keyword")
def recommend_user_keyword(user_id: int, db=Depends(get_db)):
    """
    내가 많이 본 키워드 기반 추천 API
    """
    return rec.get_user_keyword_recommendations(user_id, db)


@router.get("/user-category")
def recommend_user_category(user_id: int, db=Depends(get_db)):
    """
    내가 많이 본 카테고리 기반 추천 API
    """
    return rec.get_user_category_recommendations(user_id, db)

@router.get("/architype")
def recommend_architype():
    """
    내 아키타입을 위한 책 추천 API
    """
    return rec.get_architype_recommendations()
