# 의존성 관리 (예: DB 세션 등)

from app.config import SessionLocal

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()