# 설정 (예: DB URL 등 환경변수)

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


# MariaDB 접속 URL 작성 (mariadbconnector 사용 예)
DB_USER = "booknest"
DB_PASSWORD = "b203B203"
DB_HOST = "43.203.172.46"
DB_PORT = 3306
DB_NAME = "booknest"
DB_URL = f"mariadb+mariadbconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# 연결 풀 설정 (예: pool_size=20, max_overflow=0)
engine = create_engine(DB_URL, pool_size=20, max_overflow=0)

# 데이터베이스 세션 생성기
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
