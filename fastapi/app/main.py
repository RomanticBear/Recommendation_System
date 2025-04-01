# FastAPI 앱 실행 지점 (진입점)


from fastapi import FastAPI
from app.routers import recommendations


app = FastAPI(title="BookNest Recommendation API Server")

# 라우터 등록
app.include_router(recommendations.router)

print("앱")

# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run(app, host="0.0.0.0", port=8000)