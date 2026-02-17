from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session

from db import SessionLocal
from models import User

app = FastAPI(
    title="Аналитика транзакций",
    description="API для анализа транзакций пользователей",
    version="1.0.0"
)


# Зависимость для получения сессии БД
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.get("/")
def root():
    return {
        "message": "API Аналитика транзакций",
        "version": "1.0.0",
        "endpoints": {
            "users": "/users",
            "transactions": "/transactions",
            "report": "/report",
            "report/by-country": "/report/by-country",
        }
    }


@app.get("/users")
def get_users(
        limit: int = 10,
        active_only: bool = False,
        db: Session = Depends(get_db)
):
    """Получить список пользователей"""
    query = db.query(User)

    if active_only:
        query = query.filter(User.is_active == True)

    users = query.limit(limit).all()

    return {
        "count": len(users),
        "users": [
            {
                "id": user.id,
                "first_name": user.first_name,
                "last_name": user.last_name,
                "email": user.email,
                "is_active": user.is_active,
                "registration_date": user.registration_date.isoformat()
            }
            for user in users
        ]
    }
