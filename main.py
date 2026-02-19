import os
from datetime import datetime
from typing import Optional

import pandas as pd
from dateutil.relativedelta import relativedelta
from fastapi import FastAPI, Depends, HTTPException, Query
from sqlalchemy import and_
from sqlalchemy.orm import Session

from db import SessionLocal
from models import User, Transaction, TransactionStatus, TransactionType

app = FastAPI(
    title="Аналитика транзакций",
    description="API для анализа транзакций пользователей",
    version="1.1.0"
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
        "version": "1.1.0",
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


@app.get("/transactions")
def get_transactions(
        limit: int = 10,
        status: Optional[str] = None,
        transaction_type: Optional[str] = None,
        db: Session = Depends(get_db)
):
    """Получить список транзакций"""
    query = db.query(Transaction)

    if status and status != "all":
        try:
            status_enum = TransactionStatus[status]
            query = query.filter(Transaction.status == status_enum)
        except KeyError:
            raise HTTPException(status_code=400, detail=f"Неверный статус: {status}")

    if transaction_type and transaction_type != "all":
        try:
            type_enum = TransactionType[transaction_type]
            query = query.filter(Transaction.type == type_enum)
        except KeyError:
            raise HTTPException(status_code=400, detail=f"Неверный тип: {transaction_type}")

    transactions = query.order_by(Transaction.payment_date.desc()).limit(limit).all()

    return {
        "count": len(transactions),
        "transactions": [
            {
                "id": t.id,
                "user_id": t.user_id,
                "amount": t.amount,
                "status": t.status.value,
                "type": t.type.value,
                "description": t.description,
                "payment_date": t.payment_date.isoformat()
            }
            for t in transactions
        ]
    }


@app.get("/report")
def get_report(
        start_date: Optional[str] = Query(None, description="Start date"),
        end_date: Optional[str] = Query(None, description="End date"),
        status: str = Query("all", description="Filter by status: successful, failed, all"),
        type: str = Query("all", description="Filter by type: payment, invoice, all"),
        include_avg: bool = Query(True, description="Include average amount"),
        include_min: bool = Query(True, description="Include minimum amount"),
        include_max: bool = Query(True, description="Include maximum amount"),
        include_daily_shift: bool = Query(False, description="Include daily data with % change"),
        db: Session = Depends(get_db)
):
    """
    Возвращает аналитику по транзакциям

    Query параметры:
    - start_date: Начальная дата (YYYY-MM-DD), по умолчанию: месяц назад
    - end_date: Конечная дата (YYYY-MM-DD), по умолчанию: сегодня
    - status: Фильтр по статусу (successful, failed, all)
    - type: Фильтр по типу (payment, invoice, all)
    - include_avg: Включить среднее значение
    - include_min: Включить минимальное значение
    - include_max: Включить максимальное значение
    - include_daily_shift: Включить данные по дням с процентным изменением относительно пред. дня
    """

    # Парсинг дат
    if end_date:
        try:
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(status_code=400, detail="Неверный формат даты. Должен быть YYYY-MM-DD")
    else:
        end_dt = datetime.now()

    if start_date:
        try:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(status_code=400, detail="Неверный формат даты. Должен быть YYYY-MM-DD")
    else:
        start_dt = end_dt - relativedelta(months=1)  # Месяц назад

    # Проверка корректности дат
    if start_dt > end_dt:
        raise HTTPException(status_code=400, detail="start_date не может быть позже end_date")

    # Запрос с фильтрацией по датам
    query = db.query(Transaction).filter(
        and_(
            Transaction.payment_date >= start_dt,
            Transaction.payment_date <= end_dt
        )
    )

    # Фильтр по статусу
    if status != "all":
        try:
            status_enum = TransactionStatus[status]
            query = query.filter(Transaction.status == status_enum)
        except KeyError:
            raise HTTPException(status_code=400,
                                detail=f"Неверный статус: {status}. Должен быть: successful, failed, или all")

    # Фильтр по типу
    if type != "all":
        try:
            type_enum = TransactionType[type]
            query = query.filter(Transaction.type == type_enum)
        except KeyError:
            raise HTTPException(status_code=400, detail=f"Неверный тип: {type}. Должен быть: payment, invoice, или all")

    # Получаем все транзакции для расчетов
    transactions = query.all()
    total_count = len(transactions)

    if total_count == 0:
        return {
            "period": {
                "start_date": start_dt.strftime("%Y-%m-%d"),
                "end_date": end_dt.strftime("%Y-%m-%d"),
                "days": (end_dt - start_dt).days + 1
            },
            "filters": {
                "status": status,
                "type": type
            },
            "summary": {
                "total_transactions": 0,
                "message": "Не найдено транзакций с заданными фильтрами"
            },
            "metrics": {
                "total_amount": 0.0
            }
        }

    # Расчет метрик только для successful транзакций
    successful_transactions = [t for t in transactions if t.status == TransactionStatus.successful]
    successful_amounts = [t.amount for t in successful_transactions]

    # Основная статистика
    result = {
        "period": {
            "start_date": start_dt.strftime("%Y-%m-%d"),
            "end_date": end_dt.strftime("%Y-%m-%d"),
            "days": (end_dt - start_dt).days + 1
        },
        "filters": {
            "status": status,
            "type": type
        },
        "summary": {
            "total_transactions": total_count,
            "successful_transactions": len(successful_transactions),
            "failed_transactions": total_count - len(successful_transactions)
        }
    }

    # Метрики для successful транзакций
    if successful_amounts:
        metrics = {
            "total_amount": round(sum(successful_amounts), 2)
        }

        if include_avg:
            metrics["average_amount"] = round(sum(successful_amounts) / len(successful_amounts), 2)

        if include_min:
            metrics["minimum_amount"] = round(min(successful_amounts), 2)

        if include_max:
            metrics["maximum_amount"] = round(max(successful_amounts), 2)

        result["metrics"] = metrics
    else:
        result["metrics"] = {
            "message": "No successful transactions found",
            "total_amount": 0.0
        }

    # Daily shift - данные по дням с процентным изменением относительно предыдущего дня
    if include_daily_shift:
        # Группируем successful транзакции по дням
        daily_data = {}
        for t in successful_transactions:
            day = t.payment_date.date()
            if day not in daily_data:
                daily_data[day] = {
                    "date": day,
                    "transactions": [],
                    "amount": 0.0
                }
            daily_data[day]["transactions"].append(t)
            daily_data[day]["amount"] += t.amount

        # Сортируем по дате
        sorted_days = sorted(daily_data.items(), key=lambda x: x[0])

        daily_report = []
        previous_amount = None

        for day, data in sorted_days:
            day_info = {
                "date": day.strftime("%Y-%m-%d"),
                "transaction_count": len(data["transactions"]),
                "total_amount": round(data["amount"], 2)
            }

            # Рассчитываем процентное изменение относительно предыдущего дня
            if previous_amount is not None and previous_amount > 0:
                change = ((data["amount"] - previous_amount) / previous_amount) * 100
                day_info["change_percent"] = round(change, 2)
                day_info["change_amount"] = round(data["amount"] - previous_amount, 2)
            else:
                day_info["change_percent"] = None
                day_info["change_amount"] = None

            previous_amount = data["amount"]
            daily_report.append(day_info)

        result["daily_shift"] = daily_report

    return result


@app.get("/report/by-country")
def get_report_by_country(
        sort_by: str = Query("total", description="Sort by: count, total, avg"),
        top_n: int = Query(10, description="Limit number of countries", ge=1, le=100),
        status: str = Query("successful", description="Filter by status: successful, failed, all"),
        db: Session = Depends(get_db)
):
    """
    Возвращает агрегированную статистику по странам

    Параметры:
    - sort_by: Сортировка по метрике (count, total, avg)
    - top_n: Количество стран в ответе (1-100)
    - status: Фильтр по статусу (successful, failed, all)
    """

    csv_path = os.path.join(os.path.dirname(__file__), "user-country (Test task).csv")

    # Проверка CSV файла
    if not os.path.exists(csv_path):
        raise HTTPException(
            status_code=500,
            detail="CSV файл не найден. Убедитесь, что файл лежит в папке приложения."
        )

    # Валидация sort_by
    if sort_by not in ["count", "total", "avg"]:
        raise HTTPException(
            status_code=400,
            detail=f"Неверный тип сортировки: {sort_by}. Должен быть: count, total, или avg"
        )

    # Загрузка CSV с данными о странах
    try:
        countries_df = pd.read_csv(csv_path, delimiter=';')
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Ошибка при чтении CSV файла: {str(e)}"
        )

    # Проверка наличия необходимых колонок
    if 'user_id' not in countries_df.columns or 'country' not in countries_df.columns:
        raise HTTPException(
            status_code=500,
            detail="CSV должен содержать колонки 'user_id' и 'country'"
        )

    # Получаем транзакции из БД
    query = db.query(Transaction)

    # Фильтр по статусу
    if status != "all":
        try:
            status_enum = TransactionStatus[status]
            query = query.filter(Transaction.status == status_enum)
        except KeyError:
            raise HTTPException(
                status_code=400,
                detail=f"Неверный статус: {status}. Должен быть: successful, failed, или all"
            )

    transactions = query.all()

    if not transactions:
        return {
            "filters": {
                "status": status,
                "sort_by": sort_by,
                "top_n": top_n
            },
            "summary": {
                "total_countries": 0,
                "total_transactions": 0,
                "message": "Транзакций не найдено"
            },
            "countries": []
        }

    # Создаем DataFrame из транзакций
    transactions_data = [{
        "user_id": t.user_id,
        "amount": t.amount,
        "status": t.status.value,
        "type": t.type.value
    } for t in transactions]

    transactions_df = pd.DataFrame(transactions_data)

    # Объединяем транзакции с данными о странах
    # Left join чтобы не потерять транзакции пользователей без страны
    merged_df = pd.merge(
        transactions_df,
        countries_df,
        on='user_id',
        how='left'
    )

    # Заполняем отсутствующие страны
    merged_df['country'] = merged_df['country'].fillna('Unknown')

    # Агрегация по странам
    country_stats = merged_df.groupby('country').agg(
        transaction_count=('amount', 'count'),
        total_amount=('amount', 'sum'),
        average_amount=('amount', 'mean')
    ).reset_index()

    # Округляем суммы
    country_stats['total_amount'] = country_stats['total_amount'].round(2)
    country_stats['average_amount'] = country_stats['average_amount'].round(2)

    # Сортировка
    sort_column_map = {
        "count": "transaction_count",
        "total": "total_amount",
        "avg": "average_amount"
    }

    country_stats = country_stats.sort_values(
        by=sort_column_map[sort_by],
        ascending=False
    )

    # Ограничение количества стран
    top_countries = country_stats.head(top_n)

    # Формирование ответа
    countries_list = []
    for _, row in top_countries.iterrows():
        countries_list.append({
            "country": row['country'],
            "transaction_count": int(row['transaction_count']),
            "total_amount": float(row['total_amount']),
            "average_amount": float(row['average_amount'])
        })

    # Общая статистика
    total_transactions = len(transactions)
    total_countries = len(country_stats)
    total_amount = country_stats['total_amount'].sum()

    # Количество пользователей без страны
    users_without_country = merged_df[merged_df['country'] == 'Unknown']['user_id'].nunique()

    return {
        "filters": {
            "status": status,
            "sort_by": sort_by,
            "top_n": top_n
        },
        "summary": {
            "total_countries": total_countries,
            "total_transactions": total_transactions,
            "total_amount": round(float(total_amount), 2),
            "showing_top": len(countries_list),
            "users_without_country": users_without_country
        },
        "countries": countries_list
    }
