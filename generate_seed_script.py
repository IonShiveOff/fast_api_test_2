import random
import traceback

from faker import Faker
from sqlalchemy.orm import Session

from db import Base, engine, SessionLocal
from models import User, Transaction, TransactionStatus, TransactionType

fake = Faker()

NUM_USERS = 113
NUM_TRANSACTIONS = 10013

FIRST_NAMES = [fake.unique.first_name() for _ in range(NUM_USERS)]
LAST_NAMES = [fake.unique.last_name() for _ in range(NUM_USERS)]
EMAILS = [fake.unique.email() for _ in range(NUM_USERS)]


def generate_users(session: Session) -> list:
    """Генерирует данные пользователей"""
    users = []

    # Распределение статусов учетных записей пользователей (80% активных)
    status_weights = [0.80, 0.20]
    statuses = [True, False]

    for i in range(NUM_USERS):
        user = User(
            first_name=FIRST_NAMES[i],
            last_name=LAST_NAMES[i],
            email=EMAILS[i],
            registration_date=fake.date_time_between(start_date='-2y', end_date='now'),
            is_active=random.choices(statuses, weights=status_weights)[0]
        )
        users.append(user)
        print(f'{i+1} out of {NUM_USERS} users')
    # Сортировка по дате
    users.sort(key=lambda x: x.registration_date)

    session.add_all(users)
    session.commit()
    return users


def generate_transactions(session: Session, users: list) -> list:
    """Генерирует данные транзакций"""
    transactions = []

    # Распределение статусов транзакций (85% успешных, 15% нет)
    status_weights = [0.85, 0.15]
    statuses = [TransactionStatus.successful, TransactionStatus.failed]

    # Распределение типов транзакций (50/50)
    type_weights = [0.50, 0.50]
    types = [TransactionType.payment, TransactionType.invoice]

    for i in range(NUM_TRANSACTIONS):
        # Выбор рандомного пользователя
        user = random.choice(users)

        # Выбор статуса и типа с учетом весов
        status = random.choices(statuses, weights=status_weights)[0]
        transaction_type = random.choices(types, weights=type_weights)[0]

        transaction = Transaction(
            payment_date=fake.date_time_between(start_date=user.registration_date, end_date='now'),
            amount=round(random.uniform(1, 1000), 2),
            status=status,
            type=transaction_type,
            description=f'{transaction_type.value.capitalize()} #{i + 1}',
            user_id=user.id,
        )
        transactions.append(transaction)
        print(f'{i + 1} out of {NUM_TRANSACTIONS} transactions')

    # Сортировка по дате
    transactions.sort(key=lambda x: x.payment_date)

    session.add_all(transactions)
    session.commit()
    return transactions


def generate_seed():
    session = SessionLocal()
    try:
        # Создаем таблицы если их нет
        Base.metadata.create_all(bind=engine)

        existing_users = session.query(User).count()
        if existing_users == 0:
            users = generate_users(session)
            generate_transactions(session, users)
    except Exception as e:
        print(f"Ошибка: {e}")
        traceback.print_exc()
        raise
    finally:
        session.close()


if __name__ == "__main__":
    generate_seed()
