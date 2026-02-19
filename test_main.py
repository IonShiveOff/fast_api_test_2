"""
Unit-тесты для FastAPI приложения аналитики транзакций
"""
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta
import os
import tempfile

from main import app, get_db
from models import Base, User, Transaction, TransactionStatus, TransactionType

# Настройка тестовой БД в памяти
SQLALCHEMY_DATABASE_URL = "sqlite:///./test.db"

engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    connect_args={"check_same_thread": False}
)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


# Фикстура для переопределения зависимости БД
def override_get_db():
    try:
        db = TestingSessionLocal()
        yield db
    finally:
        db.close()


app.dependency_overrides[get_db] = override_get_db

# Создаем тестовый клиент
client = TestClient(app)


@pytest.fixture(scope="function")
def setup_database():
    """Создает таблицы перед каждым тестом и удаляет после"""
    Base.metadata.create_all(bind=engine)
    yield
    Base.metadata.drop_all(bind=engine)


@pytest.fixture
def db_session():
    """Предоставляет сессию БД для тестов"""
    db = TestingSessionLocal()
    try:
        yield db
    finally:
        db.close()


@pytest.fixture
def sample_users(db_session):
    """Создает тестовых пользователей"""
    users = [
        User(
            first_name="John",
            last_name="Doe",
            email="john@example.com",
            is_active=True,
            registration_date=datetime(2023, 1, 1)
        ),
        User(
            first_name="Jane",
            last_name="Smith",
            email="jane@example.com",
            is_active=False,
            registration_date=datetime(2023, 2, 1)
        ),
        User(
            first_name="Bob",
            last_name="Johnson",
            email="bob@example.com",
            is_active=True,
            registration_date=datetime(2023, 3, 1)
        )
    ]
    db_session.add_all(users)
    db_session.commit()
    for user in users:
        db_session.refresh(user)
    return users


@pytest.fixture
def sample_transactions(db_session, sample_users):
    """Создает тестовые транзакции"""
    transactions = [
        Transaction(
            user_id=sample_users[0].id,
            amount=100.50,
            status=TransactionStatus.successful,
            type=TransactionType.payment,
            description="Test payment 1",
            payment_date=datetime(2024, 1, 15, 10, 0)
        ),
        Transaction(
            user_id=sample_users[0].id,
            amount=200.75,
            status=TransactionStatus.successful,
            type=TransactionType.invoice,
            description="Test invoice 1",
            payment_date=datetime(2024, 1, 16, 11, 0)
        ),
        Transaction(
            user_id=sample_users[1].id,
            amount=50.25,
            status=TransactionStatus.failed,
            type=TransactionType.payment,
            description="Test payment 2",
            payment_date=datetime(2024, 1, 17, 12, 0)
        ),
        Transaction(
            user_id=sample_users[2].id,
            amount=300.00,
            status=TransactionStatus.successful,
            type=TransactionType.payment,
            description="Test payment 3",
            payment_date=datetime(2024, 2, 1, 13, 0)
        ),
        Transaction(
            user_id=sample_users[2].id,
            amount=150.50,
            status=TransactionStatus.failed,
            type=TransactionType.invoice,
            description="Test invoice 2",
            payment_date=datetime(2024, 2, 2, 14, 0)
        )
    ]
    db_session.add_all(transactions)
    db_session.commit()
    return transactions


@pytest.fixture
def csv_file():
    """Создает временный CSV файл с данными о странах"""
    csv_content = """user_id;country
                    1;United States
                    2;United Kingdom
                    3;Germany"""

    # Создаем временный файл
    fd, path = tempfile.mkstemp(suffix='.csv', text=True)
    with os.fdopen(fd, 'w') as f:
        f.write(csv_content)

    yield path

    # Удаляем файл после теста
    if os.path.exists(path):
        os.remove(path)


# ============================================================================
# ТЕСТЫ ДЛЯ ЭНДПОИНТА /
# ============================================================================

class TestRootEndpoint:
    """Тесты для корневого эндпоинта"""

    def test_root_endpoint(self, setup_database):
        """Тест GET / - должен вернуть информацию об API"""
        response = client.get("/")
        assert response.status_code == 200
        data = response.json()
        assert data["message"] == "API Аналитика транзакций"
        assert data["version"] == "1.1.0"
        assert "endpoints" in data
        assert "/users" in str(data["endpoints"])
        assert "/transactions" in str(data["endpoints"])
        assert "/report" in str(data["endpoints"])


# ============================================================================
# ТЕСТЫ ДЛЯ ЭНДПОИНТА /users
# ============================================================================

class TestUsersEndpoint:
    """Тесты для эндпоинта /users"""

    def test_get_users_default(self, setup_database, sample_users):
        """Тест GET /users - получение пользователей с параметрами по умолчанию"""
        response = client.get("/users")
        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 3
        assert len(data["users"]) == 3

    def test_get_users_with_limit(self, setup_database, sample_users):
        """Тест GET /users с limit - ограничение количества пользователей"""
        response = client.get("/users?limit=2")
        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 2
        assert len(data["users"]) == 2

    def test_get_users_active_only(self, setup_database, sample_users):
        """Тест GET /users с active_only=true - только активные пользователи"""
        response = client.get("/users?active_only=true")
        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 2  # Только John и Bob
        for user in data["users"]:
            assert user["is_active"] is True

    def test_get_users_response_structure(self, setup_database, sample_users):
        """Тест структуры ответа /users"""
        response = client.get("/users")
        data = response.json()
        user = data["users"][0]
        assert "id" in user
        assert "first_name" in user
        assert "last_name" in user
        assert "email" in user
        assert "is_active" in user
        assert "registration_date" in user

    def test_get_users_empty_database(self, setup_database):
        """Тест GET /users когда БД пуста"""
        response = client.get("/users")
        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 0
        assert len(data["users"]) == 0


# ============================================================================
# ТЕСТЫ ДЛЯ ЭНДПОИНТА /transactions
# ============================================================================

class TestTransactionsEndpoint:
    """Тесты для эндпоинта /transactions"""

    def test_get_transactions_default(self, setup_database, sample_transactions):
        """Тест GET /transactions - получение транзакций по умолчанию"""
        response = client.get("/transactions")
        assert response.status_code == 200
        data = response.json()
        assert data["count"] <= 10  # По умолчанию limit=10
        assert len(data["transactions"]) == 5

    def test_get_transactions_with_limit(self, setup_database, sample_transactions):
        """Тест GET /transactions с limit"""
        response = client.get("/transactions?limit=3")
        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 3

    def test_get_transactions_filter_by_status_successful(self, setup_database, sample_transactions):
        """Тест фильтрации по статусу successful"""
        response = client.get("/transactions?status=successful")
        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 3
        for transaction in data["transactions"]:
            assert transaction["status"] == "successful"

    def test_get_transactions_filter_by_status_failed(self, setup_database, sample_transactions):
        """Тест фильтрации по статусу failed"""
        response = client.get("/transactions?status=failed")
        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 2
        for transaction in data["transactions"]:
            assert transaction["status"] == "failed"

    def test_get_transactions_filter_by_type_payment(self, setup_database, sample_transactions):
        """Тест фильтрации по типу payment"""
        response = client.get("/transactions?transaction_type=payment")
        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 3
        for transaction in data["transactions"]:
            assert transaction["type"] == "payment"

    def test_get_transactions_filter_by_type_invoice(self, setup_database, sample_transactions):
        """Тест фильтрации по типу invoice"""
        response = client.get("/transactions?transaction_type=invoice")
        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 2
        for transaction in data["transactions"]:
            assert transaction["type"] == "invoice"

    def test_get_transactions_combined_filters(self, setup_database, sample_transactions):
        """Тест комбинированных фильтров (статус + тип)"""
        response = client.get("/transactions?status=successful&transaction_type=payment")
        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 2
        for transaction in data["transactions"]:
            assert transaction["status"] == "successful"
            assert transaction["type"] == "payment"

    def test_get_transactions_invalid_status(self, setup_database, sample_transactions):
        """Тест с неверным статусом - должна вернуться ошибка 400"""
        response = client.get("/transactions?status=invalid")
        assert response.status_code == 400
        assert "Неверный статус" in response.json()["detail"]

    def test_get_transactions_invalid_type(self, setup_database, sample_transactions):
        """Тест с неверным типом - должна вернуться ошибка 400"""
        response = client.get("/transactions?transaction_type=invalid")
        assert response.status_code == 400
        assert "Неверный тип" in response.json()["detail"]

    def test_get_transactions_response_structure(self, setup_database, sample_transactions):
        """Тест структуры ответа /transactions"""
        response = client.get("/transactions")
        data = response.json()
        transaction = data["transactions"][0]
        assert "id" in transaction
        assert "user_id" in transaction
        assert "amount" in transaction
        assert "status" in transaction
        assert "type" in transaction
        assert "description" in transaction
        assert "payment_date" in transaction

    def test_get_transactions_sorted_by_date(self, setup_database, sample_transactions):
        """Тест сортировки по дате (должны быть от новых к старым)"""
        response = client.get("/transactions")
        data = response.json()
        transactions = data["transactions"]
        dates = [t["payment_date"] for t in transactions]
        assert dates == sorted(dates, reverse=True)


# ============================================================================
# ТЕСТЫ ДЛЯ ЭНДПОИНТА /report
# ============================================================================

class TestReportEndpoint:
    """Тесты для эндпоинта /report"""

    def test_get_report_default_parameters(self, setup_database, sample_transactions):
        """Тест GET /report с параметрами по умолчанию"""
        response = client.get("/report")
        assert response.status_code == 200
        data = response.json()
        assert "period" in data
        assert "filters" in data
        assert "summary" in data
        assert "metrics" in data

    def test_get_report_with_date_range(self, setup_database, sample_transactions):
        """Тест /report с указанным диапазоном дат"""
        response = client.get("/report?start_date=2024-01-01&end_date=2024-01-31")
        assert response.status_code == 200
        data = response.json()
        assert data["period"]["start_date"] == "2024-01-01"
        assert data["period"]["end_date"] == "2024-01-31"
        # Должны быть только 3 транзакции из января
        assert data["summary"]["total_transactions"] == 3

    def test_get_report_invalid_date_format(self, setup_database, sample_transactions):
        """Тест с неверным форматом даты"""
        response = client.get("/report?start_date=2024/01/01")
        assert response.status_code == 400
        assert "Неверный формат даты" in response.json()["detail"]

    def test_get_report_start_date_after_end_date(self, setup_database, sample_transactions):
        """Тест когда start_date позже end_date"""
        response = client.get("/report?start_date=2024-12-31&end_date=2024-01-01")
        assert response.status_code == 400
        assert "не может быть позже" in response.json()["detail"]

    def test_get_report_filter_by_status_successful(self, setup_database, sample_transactions):
        """Тест фильтрации по статусу successful"""
        response = client.get("/report?status=successful&start_date=2024-01-01&end_date=2024-12-31")
        assert response.status_code == 200
        data = response.json()
        assert data["filters"]["status"] == "successful"
        assert data["summary"]["successful_transactions"] == 3
        assert data["summary"]["failed_transactions"] == 0

    def test_get_report_filter_by_type_payment(self, setup_database, sample_transactions):
        """Тест фильтрации по типу payment"""
        response = client.get("/report?type=payment&start_date=2024-01-01&end_date=2024-12-31")
        assert response.status_code == 200
        data = response.json()
        assert data["filters"]["type"] == "payment"

    def test_get_report_metrics_calculation(self, setup_database, sample_transactions):
        """Тест правильности расчета метрик"""
        response = client.get("/report?start_date=2024-01-01&end_date=2024-01-31")
        assert response.status_code == 200
        data = response.json()

        # Только successful транзакции: 100.50 + 200.75 = 301.25
        metrics = data["metrics"]
        assert metrics["total_amount"] == 301.25
        assert metrics["average_amount"] == round(301.25 / 2, 2)
        assert metrics["minimum_amount"] == 100.50
        assert metrics["maximum_amount"] == 200.75

    def test_get_report_include_avg_false(self, setup_database, sample_transactions):
        """Тест отключения average_amount"""
        response = client.get("/report?include_avg=false&start_date=2024-01-01&end_date=2024-12-31")
        assert response.status_code == 200
        data = response.json()
        assert "average_amount" not in data["metrics"]
        assert "total_amount" in data["metrics"]

    def test_get_report_include_min_false(self, setup_database, sample_transactions):
        """Тест отключения minimum_amount"""
        response = client.get("/report?include_min=false&start_date=2024-01-01&end_date=2024-12-31")
        assert response.status_code == 200
        data = response.json()
        assert "minimum_amount" not in data["metrics"]

    def test_get_report_include_max_false(self, setup_database, sample_transactions):
        """Тест отключения maximum_amount"""
        response = client.get("/report?include_max=false&start_date=2024-01-01&end_date=2024-12-31")
        assert response.status_code == 200
        data = response.json()
        assert "maximum_amount" not in data["metrics"]

    def test_get_report_no_transactions_in_period(self, setup_database, sample_transactions):
        """Тест когда в указанном периоде нет транзакций"""
        response = client.get("/report?start_date=2025-01-01&end_date=2025-12-31")
        assert response.status_code == 200
        data = response.json()
        assert data["summary"]["total_transactions"] == 0
        assert "message" in data["summary"]

    def test_get_report_with_daily_shift(self, setup_database, sample_transactions):
        """Тест включения daily_shift"""
        response = client.get("/report?include_daily_shift=true&start_date=2024-01-01&end_date=2024-01-31")
        assert response.status_code == 200
        data = response.json()
        assert "daily_shift" in data
        assert isinstance(data["daily_shift"], list)
        assert len(data["daily_shift"]) == 2  # 2 дня с successful транзакциями

    def test_get_report_daily_shift_structure(self, setup_database, sample_transactions):
        """Тест структуры daily_shift"""
        response = client.get("/report?include_daily_shift=true&start_date=2024-01-01&end_date=2024-01-31")
        assert response.status_code == 200
        data = response.json()
        day = data["daily_shift"][0]
        assert "date" in day
        assert "transaction_count" in day
        assert "total_amount" in day
        assert "change_percent" in day or day["change_percent"] is None
        assert "change_amount" in day or day["change_amount"] is None

    def test_get_report_daily_shift_first_day_no_change(self, setup_database, sample_transactions):
        """Тест что первый день в daily_shift не имеет процентного изменения"""
        response = client.get("/report?include_daily_shift=true&start_date=2024-01-01&end_date=2024-01-31")
        assert response.status_code == 200
        data = response.json()
        first_day = data["daily_shift"][0]
        assert first_day["change_percent"] is None
        assert first_day["change_amount"] is None

    def test_get_report_invalid_status(self, setup_database, sample_transactions):
        """Тест с неверным статусом"""
        response = client.get("/report?status=invalid")
        assert response.status_code == 400
        assert "Неверный статус" in response.json()["detail"]

    def test_get_report_invalid_type(self, setup_database, sample_transactions):
        """Тест с неверным типом"""
        response = client.get("/report?type=invalid")
        assert response.status_code == 400
        assert "Неверный тип" in response.json()["detail"]

    def test_get_report_only_failed_transactions(self, setup_database, sample_transactions):
        """Тест когда есть только failed транзакции"""
        response = client.get("/report?status=failed&start_date=2024-01-01&end_date=2024-12-31")
        assert response.status_code == 200
        data = response.json()
        assert data["summary"]["successful_transactions"] == 0
        assert data["metrics"]["total_amount"] == 0.0
        assert "No successful transactions" in data["metrics"]["message"]


# ============================================================================
# ТЕСТЫ ДЛЯ ЭНДПОИНТА /report/by-country
# ============================================================================

class TestReportByCountryEndpoint:
    """Тесты для эндпоинта /report/by-country"""

    def test_get_report_by_country_csv_found(self, setup_database, sample_transactions):
        """Тест когда CSV файл найден"""
        response = client.get("/report/by-country")
        assert response.status_code == 200

    def test_get_report_by_country_invalid_sort_by(self, setup_database, sample_transactions):
        """Тест с неверным параметром sort_by"""
        response = client.get("/report/by-country?sort_by=invalid")
        assert response.status_code == 400
        assert "Неверный тип сортировки" in response.json()["detail"]

    def test_get_report_by_country_invalid_status(self, setup_database, sample_transactions):
        """Тест с неверным статусом"""
        response = client.get("/report/by-country?status=invalid")
        assert response.status_code == 400
        assert "Неверный статус" in response.json()["detail"]

    def test_get_report_by_country_top_n_validation(self, setup_database, sample_transactions):
        """Тест валидации параметра top_n (должен быть 1-100)"""
        # Слишком маленькое значение
        response = client.get("/report/by-country?top_n=0")
        assert response.status_code == 422  # Validation error

        # Слишком большое значение
        response = client.get("/report/by-country?top_n=101")
        assert response.status_code == 422


# ============================================================================
# ИНТЕГРАЦИОННЫЕ ТЕСТЫ
# ============================================================================

class TestIntegration:
    """Интеграционные тесты для проверки работы всех компонентов вместе"""

    def test_full_workflow(self, setup_database, sample_users, sample_transactions):
        """Тест полного рабочего процесса: получение пользователей, транзакций и отчета"""
        # 1. Получаем пользователей
        users_response = client.get("/users")
        assert users_response.status_code == 200
        assert users_response.json()["count"] == 3

        # 2. Получаем транзакции
        transactions_response = client.get("/transactions")
        assert transactions_response.status_code == 200
        assert transactions_response.json()["count"] == 5

        # 3. Получаем отчет
        report_response = client.get("/report?start_date=2024-01-01&end_date=2024-12-31")
        assert report_response.status_code == 200
        report_data = report_response.json()
        assert report_data["summary"]["total_transactions"] == 5

    def test_api_consistency(self, setup_database, sample_transactions):
        """Тест согласованности данных между эндпоинтами"""
        # Получаем все транзакции
        all_transactions = client.get("/transactions?limit=100").json()
        total_transactions = all_transactions["count"]

        # Получаем отчет по всем транзакциям
        report = client.get("/report?start_date=2024-01-01&end_date=2024-12-31").json()

        # Количество транзакций должно совпадать
        assert report["summary"]["total_transactions"] == total_transactions


# ============================================================================
# ТЕСТЫ ПРОИЗВОДИТЕЛЬНОСТИ (ОПЦИОНАЛЬНО)
# ============================================================================

class TestPerformance:
    """Тесты производительности"""

    def test_large_dataset_performance(self, setup_database, db_session, sample_users):
        """Тест производительности с большим количеством транзакций"""
        # Создаем 1000 транзакций
        transactions = []
        for i in range(1000):
            transactions.append(Transaction(
                user_id=sample_users[0].id,
                amount=100.0 + i,
                status=TransactionStatus.successful if i % 2 == 0 else TransactionStatus.failed,
                type=TransactionType.payment if i % 3 == 0 else TransactionType.invoice,
                description=f"Test transaction {i}",
                payment_date=datetime(2024, 1, 1) + timedelta(days=i % 365)
            ))

        db_session.bulk_save_objects(transactions)
        db_session.commit()

        # Запрос отчета не должен занять слишком много времени
        import time
        start = time.time()
        response = client.get("/report?start_date=2024-01-01&end_date=2024-12-31")
        end = time.time()

        assert response.status_code == 200
        assert end - start < 5  # Должно выполниться менее чем за 5 секунд


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])