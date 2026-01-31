import sqlite3
import datetime
import random
import logging
import os
from dataclasses import dataclass
from typing import Optional, Generator

# --- КОНФИГУРАЦИЯ ---
class Config:
    # Теперь используем реальный файл, а не память
    DB_NAME = "local_hr_data.db" 
    LOG_LEVEL = logging.INFO
    MOCK_TODAY = datetime.date(2023, 10, 25)
    EMPLOYEE_COUNT = 100

# --- ЛОГИРОВАНИЕ ---
logging.basicConfig(level=Config.LOG_LEVEL, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# --- СТРУКТУРЫ ---
@dataclass
class EmployeeDetails:
    full_name: str
    is_active: bool
    birth_date: datetime.date

# --- 1. MOCK ВНЕШНЕГО API (Слой данных) ---
class HRApiClient:
    """
    Теперь этот класс читает данные ИЗ ТОЙ ЖЕ БАЗЫ, эмулируя внешний сервис.
    В реальности он бы ходил по http, но для консистентности теста
    мы будем брать данные из локальной таблицы, которую сами же и наполнили.
    """
    def __init__(self, db_path):
        self.db_path = db_path

    def get_employee_info(self, ext_id: int) -> Optional[EmployeeDetails]:
        # Эмуляция: API на самом деле лезет в ту же базу
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT fio, is_active, dob FROM api_mock_data WHERE ext_id = ?", (ext_id,))
            row = cursor.fetchone()
            
            if not row:
                return None
            
            fio, active, dob_str = row
            y, m, d = map(int, dob_str.split('-'))
            
            return EmployeeDetails(
                full_name=fio,
                is_active=bool(active),
                birth_date=datetime.date(y, m, d)
            )

# --- 2. УПРАВЛЕНИЕ БАЗОЙ (SEEDING) ---
def init_and_seed_db(db_path: str, count: int):
    """Создает таблицы и наполняет их, ТОЛЬКО если они пусты"""
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        
        # 1. Создаем структуру (если нет)
        cursor.execute("CREATE TABLE IF NOT EXISTS departments (id INTEGER PRIMARY KEY, name TEXT)")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS employees (
                id INTEGER PRIMARY KEY, 
                ext_id INTEGER NOT NULL, 
                dept_id INTEGER
            )
        """)
        # Таблица для эмуляции API (хранит детали)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS api_mock_data (
                ext_id INTEGER PRIMARY KEY,
                fio TEXT,
                is_active BOOLEAN,
                dob TEXT
            )
        """)

        # 2. Проверяем, есть ли данные
        cursor.execute("SELECT count(*) FROM employees")
        if cursor.fetchone()[0] > 0:
            logger.info("♻️ Данные уже существуют. Пропускаем генерацию.")
            return

        # 3. Если пусто — ГЕНЕРИРУЕМ (Seeding)
        logger.info(f"🌱 База пуста. Генерируем {count} сотрудников...")
        
        # Отделы
        depts = [(1, "IT Отдел"), (2, "Бухгалтерия"), (3, "Продажи")]
        cursor.executemany("INSERT INTO departments VALUES (?, ?)", depts)
        
        # Сотрудники
        names = ["Иван", "Петр", "Мария", "Ольга", "Дмитрий"]
        lastnames = ["Смирнов", "Иванов", "Кузнецов", "Соколов"]
        
        emp_rows = []
        api_rows = []
        
        # Гарантированный именинник
        emp_rows.append((1, 1001, 1)) # id, ext_id, dept_id
        api_rows.append((1001, "Счастливчик Виктор", True, "1990-10-25"))

        for i in range(2, count + 1):
            ext_id = 1000 + i
            dept_id = random.randint(1, 3)
            
            # Данные для внутренней БД
            emp_rows.append((i, ext_id, dept_id))
            
            # Данные для "API"
            y = random.randint(1970, 2000)
            m = random.randint(1, 12)
            d = random.randint(1, 28)
            fio = f"{random.choice(lastnames)} {random.choice(names)}"
            active = random.choice([True, True, False])
            
            api_rows.append((ext_id, fio, active, f"{y}-{m:02d}-{d:02d}"))

        cursor.executemany("INSERT INTO employees VALUES (?, ?, ?)", emp_rows)
        cursor.executemany("INSERT INTO api_mock_data VALUES (?, ?, ?, ?)", api_rows)
        logger.info("✅ Генерация завершена.")

# --- 3. ETL ПРОЦЕСС ---
def get_employees_batch(cursor) -> Generator:
    cursor.execute("SELECT e.id, e.ext_id, d.name FROM employees e JOIN departments d ON e.dept_id = d.id")
    while True:
        rows = cursor.fetchmany(50)
        if not rows: break
        yield rows

def run_daily_job(db_path: str, api_client: HRApiClient):
    """Основная задача: найти именинников"""
    logger.info("🚀 Запуск ежедневной проверки...")
    
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        
        found = 0
        for batch in get_employees_batch(cursor):
            for emp_id, ext_id, dept_name in batch:
                # Обогащение данными
                details = api_client.get_employee_info(ext_id)
                
                if details and details.is_active:
                    # Проверка даты (Config.MOCK_TODAY вместо today())
                    if (details.birth_date.month == Config.MOCK_TODAY.month and 
                        details.birth_date.day == Config.MOCK_TODAY.day):
                        
                        age = Config.MOCK_TODAY.year - details.birth_date.year
                        logger.info(f"🎉 НУЖНО ПОЗДРАВИТЬ: {details.full_name} ({dept_name}), {age} лет")
                        found += 1
        
        if found == 0:
            logger.info("🔕 Сегодня именинников нет.")

# --- ENTRY POINT ---
if __name__ == "__main__":
    # 1. Подготовка данных (выполняется только 1 раз при первом запуске)
    init_and_seed_db(Config.DB_NAME, Config.EMPLOYEE_COUNT)
    
    # 2. Инициализация клиента API
    api = HRApiClient(Config.DB_NAME)
    
    # 3. Запуск бизнес-процесса
    run_daily_job(Config.DB_NAME, api)