import datetime
import json
import logging
import random
import sqlite3
from typing import Optional


class Config:
    DB_NAME = "data.db"
    LOG_LEVEL = logging.INFO
    TARGET_DATE = datetime.date(2023, 10, 25) #datetime.date.today()
    EMPLOYEE_COUNT = 100

logging.basicConfig(
    level=Config.LOG_LEVEL,
    format='%(asctime)s - [%(levelname)s] - %(name)s - %(message)s'
)
logger = logging.getLogger("Birthday_notifier")

class EmployeeDetails:
    def __init__(self, full_name, is_active, birth_date):
        self.full_name = full_name
        self.is_active = is_active
        self.birth_date = birth_date

class NotificationMessage:
    def __init__(self, name, department, age):
        self.name = name
        self.department = department
        self.age = age
        self.template = "Поздравляем коллегу {name} из отдела {department}! Сегодня исполняется {age}."

    def to_text(self):
        return self.template.format(name=self.name, department=self.department, age=self.age)

class NotificationService:
    def send(self, message: NotificationMessage):
        payload = {
            "text": message.to_text(),
            "channel": "#general_test"
        }
        
        logger.warning(f"WEBHOOK: {json.dumps(payload, ensure_ascii=False)}")
        
        sep = "-" * 40
        print(f"{sep}\n[MOCK]\nОтправка сообщения...\nКанал: {payload['channel']}\nТекст: {message.to_text()}\n{sep}")

class ApiClient:
    def __init__(self, db_path):
        self.db_path = db_path

    def get_employee_info(self, ext_id):
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT fio, is_active, dob FROM api_mock_data WHERE ext_id = ?", (ext_id,))
                row = cursor.fetchone()
                
                if not row:
                    return None
               
                fio, active, dob_str = row
                y, m, d = map(int, dob_str.split('-'))
                return EmployeeDetails(fio, bool(active), datetime.date(y, m, d))
        except sqlite3.Error as e:
            logger.error(f"Ошибка API: {e}")
            return None

def init_db(db_path, count):
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS departments (id INTEGER PRIMARY KEY, name TEXT)")
        cursor.execute("CREATE TABLE IF NOT EXISTS employees (id INTEGER PRIMARY KEY, ext_id INTEGER, dept_id INTEGER)")
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS duty_schedules (id INTEGER PRIMARY KEY, employee_id INTEGER, day_of_week INTEGER, shift_start TEXT, shift_end TEXT)"
        )
        cursor.execute("CREATE TABLE IF NOT EXISTS api_mock_data (ext_id INTEGER PRIMARY KEY, fio TEXT, is_active BOOLEAN, dob TEXT)")

        cursor.execute("SELECT count(*) FROM employees")
        if cursor.fetchone()[0] > 0:
            logger.info("Данные существуют. Пропуск генерации.")
            return

        logger.info(f"Генерация {count} тестовых сотрудников...")
        
        depts = [(1, "IT"), (2, "Бухгалтерия"), (3, "Маркетинг")]
        cursor.executemany("INSERT INTO departments VALUES (?, ?)", depts)

        duty_data = [(i, i, (i % 7), "09:00", "18:00") for i in range(1, min(count + 1, 21))]
        cursor.executemany(
            "INSERT INTO duty_schedules (id, employee_id, day_of_week, shift_start, shift_end) VALUES (?, ?, ?, ?, ?)",
            duty_data,
        )

        names = ["Иван", "Петр", "Мария", "Анна", "Сергей"]
        surnames = ["Иванов", "Петров", "Сидорова", "Кузнецова", "Смирнов"]
        
        cursor.execute("INSERT INTO employees VALUES (1, 1001, 1)")
        cursor.execute("INSERT INTO api_mock_data VALUES (1001, 'Тестовый Виктор', 1, '1990-10-25')")

        emp_data, api_data = [], []
        for i in range(2, count + 1):
            ext_id = 1000 + i
            y, m, d = random.randint(1970, 2000), random.randint(1, 12), random.randint(1, 28)
            fio = f"{random.choice(surnames)} {random.choice(names)}"
            emp_data.append((i, ext_id, random.randint(1, 3)))
            api_data.append((ext_id, fio, random.choice([1, 1, 0]), f"{y}-{m:02d}-{d:02d}"))

        cursor.executemany("INSERT INTO employees VALUES (?, ?, ?)", emp_data)
        cursor.executemany("INSERT INTO api_mock_data VALUES (?, ?, ?, ?)", api_data)
        logger.info("База данных наполнена.")

def run_job():
    init_db(Config.DB_NAME, Config.EMPLOYEE_COUNT)
    
    api = ApiClient(Config.DB_NAME)
    notifier = NotificationService()
    check_date = Config.TARGET_DATE
    
    logger.info(f"Запуск проверки на: {check_date}")

    with sqlite3.connect(Config.DB_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT e.ext_id, d.name FROM employees e JOIN departments d ON e.dept_id = d.id")
        
        processed = 0
        while True:
            batch = cursor.fetchmany(50)
            if not batch: break
            
            for ext_id, dept_name in batch:
                processed += 1
                details = api.get_employee_info(ext_id)
                
                if not details or not details.is_active:
                    continue
                
                if (details.birth_date.month == check_date.month and 
                    details.birth_date.day == check_date.day):
                    
                    age = check_date.year - details.birth_date.year
                    msg = NotificationMessage(details.full_name, dept_name, age)
                    notifier.send(msg)
                    
    logger.info(f"Завершено. Обработано: {processed}.")

if __name__ == "__main__":
    run_job()