import datetime
import json
import logging
import random
import sqlite3
from typing import Optional


class Config:
    DB_NAME = "data.db"
    LOG_LEVEL = logging.INFO
    TARGET_DATE: Optional[datetime.date] = None  # None - текущая дата
    EMPLOYEE_COUNT = 1000


logging.basicConfig(
    level=Config.LOG_LEVEL,
    format="%(asctime)s - [%(levelname)s] - %(name)s - %(message)s",
)
logger = logging.getLogger("Birthday_notifier")


class NotificationMessage:
    def __init__(self, name, department, age):
        self.name = name
        self.department = department
        self.age = age
        self.template = "Поздравляем коллегу {name} из отдела {department}! Сегодня исполняется {age}."

    def to_text(self):
        return self.template.format(
            name=self.name, department=self.department, age=self.age
        )


class NotificationService:
    def send(self, message: NotificationMessage):
        payload = {"text": message.to_text(), "channel": "#general_test"}

        logger.warning(f"WEBHOOK: {json.dumps(payload, ensure_ascii=False)}")

        sep = "-" * 40
        print(
            f"{sep}\n[MOCK]\nОтправка сообщения...\nКанал: {payload['channel']}\nТекст: {message.to_text()}\n{sep}"
        )


def init_db(db_path, count):
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS departments (id INTEGER PRIMARY KEY, name TEXT)"
        )
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS employees ("
            "id INTEGER PRIMARY KEY, "
            "ext_id INTEGER, "
            "dept_id INTEGER, "
            "fio TEXT, "
            "dob TEXT"
            ")"
        )
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS duty_schedules ("
            "id INTEGER PRIMARY KEY, "
            "employee_id INTEGER, "
            "day_of_week INTEGER, "
            "is_working INTEGER, "
            "shift_start TEXT, "
            "shift_end TEXT"
            ")"
        )

        cursor.execute("SELECT count(*) FROM employees")
        if cursor.fetchone()[0] > 0:
            logger.info("Данные существуют. Пропуск генерации.")
            return

        logger.info(f"Генерация {count} тестовых сотрудников...")

        depts = [(1, "IT"), (2, "Бухгалтерия"), (3, "Маркетинг")]
        cursor.executemany("INSERT INTO departments VALUES (?, ?)", depts)

        emp_data = []
        duty_data = []
        duty_id = 1
        for employee_id in range(1, count + 1):
            ext_id = 1000 + employee_id
            dept_id = random.randint(1, 3)

            rnd_profile = random.Random(ext_id)
            names = ["Иван", "Петр", "Мария", "Анна", "Сергей"]
            surnames = ["Иванов", "Петров", "Сидорова", "Кузнецова", "Смирнов"]
            fio = f"{rnd_profile.choice(surnames)} {rnd_profile.choice(names)}"
            y, m, d = (
                rnd_profile.randint(1980, 2005),
                rnd_profile.randint(1, 12),
                rnd_profile.randint(1, 28),
            )
            dob = f"{y:04d}-{m:02d}-{d:02d}"
            emp_data.append((employee_id, ext_id, dept_id, fio, dob))

            rnd = random.Random(ext_id)
            for dow in range(7):
                if dow <= 4:
                    is_working = 1 if rnd.random() < 0.8 else 0
                else:
                    is_working = 1 if rnd.random() < 0.2 else 0
                duty_data.append(
                    (duty_id, employee_id, dow, is_working, "09:00", "18:00")
                )
                duty_id += 1

        cursor.executemany(
            "INSERT INTO employees VALUES (?, ?, ?, ?, ?)", emp_data
        )
        cursor.executemany(
            "INSERT INTO duty_schedules (id, employee_id, day_of_week, is_working, shift_start, shift_end) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            duty_data,
        )
        logger.info("База данных наполнена.")


def run_job():
    init_db(Config.DB_NAME, Config.EMPLOYEE_COUNT)

    notifier = NotificationService()
    check_date = (
        Config.TARGET_DATE
        if Config.TARGET_DATE is not None
        else datetime.date.today()
    )

    logger.info(f"Запуск проверки на: {check_date}")

    with sqlite3.connect(Config.DB_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT e.fio, d.name, "
            "CAST(strftime('%Y', ?) AS INTEGER) - CAST(strftime('%Y', e.dob) AS INTEGER) AS age "
            "FROM employees e "
            "JOIN departments d ON e.dept_id = d.id "
            "JOIN duty_schedules s ON s.employee_id = e.id "
            "WHERE s.day_of_week = ? AND s.is_working = 1 "
            "AND strftime('%m-%d', e.dob) = strftime('%m-%d', ?)",
            (
                check_date.isoformat(),
                check_date.weekday(),
                check_date.isoformat(),
            ),
        )

        processed = 0
        while True:
            batch = cursor.fetchmany(50)
            if not batch:
                break

            for fio, dept_name, age in batch:
                processed += 1
                msg = NotificationMessage(fio, dept_name, age)
                notifier.send(msg)

    logger.info(f"Завершено. Обработано: {processed}.")


if __name__ == "__main__":
    run_job()
