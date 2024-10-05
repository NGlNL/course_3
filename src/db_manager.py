import json

import psycopg2
from psycopg2.extras import DictCursor


class DBManager:
    def __init__(self, database, params):
        self.conn = psycopg2.connect(database=database, **params)
        self.conn.autocommit = True

    def close(self):
        """Закрывает соединение с базой данных."""
        self.conn.close()

    def create_table(self):
        """Создает таблицы 'companies' и 'vacancies' в базе данных, если они еще не существуют."""
        with self.conn.cursor() as cur:
            cur.execute(
                """
             CREATE TABLE IF NOT EXISTS companies(
             id SERIAL PRIMARY KEY,
             name VARCHAR(100) not null,
             employee_id INTEGER UNIQUE NOT NULL
             );
             """
            )
            cur.execute(
                """
             CREATE TABLE IF NOT EXISTS vacancies(
             id SERIAL PRIMARY KEY,
             company_id int references companies(id),
             title VARCHAR(100) not null,
             salary_min int,
             salary_max int,
             url VARCHAR(100),
             description TEXT,
             published timestamp
             );
             """
            )

    def load_companies_from_json(self, file_path):
        """Создает таблицы 'companies' и 'vacancies' в базе данных, если они еще не существуют."""
        with open(file_path, "r", encoding="utf-8") as file:
            companies = json.load(file)

        for company in companies:
            id = company["id"]
            name = company["name"]
            self.insert_company(name, id)

    def insert_company(self, name, employee_id):
        """Добавляет компанию в таблицу 'companies'."""
        with self.conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO companies (name, employee_id) VALUES (%s, %s) ON CONFLICT (employee_id) DO NOTHING;",
                (name, employee_id),
            )

    def insert_vacancy(
        self, company_id, title, salary_min, salary_max, url, description, published
    ):
        """Добавляет вакансию в таблицу 'vacancies'."""
        with self.conn.cursor() as cur:
            cur.execute(
                """
            INSERT INTO vacancies (company_id, title, salary_min, salary_max, url, description, published)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
            """,
                (
                    company_id,
                    title,
                    salary_min,
                    salary_max,
                    url,
                    description,
                    published,
                ),
            )

    def get_all_companies(self):
        """Возвращает список всех компаний из таблицы 'companies'. """
        with self.conn.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute("SELECT id, name, employee_id FROM companies;")
            return cursor.fetchall()

    def get_companies_and_vacancies_count(self):
        """Возвращает список всех компаний из таблицы 'companies'."""
        with self.conn.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(
                """
                SELECT c.name, COUNT(v.id) as vacancies_count
                FROM companies c
                LEFT JOIN vacancies v ON c.id = v.company_id
                GROUP BY c.id;
            """
            )
            return cursor.fetchall()

    def get_all_vacancies(self):
        """Возвращает список всех вакансий из таблицы 'vacancies'."""
        with self.conn.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(
                """
                SELECT c.name as company_name, v.title as vacancy_title, v.salary_min, v.salary_max, v.url
                FROM vacancies v
                JOIN companies c ON v.company_id = c.id;
            """
            )
            return cursor.fetchall()

    def get_avg_salary(self):
        """Возвращает список всех вакансий из таблицы 'vacancies'."""
        with self.conn.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(
                """
                SELECT AVG(COALESCE(salary_min, salary_max, 0)) as avg_salary
                FROM vacancies;
            """
            )
            return cursor.fetchone()[0]

    def get_vacancies_with_higher_salary(self):
        """Получает список всех вакансий, у которых зарплата выше средней."""
        avg_salary = self.get_avg_salary()
        with self.conn.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(
                """
                SELECT c.name as company_name, v.title as vacancy_title, v.salary_min, v.salary_max, v.url
                FROM vacancies v
                JOIN companies c ON v.company_id = c.id
                WHERE ((v.salary_min + v.salary_max) / 2) > %s;
            """,
                (avg_salary,),
            )
            return cursor.fetchall()

    def get_vacancies_with_keyword(self, keyword):
        """Получает список всех вакансий, в названии которых содержатся переданные в метод слова."""
        with self.conn.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(
                """
                SELECT c.name as company_name, v.title as vacancy_title, v.salary_min, v.salary_max, v.url
                FROM vacancies v
                JOIN companies c ON v.company_id = c.id
                WHERE v.title ILIKE %s;
            """,
                (f"%{keyword}%",),
            )
            return cursor.fetchall()
