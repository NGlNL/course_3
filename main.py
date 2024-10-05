from datetime import datetime

from config import config
from src.api import HeadHunterAPI
from src.db_manager import DBManager

params = config()


def main():
    """Основная функция."""
    db = DBManager("db_hh", params)
    db.load_companies_from_json("data/vacancies.json")
    api = HeadHunterAPI()
    companies = db.get_all_companies()
    while True:
        print("Добро пожаловать в HeadHunter!")
        print("1. Создать таблицы.")
        print("2. Загрузить данные в базу данных.")
        print("3. Вывести компании и количество вакансий.")
        print("4. Вывести все вакансии.")
        print("5. Вывести среднюю цену.")
        print("6. Вывести вакансии с зарплатой выше среднего.")
        print("7. Поиск вакансии по ключевому слову.")
        print("8. Выход.")

        choice = input("Введите номер действия: ")

        if choice == "1":
            db.create_table()
        if choice == "2":
            for company in companies:
                if "id" in company and company["id"] is not None:
                    company_id = company["id"]
                    employee_id = company["employee_id"]  # Используем employer_id
                    vacancies = api.get_company(employee_id)

                    for vacancy in vacancies.get("items", []):
                        title = vacancy["name"]  # Название вакансии
                        salary_min = (
                            vacancy["salary"]["from"] if vacancy["salary"] else None
                        )  # Минимальная зарплата
                        salary_max = (
                            vacancy["salary"]["to"] if vacancy["salary"] else None
                        )  # Максимальная зарплата
                        url = vacancy["alternate_url"]  # Ссылка на вакансию
                        description = (
                            vacancy["snippet"]["responsibility"]
                            if "snippet" in vacancy
                            else None
                        )  # Описание вакансии
                        published = datetime.fromisoformat(
                            vacancy["published_at"].replace("Z", "+00:00")
                        )  # Дата публикации

                        # Вставка вакансии в базу данных
                        db.insert_vacancy(
                            company_id=company_id,
                            title=title,
                            salary_min=salary_min,
                            salary_max=salary_max,
                            url=url,
                            description=description,
                            published=published,
                        )
                else:
                    print(f"Skipping company with no id: {company}")

            print("Данные успешно загружены в базу данных.")
        if choice == "3":
            companies_vac = db.get_companies_and_vacancies_count()
            for el in companies_vac:
                print(
                    f"Компания: {el['name']}, количество вакансий: {el["vacancies_count"]}"
                )

        if choice == "4":
            vacancies = db.get_all_vacancies()
            for vacancy in vacancies:
                print(
                    f"Компания: {vacancy['company_name']}, Вакансия: {vacancy['vacancy_title']}"
                )
                print(
                    f"Зарплата: {vacancy['salary_min']} - {vacancy['salary_max']}, Ссылка: {vacancy['url']}"
                )

        if choice == "5":
            avg = db.get_avg_salary()
            print(f"Средняя зарплата по вокансиям: {avg}")

        if choice == "6":
            higher_salary = db.get_vacancies_with_higher_salary()
            for el in higher_salary:
                print(
                    f"Компания: {el['company_name']}, Вакансия: {el['vacancy_title']}"
                )
                print(
                    f"Зарплата: {el['salary_min']} - {el['salary_max']}, Ссылка: {el['url']}"
                )

        if choice == "7":
            keyword = input("Введите ключевое слово: ")
            vacancies = db.get_vacancies_with_keyword(keyword)
            for el in vacancies:
                print(
                    f"Компания: {el['company_name']}, Вакансия: {el['vacancy_title']}"
                )
                print(
                    f"Зарплата: {el['salary_min']} - {el['salary_max']}, Ссылка: {el['url']}"
                )

        if choice == "8":
            db.close()
            break


if __name__ == "__main__":
    main()
