from abc import ABC, abstractmethod

import requests


class VacancyAPI(ABC):
    """Абстрактный класс для API вакансий."""

    @abstractmethod
    def _connect(self, endpoint: str, params: dict = None):
        """Подключение к API вакансий."""
        pass

    @abstractmethod
    def get_company(self, keyword):
        """Получение вакансий по ключевому слову."""
        pass


class HeadHunterAPI(VacancyAPI):
    """Класс для работы с API вакансий."""

    URL = "https://api.hh.ru/"

    def __init__(self):
        self.__session = requests.Session()

    def _connect(self, endpoint: str, params: dict = None):
        """Подключение к API вакансий."""
        url = self.URL + endpoint
        response = self.__session.get(url, params=params)
        if response.status_code == 200:
            return response
        else:
            response.raise_for_status()

    def get_company(self, company_id: str):
        """Получение вакансий по ключевому слову."""
        params = {"employer_id": company_id, "per_page": 100, "page": 0}
        response = self._connect("/vacancies", params=params)
        if response:
            return response.json()
        else:
            return {}
