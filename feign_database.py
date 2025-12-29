import asyncio
import logging
import random
from typing import Optional, Dict

logger = logging.getLogger(__name__)

class FeignDatabase:
    """
    Фейковая база данных пользователей (в памяти).
    Для тестов: возвращает пользователя по логину или None.
    """
    def __init__(self):
        logger.info("FeignDatabase инициализирована с тестовыми пользователями")

    def get_user_by_login(self, login: str) -> Optional[Dict[str, any]]:
        return {
            "id": 1,
            "login": login,
            "fullname": login
        }

    def save_pair(self, platform: int, product: int) -> Optional[Dict[str, any]]:
        logger.info(f"Сохранение пары: platform={platform}, product={product}")
        asyncio.sleep(15)
        is_success = random.random() > 0.2
        if not is_success:
            raise Exception("Ошибка")
