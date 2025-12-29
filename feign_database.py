import asyncio
import logging
import random
from typing import Optional, Dict

logger = logging.getLogger(__name__)


class FeignDatabase:
    def __init__(self):
        logger.info("FeignDatabase инициализирована (заглушка)")

    def get_user_by_login(self, login: str) -> Optional[Dict[str, any]]:
        return {"id": 1, "login": login, "fullname": login}

    async def save_pair(self, platform: int, product: int) -> bool:
        """
        Имитирует асинхронную запись в удаленную систему.
        Задержка до 10 секунд, вероятность ошибки 20%.
        """
        logger.info(f"Удаленная база: Начат процесс записи для платформы {platform}")

        # Имитация работы (рандом до 10 сек по условию)
        await asyncio.sleep(random.uniform(2, 10))

        is_success = random.random() > 0.5
        if not is_success:
            logger.error(f"Удаленная база: Ошибка записи пары {platform}-{product}")
            return False

        logger.info(f"Удаленная база: Запись успешно завершена для {platform}-{product}")
        return True