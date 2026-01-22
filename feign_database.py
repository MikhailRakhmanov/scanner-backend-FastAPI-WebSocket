import logging
import asyncio
from urllib.parse import urlparse
from datetime import datetime
from typing import Optional, Dict
from concurrent.futures import ThreadPoolExecutor

import firebirdsql

logger = logging.getLogger(__name__)


class FeignDatabase:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.connection = None
        # Пул потоков для выполнения синхронных запросов к Firebird
        self._executor = ThreadPoolExecutor(max_workers=1)

        try:
            parsed = urlparse(dsn)
            params = dict(p.split('=') for p in parsed.query.split('&') if '=' in p)
            charset = params.get('charset', 'WIN1251')
            db_path = parsed.path.lstrip('/')

            # Для отладки
            logger.info(f"Подключение к Firebird: {parsed.hostname}:{parsed.port} DB: {db_path}")

            self.connection = firebirdsql.connect(
                host=parsed.hostname,
                port=parsed.port,
                database=db_path,
                user=parsed.username,
                password=parsed.password,
                charset=charset,
                auth_plugin_name='Legacy_Auth'
            )

            logger.info(f"FeignDatabase: Успешно подключено к {parsed.hostname}")

        except Exception as e:
            logger.error(f"Ошибка инициализации FeignDatabase: {e}")
            raise

    def _get_days_since_1900_offset(self) -> float:
        # TDateTime (Delphi) starts from Dec 30, 1899.
        # Python datetime(1900, 1, 1) is day 2. So we add 2. Correct.
        now = datetime.now()
        start_date = datetime(1900, 1, 1)
        delta = now - start_date
        return (delta.total_seconds() / 86400.0) + 2.0

    # Синхронный метод для выполнения в executor
    def _get_user_sync(self, login: str) -> Optional[Dict]:
        query = "SELECT IDUSER, CAPTION, PSW FROM USERS WHERE PSW = ?"
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, (login,))
            row = cursor.fetchone()
            if row:
                return {"id": row[0], "fullname": row[1], "login": row[2]}
            return None
        finally:
            cursor.close()

    # Синхронный метод сохранения
    def _save_pair_sync(self, platform: int, product: int) -> bool:
        time_val = self._get_days_since_1900_offset()
        firstpart = product // 1000
        secondpart = product % 1000

        cursor = self.connection.cursor()
        try:
            # 1. Update LISTHAFF
            update_query = """
                           UPDATE LISTHAFF
                           SET DOTINSKL    = COALESCE(DOTINSKL, ?),
                               NCAR        = ?,
                               DOTINSKLPOV = ?
                           WHERE IDLISTHAFF = ? \
                             AND IDPRNT = ? \
                           """
            cursor.execute(update_query, (time_val, platform, time_val, firstpart, secondpart))

            # 2. Find DOGOVOR
            find_dogovor_query = """
                                 SELECT c.IDDOGOVOR
                                 FROM LISTHAFF a
                                          JOIN LISTIZD b ON a.IDIZD = b.ID
                                          JOIN DOGOVOR c ON b.IDDOG = c.IDDOGOVOR
                                 WHERE a.IDLISTHAFF = ? \
                                   AND a.IDPRNT = ? \
                                 """
            cursor.execute(find_dogovor_query, (firstpart, secondpart))
            res = cursor.fetchone()

            if res:
                iddogovor = res[0]
                # 3. Check remaining
                check_ready_query = """
                                    SELECT COUNT(*)
                                    FROM LISTHAFF a
                                             JOIN LISTIZD b ON a.IDIZD = b.ID
                                    WHERE b.IDDOG = ? \
                                      AND a.DOTINSKL IS NULL \
                                    """
                cursor.execute(check_ready_query, (iddogovor,))
                remaining = cursor.fetchone()[0]

                if remaining == 0:
                    cursor.execute("UPDATE DOGOVOR SET SOSDOG = 4 WHERE IDDOGOVOR = ?", (iddogovor,))
                    # EXECUTE PROCEDURE часто требует commit для применения эффекта
                    cursor.execute("EXECUTE PROCEDURE MAKEDOC(?, ?)", (iddogovor, time_val))

            self.connection.commit()
            logger.info(f"Firebird: Сохранено product={product} platform={platform}")
            return True

        except Exception as e:
            self.connection.rollback()
            logger.error(f"Firebird Error in save_pair: {e}")
            raise
        finally:
            cursor.close()

    # Асинхронные обертки
    def get_user_by_login(self, login: str) -> Optional[Dict]:
        # Этот метод вызывается в auth/login, который async.
        # Но если firebirdsql блокирующий, лучше бы ему быть async def и использовать run_in_executor
        # Но в вашем коде auth_login вызывает его синхронно (без await).
        # FastAPI запустит auth_login в отдельном потоке если он def, но он async def...
        # ПРАВИЛЬНО: Вызывать блокирующие операции в executor.

        # Для простоты, пока оставим синхронным, но знайте: это блокирует loop.
        # В идеале переписать API на await feign_db.get_user_by_login(login)
        return self._get_user_sync(login)

    async def save_pair(self, platform: int, product: int) -> bool:
        # Асинхронная обертка для тяжелой операции записи
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._executor,
            self._save_pair_sync,
            platform,
            product
        )

    def close(self):
        if self.connection:
            self.connection.close()
        self._executor.shutdown()
