import logging
from urllib.parse import urlparse

import fdb
from datetime import datetime
from typing import Optional, Dict

logger = logging.getLogger(__name__)


class FeignDatabase:
    def __init__(self, dll_path: str, dsn: str):
        self.dll_path = dll_path
        self.dsn = dsn
        self.connection = None

        try:
            # Парсим URL: firebirdsql://user:pass@host:port/path?params
            parsed = urlparse(dsn)

            # Извлекаем параметры (fdb не поддерживает enableProtocol, он нужен для JDBC)
            # Но мы вытащим charset, если он есть в строке
            params = dict(p.split('=') for p in parsed.query.split('&') if '=' in p)
            charset = params.get('charset', 'WIN1251')  # берем из URL или ставим дефолт

            # Формируем DSN в формате "хост/порт:путь"
            # Убираем ведущий слеш из пути, если он есть (актуально для Windows-путей в URL)
            db_path = parsed.path.lstrip('/')
            dsn = f"{parsed.hostname}/{parsed.port}:{db_path}"

            self.connection = fdb.connect(
                dsn=dsn,
                user=parsed.username,
                password=parsed.password,
                fb_library_name=self.dll_path,
                charset=charset
            )
            logger.info(f"FeignDatabase: Соединение установлено (DSN: {dsn}, Charset: {charset})")

        except Exception as e:
            logger.error(f"Ошибка инициализации БД: {e}")
            raise

    def _get_days_since_1900_offset(self) -> float:
        now = datetime.now()
        start_date = datetime(1900, 1, 1)
        delta = now - start_date
        return (delta.total_seconds() / 86400) + 2

    def get_user_by_login(self, login: str) -> Optional[Dict[str, any]]:
        query = "SELECT IDUSER, CAPTION, PSW FROM USERS WHERE PSW = ?"
        cur = self.connection.cursor()  # Создаем курсор без 'with'
        try:
            cur.execute(query, (login,))
            row = cur.fetchone()
            if row:
                return {"id": row[0], "fullname": row[1], "login": row[2]}
        except Exception as e:
            logger.error(f"Ошибка при поиске пользователя {login}: {e}")
            raise
        finally:
            cur.close()  # Обязательно закрываем вручную
        return None

    async def save_pair(self, platform: int, product: int) -> bool:
        time_val = self._get_days_since_1900_offset()
        firstpart = product // 1000
        secondpart = product % 1000

        cur = self.connection.cursor()  # Создаем курсор без 'with'
        try:
            # 1. Обновление LISTHAFF
            update_query = """
                           UPDATE LISTHAFF
                           SET DOTINSKL    = COALESCE(DOTINSKL, ?),
                               NCAR        = ?,
                               DOTINSKLPOV = ?
                           WHERE IDLISTHAFF = ?
                             AND IDPRNT = ? \
                           """
            cur.execute(update_query, (time_val, platform, time_val, firstpart, secondpart))

            # 2. Поиск договора
            find_dogovor_query = """
                                 SELECT c.IDDOGOVOR
                                 FROM LISTHAFF a
                                          JOIN LISTIZD b ON a.IDIZD = b.ID
                                          JOIN DOGOVOR c ON b.IDDOG = c.IDDOGOVOR
                                 WHERE a.IDLISTHAFF = ?
                                   AND a.IDPRNT = ? \
                                 """
            cur.execute(find_dogovor_query, (firstpart, secondpart))
            res = cur.fetchone()

            if res:
                iddogovor = res[0]
                # 3. Проверка оставшихся изделий
                check_ready_query = "SELECT COUNT(*) FROM LISTHAFF a JOIN LISTIZD b ON a.IDIZD = b.ID WHERE b.IDDOG = ? AND a.DOTINSKL IS NULL"
                cur.execute(check_ready_query, (iddogovor,))
                remaining = cur.fetchone()[0]

                if remaining == 0:
                    cur.execute("UPDATE DOGOVOR SET SOSDOG = 4 WHERE IDDOGOVOR = ?", (iddogovor,))
                    cur.execute("EXECUTE PROCEDURE MAKEDOC(?, ?)", (iddogovor, time_val))

            self.connection.commit()
            logger.info(f"Удаленная база: Успешная запись для product {product}")
            return True

        except Exception as e:
            if self.connection:
                self.connection.rollback()
            logger.error(f"Ошибка в save_pair для product {product}: {e}")
            raise
        finally:
            cur.close()  # Закрываем курсор в любом случае

