import logging
from contextlib import asynccontextmanager
from datetime import datetime, date
from typing import Optional, List, Dict, Union

import asyncpg

from models import ScanRequest

logger = logging.getLogger(__name__)


class DatabaseManager:
    def __init__(self, dsn: str = "postgresql://user:password@localhost:5432/dbname"):
        self.dsn = dsn
        self._pool: Optional[asyncpg.Pool] = None

    async def _get_pool(self) -> asyncpg.Pool:
        """Инициализирует пул соединений, если он еще не создан."""
        if self._pool is None:
            self._pool = await asyncpg.create_pool(
                dsn=self.dsn,
                min_size=5,
                max_size=20
            )
            logger.info("Пул соединений PostgreSQL успешно инициализирован")
        return self._pool

    async def ping(self) -> bool:
        """Проверяет соединение с базой данных."""
        try:
            async with self._connect() as conn:
                res = await conn.fetchval("SELECT 1")
                return res == 1
        except Exception as e:
            logger.error(f"PostgreSQL ping failed: {e}")
            return False

    @asynccontextmanager
    async def _connect(self):
        """Контекстный менеджер для получения соединения из пула."""
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            yield conn

    async def close(self):
        """Закрывает все соединения в пуле."""
        if self._pool:
            await self._pool.close()
            logger.info("Пул соединений PostgreSQL закрыт")

    async def init_database(self):
        """Создает таблицы и индексы при запуске приложения."""
        async with self._connect() as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS scans (
                    id SERIAL PRIMARY KEY,
                    platform INTEGER NOT NULL,
                    product INTEGER NOT NULL,
                    scan_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    legacy_synced INTEGER DEFAULT 0,
                    legacy_integration_error TEXT,
                    login TEXT NOT NULL DEFAULT 'unknown',
                    is_overwrite BOOLEAN DEFAULT FALSE
                );
                CREATE INDEX IF NOT EXISTS idx_platform_product ON scans (platform, product);
                CREATE INDEX IF NOT EXISTS idx_scan_date ON scans (scan_date);
                """
            )
            logger.info("База данных инициализирована: таблицы и индексы проверены")

    async def check_and_mark_overwrite(self, product_id: int) -> Optional[Dict]:
        """Проверяет наличие продукта и помечает старую запись как перезаписанную."""
        async with self._connect() as conn:
            async with conn.transaction():
                query = "SELECT id, platform FROM scans WHERE product = $1 ORDER BY scan_date DESC LIMIT 1"
                row = await conn.fetchrow(query, product_id)
                if row:
                    await conn.execute("UPDATE scans SET is_overwrite = TRUE WHERE id = $1", row['id'])
                    return {"id": row['id'], "platform": row['platform']}
        return None

    async def add_scan(self, scan_request: ScanRequest) -> int:
        """Добавляет новую запись в БД со статусом ожидания синхронизации (0)."""
        platform_int = int(scan_request.platform)
        product_int = int(scan_request.product)
        now = datetime.now()

        query = """
                INSERT INTO scans (login, platform, product, scan_date, is_overwrite, legacy_synced)
                VALUES ($1, $2, $3, $4, FALSE, 0) RETURNING id \
                """
        async with self._connect() as conn:
            scan_id = await conn.fetchval(query, scan_request.login, platform_int, product_int, now)
            return scan_id

    async def update_sync_status(self, scan_id: int, status: int, error_msg: Optional[str] = None):
        """Обновляет статус синхронизации (1 - успех, -1 - ошибка)."""
        query = """
                UPDATE scans
                SET legacy_synced            = $2,
                    legacy_integration_error = $3
                WHERE id = $1 \
                """
        async with self._connect() as conn:
            await conn.execute(query, scan_id, status, error_msg)
            logger.info(f"БД: Статус синхронизации для ID {scan_id} изменен на {status}")

    def _build_where_conditions(
            self,
            id: Optional[int] = None,
            platform: Optional[int] = None,
            login: Optional[str] = None,
            product: Optional[int] = None,
            legacy_synced: Optional[int] = None,
            is_overwrite: Optional[bool] = None,
            date_from: Optional[Union[date, str]] = None,
            date_to: Optional[Union[date, str]] = None
    ) -> tuple[str, list]:
        """Вспомогательный метод для построения SQL-условий (строго типизированный)."""
        where_clauses = []
        params = []
        counter = 1

        # 1. Фильтр по ID (если есть, остальные игнорируем или комбинируем - тут приоритет ID)
        if id is not None:
            return "WHERE id = $1", [id]

        # 2. Обычные поля
        if platform is not None:
            where_clauses.append(f"platform = ${counter}")
            params.append(platform)
            counter += 1

        if login is not None and login.strip() != "":
            where_clauses.append(f"login ILIKE ${counter}")
            params.append(f"%{login}%")
            counter += 1

        if product is not None:
            where_clauses.append(f"product = ${counter}")
            params.append(product)
            counter += 1

        if legacy_synced is not None:
            where_clauses.append(f"legacy_synced = ${counter}")
            params.append(legacy_synced)
            counter += 1

        if is_overwrite is not None:
            where_clauses.append(f"is_overwrite = ${counter}")
            params.append(is_overwrite)
            counter += 1

        # 3. Даты
        if date_from:
            where_clauses.append(f"scan_date >= ${counter}")
            # Приводим к datetime начала дня
            d_val = date_from if isinstance(date_from, datetime) else datetime.strptime(str(date_from), '%Y-%m-%d')
            # Если это date (без времени), делаем начало дня
            if isinstance(d_val, date) and not isinstance(d_val, datetime):
                d_val = datetime.combine(d_val, datetime.min.time())

            params.append(d_val)
            counter += 1

        if date_to:
            where_clauses.append(f"scan_date <= ${counter}")
            # Приводим к datetime конца дня
            d_val = date_to if isinstance(date_to, datetime) else datetime.strptime(str(date_to), '%Y-%m-%d')
            # Добавляем время 23:59:59
            if isinstance(d_val, date) and not isinstance(d_val, datetime):
                d_val = datetime.combine(d_val, datetime.max.time().replace(microsecond=0))
            else:
                # Если это datetime, просто подменяем время (или оставляем как есть, зависит от логики, тут делаем конец дня)
                d_val = d_val.replace(hour=23, minute=59, second=59)

            params.append(d_val)
            counter += 1

        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        return where_sql, params

    async def get_scan_pairs(
            self,
            id: Optional[int] = None,
            platform: Optional[int] = None,
            login: Optional[str] = None,
            product: Optional[int] = None,
            legacy_synced: Optional[int] = None,
            is_overwrite: Optional[bool] = None,
            date_from: Optional[Union[date, str]] = None,
            date_to: Optional[Union[date, str]] = None,
            limit: int = 100,
            offset: int = 0,
            sort: str = 'scan_date,desc'
    ) -> List[Dict]:
        """Возвращает список отсканированных пар с фильтрацией."""

        where_sql, params = self._build_where_conditions(
            id=id, platform=platform, login=login, product=product,
            legacy_synced=legacy_synced, is_overwrite=is_overwrite,
            date_from=date_from, date_to=date_to
        )

        # Сортировка
        sort_raw = sort.split(',')
        allowed_sort_fields = ['id', 'login', 'platform', 'product', 'legacy_synced', 'scan_date']
        sort_field = sort_raw[0] if sort_raw[0] in allowed_sort_fields else 'scan_date'
        sort_order = 'ASC' if len(sort_raw) > 1 and sort_raw[1].lower() == 'asc' else 'DESC'

        # Параметры limit/offset добавляем в конец списка params
        idx_limit = len(params) + 1
        idx_offset = len(params) + 2

        query = f"""
            SELECT * FROM scans {where_sql} 
            ORDER BY {sort_field} {sort_order} 
            LIMIT ${idx_limit} OFFSET ${idx_offset}
        """

        async with self._connect() as conn:
            rows = await conn.fetch(query, *params, limit, offset)
            return [dict(r) for r in rows]

    async def get_graphics_data(
            self,
            id: Optional[int] = None,
            platform: Optional[int] = None,
            login: Optional[str] = None,
            product: Optional[int] = None,
            legacy_synced: Optional[int] = None,
            is_overwrite: Optional[bool] = None,
            date_from: Optional[Union[date, str]] = None,
            date_to: Optional[Union[date, str]] = None
    ) -> Dict:
        """Собирает полную статистику для графиков."""

        where_sql, params = self._build_where_conditions(
            id=id, platform=platform, login=login, product=product,
            legacy_synced=legacy_synced, is_overwrite=is_overwrite,
            date_from=date_from, date_to=date_to
        )

        async with self._connect() as conn:
            # 1. Сводная статистика
            query_summary = f"""
                SELECT COUNT(*) as total,
                       SUM(CASE WHEN is_overwrite = TRUE THEN 1 ELSE 0 END) as overwrites,
                       SUM(CASE WHEN legacy_synced = -1 THEN 1 ELSE 0 END) as errors
                FROM scans {where_sql}
            """
            s = await conn.fetchrow(query_summary, *params)

            # 2. Активность по дням (by_date)
            query_date = f"""
                SELECT TO_CHAR(scan_date, 'YYYY-MM-DD') as date, COUNT(*) as count
                FROM scans {where_sql}
                GROUP BY date ORDER BY date ASC
            """
            rows_date = await conn.fetch(query_date, *params)

            # 3. Топ пользователей (by_user)
            query_user = f"""
                SELECT login, COUNT(*) as count
                FROM scans {where_sql}
                GROUP BY login ORDER BY count DESC LIMIT 10
            """
            rows_user = await conn.fetch(query_user, *params)

            # 4. Распределение по платформам (by_platform)
            query_platform = f"""
                SELECT platform, COUNT(*) as count
                FROM scans {where_sql}
                GROUP BY platform ORDER BY count DESC
            """
            rows_platform = await conn.fetch(query_platform, *params)

            return {
                "summary": {
                    "total": s["total"] or 0,
                    "overwrites": int(s["overwrites"] or 0),
                    "errors": int(s["errors"] or 0)
                },
                "by_date": [dict(r) for r in rows_date],
                "by_user": [dict(r) for r in rows_user],
                "by_platform": [dict(r) for r in rows_platform]
            }

    async def get_scan_pairs_count(
            self,
            id: Optional[int] = None,
            platform: Optional[int] = None,
            login: Optional[str] = None,
            product: Optional[int] = None,
            legacy_synced: Optional[int] = None,
            is_overwrite: Optional[bool] = None,
            date_from: Optional[Union[date, str]] = None,
            date_to: Optional[Union[date, str]] = None
    ) -> int:
        """Возвращает общее количество записей по фильтру."""
        where_sql, params = self._build_where_conditions(
            id=id, platform=platform, login=login, product=product,
            legacy_synced=legacy_synced, is_overwrite=is_overwrite,
            date_from=date_from, date_to=date_to
        )
        async with self._connect() as conn:
            count = await conn.fetchval(f"SELECT COUNT(*) FROM scans {where_sql}", *params)
            return count or 0

    async def find_product_image(self, product: int):
        firstpart = product // 1000
        secondpart = product % 1000

        query = """
                SELECT svg_filename \
                FROM processed_images
                WHERE id_listhaff = $1 \
                  AND id_prnt = $2 \
                """

        async with self._connect() as conn:
            filename = await conn.fetchval(query, firstpart, secondpart)
            return filename
