import os
import asyncpg
import logging
import asyncio
from typing import Optional, List, Dict
from datetime import datetime
from contextlib import asynccontextmanager
from models import ScanRequest

logger = logging.getLogger(__name__)


class DatabaseManager:
    def __init__(self, dsn: str = "postgresql://user:password@localhost:5432/dbname"):
        self.dsn = dsn
        self._pool: Optional[asyncpg.Pool] = None

    async def _get_pool(self) -> asyncpg.Pool:
        """Инициализирует пул соединений, если он еще не создан."""
        if self._pool is None:
            # ВНИМАНИЕ: asyncpg.create_pool требует await для инициализации
            self._pool = await asyncpg.create_pool(
                dsn=self.dsn,
                min_size=5,
                max_size=20
            )
            logger.info("Пул соединений PostgreSQL успешно инициализирован")
        return self._pool

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
            await conn.execute("""
                               CREATE TABLE IF NOT EXISTS scans
                               (
                                   id
                                   SERIAL
                                   PRIMARY
                                   KEY,
                                   platform
                                   INTEGER
                                   NOT
                                   NULL,
                                   product
                                   INTEGER
                                   NOT
                                   NULL,
                                   scan_date
                                   TIMESTAMP
                                   DEFAULT
                                   CURRENT_TIMESTAMP,
                                   legacy_synced
                                   INTEGER
                                   DEFAULT
                                   0,
                                   legacy_integration_error
                                   TEXT,
                                   login
                                   TEXT
                                   NOT
                                   NULL
                                   DEFAULT
                                   'unknown',
                                   is_overwrite
                                   BOOLEAN
                                   DEFAULT
                                   FALSE
                               );
                               CREATE INDEX IF NOT EXISTS idx_platform_product ON scans(platform, product);
                               CREATE INDEX IF NOT EXISTS idx_scan_date ON scans(scan_date);
                               """)
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

    def _build_where_conditions(self, **kwargs):
        """Вспомогательный метод для построения SQL-условий."""
        where_clauses = []
        params = []
        counter = 1

        obj_id = kwargs.get('id')
        if obj_id is not None and str(obj_id).strip() != "":
            return "WHERE id = $1", [int(obj_id)]

        mapping = {
            'platform': 'platform = ',
            'login': 'login ILIKE ',
            'product': 'product = ',
            'legacy_synced': 'legacy_synced = ',
            'is_overwrite': 'is_overwrite = '
        }

        for key, sql_part in mapping.items():
            val = kwargs.get(key)
            if val is not None and str(val).strip() != "":
                if key == 'login':
                    where_clauses.append(f"{sql_part}${counter}")
                    params.append(f"%{val}%")
                else:
                    where_clauses.append(f"{sql_part}${counter}")
                    params.append(val if key == 'is_overwrite' else int(val))
                counter += 1

        date_from = kwargs.get('date_from')
        if date_from:
            where_clauses.append(f"scan_date >= ${counter}")
            params.append(datetime.strptime(f"{date_from} 00:00:00", '%Y-%m-%d %H:%M:%S'))
            counter += 1

        date_to = kwargs.get('date_to')
        if date_to:
            where_clauses.append(f"scan_date <= ${counter}")
            params.append(datetime.strptime(f"{date_to} 23:59:59", '%Y-%m-%d %H:%M:%S'))
            counter += 1

        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        return where_sql, params

    async def get_scan_pairs(self, **kwargs) -> List[Dict]:
        """Возвращает список отсканированных пар с фильтрацией."""
        where_sql, params = self._build_where_conditions(**kwargs)
        limit = int(kwargs.get('limit', 100))
        offset = int(kwargs.get('offset', 0))

        sort_raw = kwargs.get('sort', 'scan_date,desc').split(',')
        sort_field = sort_raw[0] if sort_raw[0] in ['id', 'login', 'platform', 'product',
                                                    'legacy_synced'] else 'scan_date'
        sort_order = 'ASC' if len(sort_raw) > 1 and sort_raw[1].lower() == 'asc' else 'DESC'

        query = f"""
            SELECT * FROM scans {where_sql} 
            ORDER BY {sort_field} {sort_order} 
            LIMIT ${len(params) + 1} OFFSET ${len(params) + 2}
        """
        async with self._connect() as conn:
            rows = await conn.fetch(query, *params, limit, offset)
            return [dict(r) for r in rows]

    async def get_graphics_data(self, **kwargs) -> Dict:
        """Собирает полную статистику для графиков (Postgres версия)."""
        where_sql, params = self._build_where_conditions(**kwargs)

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

    async def get_scan_pairs_count(self, **kwargs) -> int:
        """Возвращает общее количество записей по фильтру."""
        where_sql, params = self._build_where_conditions(**kwargs)
        async with self._connect() as conn:
            count = await conn.fetchval(f"SELECT COUNT(*) FROM scans {where_sql}", *params)
            return count or 0

    async def find_product_image(self, product: int):
        # Разделение id (логика верна)
        firstpart = product // 1000
        secondpart = product % 1000

        # Используем $1, $2 для PostgreSQL
        query = """
                SELECT svg_filename \
                FROM processed_images
                WHERE id_listhaff = $1 \
                  AND id_prnt = $2 \
                """

        async with self._connect() as conn:
            # fetchval возвращает сразу значение svg_filename или None
            filename = await conn.fetchval(query, firstpart, secondpart)
            return filename


