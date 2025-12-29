import os
import asyncpg
import logging
import asyncio
import random
from typing import Optional, List, Dict
from datetime import datetime
from contextlib import asynccontextmanager
from models import ScanRequest

logger = logging.getLogger(__name__)


class DatabaseManager:
    def __init__(self, dsn: str = "postgresql://user:password@localhost:5432/dbname"):
        # DSN: postgresql://[user[:password]@][host][:port][/[dbname]]
        self.dsn = dsn
        self._pool: Optional[asyncpg.Pool] = None

    async def _get_pool(self) -> asyncpg.Pool:
        if self._pool is None:
            # Создаем пул соединений. Настройки WAL и Synchronous задаются в postgresql.conf
            self._pool = asyncpg.create_pool(
                dsn=self.dsn,
                min_size=5,
                max_size=20
            )
        return self._pool

    @asynccontextmanager
    async def _connect(self):
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            yield conn

    async def init_database(self):
        async with self._connect() as conn:
            # PostgreSQL использует SERIAL для автоинкремента и BOOLEAN для логических значений
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

    async def check_and_mark_overwrite(self, product_id: int) -> Optional[Dict]:
        async with self._connect() as conn:
            # Используем транзакцию, чтобы гарантировать целостность при UPDATE
            async with conn.transaction():
                query = "SELECT id, platform FROM scans WHERE product = $1 ORDER BY scan_date DESC LIMIT 1"
                row = await conn.fetchrow(query, product_id)
                if row:
                    await conn.execute("UPDATE scans SET is_overwrite = TRUE WHERE id = $1", row['id'])
                    return {"id": row['id'], "platform": row['platform']}
        return None

    async def add_scan(self, scan_request: ScanRequest) -> int:
        platform_int = int(scan_request.platform)
        product_int = int(scan_request.product)
        # В asyncpg передаем объект datetime напрямую, драйвер сам конвертирует в TIMESTAMP
        now = datetime.now()

        query = """
                INSERT INTO scans (login, platform, product, scan_date, is_overwrite, legacy_synced)
                VALUES ($1, $2, $3, $4, FALSE, 0) RETURNING id \
                """
        async with self._connect() as conn:
            scan_id = await conn.fetchval(query, scan_request.login, platform_int, product_int, now)
            asyncio.create_task(self._simulate_legacy_sync(scan_id))
            return scan_id



    def _build_where_conditions(self, **kwargs):
        where_clauses = []
        params = []
        counter = 1

        # 1. ID Записи
        obj_id = kwargs.get('id')
        if obj_id is not None and str(obj_id).strip() != "":
            return "WHERE id = $1", [int(obj_id)]

        # 2. Платформа
        platform = kwargs.get('platform')
        if platform is not None and str(platform).strip() != "":
            where_clauses.append(f"platform = ${counter}")
            params.append(int(platform))
            counter += 1

        # 3. Логин
        login = kwargs.get('login')
        if login:
            where_clauses.append(f"login ILIKE ${counter}")
            params.append(f"%{login}%")
            counter += 1

        # 4. Продукт
        product = kwargs.get('product')
        if product is not None and str(product).strip() != "":
            where_clauses.append(f"product = ${counter}")
            params.append(int(product))
            counter += 1

        # 5. Статус синхронизации
        status = kwargs.get('legacy_synced')
        if status is not None and str(status).strip() != "":
            where_clauses.append(f"legacy_synced = ${counter}")
            params.append(int(status))
            counter += 1

        # 6. Перезапись
        overwrite = kwargs.get('is_overwrite')
        if overwrite is not None:
            where_clauses.append(f"is_overwrite = ${counter}")
            params.append(bool(overwrite))
            counter += 1

        # 7. Даты
        date_from = kwargs.get('date_from')
        date_to = kwargs.get('date_to')
        if date_from:
            where_clauses.append(f"scan_date >= ${counter}")
            params.append(datetime.strptime(f"{date_from} 00:00:00", '%Y-%m-%d %H:%M:%S'))
            counter += 1
        if date_to:
            where_clauses.append(f"scan_date <= ${counter}")
            params.append(datetime.strptime(f"{date_to} 23:59:59", '%Y-%m-%d %H:%M:%S'))
            counter += 1

        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        return where_sql, params

    async def get_scan_pairs(self, **kwargs) -> List[Dict]:
        where_sql, params = self._build_where_conditions(**kwargs)

        limit = int(kwargs.get('limit', 100))
        offset = int(kwargs.get('offset', 0))

        sort_raw = kwargs.get('sort', 'scan_date')
        sort_field = 'scan_date'
        sort_order = 'DESC'

        if sort_raw:
            parts = sort_raw.split(',')
            field = parts[0]
            if field == 'timestamp':
                sort_field = 'scan_date'
            elif field in ['id', 'login', 'platform', 'product', 'legacy_synced']:
                sort_field = field
            if len(parts) > 1:
                sort_order = 'ASC' if parts[1].lower() == 'asc' else 'DESC'

        # В asyncpg параметры нумеруются по порядку. Добавляем limit и offset в конец списка.
        idx_limit = len(params) + 1
        idx_offset = len(params) + 2
        query = f"SELECT * FROM scans {where_sql} ORDER BY {sort_field} {sort_order} LIMIT ${idx_limit} OFFSET ${idx_offset}"

        async with self._connect() as conn:
            rows = await conn.fetch(query, *params, limit, offset)

        return [{
            "id": r["id"],
            "login": r["login"],
            "platform": r["platform"],
            "product": r["product"],
            "timestamp": r["scan_date"],
            "legacy_synced": r["legacy_synced"],
            "is_overwrite": r["is_overwrite"]
        } for r in rows]

    async def get_graphics_data(self, **kwargs) -> Dict:
        where_sql, params = self._build_where_conditions(**kwargs)

        async with self._connect() as conn:
            # PostgreSQL использует ::date для приведения TIMESTAMP к дате
            query_date = f"SELECT scan_date::date as day, COUNT(*) as count FROM scans {where_sql} GROUP BY day ORDER BY day ASC"
            query_platform = f"SELECT platform, COUNT(*) as count FROM scans {where_sql} GROUP BY platform"
            query_user = f"SELECT login, COUNT(*) as count FROM scans {where_sql} GROUP BY login ORDER BY count DESC LIMIT 10"
            query_summary = f"""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN is_overwrite = TRUE THEN 1 ELSE 0 END) as overwrites,
                    SUM(CASE WHEN legacy_synced = -1 THEN 1 ELSE 0 END) as errors
                FROM scans {where_sql}
            """

            res = {
                "by_date": [],
                "by_platform": [],
                "by_user": [],
                "summary": {"total": 0, "overwrites": 0, "errors": 0}
            }

            for r in await conn.fetch(query_date, *params):
                res["by_date"].append({"date": str(r["day"]), "count": r["count"]})

            for r in await conn.fetch(query_platform, *params):
                res["by_platform"].append({"platform": r["platform"], "count": r["count"]})

            for r in await conn.fetch(query_user, *params):
                res["by_user"].append({"login": r["login"], "count": r["count"]})

            summary_row = await conn.fetchrow(query_summary, *params)
            if summary_row:
                res["summary"] = {
                    "total": summary_row["total"] or 0,
                    "overwrites": int(summary_row["overwrites"] or 0),
                    "errors": int(summary_row["errors"] or 0)
                }

            return res

    async def get_scan_pairs_count(self, **kwargs) -> int:
        where_sql, params = self._build_where_conditions(**kwargs)
        async with self._connect() as conn:
            count = await conn.fetchval(f"SELECT COUNT(*) FROM scans {where_sql}", *params)
            return count or 0
