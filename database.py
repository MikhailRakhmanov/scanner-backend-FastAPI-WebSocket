import os
import aiosqlite
import logging
import asyncio
import random
from typing import Optional, List, Dict
from datetime import datetime
from contextlib import asynccontextmanager
from models import ScanRequest

logger = logging.getLogger(__name__)


class DatabaseManager:
    def __init__(self, db_path: str = "data/scanner_data.db"):
        self.db_path = db_path
        directory = os.path.dirname(self.db_path)
        if directory: os.makedirs(directory, exist_ok=True)

    @asynccontextmanager
    async def _connect(self) -> aiosqlite.Connection:
        db = await aiosqlite.connect(self.db_path)
        await db.execute("PRAGMA journal_mode=WAL;")
        await db.execute("PRAGMA synchronous=NORMAL;")
        try:
            yield db
        finally:
            await db.close()

    async def init_database(self):
        async with self._connect() as db:
            await db.execute("""
                             CREATE TABLE IF NOT EXISTS scans
                             (
                                 id
                                 INTEGER
                                 PRIMARY
                                 KEY
                                 AUTOINCREMENT,
                                 platform
                                 INTEGER
                                 NOT
                                 NULL,
                                 product
                                 INTEGER
                                 NOT
                                 NULL,
                                 scan_date
                                 DATETIME
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
                                 0
                             )
                             """)
            await db.execute("CREATE INDEX IF NOT EXISTS idx_platform_product ON scans(platform, product)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_scan_date ON scans(scan_date)")
            await db.commit()

    async def check_and_mark_overwrite(self, product_id: int) -> Optional[Dict]:
        async with self._connect() as db:
            db.row_factory = aiosqlite.Row
            query = "SELECT id, platform FROM scans WHERE product = ? ORDER BY scan_date DESC LIMIT 1"
            async with db.execute(query, (product_id,)) as cursor:
                row = await cursor.fetchone()
                if row:
                    await db.execute("UPDATE scans SET is_overwrite = 1 WHERE id = ?", (row['id'],))
                    await db.commit()
                    return {"id": row['id'], "platform": row['platform']}
        return None

    async def add_scan(self, scan_request: ScanRequest) -> int:
        platform_int = int(scan_request.platform)
        product_int = int(scan_request.product)
        scan_date_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        async with self._connect() as db:
            cursor = await db.execute(
                "INSERT INTO scans (login, platform, product, scan_date, is_overwrite, legacy_synced) VALUES (?, ?, ?, ?, 0, 0)",
                (scan_request.login, platform_int, product_int, scan_date_str),
            )
            await db.commit()
            scan_id = cursor.lastrowid
            asyncio.create_task(self._simulate_legacy_sync(scan_id))
            return scan_id

    async def _simulate_legacy_sync(self, scan_id: int):
        try:
            await asyncio.sleep(15)
            is_success = random.random() > 0.2
            new_status = 1 if is_success else -1
            async with self._connect() as db:
                await db.execute(
                    "UPDATE scans SET legacy_synced = ? WHERE id = ?",
                    (new_status, scan_id)
                )
                await db.commit()
            logger.info(f"🔄 ID {scan_id}: Статус обновлен на {new_status}")
        except Exception as e:
            logger.error(f"❌ Симулятор ошибки: {e}")

    def _build_where_conditions(self, **kwargs):
        where_clauses = []
        params = []

        # 1. ID Записи (Приоритет)
        obj_id = kwargs.get('id')
        if obj_id is not None and str(obj_id).strip() != "":
            return "WHERE id = ?", [int(obj_id)]

        # 2. Платформа (ДОБАВЛЕНО)
        platform = kwargs.get('platform')
        if platform is not None and str(platform).strip() != "":
            where_clauses.append("platform = ?")
            params.append(int(platform))

        # 3. Логин
        login = kwargs.get('login')
        if login:
            where_clauses.append("login LIKE ?")
            params.append(f"%{login}%")

        # 4. Продукт
        product = kwargs.get('product')
        if product is not None and str(product).strip() != "":
            where_clauses.append("product = ?")
            params.append(int(product))

        # 5. Статус синхронизации
        status = kwargs.get('legacy_synced')
        if status is not None and str(status).strip() != "":
            where_clauses.append("legacy_synced = ?")
            params.append(int(status))

        # 6. Перезапись
        overwrite = kwargs.get('is_overwrite')
        if overwrite is not None:
            where_clauses.append("is_overwrite = ?")
            params.append(1 if overwrite else 0)

        # 7. Даты
        date_from = kwargs.get('date_from')
        date_to = kwargs.get('date_to')
        if date_from:
            where_clauses.append("scan_date >= ?")
            params.append(f"{date_from} 00:00:00")
        if date_to:
            where_clauses.append("scan_date <= ?")
            params.append(f"{date_to} 23:59:59")

        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        return where_sql, params

    async def get_scan_pairs(self, **kwargs) -> List[Dict]:
        where_sql, params = self._build_where_conditions(**kwargs)

        limit = int(kwargs.get('limit', 100))
        offset = int(kwargs.get('offset', 0))

        # ИСПРАВЛЕНИЕ ОШИБКИ "no such column: timestamp"
        sort_raw = kwargs.get('sort', 'scan_date')
        sort_field = 'scan_date'
        sort_order = 'DESC'

        # Парсим формат "field,order" или просто "field"
        if sort_raw:
            parts = sort_raw.split(',')
            field = parts[0]
            if field == 'timestamp':
                sort_field = 'scan_date'
            elif field in ['id', 'login', 'platform', 'product', 'legacy_synced']:
                sort_field = field

            if len(parts) > 1:
                sort_order = 'ASC' if parts[1].lower() == 'asc' else 'DESC'

        query = f"SELECT * FROM scans {where_sql} ORDER BY {sort_field} {sort_order} LIMIT ? OFFSET ?"
        full_params = params + [limit, offset]

        async with self._connect() as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(query, full_params) as cursor:
                rows = await cursor.fetchall()

        return [{
            "id": r["id"],
            "login": r["login"],
            "platform": r["platform"],
            "product": r["product"],
            "timestamp": r["scan_date"],  # На фронт отдаем как timestamp
            "legacy_synced": r["legacy_synced"],
            "is_overwrite": bool(r["is_overwrite"])
        } for r in rows]

    async def get_graphics_data(self, **kwargs) -> Dict:
        where_sql, params = self._build_where_conditions(**kwargs)

        async with self._connect() as db:
            db.row_factory = aiosqlite.Row

            # 1. Группировка по датам (активность)
            # Используем DATE() для обрезки времени в SQLite
            query_date = f"SELECT DATE(scan_date) as day, COUNT(*) as count FROM scans {where_sql} GROUP BY day ORDER BY day ASC"

            # 2. Группировка по платформам
            query_platform = f"SELECT platform, COUNT(*) as count FROM scans {where_sql} GROUP BY platform"

            # 3. Группировка по пользователям
            query_user = f"SELECT login, COUNT(*) as count FROM scans {where_sql} GROUP BY login ORDER BY count DESC LIMIT 10"

            # 4. Общая сводка (Всего, Перезаписи, Ошибки)
            query_summary = f"""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN is_overwrite = 1 THEN 1 ELSE 0 END) as overwrites,
                    SUM(CASE WHEN legacy_synced = -1 THEN 1 ELSE 0 END) as errors
                FROM scans {where_sql}
            """

            res = {
                "by_date": [],
                "by_platform": [],
                "by_user": [],
                "summary": {"total": 0, "overwrites": 0, "errors": 0}
            }

            async with db.execute(query_date, params) as cursor:
                rows = await cursor.fetchall()
                res["by_date"] = [{"date": r["day"], "count": r["count"]} for r in rows]

            async with db.execute(query_platform, params) as cursor:
                rows = await cursor.fetchall()
                res["by_platform"] = [{"platform": r["platform"], "count": r["count"]} for r in rows]

            async with db.execute(query_user, params) as cursor:
                rows = await cursor.fetchall()
                res["by_user"] = [{"login": r["login"], "count": r["count"]} for r in rows]

            async with db.execute(query_summary, params) as cursor:
                r = await cursor.fetchone()
                if r:
                    res["summary"] = {
                        "total": r["total"] or 0,
                        "overwrites": r["overwrites"] or 0,
                        "errors": r["errors"] or 0
                    }

            return res

    async def get_scan_pairs_count(self, **kwargs) -> int:
        where_sql, params = self._build_where_conditions(**kwargs)
        async with self._connect() as db:
            async with db.execute(f"SELECT COUNT(*) FROM scans {where_sql}", params) as cursor:
                res = await cursor.fetchone()
                return res[0] if res else 0
