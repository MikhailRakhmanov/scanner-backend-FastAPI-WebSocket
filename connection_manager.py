import os
from datetime import date
from typing import Dict, Optional
from fastapi import WebSocket
import json
import logging
import asyncio

from fastapi.encoders import jsonable_encoder

from models import ScanRequest, ConnectionType
from user_context import UserContext
from database import DatabaseManager
from feign_database import FeignDatabase

logger = logging.getLogger(__name__)


class ConnectionManager:
    def __init__(self, db: DatabaseManager, feign_db: FeignDatabase):
        self.users: Dict[str, UserContext] = {}
        self.db: DatabaseManager = db
        self.feign_db: FeignDatabase = feign_db
        logger.info("ConnectionManager инициализирован")

    async def connect(self, websocket: WebSocket, user_data: Dict[str, any], conn_type: ConnectionType):
        login = user_data["login"]

        # 1. Создаем контекст, если его нет
        if login not in self.users:
            self.users[login] = UserContext(
                login=login,
                id=user_data["id"],
                fullname=user_data["fullname"]
            )
            logger.info(f"Контекст создан. Пользователь: {login}")

        user_ctx = self.users[login]

        if conn_type in [ConnectionType.WRITER, ConnectionType.READWRITER]:
            user_ctx.input_connections.append(websocket)
            if len(user_ctx.input_connections) >= 1:
                await self._broadcast_event(user_ctx, {"event": "scanner_connected"})

        if conn_type in [ConnectionType.READER, ConnectionType.READWRITER]:
            user_ctx.output_connections.append(websocket)
            if user_ctx.input_connections:
                try:
                    await websocket.send_json({"event": "scanner_connected"})
                except Exception:
                    pass

        # 5. Логирование (только в DEV)
        if os.getenv("MODE") == "DEV":
            await self._log_all_users()

    async def disconnect(self, websocket: WebSocket, login: str, type: ConnectionType):
        if login not in self.users: return
        user = self.users[login]

        if type in [ConnectionType.WRITER, ConnectionType.READWRITER]:
            if websocket in user.input_connections: user.input_connections.remove(websocket)
            if not user.input_connections:
                await self._broadcast_event(user, {"event": "scanner_refused"})

        if type in [ConnectionType.READER, ConnectionType.READWRITER]:
            if websocket in user.output_connections: user.output_connections.remove(websocket)

        if not user.input_connections and not user.output_connections:
            del self.users[login]

    async def _log_all_users(self):
        users_state = [v.to_dict() for _, v in self.users.items()]
        logger.info(f"Users State: {json.dumps(users_state, ensure_ascii=False)}")

    async def get_user(self, login: str) -> Optional[UserContext]:
        return self.users.get(login)

    async def get_users(self):
        return [v.to_dict() for _, v in self.users.items()]

    async def _broadcast_event(self, user: UserContext, msg: dict):
        for conn in user.output_connections[:]:
            try:
                await conn.send_json(jsonable_encoder(msg))
            except Exception as ex:
                logger.error(f"WebSocket error: {ex}")

    async def _send_to_platform(self, platform_id: int, msg: dict):
        for user in self.users.values():
            if user.current_platform == platform_id:
                await self._broadcast_event(user, msg)

    async def handle_new_pair(self, login: str, platform: int, product: int | None):

        if login not in self.users: return

        user: UserContext = self.users[login]

        # 1. СМЕНА ПЛАТФОРМЫ
        if user.current_platform != platform:
            user.current_platform = platform
            await self._broadcast_event(user, {
                "event": "change_platform",
                "data": {
                    "platform": platform,
                    "products": await self.db.get_scan_pairs(platform=platform,
                                                             is_overwritten=False,
                                                             date_from=date.today(),
                                                             date_to=date.today()
                                                             , sort="scan_date,desc"
                                                             )
                }
            }
                                        )

        # 2. НОВЫЙ ПРОДУКТ
        if product is not None:
            # Проверяем, был ли такой продукт ранее (логика перезаписи)
            old_entry = await self.db.check_and_mark_overwrite(product)
            is_overwriting = old_entry is not None
            old_platform = old_entry['platform'] if is_overwriting else None

            # Записываем в нашу новую базу (по умолчанию legacy_synced = 0)
            scan_id = await self.db.add_scan(ScanRequest(login=login, platform=platform, product=product))

            new_pair_msg = {
                "event": "new_pair",
                "data": {
                    "platform": platform,
                    "product": {"id": product},
                    "scanId": scan_id,
                    "is_overwriting": is_overwriting
                }
            }

            # Рассылка уведомлений о новой паре или перемещении
            if is_overwriting and old_platform != platform:
                await self._send_to_platform(platform, new_pair_msg)
                move_msg = {
                    "event": "product_moved",
                    "data": {"product": product, "from_platform": old_platform, "to_platform": platform}
                }
                await self._send_to_platform(old_platform, move_msg)
            else:
                await self._send_to_platform(platform, new_pair_msg)

            # 3. АСИНХРОННАЯ СИНХРОНИЗАЦИЯ С УДАЛЕННОЙ БАЗОЙ
            # Мы не используем to_thread, так как feign_db.save_pair теперь асинхронная
            asyncio.create_task(self._sync_with_legacy(scan_id, platform, product))

    async def _sync_with_legacy(self, scan_id: int, platform: int, product: int):
        """
        Фоновая задача: ждет ответа от удаленной базы и обновляет статус в нашей.
        """
        try:
            # Вызываем асинхронную заглушку (внутри которой random и sleep)
            success = await self.feign_db.save_pair(platform, product)

            if success:
                # Синхронизация прошла успешно (1)
                await self.db.update_sync_status(scan_id, 1)
            else:
                # Ошибка на стороне удаленной базы (-1)
                await self.db.update_sync_status(scan_id, -1, "Remote database: Random failure")
        except Exception as e:
            # В случае любого исключения фиксируем ошибку (-1)
            logger.error(f"Sync error for scan_id {scan_id}: {e}")
            await self.db.update_sync_status(scan_id, -1, str(e))
