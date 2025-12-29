from typing import Dict, List, Optional
from fastapi import WebSocket
from datetime import datetime
import json
import logging
import random
import asyncio
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

    async def connect(self, websocket: WebSocket, login: str, type: ConnectionType):
        if login not in self.users:
            # ВОССТАНОВЛЕНИЕ ПЛАТФОРМЫ: Ищем последнюю запись этого юзера в БД
            last_scans = await self.db.get_scan_pairs(login=login, limit=1)
            recovered_platform = last_scans[0]["platform"] if last_scans else None

            self.users[login] = UserContext(login=login, current_platform=recovered_platform)
            logger.info(f"Контекст создан. Пользователь: {login}, Восстановленная платформа: {recovered_platform}")

        user = self.users[login]
        if type == ConnectionType.WRITER or type == ConnectionType.READWRITER:
            user.input_connections.append(websocket)
            if len(user.input_connections) >= 1:
                await self._broadcast_event(user, {"event": "scanner_connected"})
        if type == ConnectionType.READER or type == ConnectionType.READWRITER:
            user.output_connections.append(websocket)
            if user.input_connections:
                try:
                    await websocket.send_json({"event": "scanner_connected"})
                except:
                    pass

        await self._log_all_users()

    async def disconnect(self, websocket: WebSocket, login: str, type: ConnectionType):
        if login not in self.users: return
        user = self.users[login]
        if type == ConnectionType.WRITER or type == ConnectionType.READWRITER:
            if websocket in user.input_connections: user.input_connections.remove(websocket)
            if not user.input_connections:
                await self._broadcast_event(user, {"event": "scanner_refused"})
        if type == ConnectionType.READER or type == ConnectionType.READWRITER:
            if websocket in user.output_connections: user.output_connections.remove(websocket)

        if not user.input_connections and not user.output_connections:
            del self.users[login]

    async def _log_all_users(self):
        users_state = [v.to_dict() for _, v in self.users.items()]
        logger.info(f"Users State: {json.dumps(users_state)}")

    async def get_user(self, login: str) -> Optional[UserContext]:
        return self.users.get(login)

    async def _broadcast_event(self, user: UserContext, msg: dict):
        for conn in user.output_connections[:]:
            try:
                await conn.send_json(msg)
            except:
                pass

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
            change_msg = {"type": "change_platform", "data": {"platform": platform}}
            await self._broadcast_event(user, change_msg)

        # 2. НОВЫЙ ПРОДУКТ
        if product is not None:
            old_entry = await self.db.check_and_mark_overwrite(product)
            is_overwrite = old_entry is not None
            old_platform = old_entry['platform'] if is_overwrite else None

            # add_scan всегда пишет is_overwrite=0 для новой записи
            scan_id = await self.db.add_scan(ScanRequest(login=login, platform=platform, product=product))

            new_pair_msg = {
                "type": "new_pair",
                "data": {
                    "platform": platform,
                    "product": {"id": product},
                    "scanId": scan_id,
                    "is_overwrite": is_overwrite
                }
            }

            if is_overwrite and old_platform != platform:
                await self._send_to_platform(platform, new_pair_msg)
                move_msg = {
                    "type": "product_moved",
                    "data": {"product": product, "from_platform": old_platform, "to_platform": platform}
                }
                await self._send_to_platform(old_platform, move_msg)
            else:
                await self._send_to_platform(platform, new_pair_msg)

            asyncio.create_task(asyncio.to_thread(self.feign_db.save_pair, platform, product))
