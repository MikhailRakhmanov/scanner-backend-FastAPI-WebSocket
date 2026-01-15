from typing import List, Optional

from starlette.websockets import WebSocket


class UserContext:
    def __init__(self, login: str, current_platform: int = None, id: int = None, fullname: str = None):
        self.login: str = login
        self.id: int = id
        self.fullname: str = fullname
        self.input_connections: List[WebSocket] = []  # Входные соединения от сканнеров
        self.output_connections: List[WebSocket] = []  # Выходные соединения от браузеров
        self.current_platform: Optional[int] = current_platform

    def to_dict(self):
        return {
                "login": self.login,
                "id": self.id,
                "fullname": self.fullname,
                "input_count": len(self.input_connections),
                "output_count": len(self.output_connections),
                "current_platform": self.current_platform,
                # Не логируем сами WebSocket (id для безопасности)
                "input_ids": [id(conn) for conn in self.input_connections],
                "output_ids": [id(conn) for conn in self.output_connections],
                "event": "register_success"
            }