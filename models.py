from pydantic import BaseModel
from typing import Optional
from datetime import datetime


# Модель для запроса сканирования
class ScanRequest(BaseModel):
    login: str  # Новый: логин сканировавшего пользователя
    platform: int
    product: Optional[int]
    scan_date: Optional[datetime] = None

