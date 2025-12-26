from urllib.request import Request

from fastapi import Depends

from connection_manager import ConnectionManager
from database import DatabaseManager


def get_db(request: Request) -> DatabaseManager:
    return request.app.state.db

def get_manager(db: DatabaseManager = Depends(get_db)) -> ConnectionManager:
    return ConnectionManager(db)