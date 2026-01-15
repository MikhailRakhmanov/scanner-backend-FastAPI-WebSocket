import logging
import jwt
import os
from contextlib import asynccontextmanager
from typing import Optional

from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from starlette.responses import FileResponse

from database import DatabaseManager
from connection_manager import ConnectionManager
from feign_database import FeignDatabase
from models import ConnectionType
load_dotenv()
# Настройка логирования
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

JWT_SECRET = os.getenv("JWT_SECRET", "your-super-secret-key-change-me")

# Укажите свои данные подключения здесь или через переменную окружения DATABASE_URL
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@tsp.cloudpub.ru:20286/production?options=-c search_path=fgs")
FEIGN_DATABASE_URL = os.getenv("FEIGN_DATABASE_URL", "firebirdsql://SYSDBA:masterkey@tcp.cloudpub.ru:16501/C:/DataBase/BASEPLAST_29122025_V152.fdb?charset=WIN1251&enableProtocol=*")
IMAGE_DIR = os.getenv("IMAGE_DIR", "./img")
DLL_PATH = os.getenv("DLL_PATH","./Firebird-4.0.6.3221-0-x64/fbclient.dll")
PORT = int(os.getenv("PORT", "8000"))

class LoginCredentials(BaseModel):
    login: str


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. Инициализация базы данных с корректным DSN
    db = DatabaseManager(dsn=DATABASE_URL)

    try:
        # Пробуем инициализировать таблицы (теперь с корректным await внутри)
        await db.init_database()
        feign_db = FeignDatabase(DLL_PATH, FEIGN_DATABASE_URL)
        manager = ConnectionManager(db, feign_db)

        # Сохраняем объекты в state приложения
        app.state.db = db
        app.state.feign_db = feign_db
        app.state.manager = manager

        logger.info("Приложение и база данных успешно инициализированы")
        yield
    finally:
        # 2. Корректное закрытие пула соединений при выключении
        await db.close()
        logger.info("Соединение с базой данных закрыто")


app = FastAPI(title="Scanner Backend", lifespan=lifespan)

# Middlewares
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# --- ЭНДПОИНТЫ ---

@app.get("/api/history")
async def get_history(
        id: Optional[int] = Query(None),
        date_from: Optional[str] = Query(None),
        date_to: Optional[str] = Query(None),
        platform: Optional[int] = Query(None),
        login: Optional[str] = Query(None),
        product: Optional[int] = Query(None),
        legacy_synced: Optional[int] = Query(None),
        is_overwrite: Optional[bool] = Query(None),
        size: int = Query(100),
        page: int = Query(1),
        sort: Optional[str] = Query(None),
):
    db: DatabaseManager = app.state.db
    offset = (page - 1) * size

    items = await db.get_scan_pairs(
        id=id,
        platform=platform,
        login=login,
        product=product,
        legacy_synced=legacy_synced,
        date_from=date_from,
        date_to=date_to,
        is_overwrite=is_overwrite,
        limit=size,
        offset=offset,
        sort=sort
    )

    total = await db.get_scan_pairs_count(
        id=id,
        platform=platform,
        login=login,
        product=product,
        legacy_synced=legacy_synced,
        date_from=date_from,
        date_to=date_to,
        is_overwrite=is_overwrite
    )

    return {
        "items": items,
        "total": total,
        "page": page,
        "size": size,
        "pages": (total + size - 1) // size if total > 0 else 0
    }


@app.post("/auth/login")
async def authenticate_user(credentials: LoginCredentials):
    feign_db: FeignDatabase = app.state.feign_db
    user = feign_db.get_user_by_login(credentials.login)
    if not user:
        raise HTTPException(status_code=401, detail="Пользователь не найден")
    token = jwt.encode({"login": credentials.login, "name": user["fullname"], "id": user["id"]}, JWT_SECRET, algorithm="HS256")
    return {"login": credentials.login, "token": token}

@app.get("/api/scanners")
async def get_scanners():
    return await app.state.manager.get_users()

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    manager: ConnectionManager = websocket.app.state.manager
    feign_db: FeignDatabase = websocket.app.state.feign_db
    await websocket.accept()
    login, conn_type = None, ConnectionType.NONE

    try:
        data = await websocket.receive_json()
        if data.get("event") != "register":
            await websocket.close(code=1008)
            return

        token = data.get("token")
        if token:
            try:
                payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
                login = payload.get("login")
            except Exception:
                await websocket.close(code=1008)
                return
        else:
            login = data.get("login")
        user = feign_db.get_user_by_login(login)
        if not login or not user:
            await websocket.close(code=1008)
            return

        conn_type = ConnectionType[data.get("type", "NONE")]
        await manager.connect(websocket, user, conn_type)

        user_context = await manager.get_user(login)
        await websocket.send_json(user_context.to_dict())

        while True:
            msg = await websocket.receive_json()
            if msg.get("event") == "new_pair":
                p_id = int(msg["platform"]) if msg.get("platform") else None
                # handle_new_pair теперь сам запускает фоновую задачу синхронизации
                await manager.handle_new_pair(login, p_id, msg.get("product"))

    except WebSocketDisconnect:
        logger.info(f"WebSocket отключен: {login}")
    except Exception as e:
        logger.error(f"WebSocket ошибка: {e}")
    finally:
        if login and conn_type != ConnectionType.NONE:
            await manager.disconnect(websocket, login, conn_type)


@app.get("/api/graphics")
async def get_graphics_endpoint(
        date_from: Optional[str] = Query(None),
        date_to: Optional[str] = Query(None),
        platform: Optional[int] = Query(None),
):
    db: DatabaseManager = app.state.db
    data = await db.get_graphics_data(
        date_from=date_from,
        date_to=date_to,
        platform=platform
    )
    return data


@app.get("/api/image/{product}")
async def get_image(product: int):
    db: DatabaseManager = app.state.db
    image_path = await db.find_product_image(product)
    # Construct path and prevent directory traversal attacks
    file_path = os.path.join(IMAGE_DIR, image_path)

    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="Image not found")

    return FileResponse(file_path)


if __name__ == "__main__":
    import uvicorn

    logger.info("Запуск сервера uvicorn...")
    uvicorn.run(app, host="0.0.0.0", port=PORT)
