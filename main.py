import json
import logging
import jwt
from contextlib import asynccontextmanager
from typing import Optional
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Depends, Query, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from jwt.exceptions import PyJWTError
from pydantic import BaseModel

from database import DatabaseManager
from connection_manager import ConnectionManager
from feign_database import FeignDatabase

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

JWT_SECRET = "your-super-secret-key-change-me"


class LoginCredentials(BaseModel):
    login: str


@asynccontextmanager
async def lifespan(app: FastAPI):
    db = DatabaseManager()
    await db.init_database()
    app.state.db = db
    feign_db = FeignDatabase()
    app.state.feign_db = feign_db
    app.state.manager = ConnectionManager(db, feign_db)
    yield


app = FastAPI(title="Scanner Backend", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# --- НОВЫЙ ЭНДПОИНТ ИСТОРИИ (КОТОРОГО НЕ БЫЛО) ---
@app.get("/api/history")
async def get_history(
        id: Optional[int] = Query(None),  # ДОБАВЛЕНО
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

    # Передаем id в оба метода
    items = await db.get_scan_pairs(
        id=id, # ДОБАВЛЕНО
        platform=platform,
        login=login,
        product=product,
        legacy_synced=legacy_synced,
        date_from=date_from,
        date_to=date_to,
        is_overwrite=is_overwrite,
        limit=size,
        offset=offset,
        sort=sort # Передаем как есть, DatabaseManager сам разберется
    )

    total = await db.get_scan_pairs_count(
        id=id, # ДОБАВЛЕНО
        platform=platform,
        login=login,
        product=product,
        legacy_synced=legacy_synced,
        date_from=date_from,
        date_to=date_to, # ДОБАВЛЕНО
        is_overwrite=is_overwrite
    )

    return {
        "items": items,
        "total": total,
        "page": page,
        "size": size,
        "pages": (total + size - 1) // size
    }

@app.post("/auth/login")
async def authenticate_user(credentials: LoginCredentials):
    login = credentials.login
    feign_db: FeignDatabase = app.state.feign_db
    if not feign_db.get_user_by_login(login):
        raise HTTPException(status_code=401, detail="Пользователь не найден")
    token = jwt.encode({"login": login}, JWT_SECRET, algorithm="HS256")
    return {"login": login, "token": token}


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    manager: ConnectionManager = websocket.app.state.manager
    feign_db: FeignDatabase = websocket.app.state.feign_db
    await websocket.accept()
    login, is_input = None, None

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
            except:
                await websocket.close(code=1008)
                return
        else:
            login = data.get("login")

        if not login or not feign_db.get_user_by_login(login):
            await websocket.close(code=1008)
            return

        is_input = data.get("is_input", False)
        await manager.connect(websocket, login, is_input)

        user_context = await manager.get_user(login)
        await websocket.send_json(user_context.to_dict())

        while True:
            msg = await websocket.receive_json()
            if msg.get("event") == "new_pair" and is_input:
                # Приводим к int, так как со сканера может прийти строка
                p_id = int(msg["platform"]) if msg.get("platform") else None
                await manager.handle_new_pair(login, p_id, msg.get("product"))

    except WebSocketDisconnect:
        logger.info(f"Disconnect: {login}")
    except Exception as e:
        logger.error(f"WS Error: {e}")
    finally:
        if login and is_input is not None:
            await manager.disconnect(websocket, login, is_input)

@app.get("/api/graphics")
async def get_graphics_endpoint(
        date_from: Optional[str] = Query(None),
        date_to: Optional[str] = Query(None),
        platform: Optional[int] = Query(None),
):
    db: DatabaseManager = app.state.db
    # Мы используем те же фильтры, что и в истории, чтобы графики
    # соответствовали данным в таблице при выборе диапазона дат
    data = await db.get_graphics_data(
        date_from=date_from,
        date_to=date_to,
        platform=platform
    )
    return data

if __name__ == "__main__":
    import uvicorn
    logger.info("Запуск сервера uvicorn на порту 8000...")
    uvicorn.run(app, host="0.0.0.0", port=8000)
