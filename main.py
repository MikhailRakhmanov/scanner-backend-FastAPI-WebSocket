import logging
import jwt
import os
from contextlib import asynccontextmanager
from typing import Optional
from pathlib import Path
import asyncio
import sys
from datetime import datetime

from fastapi.encoders import jsonable_encoder

if "_patch_asyncio" in asyncio.run.__qualname__:
    import asyncio.runners

    asyncio.run = asyncio.runners.run

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

# ===================== LOGGING SETUP =====================

COLORS = {
    'DEBUG': '\033[36m',  # Cyan
    'INFO': '\033[32m',  # Green
    'WARNING': '\033[33m',  # Yellow
    'ERROR': '\033[31m',  # Red
    'CRITICAL': '\033[1;31m'  # Bold Red
}
RESET = '\033[0m'
GREY = '\033[90m'


class SpringLikeFormatter(logging.Formatter):
    """Форматер логов в стиле Spring Boot"""

    def format(self, record):
        # Timestamp
        asctime = datetime.fromtimestamp(record.created).strftime('%Y-%m-%d %H:%M:%S')
        msec = int(record.msecs)
        timestamp = f"{asctime}.{msec:03d}"

        # Log level с цветом
        level_name = record.levelname
        if level_name == "WARNING":
            level_name = "WARN"

        level_display = level_name[:5].ljust(5)
        color = COLORS.get(record.levelname, '')
        if color:
            level_display = f"{color}{level_display}{RESET}"

        # Logger name
        logger_name = record.name
        if logger_name == "uvicorn.access":
            logger_name = "http.access"
        elif logger_name == "uvicorn.error":
            logger_name = "http.error"
        elif logger_name == "__main__":
            logger_name = "main"

        logger_name = logger_name[-30:].ljust(30)
        logger_name_display = f"{GREY}{logger_name}{RESET}"

        # Process ID
        pid = str(record.process).ljust(5)
        pid_display = f"{GREY}{pid}{RESET}"

        # Message
        message = record.getMessage()

        return f"{timestamp}  {level_display} {pid_display} --- [{logger_name_display}] : {message}"


class EndpointFilter(logging.Filter):
    """Фильтрует шумные endpoints из логов доступа"""

    def filter(self, record):
        msg = record.getMessage()
        # Не логируем health checks и stats
        if "/health" in msg or "/api/stats" in msg:
            return False
        return True


def setup_logging():
    """Инициализация логирования в стиле Spring Boot"""
    formatter = SpringLikeFormatter()

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.handlers = []
    root_logger.addHandler(handler)
    root_logger.setLevel(logging.INFO)

    # Применяем форматер к внешним логам
    for logger_name in ["uvicorn", "uvicorn.error", "uvicorn.access", "asyncpg", "watchfiles"]:
        logger = logging.getLogger(logger_name)
        logger.handlers = []
        logger.addHandler(handler)
        logger.propagate = False

        if logger_name == "uvicorn.access":
            logger.addFilter(EndpointFilter())

    return logging.getLogger(__name__)


logger = setup_logging()

# ===================== CONFIG =====================

JWT_SECRET = os.getenv("JWT_SECRET", "your-super-secret-key-change-me")
DATABASE_URL = os.getenv("DATABASE_URL",
                         "postgresql://postgres:postgres@tsp.cloudpub.ru:20286/production?options=-c search_path=fgs")
FEIGN_DATABASE_URL = os.getenv("FEIGN_DATABASE_URL",
                               "firebirdsql://SYSDBA:masterkey@tcp.cloudpub.ru:16501/C:/DataBase/BASEPLAST_29122025_V152.fdb?charset=WIN1251&enableProtocol=*")

IMAGE_DIR = Path(os.getenv("IMAGE_DIR", "./svg_output")).resolve()
PORT = int(os.getenv("PORT", "8000"))

logger.info(f"CONFIG: IMAGE_DIR={IMAGE_DIR}")
logger.info(f"CONFIG: PORT={PORT}")
logger.info(f"CONFIG: DATABASE_URL={DATABASE_URL.split('@')[0]}@***")
logger.info(f"CONFIG: FEIGN_DATABASE_URL={FEIGN_DATABASE_URL.split('@')[0]}@***")


# ===================== MODELS =====================

class LoginCredentials(BaseModel):
    login: str


# ===================== LIFESPAN =====================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Инициализация и завершение приложения"""

    logger.info("═" * 80)
    logger.info("STARTING APPLICATION")
    logger.info("═" * 80)

    db = DatabaseManager(dsn=DATABASE_URL)
    try:
        logger.info("Инициализация PostgreSQL...")
        await db.init_database()
        logger.info("PostgreSQL инициализирована")

        logger.info("Инициализация Firebird...")
        feign_db = FeignDatabase(FEIGN_DATABASE_URL)
        logger.info("Firebird инициализирована")

        logger.info("Инициализация Connection Manager...")
        manager = ConnectionManager(db, feign_db)
        logger.info("Connection Manager готов")

        app.state.db = db
        app.state.feign_db = feign_db
        app.state.manager = manager

        logger.info("═" * 80)
        logger.info("APPLICATION READY")
        logger.info("═" * 80)
        yield

    except Exception as e:
        logger.error(f"Ошибка инициализации: {e}", exc_info=True)
        raise
    finally:
        logger.info("═" * 80)
        logger.info("SHUTTING DOWN")
        logger.info("═" * 80)
        await db.close()
        logger.info("Соединения закрыты")


# ===================== FASTAPI APP =====================

app = FastAPI(title="Scanner Backend", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ===================== ENDPOINTS =====================

@app.get("/api/history")
async def get_history(
        id: Optional[int] = Query(None),
        date_from: Optional[str] = Query(None),
        date_to: Optional[str] = Query(None),
        platform: Optional[int] = Query(None),
        login: Optional[str] = Query(None),
        product: Optional[int] = Query(None),
        legacy_synced: Optional[int] = Query(None),
        is_overwritten: Optional[bool] = Query(None),
        size: int = Query(100),
        page: int = Query(1),
        sort: Optional[str] = Query(None),
):
    db: DatabaseManager = app.state.db
    offset = (page - 1) * size

    logger.debug(f"GET /api/history: platform={platform}, product={product}, page={page}, size={size}")

    try:
        items = await db.get_scan_pairs(
            id=id, platform=platform, login=login, product=product,
            legacy_synced=legacy_synced, date_from=date_from, date_to=date_to,
            is_overwritten=is_overwritten, limit=size, offset=offset, sort=sort
        )

        total = await db.get_scan_pairs_count(
            id=id, platform=platform, login=login, product=product,
            legacy_synced=legacy_synced, date_from=date_from, date_to=date_to,
            is_overwritten=is_overwritten
        )

        logger.info(f"/api/history returned {len(items)}/{total} items")

        return {
            "items": items,
            "total": total,
            "page": page,
            "size": size,
            "pages": (total + size - 1) // size if total > 0 else 0
        }
    except Exception as e:
        logger.error(f"/api/history error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/auth/login")
async def authenticate_user(credentials: LoginCredentials):
    feign_db: FeignDatabase = app.state.feign_db

    logger.info(f"Auth attempt for user: {credentials.login}")

    try:
        user = feign_db.get_user_by_login(credentials.login)
        if not user:
            logger.warn(f"User not found: {credentials.login}")
            raise HTTPException(status_code=401, detail="Пользователь не найден")

        token = jwt.encode({
            "login": credentials.login,
            "name": user["fullname"],
            "id": user["id"]
        }, JWT_SECRET, algorithm="HS256")

        logger.info(f"Auth successful: {credentials.login} ({user['fullname']})")

        return {"login": credentials.login, "token": token}
    except Exception as e:
        logger.error(f"Auth error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Authentication error")


@app.get("/api/scanners")
async def get_scanners():
    logger.debug("GET /api/scanners")
    try:
        scanners = await app.state.manager.get_users()
        logger.info(f"Returned {len(scanners)} scanners")
        return scanners
    except Exception as e:
        logger.error(f"Error fetching scanners: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    manager: ConnectionManager = websocket.app.state.manager
    feign_db: FeignDatabase = websocket.app.state.feign_db

    client_addr = websocket.client
    logger.info(f"WebSocket connection from {client_addr}")

    await websocket.accept()
    login, conn_type = None, ConnectionType.NONE

    try:
        data = await websocket.receive_json()
        event = data.get("event")

        if event != "register":
            logger.warn(f"Invalid first event from {client_addr}: {event}")
            await websocket.close(code=1008)
            return

        token = data.get("token")
        if token:
            try:
                payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
                login = payload.get("login")
                logger.debug(f"  Token validated for: {login}")
            except Exception as e:
                logger.warn(f"Invalid token from {client_addr}: {e}")
                await websocket.close(code=1008)
                return
        else:
            login = data.get("login")

        user = feign_db.get_user_by_login(login)
        if not login or not user:
            logger.warn(f"User not found: {login}")
            await websocket.close(code=1008)
            return

        conn_type = ConnectionType[data.get("type", "NONE")]
        await manager.connect(websocket, user, conn_type)

        logger.info(f"WebSocket registered: {login} ({user['fullname']}) as {conn_type.name}")

        user_context = await manager.get_user(login)
        await websocket.send_json(jsonable_encoder(user_context.to_dict()))

        while True:
            msg = await websocket.receive_json()
            if msg.get("event") == "new_pair":
                p_id = int(msg["platform"]) if msg.get("platform") else None
                logger.debug(f"  {login}: new_pair event - platform={p_id}, product={msg.get('product')}")
                await manager.handle_new_pair(login, p_id, msg.get("product"))

    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected: {login}")
    except Exception as e:
        logger.error(f"WebSocket error ({login}): {e}", exc_info=True)
    finally:
        if login and conn_type != ConnectionType.NONE:
            await manager.disconnect(websocket, login, conn_type)
            logger.info(f"  Cleaned up connection: {login}")


@app.get("/api/graphics")
async def get_graphics_endpoint(
        date_from: Optional[str] = Query(None),
        date_to: Optional[str] = Query(None),
        platform: Optional[int] = Query(None),
):
    logger.debug(f"GET /api/graphics: platform={platform}, date_from={date_from}, date_to={date_to}")

    try:
        db: DatabaseManager = app.state.db
        data = await db.get_graphics_data(date_from=date_from, date_to=date_to, platform=platform)
        logger.info(f"/api/graphics returned {len(data)} data points")
        return data
    except Exception as e:
        logger.error(f"/api/graphics error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    status = {
        "status": "ok",
        "postgresql": "unknown",
        "firebird": "unknown"
    }

    try:
        db: DatabaseManager = app.state.db
        if await db.ping():
            status["postgresql"] = "ok"
        else:
            status["postgresql"] = "disconnected"
            status["status"] = "degraded"
    except Exception as e:
        logger.error(f"PostgreSQL health check failed: {e}")
        status["postgresql"] = f"error"
        status["status"] = "degraded"

    try:
        feign_db: FeignDatabase = app.state.feign_db
        if feign_db and feign_db.connection:
            try:
                cur = feign_db.connection.cursor()
                cur.execute("SELECT 1 FROM RDB$DATABASE")
                cur.fetchone()
                cur.close()
                status["firebird"] = "ok"
            except:
                status["firebird"] = "disconnected"
                status["status"] = "degraded"
        else:
            status["firebird"] = "disconnected"
            status["status"] = "degraded"
    except Exception as e:
        logger.error(f"Firebird health check failed: {e}")
        status["firebird"] = "error"
        status["status"] = "degraded"

    return status


@app.get("/api/image/{product}")
async def get_image(product: int):
    db: DatabaseManager = app.state.db
    image_path = await db.find_product_image(product)

    # Безопасный путь с абсолютным IMAGE_DIR
    file_path = IMAGE_DIR / image_path

    if not file_path.exists() or not file_path.is_file():
        raise HTTPException(status_code=404, detail="Image not found")

    return FileResponse(file_path)


if __name__ == "__main__":
    import uvicorn

    logger.info(f"Starting uvicorn on 0.0.0.0:{PORT}")
    uvicorn.run(app, host="0.0.0.0", port=PORT)
