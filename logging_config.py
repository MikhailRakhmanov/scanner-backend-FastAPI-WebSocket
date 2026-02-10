import logging
import sys
from datetime import datetime

# Цвета ANSI
COLORS = {
    'DEBUG': '\033[36m',  # Cyan
    'INFO': '\033[32m',  # Green
    'WARN': '\033[33m',  # Yellow
    'ERROR': '\033[31m',  # Red
    'CRITICAL': '\033[1;31m'  # Bold Red
}
RESET = '\033[0m'
GREY = '\033[90m'


class SpringLikeFormatter(logging.Formatter):
    def format(self, record):
        asctime = datetime.fromtimestamp(record.created).strftime('%Y-%m-%d %H:%M:%S')
        msec = int(record.msecs)
        timestamp = f"{asctime}.{msec:03d}"

        level_raw = record.levelname
        level_code = "WARN" if level_raw == "WARNING" else level_raw
        level_display = level_code[:5].ljust(5)

        color = COLORS.get(level_code, "")
        if color:
            level_display = f"{color}{level_display}{RESET}"

        name = record.name
        if name == "uvicorn.access": name = "http.access"
        if name == "uvicorn.error": name = "http.error"
        if name == "__main__": name = "main"

        logger_name = name[-25:].ljust(25)
        logger_name_display = f"{GREY}{logger_name}{RESET}"
        message = record.getMessage()
        pid = str(record.process).ljust(5)
        pid_display = f"{GREY}{pid}{RESET}"

        return f"{timestamp}  {level_display} {pid_display} --- [{logger_name_display}] : {message}"


# --- НОВЫЙ КЛАСС ФИЛЬТРА ---
class EndpointFilter(logging.Filter):
    """Фильтрует шумные endpoint-ы из логов доступа"""

    def filter(self, record):
        # Получаем итоговое сообщение
        msg = record.getMessage()
        # Если в сообщении есть эти пути — не выводим лог
        if "/api/stats" in msg or "/health" in msg:
            return False
        return True


def setup_logging():
    formatter = SpringLikeFormatter()

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.handlers = []
    root_logger.addHandler(handler)
    root_logger.setLevel(logging.INFO)

    loggers_to_patch = [
        "uvicorn",
        "uvicorn.error",
        "uvicorn.access",
        "apscheduler",
        "asyncpg",
        "watchfiles",
    ]

    for logger_name in loggers_to_patch:
        logger = logging.getLogger(logger_name)
        logger.handlers = []
        logger.addHandler(handler)
        logger.propagate = False

        # --- ПРИМЕНЯЕМ ФИЛЬТР ТОЛЬКО ТУТ ---
        if logger_name == "uvicorn.access":
            logger.addFilter(EndpointFilter())
