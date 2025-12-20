import csv
import json
import logging
import os
import time
import random
from typing import Optional, Dict, Any
from functools import wraps

# Настройки через переменные окружения
INPUT_PATH = os.getenv("INPUT_PATH", "/data/local_data/2019-Oct.csv")
OUTPUT_PATH = os.getenv("OUTPUT_PATH", "/data/output.csv")
DLQ_PATH = os.getenv("DLQ_PATH", "/data/dlq.csv")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "/data/checkpoint.json")
LOG_PATH = os.getenv("LOG_PATH", "/data/processing.log")

MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))
INITIAL_BACKOFF = float(os.getenv("INITIAL_BACKOFF", "0.5"))
BACKOFF_MULTIPLIER = float(os.getenv("BACKOFF_MULTIPLIER", "2.0"))
MAX_BACKOFF = float(os.getenv("MAX_BACKOFF", "10.0"))
CHECKPOINT_EVERY = int(os.getenv("CHECKPOINT_EVERY", "1000"))
FAILURE_PROBABILITY = float(os.getenv("FAILURE_PROBABILITY", "0.0"))

# Логирование
logger = logging.getLogger("streaming_processor")
logger.setLevel(logging.INFO)
try:
    fh = logging.FileHandler(LOG_PATH)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
except Exception:
    # если не удалось создать файловый лог (например, права) — продолжаем с консолью
    pass
logger.addHandler(logging.StreamHandler())

# Retry декоратор
def retry_exponential(max_attempts=MAX_RETRIES,
                      initial_backoff=INITIAL_BACKOFF,
                      multiplier=BACKOFF_MULTIPLIER,
                      max_backoff=MAX_BACKOFF):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempt = 0
            backoff = initial_backoff
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    attempt += 1
                    logger.warning("Error in %s: %s (attempt %d/%d)", func.__name__, str(e), attempt, max_attempts)
                    if attempt >= max_attempts:
                        logger.error("Max retries reached for %s", func.__name__)
                        raise
                    sleep = min(backoff, max_backoff)
                    logger.info("Sleeping %.2fs before retrying %s", sleep, func.__name__)
                    time.sleep(sleep)
                    backoff *= multiplier
        return wrapper
    return decorator

class ValidationError(Exception):
    pass

def validate_row(row: Dict[str, str]) -> Dict[str, Any]:
    # Валидация и преобразование полей
    if not row.get("event_time"):
        raise ValidationError("missing event_time")
    price_raw = row.get("price", "")
    price = None
    if price_raw:
        try:
            price = float(price_raw)
        except ValueError as e:
            raise ValidationError(f"parse error: {e}")
        if price < 0:
            raise ValidationError("negative price")
    if not row.get("user_id"):
        raise ValidationError("missing user_id")
    return {
        "event_time": row.get("event_time"),
        "event_type": row.get("event_type"),
        "product_id": row.get("product_id"),
        "category_id": row.get("category_id"),
        "category_code": row.get("category_code"),
        "brand": row.get("brand"),
        "price": price,
        "user_id": row.get("user_id"),
        "user_session": row.get("user_session"),
    }

@retry_exponential()
def write_to_sink(row: Dict[str, Any], writer):
    # Симуляция транзиентной ошибки sink
    if random.random() < 0.01:
        raise RuntimeError("simulated transient sink failure")
    writer.writerow(row)

def load_checkpoint() -> Optional[Dict[str, Any]]:
    if not os.path.exists(CHECKPOINT_PATH):
        return None
    try:
        with open(CHECKPOINT_PATH, "r") as f:
            return json.load(f)
    except Exception as e:
        logger.warning("Could not load checkpoint: %s", e)
        return None

def save_checkpoint(file_pos: int, processed: int):
    data = {"file_pos": file_pos, "processed": processed, "ts": time.time()}
    tmp = CHECKPOINT_PATH + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f)
    os.replace(tmp, CHECKPOINT_PATH)
    logger.info("Saved checkpoint: pos=%d processed=%d", file_pos, processed)

def ensure_file_with_header(path: str, header: list):
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(header)

def process_stream():
    checkpoint = load_checkpoint()
    start_pos = checkpoint["file_pos"] if checkpoint else 0
    processed = checkpoint["processed"] if checkpoint else 0
    logger.info("Starting processing from pos=%d processed=%d", start_pos, processed)

    fieldnames = ["event_time","event_type","product_id","category_id",
                  "category_code","brand","price","user_id","user_session"]

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True) if os.path.dirname(OUTPUT_PATH) else None
    os.makedirs(os.path.dirname(DLQ_PATH), exist_ok=True) if os.path.dirname(DLQ_PATH) else None
    ensure_file_with_header(OUTPUT_PATH, fieldnames)
    ensure_file_with_header(DLQ_PATH, fieldnames + ["error"])

    # Открываем с newline='' — рекомендовано для csv
    with open(INPUT_PATH, "r", newline="", encoding="utf-8") as infile, \
         open(OUTPUT_PATH, "a", newline="", encoding="utf-8") as outfile, \
         open(DLQ_PATH, "a", newline="", encoding="utf-8") as dlqfile:

        # читаем заголовок и получаем список полей
        header_line = infile.readline()
        if not header_line:
            logger.info("Input file is empty")
            return
        headers = next(csv.reader([header_line]))

        # Перемещаемся в позицию из чекпоинта (байтовая позиция)
        if start_pos:
            infile.seek(start_pos)

        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        dlq_writer = csv.DictWriter(dlqfile, fieldnames=fieldnames + ["error"])

        while True:
            # безопасно получить позицию до чтения строки
            pos_before = infile.tell()
            line = infile.readline()
            if not line:
                break
            # парсим строку в поля
            try:
                values = next(csv.reader([line]))
            except Exception as e:
                # если парсинг упал — отправляем в DLQ
                row_out = {k: "" for k in fieldnames}
                row_out["error"] = f"csv_parse_error: {e}"
                dlq_writer.writerow(row_out)
                processed += 1
                if processed % CHECKPOINT_EVERY == 0:
                    save_checkpoint(infile.tell(), processed)
                continue

            # Собираем словарь строки
            row = dict(zip(headers, values))
            try:
                if FAILURE_PROBABILITY > 0 and random.random() < FAILURE_PROBABILITY:
                    logger.error("Artificial crash triggered")
                    raise RuntimeError("Artificial crash for fault-tolerance test")

                try:
                    valid = validate_row(row)
                except ValidationError as ve:
                    row_out = dict(row)
                    row_out["error"] = str(ve)
                    dlq_writer.writerow(row_out)
                    logger.info("DLQ: %s", str(ve))
                    processed += 1
                    if processed % CHECKPOINT_EVERY == 0:
                        save_checkpoint(infile.tell(), processed)
                    continue

                try:
                    write_to_sink(valid, writer)
                except Exception as sink_exc:
                    row_out = dict(row)
                    row_out["error"] = f"sink_failure: {sink_exc}"
                    dlq_writer.writerow(row_out)
                    logger.error("Sent to DLQ due to sink failure: %s", sink_exc)
                    processed += 1
                    if processed % CHECKPOINT_EVERY == 0:
                        save_checkpoint(infile.tell(), processed)
                    continue

                processed += 1
                if processed % 100 == 0:
                    logger.info("Processed %d rows", processed)

                if processed % CHECKPOINT_EVERY == 0:
                    # здесь tell() безопасен, потому что мы не использовали итератор next()
                    save_checkpoint(infile.tell(), processed)

            except Exception as e:
                logger.exception("Unhandled processing exception at processed=%d: %s", processed, e)
                try:
                    save_checkpoint(infile.tell(), processed)
                except Exception as e2:
                    logger.exception("Failed to save checkpoint during crash: %s", e2)
                raise