import os
import re
import asyncio
import logging
import urllib.parse
import contextlib
import mimetypes
import time
from pathlib import Path
from uuid import uuid4
from typing import Optional, Tuple, Set

import httpx
from aiohttp import web
from telegram import Update
from telegram.constants import ParseMode
from telegram.error import TelegramError, TimedOut, NetworkError, BadRequest
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)
from telegram.request import HTTPXRequest
import yt_dlp

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("render-audio-bot")
logger.info("Booting service... v5")

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
RENDER_EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL", "").strip().rstrip("/")
PORT = int(os.getenv("PORT", "10000"))

MAX_FILE_SIZE_MB = int(os.getenv("MAX_FILE_SIZE_MB", "48"))
MAX_FILE_SIZE = MAX_FILE_SIZE_MB * 1024 * 1024

CONNECT_TIMEOUT = int(os.getenv("CONNECT_TIMEOUT", "20"))
READ_TIMEOUT = int(os.getenv("READ_TIMEOUT", "300"))
WRITE_TIMEOUT = int(os.getenv("WRITE_TIMEOUT", "300"))
POOL_TIMEOUT = int(os.getenv("POOL_TIMEOUT", "60"))

MAX_CONCURRENT_JOBS = int(os.getenv("MAX_CONCURRENT_JOBS", "3"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
PROGRESS_EDIT_INTERVAL = float(os.getenv("PROGRESS_EDIT_INTERVAL", "1.2"))
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", str(256 * 1024)))

DOWNLOAD_DIR = Path("./downloads")
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

ALLOWED_AUDIO_EXTENSIONS = {
    ".mp3", ".m4a", ".aac", ".ogg", ".wav", ".flac", ".opus"
}

AUDIO_MIME_HINTS = {
    "audio/mpeg": ".mp3",
    "audio/mp3": ".mp3",
    "audio/mp4": ".m4a",
    "audio/x-m4a": ".m4a",
    "audio/aac": ".aac",
    "audio/ogg": ".ogg",
    "audio/wav": ".wav",
    "audio/x-wav": ".wav",
    "audio/flac": ".flac",
    "audio/x-flac": ".flac",
    "audio/opus": ".opus",
    "audio/webm": ".opus",
}

URL_RE = re.compile(r"^https?://", re.IGNORECASE)


def sanitize_filename(name: str, max_len: int = 120) -> str:
    if not name:
        return "audio"
    name = re.sub(r'[\\/*?:"<>|]+', "_", name)
    name = re.sub(r"\s+", " ", name).strip(" .")
    return name[:max_len] or "audio"


def make_uid() -> str:
    return uuid4().hex[:10]


def html_escape(text: str) -> str:
    if text is None:
        return ""
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def is_url(text: str) -> bool:
    return bool(URL_RE.match(text or ""))


def get_extension_from_url(url: str) -> str:
    path = urllib.parse.urlparse(url).path
    return Path(path).suffix.lower()


def is_soundcloud_url(url: str) -> bool:
    return "soundcloud.com" in (url or "").lower()


def is_yandex_music_url(url: str) -> bool:
    u = (url or "").lower()
    return "music.yandex.ru" in u or "yandex.ru/music" in u or "ya.ru/music" in u


def format_size(num_bytes: Optional[int]) -> str:
    if num_bytes is None:
        return "неизвестно"
    units = ["B", "KB", "MB", "GB"]
    size = float(num_bytes)
    for unit in units:
        if size < 1024 or unit == units[-1]:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} GB"


def extension_from_content_type(content_type: Optional[str]) -> str:
    if not content_type:
        return ".mp3"
    ct = content_type.split(";")[0].strip().lower()
    if ct in AUDIO_MIME_HINTS:
        return AUDIO_MIME_HINTS[ct]
    guessed = mimetypes.guess_extension(ct)
    if guessed in ALLOWED_AUDIO_EXTENSIONS:
        return guessed
    if ct.startswith("audio/"):
        return ".mp3"
    return ".mp3"


def choose_filename(url: str, content_type: Optional[str] = None, fallback: Optional[str] = None) -> str:
    if fallback:
        raw_name = sanitize_filename(fallback)
    else:
        parsed = urllib.parse.urlparse(url)
        raw_name = sanitize_filename(Path(parsed.path).name or f"audio_{make_uid()}")

    suffix = Path(raw_name).suffix.lower()
    if suffix not in ALLOWED_AUDIO_EXTENSIONS:
        raw_name = f"{Path(raw_name).stem}{extension_from_content_type(content_type)}"
    return raw_name


def build_progress_bar(percent: float, width: int = 12) -> str:
    percent = max(0.0, min(100.0, percent))
    filled = round(width * percent / 100.0)
    return "█" * filled + "░" * (width - filled)


def render_progress_text(title: str, downloaded: int, total: Optional[int], phase: str) -> str:
    safe_title = html_escape(title or "Трек")
    if total and total > 0:
        percent = downloaded / total * 100
        bar = build_progress_bar(percent)
        return (
            f"🎵 <b>{phase}</b>\n"
            f"<b>{safe_title}</b>\n\n"
            f"<code>{bar}</code> <b>{percent:5.1f}%</b>\n"
            f"{html_escape(format_size(downloaded))} / {html_escape(format_size(total))}"
        )
    return (
        f"🎵 <b>{phase}</b>\n"
        f"<b>{safe_title}</b>\n\n"
        f"Уже скачано: <b>{html_escape(format_size(downloaded))}</b>"
    )


class ProgressThrottler:
    def __init__(self, interval_sec: float = 1.2):
        self.interval_sec = interval_sec
        self._last_ts = 0.0
        self._last_text = None

    def should_emit(self, text: str, force: bool = False) -> bool:
        now = time.monotonic()
        if force:
            self._last_ts = now
            self._last_text = text
            return True
        if text == self._last_text:
            return False
        if now - self._last_ts >= self.interval_sec:
            self._last_ts = now
            self._last_text = text
            return True
        return False


async def safe_edit(msg, text: str, throttler: Optional[ProgressThrottler] = None, force: bool = False) -> None:
    try:
        if throttler and not throttler.should_emit(text, force=force):
            return
        await msg.edit_text(text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    except BadRequest as e:
        if "Message is not modified" not in str(e):
            logger.exception("safe_edit bad request")
    except Exception:
        logger.exception("safe_edit failed")


def create_timeout() -> httpx.Timeout:
    return httpx.Timeout(
        connect=CONNECT_TIMEOUT,
        read=READ_TIMEOUT,
        write=WRITE_TIMEOUT,
        pool=POOL_TIMEOUT,
    )


async def http_request_with_retry(client: httpx.AsyncClient, method: str, url: str, **kwargs):
    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return await client.request(method, url, **kwargs)
        except Exception as e:
            last_exc = e
            logger.warning("HTTP %s failed attempt %s/%s for %s: %s", method, attempt, MAX_RETRIES, url, e)
            if attempt < MAX_RETRIES:
                await asyncio.sleep(min(2 ** (attempt - 1), 4))
    raise last_exc


async def fetch_head_info(url: str) -> Tuple[Optional[int], Optional[str]]:
    async with httpx.AsyncClient(timeout=create_timeout(), follow_redirects=True) as client:
        try:
            resp = await http_request_with_retry(client, "HEAD", url)
            content_length = resp.headers.get("content-length")
            content_type = resp.headers.get("content-type")
            size = int(content_length) if content_length and content_length.isdigit() else None
            return size, content_type
        except Exception:
            logger.warning("HEAD failed for %s, trying GET fallback", url, exc_info=True)

        try:
            resp = await http_request_with_retry(
                client,
                "GET",
                url,
                headers={"Range": "bytes=0-0"},
            )
            content_length = resp.headers.get("content-length")
            content_type = resp.headers.get("content-type")
            size = int(content_length) if content_length and content_length.isdigit() else None
            return size, content_type
        except Exception:
            logger.exception("GET fallback failed for %s", url)
            return None, None


async def detect_direct_audio(url: str) -> Tuple[bool, Optional[str], Optional[int]]:
    ext = get_extension_from_url(url)
    size, content_type = await fetch_head_info(url)

    if ext in ALLOWED_AUDIO_EXTENSIONS:
        return True, content_type, size

    if content_type:
        ct = content_type.split(";")[0].strip().lower()
        if ct.startswith("audio/"):
            return True, content_type, size

    return False, content_type, size


async def stream_download_file(
    url: str,
    output_path: Path,
    max_size: int,
    progress_cb=None,
) -> int:
    downloaded = 0

    async with httpx.AsyncClient(timeout=create_timeout(), follow_redirects=True) as client:
        last_exc = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                async with client.stream("GET", url) as resp:
                    resp.raise_for_status()

                    total = None
                    content_length = resp.headers.get("content-length")
                    if content_length and content_length.isdigit():
                        total = int(content_length)
                        if total > max_size:
                            raise ValueError(
                                f"Файл получился слишком большим: {format_size(total)}. "
                                f"Лимит — {format_size(max_size)}."
                            )

                    with output_path.open("wb") as f:
                        async for chunk in resp.aiter_bytes(chunk_size=CHUNK_SIZE):
                            if not chunk:
                                continue
                            downloaded += len(chunk)
                            if downloaded > max_size:
                                raise ValueError(
                                    f"Размер превысил лимит {format_size(max_size)} во время загрузки."
                                )
                            f.write(chunk)
                            if progress_cb:
                                await progress_cb(downloaded, total)

                    return downloaded

            except Exception as e:
                last_exc = e
                logger.warning("Download failed attempt %s/%s for %s: %s", attempt, MAX_RETRIES, url, e)
                with contextlib.suppress(Exception):
                    output_path.unlink(missing_ok=True)
                if attempt < MAX_RETRIES:
                    downloaded = 0
                    await asyncio.sleep(min(2 ** (attempt - 1), 4))
                else:
                    raise last_exc


async def download_with_ytdlp(
    url: str,
    output_path: Path,
    max_size: int,
    progress_msg=None,
) -> Tuple[int, str, Optional[str]]:
    loop = asyncio.get_running_loop()
    throttler = ProgressThrottler(PROGRESS_EDIT_INTERVAL)

    async def update_stage(text: str):
        if progress_msg:
            await safe_edit(progress_msg, text, throttler=throttler)

    await update_stage(
        "🎧 <b>Ловлю ссылку на аудио…</b>\n"
        "Секунду, сейчас аккуратно всё подготовлю ✨"
    )

    ydl_opts = {
        "format": "bestaudio/best",
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "socket_timeout": CONNECT_TIMEOUT,
        "noplaylist": True,
    }

    def extract():
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            return ydl.extract_info(url, download=False)

    info = await loop.run_in_executor(None, extract)

    title = sanitize_filename(info.get("title") or "Track")
    direct_url = info.get("url")
    ext = info.get("ext")
    if not direct_url:
        raise ValueError("Не удалось получить прямую ссылку на аудио через yt-dlp.")

    content_type = None
    if ext:
        ext = f".{ext.lower().lstrip('.')}"
        if ext in ALLOWED_AUDIO_EXTENSIONS:
            content_type = mimetypes.guess_type(f"file{ext}")[0]

    async def progress_cb(downloaded: int, total: Optional[int]):
        if not progress_msg:
            return
        text = render_progress_text(title, downloaded, total, "Скачиваю трек")
        await safe_edit(progress_msg, text, throttler=throttler)

    downloaded = await stream_download_file(
        direct_url,
        output_path,
        max_size,
        progress_cb=progress_cb,
    )
    return downloaded, title, content_type


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return

    await update.message.reply_text(
        "Привет! 🎶\n\n"
        "Я с радостью помогу достать аудио по ссылке.\n\n"
        "Отправь мне:\n"
        "• прямую ссылку на аудиофайл\n"
        "• трек с SoundCloud\n"
        "• трек с Яндекс.Музыки\n\n"
        f"Лимит на файл: {MAX_FILE_SIZE_MB} MB 💫"
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return

    await update.message.reply_text(
        "Вот что я умею 🎵\n\n"
        "• прямые ссылки на аудиофайлы\n"
        "• SoundCloud\n"
        "• Яндекс.Музыка\n\n"
        f"Максимальный размер файла: {MAX_FILE_SIZE_MB} MB\n\n"
        "Просто пришли ссылку — остальное беру на себя ✨"
    )


async def ping(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    await update.message.reply_text("Я на месте и в отличном настроении 😄")


async def process_audio_request(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.message.text:
        return

    url = update.message.text.strip()
    logger.info("Received URL: %s", url)

    if not is_url(url):
        await update.message.reply_text(
            "Пришли, пожалуйста, ссылку формата http/https 🌷"
        )
        return

    semaphore: asyncio.Semaphore = context.application.bot_data["download_semaphore"]

    if semaphore.locked() and semaphore._value == 0:
        await update.message.reply_text(
            "Сейчас у меня уже есть несколько загрузок в работе 🙏\n"
            "Попробуй ещё раз чуть-чуть позже."
        )
        return

    status_msg = await update.message.reply_text(
        "🎵 <b>Поехали!</b>\nСейчас посмотрю ссылку и начну загрузку…",
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )
    throttler = ProgressThrottler(PROGRESS_EDIT_INTERVAL)
    tmp_path: Optional[Path] = None

    async with semaphore:
        try:
            is_direct, content_type, size = await detect_direct_audio(url)

            if is_direct:
                filename = choose_filename(url, content_type)
                tmp_path = DOWNLOAD_DIR / f"{make_uid()}_{filename}"
                title = sanitize_filename(Path(filename).stem)

                await safe_edit(
                    status_msg,
                    (
                        f"🎶 <b>Нашёл прямой аудиофайл!</b>\n"
                        f"<b>{html_escape(title)}</b>\n\n"
                        f"Размер: <b>{html_escape(format_size(size))}</b>\n"
                        "Начинаю скачивание 🚀"
                    ),
                    throttler=throttler,
                    force=True,
                )

                if size is not None and size > MAX_FILE_SIZE:
                    await safe_edit(
                        status_msg,
                        (
                            "😔 <b>Файл слишком большой.</b>\n\n"
                            f"Размер: <b>{html_escape(format_size(size))}</b>\n"
                            f"Лимит: <b>{html_escape(format_size(MAX_FILE_SIZE))}</b>"
                        ),
                        throttler=throttler,
                        force=True,
                    )
                    return

                async def progress_cb(downloaded: int, total: Optional[int]):
                    text = render_progress_text(title, downloaded, total, "Скачиваю файл")
                    await safe_edit(status_msg, text, throttler=throttler)

                downloaded = await stream_download_file(
                    url,
                    tmp_path,
                    MAX_FILE_SIZE,
                    progress_cb=progress_cb,
                )

            elif is_soundcloud_url(url) or is_yandex_music_url(url):
                filename = f"yt_{make_uid()}.m4a"
                tmp_path = DOWNLOAD_DIR / filename

                await safe_edit(
                    status_msg,
                    "🎧 <b>Отлично!</b>\nРаспознаю трек и готовлю загрузку…",
                    throttler=throttler,
                    force=True,
                )

                downloaded, title, detected_ct = await download_with_ytdlp(
                    url,
                    tmp_path,
                    MAX_FILE_SIZE,
                    progress_msg=status_msg,
                )
                if detected_ct and tmp_path.suffix.lower() not in ALLOWED_AUDIO_EXTENSIONS:
                    new_name = tmp_path.with_suffix(extension_from_content_type(detected_ct))
                    with contextlib.suppress(Exception):
                        tmp_path.rename(new_name)
                        tmp_path = new_name

            else:
                await safe_edit(
                    status_msg,
                    (
                        "Пока я уверенно поддерживаю только эти варианты 💛\n\n"
                        "• прямые ссылки на аудиофайлы\n"
                        "• SoundCloud\n"
                        "• Яндекс.Музыка"
                    ),
                    throttler=throttler,
                    force=True,
                )
                return

            await safe_edit(
                status_msg,
                (
                    f"📤 <b>Готово, отправляю!</b>\n"
                    f"<b>{html_escape(title)}</b>\n"
                    f"Размер: <b>{html_escape(format_size(downloaded))}</b>\n\n"
                    "Ещё секундочка ✨"
                ),
                throttler=throttler,
                force=True,
            )

            sent = False
            with tmp_path.open("rb") as f:
                try:
                    await update.message.reply_audio(
                        audio=f,
                        filename=tmp_path.name,
                        title=title,
                        performer="Sunny Audio Bot",
                        read_timeout=READ_TIMEOUT,
                        write_timeout=WRITE_TIMEOUT,
                        connect_timeout=CONNECT_TIMEOUT,
                    )
                    sent = True
                except TelegramError:
                    logger.exception("reply_audio failed, fallback to document")

            if not sent:
                with tmp_path.open("rb") as f:
                    await update.message.reply_document(
                        document=f,
                        filename=tmp_path.name,
                        caption=f"🎵 {title}",
                        read_timeout=READ_TIMEOUT,
                        write_timeout=WRITE_TIMEOUT,
                        connect_timeout=CONNECT_TIMEOUT,
                    )

            with contextlib.suppress(Exception):
                await status_msg.delete()

        except asyncio.CancelledError:
            logger.warning("Task cancelled for url=%s", url)
            with contextlib.suppress(Exception):
                await safe_edit(
                    status_msg,
                    "⚠️ Загрузка была остановлена из-за перезапуска сервиса.",
                    force=True,
                )
            raise
        except Exception as e:
            logger.exception("process_audio_request error")
            with contextlib.suppress(Exception):
                await safe_edit(
                    status_msg,
                    (
                        "😔 <b>Упс, что-то пошло не так.</b>\n\n"
                        f"<code>{html_escape(str(e))}</code>\n\n"
                        "Попробуй ещё раз или пришли другую ссылку."
                    ),
                    throttler=throttler,
                    force=True,
                )
        finally:
            if tmp_path:
                with contextlib.suppress(Exception):
                    tmp_path.unlink(missing_ok=True)


async def handle_link(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    app = context.application
    task = asyncio.create_task(process_audio_request(update, context))
    tasks: Set[asyncio.Task] = app.bot_data["background_tasks"]
    tasks.add(task)

    def _cleanup(t: asyncio.Task):
        tasks.discard(t)
        with contextlib.suppress(Exception):
            t.result()

    task.add_done_callback(_cleanup)


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.exception("Unhandled bot exception", exc_info=context.error)


def build_ptb_app() -> Application:
    request = HTTPXRequest(
        connection_pool_size=8,
        http_version="1.1",
        read_timeout=READ_TIMEOUT,
        write_timeout=WRITE_TIMEOUT,
        connect_timeout=CONNECT_TIMEOUT,
        pool_timeout=POOL_TIMEOUT,
    )

    app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .request(request)
        .build()
    )

    app.bot_data["background_tasks"] = set()
    app.bot_data["download_semaphore"] = asyncio.Semaphore(MAX_CONCURRENT_JOBS)

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("ping", ping))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_link))
    app.add_error_handler(error_handler)
    return app


async def health(request: web.Request) -> web.Response:
    app: web.Application = request.app
    bg_tasks = len(app["ptb_app"].bot_data.get("background_tasks", set()))
    return web.json_response({"ok": True, "background_tasks": bg_tasks})


async def telegram_webhook(request: web.Request) -> web.Response:
    ptb_app: Application = request.app["ptb_app"]
    data = await request.json()
    update = Update.de_json(data, ptb_app.bot)

    logger.info("Webhook update received")
    asyncio.create_task(ptb_app.process_update(update))

    return web.json_response({"ok": True})


async def on_startup(aio_app: web.Application) -> None:
    logger.info("Starting app...")
    logger.info("PORT=%s", PORT)
    logger.info("BOT_TOKEN set=%s", bool(BOT_TOKEN))
    logger.info("RENDER_EXTERNAL_URL=%s", RENDER_EXTERNAL_URL or "<empty>")

    if not BOT_TOKEN:
        raise RuntimeError("Не задан BOT_TOKEN в переменных окружения.")
    if not RENDER_EXTERNAL_URL:
        raise RuntimeError("Не задан RENDER_EXTERNAL_URL в переменных окружения.")

    ptb_app: Application = aio_app["ptb_app"]

    logger.info("Initializing telegram application...")
    await ptb_app.initialize()

    logger.info("Starting telegram application...")
    await ptb_app.start()

    webhook_url = f"{RENDER_EXTERNAL_URL}/telegram"
    logger.info("Setting webhook to %s", webhook_url)
    await ptb_app.bot.set_webhook(url=webhook_url, drop_pending_updates=True)
    logger.info("Webhook set successfully")


async def on_shutdown(aio_app: web.Application) -> None:
    logger.info("Shutting down...")
    ptb_app: Application = aio_app["ptb_app"]

    tasks: Set[asyncio.Task] = ptb_app.bot_data.get("background_tasks", set())
    if tasks:
        logger.info("Cancelling %s background task(s)", len(tasks))
        for task in list(tasks):
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    with contextlib.suppress(Exception):
        await ptb_app.bot.delete_webhook()

    with contextlib.suppress(Exception):
        await ptb_app.stop()

    with contextlib.suppress(Exception):
        await ptb_app.shutdown()


def create_web_app() -> web.Application:
    aio_app = web.Application(client_max_size=2 * 1024 * 1024)
    aio_app["ptb_app"] = build_ptb_app()
    aio_app.router.add_get("/", health)
    aio_app.router.add_get("/health", health)
    aio_app.router.add_post("/telegram", telegram_webhook)
    aio_app.on_startup.append(on_startup)
    aio_app.on_shutdown.append(on_shutdown)
    return aio_app


if __name__ == "__main__":
    try:
        logger.info("Booting service...")
        app = create_web_app()
        logger.info("Running aiohttp on 0.0.0.0:%s", PORT)
        web.run_app(app, host="0.0.0.0", port=PORT)
    except Exception:
        logger.exception("Fatal startup error")
        raise
