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
from typing import Optional, Tuple, Set, Dict

import httpx
from aiohttp import web
from telegram import Update
from telegram.constants import ParseMode
from telegram.error import TelegramError, BadRequest
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
logger.info("Booting service... stable")

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
BOT_USERNAME_ENV = os.getenv("BOT_USERNAME", "").strip().lstrip("@")
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
    ".mp3", ".m4a", ".aac", ".ogg", ".wav", ".flac", ".opus", ".webm"
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
    "audio/webm": ".webm",
}

URL_RE = re.compile(r"^https?://", re.IGNORECASE)


def sanitize_filename(name: str, max_len: int = 160) -> str:
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


def format_track_title(artist: Optional[str], track: Optional[str], fallback: str = "Unknown Artist - Unknown Track") -> str:
    artist = (artist or "").strip()
    track = (track or "").strip()

    if artist and track:
        return sanitize_filename(f"{artist} - {track}")
    if track:
        return sanitize_filename(track)
    if artist:
        return sanitize_filename(artist)
    return sanitize_filename(fallback)


async def stream_download_file(url: str, output_path: Path, max_size: int, progress_cb=None) -> int:
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
                                f"Файл слишком большой: {format_size(total)}. "
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


async def download_with_ytdlp(url: str, output_dir: Path, max_size: int, progress_msg=None) -> Tuple[Path, str]:
    loop = asyncio.get_running_loop()
    throttler = ProgressThrottler(PROGRESS_EDIT_INTERVAL)
    uid = make_uid()
    outtmpl = str(output_dir / f"{uid}.%(ext)s")

    def progress_hook(d):
        if not progress_msg:
            return

        status = d.get("status")
        if status == "downloading":
            total = d.get("total_bytes") or d.get("total_bytes_estimate")
            downloaded = d.get("downloaded_bytes", 0)
            filename = d.get("filename") or "Трек"
            title = Path(filename).stem

            text = render_progress_text(title, downloaded, total, "Скачиваю трек")
            asyncio.run_coroutine_threadsafe(
                safe_edit(progress_msg, text, throttler=throttler),
                loop,
            )

        elif status == "finished":
            filename = d.get("filename") or "Трек"
            title = Path(filename).stem
            text = (
                f"✨ <b>Супер, файл уже у меня!</b>\n"
                f"<b>{html_escape(title)}</b>\n\n"
                "Осталось совсем чуть-чуть — подготавливаю отправку 🎧"
            )
            asyncio.run_coroutine_threadsafe(
                safe_edit(progress_msg, text, throttler=throttler, force=True),
                loop,
            )

    ydl_opts = {
        "format": "bestaudio/best",
        "outtmpl": outtmpl,
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "socket_timeout": CONNECT_TIMEOUT,
        "progress_hooks": [progress_hook],
        "nopart": False,
    }

    def run_download():
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=True)

                requested = info.get("requested_downloads")
                filepath = None
                if requested and isinstance(requested, list):
                    filepath = requested[0].get("filepath")

                if not filepath:
                    filepath = ydl.prepare_filename(info)

                file_path = Path(filepath)
                artist = info.get("artist") or info.get("uploader") or info.get("creator")
                track = info.get("track") or info.get("title")
                title = format_track_title(artist, track, fallback=file_path.stem)

                return file_path, title
        except Exception as e:
            msg = str(e)
            if "HTTP Error 451" in msg or "Unavailable For Legal Reasons" in msg:
                raise ValueError(
                    "Этот трек недоступен из текущего региона сервера или ограничен правообладателем."
                )
            raise

    file_path, title = await loop.run_in_executor(None, run_download)

    if not file_path.exists():
        raise ValueError("yt-dlp сообщил об успешной загрузке, но файл не найден.")

    size = file_path.stat().st_size
    if size <= 0:
        raise ValueError("Скачанный файл пустой.")
    if size < 1024:
        raise ValueError("Скачался подозрительно маленький файл. Похоже, источник не отдал аудио.")
    if size > max_size:
        with contextlib.suppress(Exception):
            file_path.unlink(missing_ok=True)
        raise ValueError(
            f"Файл слишком большой: {format_size(size)}. Лимит — {format_size(max_size)}."
        )

    return file_path, title


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message:
        await update.message.reply_text(
            "Привет! 🎉\n\n"
            "Отправь ссылку на трек, и я постараюсь скачать его для тебя 🎶\n\n"
            "Команды:\n"
            "/status — показать статус\n"
            "/cancel — отменить текущую загрузку"
        )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message:
        await update.message.reply_text(
            "Поддерживаются:\n"
            "• прямые ссылки на аудио\n"
            "• SoundCloud\n"
            "• Яндекс.Музыка\n\n"
            "Команды:\n"
            "/status\n"
            "/cancel"
        )


async def ping(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message:
        await update.message.reply_text("Я здесь и работаю 😄")


async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.message:
        return

    chat_id = update.effective_chat.id
    active_jobs: Dict[int, asyncio.Task] = context.application.bot_data["active_chat_jobs"]

    task = active_jobs.get(chat_id)
    if task and not task.done():
        await update.message.reply_text("Сейчас в этом чате идёт загрузка 🎵")
    else:
        await update.message.reply_text("Чат свободен, можно отправлять новую ссылку ✨")


async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.message:
        return

    chat_id = update.effective_chat.id
    active_jobs: Dict[int, asyncio.Task] = context.application.bot_data["active_chat_jobs"]
    task = active_jobs.get(chat_id)

    if task and not task.done():
        task.cancel()
        await update.message.reply_text("Текущую загрузку отменил 🛑")
    else:
        await update.message.reply_text("Сейчас нечего отменять 🙂")


async def process_audio_request(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.message.text or not update.effective_chat:
        return

    url = update.message.text.strip()
    chat_id = update.effective_chat.id
    semaphore: asyncio.Semaphore = context.application.bot_data["download_semaphore"]
    performer_name = context.application.bot_data.get("performer_name", "@userbot")
    active_jobs: Dict[int, asyncio.Task] = context.application.bot_data["active_chat_jobs"]

    status_msg = await update.message.reply_text(
        "🎵 <b>Погнали!</b>\nСейчас посмотрю ссылку и начну загрузку…",
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )
    throttler = ProgressThrottler(PROGRESS_EDIT_INTERVAL)
    tmp_path: Optional[Path] = None

    try:
        async with semaphore:
            is_direct, content_type, size = await detect_direct_audio(url)

            if is_direct:
                filename = choose_filename(url, content_type)
                pretty_title = sanitize_filename(Path(filename).stem)
                send_title = format_track_title(None, pretty_title, fallback=pretty_title)
                tmp_path = DOWNLOAD_DIR / f"{make_uid()}_{filename}"

                await safe_edit(
                    status_msg,
                    (
                        f"🎶 <b>Нашёл прямой аудиофайл!</b>\n"
                        f"<b>{html_escape(send_title)}</b>\n\n"
                        f"Размер: <b>{html_escape(format_size(size))}</b>\n"
                        "Начинаю скачивание 🚀"
                    ),
                    throttler=throttler,
                    force=True,
                )

                if size is not None and size > MAX_FILE_SIZE:
                    raise ValueError(
                        f"Файл слишком большой: {format_size(size)}. Лимит — {format_size(MAX_FILE_SIZE)}."
                    )

                async def progress_cb(downloaded: int, total: Optional[int]):
                    text = render_progress_text(send_title, downloaded, total, "Скачиваю файл")
                    await safe_edit(status_msg, text, throttler=throttler)

                downloaded = await stream_download_file(
                    url,
                    tmp_path,
                    MAX_FILE_SIZE,
                    progress_cb=progress_cb,
                )

                if not tmp_path.exists():
                    raise ValueError("Файл не был создан.")
                real_size = tmp_path.stat().st_size
                if downloaded <= 0 or real_size <= 0:
                    raise ValueError("Скачанный файл пустой.")
                if real_size < 1024:
                    raise ValueError("Скачался слишком маленький файл. Похоже, источник не отдал аудио.")

            elif is_soundcloud_url(url) or is_yandex_music_url(url):
                tmp_path, send_title = await download_with_ytdlp(
                    url,
                    DOWNLOAD_DIR,
                    MAX_FILE_SIZE,
                    progress_msg=status_msg,
                )
                downloaded = tmp_path.stat().st_size

            else:
                raise ValueError(
                    "Пока поддерживаются только прямые аудиоссылки, SoundCloud и Яндекс.Музыка."
                )

            await safe_edit(
                status_msg,
                (
                    f"📤 <b>Готово, отправляю!</b>\n"
                    f"<b>{html_escape(send_title)}</b>\n"
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
                        title=send_title,
                        performer=performer_name,
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
                        caption=f"🎵 {send_title}",
                        read_timeout=READ_TIMEOUT,
                        write_timeout=WRITE_TIMEOUT,
                        connect_timeout=CONNECT_TIMEOUT,
                    )

            with contextlib.suppress(Exception):
                await status_msg.delete()

    except asyncio.CancelledError:
        with contextlib.suppress(Exception):
            await safe_edit(
                status_msg,
                "🛑 <b>Загрузка отменена.</b>\nМожно отправлять новую ссылку.",
                force=True,
            )
        raise
    except Exception as e:
        logger.exception("process_audio_request error")
        with contextlib.suppress(Exception):
            await safe_edit(
                status_msg,
                (
                    "😔 <b>Не получилось обработать ссылку.</b>\n\n"
                    f"{html_escape(str(e))}\n\n"
                    "Попробуй ещё раз или пришли другую ссылку."
                ),
                throttler=throttler,
                force=True,
            )
    finally:
        active_jobs.pop(chat_id, None)
        if tmp_path:
            with contextlib.suppress(Exception):
                tmp_path.unlink(missing_ok=True)


async def handle_link(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.message:
        return

    chat_id = update.effective_chat.id
    active_jobs: Dict[int, asyncio.Task] = context.application.bot_data["active_chat_jobs"]

    current_task = active_jobs.get(chat_id)
    if current_task and not current_task.done():
        await update.message.reply_text(
            "В этом чате уже идёт загрузка 🎵\n"
            "Дождись завершения или нажми /cancel"
        )
        return

    current = asyncio.current_task()
    if current is not None:
        active_jobs[chat_id] = current

    await process_audio_request(update, context)


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

    app.bot_data["active_chat_jobs"] = {}
    app.bot_data["webhook_tasks"] = set()
    app.bot_data["download_semaphore"] = asyncio.Semaphore(MAX_CONCURRENT_JOBS)
    app.bot_data["performer_name"] = f"@{BOT_USERNAME_ENV}" if BOT_USERNAME_ENV else "@userbot"

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("ping", ping))
    app.add_handler(CommandHandler("status", status_command))
    app.add_handler(CommandHandler("cancel", cancel_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_link))
    app.add_error_handler(error_handler)
    return app


async def health(request: web.Request) -> web.Response:
    ptb_app: Application = request.app["ptb_app"]
    active_jobs = len(ptb_app.bot_data.get("active_chat_jobs", {}))
    webhook_tasks = len(ptb_app.bot_data.get("webhook_tasks", set()))
    return web.json_response(
        {
            "ok": True,
            "active_chat_jobs": active_jobs,
            "webhook_tasks": webhook_tasks,
        }
    )


async def telegram_webhook(request: web.Request) -> web.Response:
    ptb_app: Application = request.app["ptb_app"]
    data = await request.json()
    update = Update.de_json(data, ptb_app.bot)

    task = asyncio.create_task(ptb_app.process_update(update))
    webhook_tasks: Set[asyncio.Task] = ptb_app.bot_data["webhook_tasks"]
    webhook_tasks.add(task)

    def _cleanup(t: asyncio.Task):
        webhook_tasks.discard(t)
        try:
            t.result()
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception("Webhook task failed")

    task.add_done_callback(_cleanup)

    return web.json_response({"ok": True})


async def on_startup(aio_app: web.Application) -> None:
    if not BOT_TOKEN:
        raise RuntimeError("Не задан BOT_TOKEN.")
    if not RENDER_EXTERNAL_URL:
        raise RuntimeError("Не задан RENDER_EXTERNAL_URL.")

    ptb_app: Application = aio_app["ptb_app"]

    await ptb_app.initialize()

    try:
        me = await ptb_app.bot.get_me()
        if me.username:
            ptb_app.bot_data["performer_name"] = f"@{me.username}"
            logger.info("Resolved bot username: %s", ptb_app.bot_data["performer_name"])
    except Exception:
        logger.exception("Failed to resolve bot username")

    await ptb_app.start()

    webhook_url = f"{RENDER_EXTERNAL_URL}/telegram"
    await ptb_app.bot.set_webhook(url=webhook_url, drop_pending_updates=True)
    logger.info("Webhook set: %s", webhook_url)


async def on_shutdown(aio_app: web.Application) -> None:
    ptb_app: Application = aio_app["ptb_app"]

    tasks = set(ptb_app.bot_data.get("webhook_tasks", set()))
    if tasks:
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
    logger.info("Running aiohttp on 0.0.0.0:%s", PORT)
    web.run_app(create_web_app(), host="0.0.0.0", port=PORT)
