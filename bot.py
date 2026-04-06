import os
import re
import asyncio
import logging
import urllib.parse
from pathlib import Path
from uuid import uuid4

import httpx
from aiohttp import web
from telegram import Update
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)
from telegram.request import HTTPXRequest
import yt_dlp  # не забудь добавить в requirements.txt

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("render-audio-bot")

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
RENDER_EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL", "").strip().rstrip("/")
PORT = int(os.getenv("PORT", "10000"))

MAX_FILE_SIZE_MB = int(os.getenv("MAX_FILE_SIZE_MB", "48"))
MAX_FILE_SIZE = MAX_FILE_SIZE_MB * 1024 * 1024

CONNECT_TIMEOUT = int(os.getenv("CONNECT_TIMEOUT", "20"))
READ_TIMEOUT = int(os.getenv("READ_TIMEOUT", "300"))
WRITE_TIMEOUT = int(os.getenv("WRITE_TIMEOUT", "300"))
POOL_TIMEOUT = int(os.getenv("POOL_TIMEOUT", "60"))

DOWNLOAD_DIR = Path("./downloads")
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

ALLOWED_AUDIO_EXTENSIONS = {
    ".mp3", ".m4a", ".aac", ".ogg", ".wav", ".flac", ".opus"
}


def sanitize_filename(name: str, max_len: int = 120) -> str:
    if not name:
        return "audio"
    name = re.sub(r'[\\/*?:"<>|]+', "_", name)
    name = re.sub(r"\s+", " ", name).strip(" .")
    return name[:max_len] or "audio"


def make_uid() -> str:
    return uuid4().hex[:10]


def escape_md(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"([_*[\]()~`>#+\-=|{}.!])", r"\\\1", text)


def is_url(text: str) -> bool:
    return text.startswith("http://") or text.startswith("https://")


def get_extension_from_url(url: str) -> str:
    path = urllib.parse.urlparse(url).path
    return Path(path).suffix.lower()


def is_direct_audio_url(url: str) -> bool:
    return get_extension_from_url(url) in ALLOWED_AUDIO_EXTENSIONS


def is_soundcloud_url(url: str) -> bool:
    return "soundcloud.com" in url.lower()


def is_yandex_music_url(url: str) -> bool:
    u = url.lower()
    return "music.yandex.ru" in u or "yandex.ru/music" in u or "ya.ru/music" in u


def format_size(num_bytes: int) -> str:
    units = ["B", "KB", "MB", "GB"]
    size = float(num_bytes)
    for unit in units:
        if size < 1024 or unit == units[-1]:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} GB"


def choose_filename_from_url(url: str) -> str:
    parsed = urllib.parse.urlparse(url)
    raw_name = Path(parsed.path).name
    raw_name = sanitize_filename(raw_name or f"audio_{make_uid()}.mp3")
    if not Path(raw_name).suffix:
        raw_name += ".mp3"
    return raw_name


async def safe_edit(msg, text: str) -> None:
    try:
        await msg.edit_text(text, parse_mode="Markdown")
    except Exception:
        pass


async def fetch_head_info(url: str):
    timeout = httpx.Timeout(
        connect=CONNECT_TIMEOUT,
        read=READ_TIMEOUT,
        write=WRITE_TIMEOUT,
        pool=POOL_TIMEOUT,
    )
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
        try:
            resp = await client.head(url)
            content_length = resp.headers.get("content-length")
            content_type = resp.headers.get("content-type")
            size = int(content_length) if content_length and content_length.isdigit() else None
            return size, content_type
        except Exception:
            return None, None


async def stream_download_file(url: str, output_path: Path, max_size: int) -> int:
    downloaded = 0
    timeout = httpx.Timeout(
        connect=CONNECT_TIMEOUT,
        read=READ_TIMEOUT,
        write=WRITE_TIMEOUT,
        pool=POOL_TIMEOUT,
    )

    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
        async with client.stream("GET", url) as resp:
            resp.raise_for_status()

            content_length = resp.headers.get("content-length")
            if content_length and content_length.isdigit():
                total = int(content_length)
                if total > max_size:
                    raise ValueError(
                        f"Файл слишком большой: {format_size(total)}. "
                        f"Лимит: {format_size(max_size)}."
                    )

            with output_path.open("wb") as f:
                async for chunk in resp.aiter_bytes(chunk_size=256 * 1024):
                    if not chunk:
                        continue
                    downloaded += len(chunk)
                    if downloaded > max_size:
                        raise ValueError(
                            f"Размер превысил лимит {format_size(max_size)} во время скачивания."
                        )
                    f.write(chunk)

    return downloaded


async def download_with_ytdlp(url: str, output_path: Path, max_size: int) -> tuple[int, str]:
    """
    Универсальный загрузчик для SoundCloud / Яндекс.Музыки через yt-dlp.
    yt-dlp сам разруливает форматы и API этих сервисов.
    """
    ydl_opts = {
        "format": "bestaudio/best",
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
    }

    loop = asyncio.get_running_loop()
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = await loop.run_in_executor(None, lambda: ydl.extract_info(url, download=False))

    title = sanitize_filename(info.get("title") or "Track")
    direct_url = info.get("url")
    if not direct_url:
        raise ValueError("yt-dlp не вернул прямую ссылку на аудио")

    downloaded = await stream_download_file(direct_url, output_path, max_size)
    return downloaded, title


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "Привет.\n\n"
        "Отправь ссылку:\n"
        "• Прямой аудиофайл (.mp3, .m4a, .aac, .ogg, .wav, .flac, .opus)\n"
        "• Трек с SoundCloud\n"
        "• Трек с Яндекс.Музыки"
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        f"✅ Поддерживается:\n"
        f"• Прямые ссылки на аудиофайлы (.mp3, .m4a, .aac, .ogg, .wav, .flac, .opus)\n"
        f"• SoundCloud треки\n"
        f"• Яндекс.Музыка треки\n\n"
        f"⚠️ Лимит файла: {MAX_FILE_SIZE_MB} MB."
    )


async def ping(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("pong")


async def handle_link(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.message.text:
        return

    url = update.message.text.strip()
    if not is_url(url):
        await update.message.reply_text("Пришли http/https ссылку на аудио или трек.")
        return

    status_msg = await update.message.reply_text("🔍 Анализирую ссылку...")

    try:
        # Прямой файл
        if is_direct_audio_url(url):
            filename = choose_filename_from_url(url)
            tmp_path = DOWNLOAD_DIR / f"{make_uid()}_{filename}"

            await safe_edit(status_msg, "📥 Скачиваю прямой файл...")
            size, _ = await fetch_head_info(url)
            if size is not None and size > MAX_FILE_SIZE:
                await safe_edit(
                    status_msg,
                    f"❌ Файл слишком большой: *{escape_md(format_size(size))}*\n\n"
                    f"Лимит: *{escape_md(format_size(MAX_FILE_SIZE))}*",
                )
                return

            downloaded = await stream_download_file(url, tmp_path, MAX_FILE_SIZE)
            title = sanitize_filename(Path(filename).stem)

        # SC / ЯМ через yt-dlp
        elif is_soundcloud_url(url) or is_yandex_music_url(url):
            filename = f"yt_{make_uid()}.m4a"
            tmp_path = DOWNLOAD_DIR / filename
            await safe_edit(status_msg, "🎵 Получаю трек через yt-dlp...")
            downloaded, title = await download_with_ytdlp(url, tmp_path, MAX_FILE_SIZE)

        else:
            await safe_edit(
                status_msg,
                "❌ Пока поддерживаются только:\n"
                "• Прямые аудиофайлы\n"
                "• SoundCloud\n"
                "• Яндекс.Музыка"
            )
            return

        await safe_edit(
            status_msg,
            f"📤 Отправляю: *{escape_md(title)}*\n"
            f"Размер: *{escape_md(format_size(downloaded))}*"
        )

        try:
            with tmp_path.open("rb") as f:
                await update.message.reply_audio(
                    audio=f,
                    filename=tmp_path.name,
                    title=title,
                    performer="Audio bot",
                    read_timeout=READ_TIMEOUT,
                    write_timeout=WRITE_TIMEOUT,
                    connect_timeout=CONNECT_TIMEOUT,
                )
        finally:
            try:
                tmp_path.unlink(missing_ok=True)
            except Exception:
                pass

        try:
            await status_msg.delete()
        except Exception:
            pass

    except Exception as e:
        logger.exception("handle_link error")
        await safe_edit(status_msg, f"❌ Ошибка: `{escape_md(str(e))}`")


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
        # НИЧЕГО больше тут не добавляем, особенно .get_updates_request()
        .build()
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("ping", ping))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_link))
    app.add_error_handler(error_handler)
    return app


async def health(request: web.Request) -> web.Response:
    return web.json_response({"ok": True})


async def telegram_webhook(request: web.Request) -> web.Response:
    ptb_app: Application = request.app["ptb_app"]
    data = await request.json()
    update = Update.de_json(data, ptb_app.bot)
    await ptb_app.process_update(update)
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
    try:
        await ptb_app.bot.delete_webhook()
    except Exception:
        logger.exception("Failed deleting webhook")
    await ptb_app.stop()
    await ptb_app.shutdown()


def create_web_app() -> web.Application:
    aio_app = web.Application()
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
