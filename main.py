import os
import re
import html
import asyncio
import urllib.parse
from pathlib import Path
from uuid import uuid4
from concurrent.futures import ThreadPoolExecutor

import httpx
import yt_dlp
import imageio_ffmpeg

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder,
    MessageHandler,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)
from telegram.request import HTTPXRequest


BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()

BASE_DIR = Path(__file__).resolve().parent
DOWNLOAD_DIR = BASE_DIR / "downloads"
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

MAX_WORKERS = 2
MAX_FILE_MB = 49
EXECUTOR = ThreadPoolExecutor(max_workers=MAX_WORKERS)
DOWNLOAD_SEMAPHORE = asyncio.Semaphore(MAX_WORKERS)


def get_ffmpeg_path():
    try:
        return imageio_ffmpeg.get_ffmpeg_exe()
    except Exception:
        return None


FFMPEG_PATH = get_ffmpeg_path()


def sanitize_filename(name: str, max_len: int = 120) -> str:
    if not name:
        return "file"
    name = re.sub(r'[\\/*?:"<>|]+', "_", name)
    name = re.sub(r"\s+", " ", name).strip(" .")
    return name[:max_len] or "file"


def escape_md(text: str) -> str:
    if not text:
        return ""
    return re.sub(r'([_*\[\]()~`>#+\-=|{}.!])', r'\\\1', text)


def make_uid() -> str:
    return uuid4().hex[:10]


def make_progress_bar(percent: float, length: int = 10) -> str:
    filled = int(length * percent / 100)
    bar = "█" * filled + "░" * (length - filled)
    return f"[{bar}] {percent:.1f}%"


def detect_platform(url: str) -> str:
    u = url.lower()

    if "soundcloud.com" in u or "on.soundcloud.com" in u:
        return "soundcloud"
    if "youtube.com" in u or "youtu.be" in u:
        return "youtube"
    if "music.yandex." in u:
        return "yandex_music"
    return "unknown"


def is_yandex_music_track(url: str) -> bool:
    return bool(
        re.search(
            r"music\.yandex\.[^/]+/.*/track/\d+|music\.yandex\.[^/]+/album/\d+/track/\d+",
            url,
        )
    )


def expand_soundcloud_url(url: str) -> str:
    try:
        api_url = f"https://soundcloud.com/oembed?format=json&url={urllib.parse.quote(url, safe='')}"
        with httpx.Client(timeout=15, follow_redirects=True) as client:
            r = client.get(api_url)
            r.raise_for_status()
            data = r.json()
            match = re.search(r'src="([^"]+soundcloud\.com[^"]+)"', data.get("html", ""))
            if match:
                src = html.unescape(match.group(1))
                m2 = re.search(r"url=([^&]+)", src)
                if m2:
                    return urllib.parse.unquote(m2.group(1))
    except Exception as e:
        print(f"[SC expand error] {e}")
    return url


async def safe_edit(msg, text: str, reply_markup=None):
    try:
        await msg.edit_text(text, parse_mode="Markdown", reply_markup=reply_markup)
    except Exception:
        pass


def make_progress_hook(loop, msg, title: str):
    state = {"bucket": -1, "finished": False}

    def hook(d):
        try:
            status = d.get("status")

            if status == "downloading":
                raw = str(d.get("_percent_str", "0%")).strip()
                raw = re.sub(r"\x1b\[[0-9;]*m", "", raw)

                try:
                    percent = float(raw.replace("%", "").strip())
                except ValueError:
                    return

                bucket = int(percent // 10)
                if bucket == state["bucket"]:
                    return
                state["bucket"] = bucket

                bar = make_progress_bar(percent)
                speed = str(d.get("_speed_str", "")).strip()
                eta = str(d.get("_eta_str", "")).strip()

                text = (
                    f"⏳ Скачиваю: *{escape_md(title or 'Файл')}*\n\n"
                    f"{bar}\n\n"
                    f"🚀 {escape_md(speed)}  ⏱ ETA: {escape_md(eta)}"
                )
                asyncio.run_coroutine_threadsafe(safe_edit(msg, text), loop)

            elif status == "finished" and not state["finished"]:
                state["finished"] = True
                asyncio.run_coroutine_threadsafe(
                    safe_edit(msg, "🔄 Обрабатываю файл...\n\n[██████████] 100%"),
                    loop,
                )
        except Exception:
            pass

    return hook


def build_outtmpl(uid: str) -> str:
    return str(DOWNLOAD_DIR / f"{uid}_%(title).120B.%(ext)s")


def base_opts(extra=None) -> dict:
    opts = {
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "overwrites": True,
        "retries": 10,
        "fragment_retries": 10,
        "skip_unavailable_fragments": True,
        "nocheckcertificate": True,
        "legacyserverconnect": True,
        "socket_timeout": 20,
        "geo_bypass": True,
        "windowsfilenames": True,
        "http_headers": {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            )
        },
    }

    if FFMPEG_PATH:
        opts["ffmpeg_location"] = FFMPEG_PATH

    if extra:
        opts.update(extra)

    return opts


def extract_info(url: str) -> dict:
    with yt_dlp.YoutubeDL(base_opts()) as ydl:
        return ydl.extract_info(url, download=False)


def download_audio(url: str, loop, msg, title_hint: str = "", quality: str = "192"):
    uid = make_uid()

    if "on.soundcloud.com" in url:
        url = expand_soundcloud_url(url)

    opts = base_opts({
        "format": "bestaudio/best",
        "outtmpl": build_outtmpl(uid),
        "progress_hooks": [make_progress_hook(loop, msg, title_hint)],
        "postprocessors": [{
            "key": "FFmpegExtractAudio",
            "preferredcodec": "mp3",
            "preferredquality": quality,
        }],
        "concurrent_fragment_downloads": 3,
    })

    with yt_dlp.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(url, download=True)
        title = sanitize_filename(info.get("title") or title_hint or "audio")
        prepared = ydl.prepare_filename(info)
        final_path = str(Path(prepared).with_suffix(".mp3"))

    return final_path, title


def get_youtube_formats(url: str):
    info = extract_info(url)
    title = info.get("title", "video")

    allowed = [2160, 1440, 1080, 720, 480, 360, 240, 144]
    found = {}

    for f in info.get("formats", []):
        if f.get("vcodec") == "none":
            continue
        height = f.get("height")
        if not height:
            continue

        candidate = None
        for a in allowed:
            if height >= a:
                candidate = a
                break

        if candidate is not None:
            found[candidate] = {"label": f"{candidate}p", "height": candidate}

    formats = [found[h] for h in sorted(found.keys(), reverse=True)]
    formats.append({"label": "🎵 Только MP3", "height": 0})

    return formats, title, url


def download_youtube(url: str, height: int, loop, msg, title_hint: str = ""):
    uid = make_uid()
    hook = make_progress_hook(loop, msg, title_hint)

    if height == 0:
        opts = base_opts({
            "format": "bestaudio/best",
            "outtmpl": build_outtmpl(uid),
            "progress_hooks": [hook],
            "postprocessors": [{
                "key": "FFmpegExtractAudio",
                "preferredcodec": "mp3",
                "preferredquality": "192",
            }],
        })

        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=True)
            title = sanitize_filename(info.get("title") or title_hint or "audio")
            prepared = ydl.prepare_filename(info)
            return str(Path(prepared).with_suffix(".mp3")), title

    fmt = (
        f"(bestvideo[height<={height}][ext=mp4]+bestaudio[ext=m4a])/"
        f"(bestvideo[height<={height}]+bestaudio)/"
        f"(best[height<={height}])"
    )

    opts = base_opts({
        "format": fmt,
        "outtmpl": build_outtmpl(uid),
        "merge_output_format": "mp4",
        "progress_hooks": [hook],
    })

    with yt_dlp.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(url, download=True)
        title = sanitize_filename(info.get("title") or title_hint or "video")
        prepared = ydl.prepare_filename(info)
        base = str(Path(prepared).with_suffix(""))

        for ext in ("mp4", "mkv", "webm", "mov"):
            candidate = f"{base}.{ext}"
            if os.path.exists(candidate):
                return candidate, title

    raise FileNotFoundError("Не удалось найти итоговый видеофайл")


async def check_and_send(status_msg, reply_message, file_path: str, title: str, is_video: bool):
    if not os.path.exists(file_path):
        await safe_edit(status_msg, "❌ Файл не был создан.")
        return False

    size_mb = os.path.getsize(file_path) / (1024 * 1024)
    if size_mb > MAX_FILE_MB:
        await safe_edit(
            status_msg,
            f"❌ Файл слишком большой: *{size_mb:.1f} МБ*\n\n"
            + ("Выбери качество ниже." if is_video else "Telegram не примет такой размер.")
        )
        try:
            os.remove(file_path)
        except Exception:
            pass
        return False

    await safe_edit(status_msg, f"📤 Отправляю: *{escape_md(title)}*...")

    try:
        with open(file_path, "rb") as f:
            if is_video:
                await reply_message.reply_video(
                    video=f,
                    caption=title,
                    supports_streaming=True,
                    read_timeout=180,
                    write_timeout=180,
                    connect_timeout=60,
                )
            else:
                await reply_message.reply_audio(
                    audio=f,
                    title=title,
                    performer="Bot",
                    read_timeout=180,
                    write_timeout=180,
                    connect_timeout=60,
                )
    finally:
        try:
            os.remove(file_path)
        except Exception:
            pass

    try:
        await status_msg.delete()
    except Exception:
        pass

    return True


async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 Привет!\n\n"
        "Поддерживаются ссылки:\n"
        "• YouTube — видео или MP3\n"
        "• SoundCloud — MP3\n"
        "• Яндекс Музыка — MP3\n\n"
        "Просто отправь ссылку."
    )


async def handle_link(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return

    url = update.message.text.strip()
    platform = detect_platform(url)

    if platform == "unknown":
        await update.message.reply_text(
            "⚠️ Поддерживаются только ссылки YouTube, SoundCloud и Яндекс Музыки."
        )
        return

    loop = asyncio.get_running_loop()

    async with DOWNLOAD_SEMAPHORE:
        if platform == "soundcloud":
            msg = await update.message.reply_text("⏳ Обрабатываю SoundCloud...")

            try:
                if "on.soundcloud.com" in url:
                    await safe_edit(msg, "🔗 Разворачиваю короткую ссылку...")
                    url = await loop.run_in_executor(EXECUTOR, expand_soundcloud_url, url)

                await safe_edit(msg, "⏳ Начинаю скачивание...")

                file_path, title = await loop.run_in_executor(
                    EXECUTOR, download_audio, url, loop, msg, "", "192"
                )

                await check_and_send(msg, update.message, file_path, title, False)

            except Exception as e:
                print(f"[SoundCloud error] {e}")
                await safe_edit(msg, f"❌ Ошибка SoundCloud:\n`{escape_md(str(e))}`")

        elif platform == "yandex_music":
            msg = await update.message.reply_text("⏳ Обрабатываю Яндекс Музыку...")

            if not is_yandex_music_track(url):
                await safe_edit(msg, "⚠️ Пока поддерживаю только прямые ссылки на трек Яндекс Музыки.")
                return

            try:
                await safe_edit(msg, "⏳ Начинаю скачивание MP3...")

                file_path, title = await loop.run_in_executor(
                    EXECUTOR, download_audio, url, loop, msg, "", "192"
                )

                await check_and_send(msg, update.message, file_path, title, False)

            except Exception as e:
                print(f"[Yandex error] {e}")
                await safe_edit(msg, "❌ Ошибка Яндекс Музыки. Попробуй позже или обнови yt-dlp.")

        elif platform == "youtube":
            msg = await update.message.reply_text("🔍 Получаю информацию о YouTube-видео...")

            try:
                formats, title, clean_url = await loop.run_in_executor(
                    EXECUTOR, get_youtube_formats, url
                )

                req_id = make_uid()
                ctx.user_data[f"yt:{req_id}"] = {"url": clean_url, "title": title}

                keyboard = [
                    [InlineKeyboardButton(f["label"], callback_data=f"yt|{req_id}|{f['height']}")]
                    for f in formats
                ]

                await safe_edit(
                    msg,
                    f"🎬 *{escape_md(title)}*\n\nВыбери качество:",
                    reply_markup=InlineKeyboardMarkup(keyboard),
                )

            except Exception as e:
                print(f"[YouTube info error] {e}")
                await safe_edit(msg, f"❌ Ошибка YouTube:\n`{escape_md(str(e))}`")


async def youtube_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    data = query.data or ""
    if not data.startswith("yt|"):
        return

    try:
        _, req_id, raw_height = data.split("|", 2)
        height = int(raw_height)
    except Exception:
        await query.edit_message_text("❌ Некорректные данные кнопки.")
        return

    payload = ctx.user_data.get(f"yt:{req_id}")
    if not payload:
        await query.edit_message_text("❌ Ссылка устарела. Отправь видео заново.")
        return

    url = payload["url"]
    title = payload.get("title", "video")

    await query.edit_message_text(
        f"⏳ Начинаю скачивание {'MP3' if height == 0 else f'{height}p'}..."
    )

    loop = asyncio.get_running_loop()

    async with DOWNLOAD_SEMAPHORE:
        try:
            file_path, final_title = await loop.run_in_executor(
                EXECUTOR, download_youtube, url, height, loop, query.message, title
            )

            await check_and_send(
                query.message,
                query.message,
                file_path,
                final_title,
                is_video=(height != 0),
            )
        except Exception as e:
            print(f"[YouTube download error] {e}")
            try:
                await query.edit_message_text(
                    f"❌ Ошибка загрузки:\n`{escape_md(str(e))}`",
                    parse_mode="Markdown",
                )
            except Exception:
                pass
        finally:
            ctx.user_data.pop(f"yt:{req_id}", None)


def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN не найден. Добавь его в Secrets Replit.")

    request = HTTPXRequest(
        connection_pool_size=8,
        http_version="1.1",
        read_timeout=180,
        write_timeout=180,
        connect_timeout=60,
        pool_timeout=60,
    )

    app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .request(request)
        .get_updates_request(request)
        .build()
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(youtube_callback, pattern=r"^yt\|"))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_link))

    print("Бот запущен на Replit...")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()