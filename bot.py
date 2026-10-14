import asyncio
import html
import io
import json
import logging
import mimetypes
import os
import re
import time
from urllib.parse import quote

# --- Python 3.10+ / 3.14 compatibility -------------------------------------
# Pyrogram's sync helper calls asyncio.get_event_loop() at import time.
# On newer Python versions this raises RuntimeError if no loop exists.
try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

import aiohttp  # noqa: E402
from aiohttp import web  # noqa: E402
from pyrogram.errors import FloodWait  # noqa: E402
from pyrogram import Client, filters, idle  # noqa: E402
from pyrogram.handlers import MessageHandler  # noqa: E402
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("stream-bot")
logging.getLogger("pyrogram").setLevel(logging.WARNING)


# --- Configuration ----------------------------------------------------------
def require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise SystemExit(f"Missing required environment variable: {name}")
    return value


def format_base_url(raw: str, port: int) -> str:
    """Normalize SERVER_URL / FQDN: strip protocol, path, slashes; enforce https."""
    url = (raw or "").strip().strip('"').strip("'")
    if not url:
        return f"http://localhost:{port}"
    url = re.sub(r"^[a-zA-Z][a-zA-Z0-9+.-]*://", "", url)
    url = url.split("/")[0].strip()
    if not url:
        return f"http://localhost:{port}"
    if url.startswith(("localhost", "127.0.0.1", "0.0.0.0")):
        return f"http://{url}"
    return f"https://{url}"


def normalize_channel_id(raw: str) -> int:
    value = int(raw.strip())
    if value > 0:  # user forgot the -100 prefix
        value = int(f"-100{value}")
    return value


API_ID = int(require_env("API_ID"))
API_HASH = require_env("API_HASH")
BOT_TOKEN = require_env("BOT_TOKEN")
BIN_CHANNEL = normalize_channel_id(require_env("BIN_CHANNEL"))
PORT = int(os.environ.get("PORT", "8080"))
BASE_URL = format_base_url(
    os.environ.get("SERVER_URL")
    or os.environ.get("FQDN")
    or os.environ.get("RENDER_EXTERNAL_URL")
    or "",
    PORT,
)
PING_INTERVAL = int(os.environ.get("PING_INTERVAL", "300"))
API_KEY = os.environ.get("API_KEY", "").strip()  # app uses this to read the video list
ALLOWED_USERS = {
    int(x) for x in os.environ.get("ALLOWED_USERS", "").replace(" ", "").split(",") if x
}

CHUNK_SIZE = 1024 * 1024  # Pyrogram stream_media yields 1 MiB chunks
CACHE_TTL = 600
CACHE_MAX = 500

tg: Client = None  # created inside main() so it binds to the running loop
_cache: dict = {}
_queue: "asyncio.PriorityQueue" = None  # (message_id, seq, message) -> processed strictly in order
_seq = 0
_catalog: dict = {}  # channel msg id -> video record (what the app lists)
_scan_next = 1  # next channel message id to scan
_thumb_cache: dict = {}
BACKUP_PREFIX = "mytube_backup_"
SCAN_BATCH = 200


# --- Helpers ----------------------------------------------------------------
def human_size(size: int) -> str:
    size = float(size or 0)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024:
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} PB"


def get_media(message):
    return message.video or message.document or message.audio


def build_info(msg) -> dict | None:
    media = get_media(msg)
    if not media:
        return None
    mime = getattr(media, "mime_type", None) or "application/octet-stream"
    name = getattr(media, "file_name", None)
    if not name:
        ext = mimetypes.guess_extension(mime) or ""
        name = f"file_{msg.id}{ext}"
    return {
        "ts": time.time(),
        "message": msg,
        "name": name,
        "mime": mime,
        "size": int(getattr(media, "file_size", 0) or 0),
        "hash": media.file_unique_id[:6],
    }


async def get_file_info(msg_id: int) -> dict | None:
    now = time.time()
    cached = _cache.get(msg_id)
    if cached and now - cached["ts"] < CACHE_TTL:
        return cached
    try:
        msg = await tg.get_messages(BIN_CHANNEL, msg_id)
    except Exception as exc:
        log.warning("get_messages failed for %s: %s", msg_id, exc)
        return None
    if not msg or msg.empty:
        return None
    info = build_info(msg)
    if not info:
        return None
    if len(_cache) >= CACHE_MAX:
        oldest = sorted(_cache.items(), key=lambda kv: kv[1]["ts"])[: CACHE_MAX // 5]
        for key, _ in oldest:
            _cache.pop(key, None)
    _cache[msg_id] = info
    return info


# --- Telegram handlers ------------------------------------------------------
async def start_handler(client: Client, message):
    await message.reply_text(
        "👋 হ্যালো!\n\n"
        "আমাকে যেকোনো Video, Document বা Audio ফাইল পাঠান। "
        "আমি সেগুলো সিরিয়াল অনুযায়ী MyTube অ্যাপে যোগ করে দেব।"
    )


async def file_handler(client: Client, message):
    """Only enqueue. A single worker handles videos one by one, in forward order."""
    global _seq
    user = message.from_user
    if ALLOWED_USERS and (not user or user.id not in ALLOWED_USERS):
        await message.reply_text("⛔ আপনি এই বট ব্যবহার করার অনুমতি পাননি।")
        return
    _seq += 1
    await _queue.put((message.id, _seq, message))


def make_record(msg_id: int, info: dict, date=None) -> dict:
    media = get_media(info["message"])
    thumbs = getattr(media, "thumbs", None) or []
    return {
        "id": msg_id,
        "name": info["name"],
        "size": info["size"],
        "mime": info["mime"],
        "duration": int(getattr(media, "duration", 0) or 0),
        "width": int(getattr(media, "width", 0) or 0),
        "height": int(getattr(media, "height", 0) or 0),
        "date": int(date.timestamp()) if date else int(time.time()),
        "hash": info["hash"],
        "dl": f"{BASE_URL}/dl/{msg_id}/{info['hash']}",
        "stream": f"{BASE_URL}/stream/{msg_id}/{info['hash']}",
        "thumb": f"{BASE_URL}/thumb/{msg_id}/{info['hash']}" if thumbs else None,
    }


def is_video_info(info: dict) -> bool:
    return info["mime"].startswith("video/") or info["name"].lower().endswith(
        (".mp4", ".mkv", ".webm", ".mov", ".avi", ".m4v", ".ts")
    )


async def process_one(message):
    user = message.from_user
    try:
        status = await message.reply_text("⏳ প্রসেস হচ্ছে...", quote=True)
    except Exception:
        status = None

    async def say(text, **kw):
        if status:
            try:
                await status.edit_text(text, **kw)
            except Exception:
                pass

    log_msg = None
    for attempt in range(3):
        try:
            log_msg = await message.copy(BIN_CHANNEL)
            break
        except FloodWait as fw:
            await asyncio.sleep(fw.value + 1)
        except Exception as exc:
            log.exception("Copy to BIN_CHANNEL failed")
            await say(
                "❌ ফাইল চ্যানেলে পাঠানো যায়নি।\n"
                "বট চ্যানেলের Admin কি না এবং BIN_CHANNEL ঠিক আছে কি না দেখুন।\n"
                f"Error: {exc}"
            )
            return
    if log_msg is None:
        await say("❌ বারবার চেষ্টা করেও পাঠানো যায়নি।")
        return

    info = build_info(log_msg)
    if not info:
        await say("❌ এই ফাইল টাইপ সাপোর্ট করা হয় না।")
        return
    _cache[log_msg.id] = info
    rec = make_record(log_msg.id, info, log_msg.date)
    _catalog[log_msg.id] = rec

    text = (
        f"✅ অ্যাপে যোগ হয়েছে\n"
        f"📁 {info['name']}\n"
        f"📦 {human_size(info['size'])}\n\n"
        f"⬇️ {rec['dl']}"
    )
    await say(text, disable_web_page_preview=True)


async def queue_worker():
    while True:
        first = await _queue.get()
        # tiny wait so a burst of forwards arrives, then sort by message id
        await asyncio.sleep(1.5)
        batch = [first]
        while not _queue.empty():
            batch.append(_queue.get_nowait())
        batch.sort(key=lambda x: (x[0], x[1]))
        for _, _, msg in batch:
            try:
                await process_one(msg)
            except Exception:
                log.exception("process_one failed")
            await asyncio.sleep(0.4)
            _queue.task_done()


# --- Web server -------------------------------------------------------------
PLAYER_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>__TITLE__</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/plyr@3.7.8/dist/plyr.css">
<style>
  :root { --bg:#0f1115; --card:#181b22; --text:#e8eaf0; --accent:#4f8cff; }
  * { box-sizing: border-box; }
  body { margin:0; background:var(--bg); color:var(--text);
         font-family:-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif; }
  .wrap { max-width:960px; margin:0 auto; padding:16px; }
  h1 { font-size:1.05rem; word-break:break-word; margin:8px 0 14px; }
  .card { background:var(--card); border-radius:12px; padding:12px; }
  video, audio { width:100%; border-radius:8px; background:#000; }
  .meta { opacity:.7; font-size:.85rem; margin:10px 0; }
  .btn { display:inline-block; margin-top:10px; padding:11px 18px; background:var(--accent);
         color:#fff; text-decoration:none; border-radius:8px; font-weight:600; }
</style>
</head>
<body>
<div class="wrap">
  <h1>__TITLE__</h1>
  <div class="card">
    __PLAYER__
    <div class="meta">Size: __SIZE__</div>
    <a class="btn" href="__DL__">⬇️ Download</a>
  </div>
</div>
<script src="https://cdn.jsdelivr.net/npm/plyr@3.7.8/dist/plyr.polyfilled.min.js"></script>
<script>
  var el = document.getElementById('player');
  if (el) {
    new Plyr(el, {
      controls: ['play-large','play','progress','current-time','duration','mute','volume','settings','pip','fullscreen'],
      settings: ['speed'],
      speed: { selected: 1, options: [0.5, 0.75, 1, 1.25, 1.5, 2] },
      keyboard: { focused: true, global: true }
    });
  }
</script>
</body>
</html>
"""


async def index_handler(request: web.Request) -> web.Response:
    return web.Response(text="✅ Bot is running.", content_type="text/plain")


async def watch_handler(request: web.Request) -> web.Response:
    try:
        msg_id = int(request.match_info["msg_id"])
    except ValueError:
        raise web.HTTPNotFound()
    file_hash = request.match_info["hash"]
    info = await get_file_info(msg_id)
    if not info or info["hash"] != file_hash:
        raise web.HTTPNotFound(text="File not found")

    stream_url = f"/stream/{msg_id}/{file_hash}"
    dl_url = f"/dl/{msg_id}/{file_hash}"
    mime = info["mime"]

    if mime.startswith("video/"):
        player = f'<video id="player" controls playsinline preload="metadata"><source src="{stream_url}"></video>'
    elif mime.startswith("audio/"):
        player = f'<audio id="player" controls preload="metadata"><source src="{stream_url}" type="{html.escape(mime)}"></audio>'
    else:
        player = "<p>এই ফাইল ব্রাউজারে প্লে করা যাবে না। নিচের বাটন থেকে ডাউনলোড করুন।</p>"

    page = (
        PLAYER_HTML.replace("__TITLE__", html.escape(info["name"]))
        .replace("__PLAYER__", player)
        .replace("__SIZE__", human_size(info["size"]))
        .replace("__DL__", dl_url)
    )
    return web.Response(text=page, content_type="text/html")


def parse_range(header: str | None, size: int):
    """Return (start, end) inclusive, or None if invalid."""
    if not header:
        return 0, size - 1
    match = re.match(r"^bytes=(\d*)-(\d*)$", header.strip())
    if not match:
        return 0, size - 1
    start_s, end_s = match.groups()
    if start_s == "" and end_s == "":
        return 0, size - 1
    if start_s == "":  # suffix range: last N bytes
        length = int(end_s)
        start = max(size - length, 0)
        end = size - 1
    else:
        start = int(start_s)
        end = int(end_s) if end_s else size - 1
        end = min(end, size - 1)
    if start > end or start >= size:
        return None
    return start, end


async def stream_file(request: web.Request, download: bool) -> web.StreamResponse:
    try:
        msg_id = int(request.match_info["msg_id"])
    except ValueError:
        raise web.HTTPNotFound()
    file_hash = request.match_info["hash"]
    info = await get_file_info(msg_id)
    if not info or info["hash"] != file_hash:
        raise web.HTTPNotFound(text="File not found")

    size = info["size"]
    if size <= 0:
        raise web.HTTPNotFound(text="Empty file")

    range_header = request.headers.get("Range")
    rng = parse_range(range_header, size)
    if rng is None:
        return web.Response(
            status=416, headers={"Content-Range": f"bytes */{size}"}
        )
    start, end = rng
    length = end - start + 1

    disposition = "attachment" if download else "inline"
    headers = {
        "Content-Type": info["mime"] if not download else "application/octet-stream",
        "Content-Length": str(length),
        "Accept-Ranges": "bytes",
        "Content-Disposition": f"{disposition}; filename*=UTF-8''{quote(info['name'])}",
        "Cache-Control": "public, max-age=86400",
    }
    status = 200
    if range_header:
        status = 206
        headers["Content-Range"] = f"bytes {start}-{end}/{size}"

    response = web.StreamResponse(status=status, headers=headers)
    await response.prepare(request)
    if request.method == "HEAD":
        return response

    first_chunk = start // CHUNK_SIZE
    cut = start % CHUNK_SIZE
    remaining = length
    generator = tg.stream_media(info["message"], offset=first_chunk)
    try:
        async for chunk in generator:
            if cut:
                chunk = chunk[cut:]
                cut = 0
            if len(chunk) > remaining:
                chunk = chunk[:remaining]
            await response.write(chunk)
            remaining -= len(chunk)
            if remaining <= 0:
                break
    except (ConnectionResetError, ConnectionError, asyncio.CancelledError):
        pass
    except Exception as exc:
        log.warning("Streaming error for %s: %s", msg_id, exc)
    finally:
        try:
            await generator.aclose()
        except Exception:
            pass
    try:
        await response.write_eof()
    except Exception:
        pass
    return response


async def stream_handler(request: web.Request):
    return await stream_file(request, download=False)


async def download_handler(request: web.Request):
    return await stream_file(request, download=True)


def check_key(request: web.Request):
    if API_KEY and request.headers.get("X-API-Key", "") != API_KEY and request.query.get("key", "") != API_KEY:
        raise web.HTTPUnauthorized(text="bad api key")


async def scan_channel(full: bool = False):
    """Bots can't read channel history, so walk message ids with get_messages."""
    global _scan_next
    if full:
        _catalog.clear()
        _scan_next = 1
    empty_batches = 0
    while empty_batches < 1:
        ids = list(range(_scan_next, _scan_next + SCAN_BATCH))
        try:
            msgs = await tg.get_messages(BIN_CHANNEL, ids)
        except FloodWait as fw:
            await asyncio.sleep(fw.value + 1)
            continue
        except Exception as exc:
            log.warning("scan failed: %s", exc)
            break
        found = False
        last_ok = None
        for m in msgs:
            if not m or m.empty:
                continue
            found = True
            last_ok = m.id
            info = build_info(m)
            if not info:
                continue
            if info["name"].startswith(BACKUP_PREFIX):
                continue
            if not is_video_info(info):
                continue
            _catalog[m.id] = make_record(m.id, info, m.date)
        if not found:
            empty_batches += 1
        else:
            _scan_next = last_ok + 1


async def api_videos(request: web.Request) -> web.Response:
    check_key(request)
    await scan_channel(full=request.query.get("full") == "1")
    try:
        after = int(request.query.get("after", "0"))
    except ValueError:
        after = 0
    items = [v for k, v in sorted(_catalog.items()) if k > after]
    return web.json_response({"ok": True, "base": BASE_URL, "videos": items})


async def thumb_handler(request: web.Request) -> web.Response:
    try:
        msg_id = int(request.match_info["msg_id"])
    except ValueError:
        raise web.HTTPNotFound()
    info = await get_file_info(msg_id)
    if not info or info["hash"] != request.match_info["hash"]:
        raise web.HTTPNotFound()
    if msg_id in _thumb_cache:
        data = _thumb_cache[msg_id]
    else:
        media = get_media(info["message"])
        thumbs = getattr(media, "thumbs", None) or []
        if not thumbs:
            raise web.HTTPNotFound()
        buf = await tg.download_media(thumbs[-1].file_id, in_memory=True)
        data = bytes(buf.getbuffer()) if buf else b""
        if not data:
            raise web.HTTPNotFound()
        if len(_thumb_cache) > 300:
            _thumb_cache.clear()
        _thumb_cache[msg_id] = data
    return web.Response(
        body=data, content_type="image/jpeg",
        headers={"Cache-Control": "public, max-age=31536000"},
    )


async def api_backup_get(request: web.Request) -> web.Response:
    """Return the newest backup JSON stored in the channel."""
    check_key(request)
    await scan_channel()
    # backups are not in _catalog, so look at the tail of the channel directly
    top = _scan_next + SCAN_BATCH
    ids = list(range(max(1, top - 400), top))
    msgs = await tg.get_messages(BIN_CHANNEL, ids)
    best = None
    for m in msgs:
        if not m or m.empty or not m.document:
            continue
        if (m.document.file_name or "").startswith(BACKUP_PREFIX):
            if best is None or m.id > best.id:
                best = m
    if not best:
        raise web.HTTPNotFound(text="no backup yet")
    buf = await tg.download_media(best, in_memory=True)
    return web.Response(body=bytes(buf.getbuffer()), content_type="application/json")


async def api_backup_post(request: web.Request) -> web.Response:
    check_key(request)
    data = await request.read()
    try:
        json.loads(data.decode("utf-8"))
    except Exception:
        raise web.HTTPBadRequest(text="invalid json")
    bio = io.BytesIO(data)
    bio.name = f"{BACKUP_PREFIX}{int(time.time())}.json"
    msg = await tg.send_document(BIN_CHANNEL, bio, caption="MyTube backup")
    return web.json_response({"ok": True, "id": msg.id})


def create_web_app() -> web.Application:
    app = web.Application(client_max_size=50 * 1024**2)
    app.router.add_get("/", index_handler)
    app.router.add_get("/watch/{msg_id}/{hash}", watch_handler)
    app.router.add_get("/stream/{msg_id}/{hash}", stream_handler)
    app.router.add_get("/dl/{msg_id}/{hash}", download_handler)
    app.router.add_get("/thumb/{msg_id}/{hash}", thumb_handler)
    app.router.add_get("/api/videos", api_videos)
    app.router.add_get("/api/backup", api_backup_get)
    app.router.add_post("/api/backup", api_backup_post)
    return app


async def keep_alive():
    """Ping our own public URL so Render's free instance sleeps less often."""
    if BASE_URL.startswith("http://localhost"):
        return
    await asyncio.sleep(60)
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.get(BASE_URL + "/", timeout=aiohttp.ClientTimeout(total=20)) as resp:
                    log.info("Keep-alive ping: %s", resp.status)
            except Exception as exc:
                log.warning("Keep-alive failed: %s", exc)
            await asyncio.sleep(PING_INTERVAL)


# --- Runner -----------------------------------------------------------------
async def main():
    global tg, _queue
    _queue = asyncio.PriorityQueue()

    # 1) Start web server first so Render detects the open port quickly.
    runner = web.AppRunner(create_web_app())
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    log.info("Web server listening on 0.0.0.0:%s", PORT)
    log.info("Public base URL: %s", BASE_URL)

    # 2) Start Pyrogram bot inside the running loop.
    tg = Client(
        "stream_bot",
        api_id=API_ID,
        api_hash=API_HASH,
        bot_token=BOT_TOKEN,
        in_memory=True,
        workers=8,
    )
    tg.add_handler(MessageHandler(start_handler, filters.command("start") & filters.private))
    tg.add_handler(
        MessageHandler(
            file_handler,
            filters.private & (filters.video | filters.document | filters.audio),
        )
    )
    await tg.start()
    me = await tg.get_me()
    log.info("Bot started as @%s", me.username)

    # Verify access to the log channel early with a clear error.
    try:
        await tg.get_chat(BIN_CHANNEL)
    except Exception as exc:
        log.error("Cannot access BIN_CHANNEL (%s): %s", BIN_CHANNEL, exc)

    keepalive_task = asyncio.create_task(keep_alive())
    worker_task = asyncio.create_task(queue_worker())
    try:
        await idle()
    finally:
        keepalive_task.cancel()
        worker_task.cancel()
        await tg.stop()
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
