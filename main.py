import asyncio
import logging
import os
import re
import tempfile
import gzip
from urllib.parse import urlparse, urljoin

import aiohttp
import pandas as pd
from dotenv import load_dotenv
from telegram import Update, Document
from telegram.constants import ChatAction
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)
import xml.etree.ElementTree as ET

# -------------------------
# Config & Logging
# -------------------------
load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
ONEHPING_API_KEY = os.getenv("ONEHPING_API_KEY", "").strip()
ONEHPING_API_URL = "https://app.1hping.com/external/api/campaign/create?culture=vi-VN"

if not TELEGRAM_BOT_TOKEN:
    raise RuntimeError("Missing TELEGRAM_BOT_TOKEN in environment.")
if not ONEHPING_API_KEY:
    raise RuntimeError("Missing ONEHPING_API_KEY in environment.")

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("1hping-bot")

# -------------------------
# Helpers
# -------------------------
def sanitize_campaign_name(name: str) -> str:
    """Loại bỏ ký tự lạ, rút gọn chiều dài cho an toàn API."""
    name = re.sub(r"\s+", " ", name).strip()
    name = re.sub(r"[^A-Za-z0-9 _\-\.\(\)\[\]]+", "", name)
    return name[:120] if len(name) > 120 else name

def extract_urls_from_excel(path: str) -> list[str]:
    """Đọc mọi sheet, mọi cột; gom toàn bộ ô dạng text rồi lọc URL hợp lệ http/https."""
    dfs = pd.read_excel(path, sheet_name=None, header=None, dtype=str, engine="openpyxl")
    urls = []
    for _, df in dfs.items():
        # Flatten các ô
        for val in df.to_numpy().flatten():
            if isinstance(val, str):
                val = val.strip()
                if val:
                    urls.append(val)
    # Lọc hợp lệ
    valid = []
    for u in urls:
        parsed = urlparse(u)
        if parsed.scheme in ("http", "https") and parsed.netloc:
            valid.append(u)
    # Loại trùng, giữ nguyên thứ tự
    seen = set()
    deduped = []
    for u in valid:
        if u not in seen:
            seen.add(u)
            deduped.append(u)
    return deduped

async def call_1hping_create_campaign(
    session: aiohttp.ClientSession,
    campaign_name: str,
    number_of_day: int,
    urls: list[str],
) -> dict:
    headers = {
        "ApiKey": ONEHPING_API_KEY,
        "Content-Type": "application/json",
    }
    payload = {
        "CampaignName": campaign_name,
        "NumberOfDay": number_of_day,
        "Urls": urls,
    }
    async with session.post(ONEHPING_API_URL, headers=headers, json=payload, timeout=120) as resp:
        text = await resp.text()
        try:
            data = await resp.json()
        except Exception:
            # Phòng khi server trả text không phải JSON
            data = {"raw": text}
        return {"status": resp.status, "data": data}

async def send_typing(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    await context.bot.send_chat_action(chat_id=chat_id, action=ChatAction.TYPING)

# -------------------------
# Sitemap utilities
# -------------------------
def _norm_domain(raw: str) -> str:
    """Chuẩn hoá domain người dùng nhập: bỏ scheme/đường dẫn, chỉ giữ host (kèm sub nếu có)."""
    raw = raw.strip()
    if not raw:
        return ""
    # Thêm scheme giả để parse nếu thiếu
    if not re.match(r"^https?://", raw, flags=re.I):
        raw = "http://" + raw
    p = urlparse(raw)
    host = (p.netloc or "").strip().lower()
    # Bỏ cổng
    host = host.split(":")[0]
    # Bỏ www. nếu muốn; ở đây giữ nguyên để thử cả 2 biến thể
    return host

def _candidate_sitemap_urls(host: str) -> list[str]:
    """Sinh danh sách URL sitemap phổ biến để thử lần lượt (ưu tiên https)."""
    # Thử cả host và www.host
    hosts = [host]
    if not host.startswith("www."):
        hosts.append("www." + host)
    cands = []
    for h in hosts:
        # RankMath/Yoast phổ biến
        cands.extend([
            f"https://{h}/sitemap_index.xml",
            f"https://{h}/sitemap.xml",
            f"http://{h}/sitemap_index.xml",
            f"http://{h}/sitemap.xml",
        ])
    return cands

async def _fetch_bytes(session: aiohttp.ClientSession, url: str) -> bytes | None:
    try:
        async with session.get(url, timeout=60) as resp:
            if resp.status != 200:
                return None
            content = await resp.read()
            # Nếu là .gz hoặc header nén
            ct = resp.headers.get("Content-Type", "")
            if url.lower().endswith(".gz") or "gzip" in ct:
                try:
                    return gzip.decompress(content)
                except Exception:
                    # Có thể server đã giải nén tự động
                    return content
            return content
    except Exception as e:
        logger.warning("Fetch error %s: %s", url, e)
        return None

def _xml_findall(root: ET.Element, localname: str) -> list[ET.Element]:
    """Tìm mọi node theo localname, bất chấp namespace."""
    return root.findall(f".//{{*}}{localname}")

def _parse_sitemap_xml(xml_bytes: bytes) -> tuple[list[str], list[str]]:
    """
    Trả về (sitemap_links, page_urls).
    - Nếu là sitemap index: trả về list các <loc> của sitemap con trong sitemap_links.
    - Nếu là urlset: trả về list các <loc> trang trong page_urls.
    """
    try:
        root = ET.fromstring(xml_bytes)
    except Exception:
        return [], []

    sitemap_links = []
    page_urls = []

    # sitemap index
    for sm in _xml_findall(root, "sitemap"):
        loc_el = sm.find("{*}loc")
        if loc_el is not None and (loc := (loc_el.text or "").strip()):
            sitemap_links.append(loc)

    # urlset
    for u in _xml_findall(root, "url"):
        loc_el = u.find("{*}loc")
        if loc_el is not None and (loc := (loc_el.text or "").strip()):
            page_urls.append(loc)

    return sitemap_links, page_urls

async def _collect_urls_from_sitemaps(session: aiohttp.ClientSession, entry_urls: list[str], limit_depth: int = 5) -> list[str]:
    """
    Duyệt đệ quy sitemap index -> sitemap con -> urlset, loại trùng, giữ thứ tự.
    limit_depth: giới hạn độ sâu đệ quy để tránh vòng lặp hiếm gặp.
    """
    seen_sitemaps = set()
    seen_urls = set()
    ordered_urls = []

    queue = list(entry_urls)
    depth = 0

    while queue and depth < limit_depth:
        next_queue = []
        for sm_url in queue:
            if sm_url in seen_sitemaps:
                continue
            seen_sitemaps.add(sm_url)

            data = await _fetch_bytes(session, sm_url)
            if not data:
                continue
            child_sitemaps, page_urls = _parse_sitemap_xml(data)

            # Thêm URL trang
            for u in page_urls:
                if u not in seen_urls:
                    seen_urls.add(u)
                    ordered_urls.append(u)

            # Thêm sitemap con vào hàng đợi
            for c in child_sitemaps:
                if c not in seen_sitemaps:
                    next_queue.append(c)

        queue = next_queue
        depth += 1

    return ordered_urls

async def _discover_sitemap_entry_points(session: aiohttp.ClientSession, host: str) -> list[str]:
    """
    Thử tải các URL sitemap phổ biến. Nếu sitemap_index.xml trả về thành công → dùng nó.
    Nếu không, thử sitemap.xml. Nếu tìm được sitemap index trong nội dung thì thêm các <sitemap><loc>.
    """
    candidates = _candidate_sitemap_urls(host)
    entry_points = []

    for url in candidates:
        data = await _fetch_bytes(session, url)
        if not data:
            continue
        child_sitemaps, page_urls = _parse_sitemap_xml(data)

        if child_sitemaps:
            # Đây là sitemap index → dùng chính nó làm entry
            entry_points.append(url)
            # Đồng thời nối thêm các sitemap con trực tiếp để tăng độ phủ
            entry_points.extend(child_sitemaps)
            break
        elif page_urls:
            # Đây là urlset → vẫn dùng được như entry
            entry_points.append(url)
            break

    return list(dict.fromkeys(entry_points))  # dedupe giữ thứ tự

# -------------------------
# Bot Handlers
# -------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    msg = (
        "Chào bạn. Gửi **file Excel (.xlsx)** chứa list URL cần ép index.\n"
        "Hoặc dùng lệnh: `/indexweb <domain>` để lấy toàn bộ URL từ sitemap.\n"
        "Sau khi có danh sách URL, mình sẽ hỏi bạn muốn chia trong **bao nhiêu ngày**.\n"
        "Tên chiến dịch sẽ là: `TelegramName_UserID`.\n\n"
        "Lệnh hữu ích:\n"
        "• /indexweb abc.com — quét sitemap và gom URL\n"
        "• /cancel — hủy phiên hiện tại và xóa trạng thái.\n"
    )
    await update.message.reply_text(msg, disable_web_page_preview=True)

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    await update.message.reply_text("Đã hủy và xóa trạng thái. Gửi lại file Excel hoặc dùng /indexweb để tạo chiến dịch mới.")

def _is_excel(doc: Document) -> bool:
    # Một số client gửi mime xlsx hoặc octet-stream; kiểm thêm phần mở rộng.
    excel_mimes = {
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "application/vnd.ms-excel",
        "application/octet-stream",
    }
    name = (doc.file_name or "").lower()
    return (doc.mime_type in excel_mimes) and (name.endswith(".xlsx") or name.endswith(".xls"))

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    doc = update.message.document
    if not _is_excel(doc):
        await update.message.reply_text("Vui lòng gửi **file Excel (.xlsx)**. Các định dạng khác không được hỗ trợ.")
        return

    await send_typing(context, update.effective_chat.id)

    # Tải file tạm
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, doc.file_name or "urls.xlsx")
            file = await context.bot.get_file(doc.file_id)
            await file.download_to_drive(path)

            urls = extract_urls_from_excel(path)
    except Exception as e:
        logger.exception("Error while reading excel")
        await update.message.reply_text(f"Lỗi đọc file Excel: {e}")
        return

    if not urls:
        await update.message.reply_text("Không tìm thấy URL hợp lệ (http/https) trong file. Kiểm tra lại nội dung.")
        return

    context.user_data["urls"] = urls
    context.user_data["awaiting_days"] = True

    await update.message.reply_text(
        f"Đã nhận **{len(urls)} URL hợp lệ**. Bạn muốn chia ép trong **bao nhiêu ngày**? (nhập số nguyên, ví dụ: 1, 3, 7)"
    )

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Chỉ xử lý khi đang chờ số ngày
    if not context.user_data.get("awaiting_days"):
        await update.message.reply_text("Gửi file Excel (.xlsx) để bắt đầu hoặc dùng /indexweb <domain>.")
        return

    raw = (update.message.text or "").strip()
    if not re.fullmatch(r"\d{1,3}", raw):
        await update.message.reply_text("Vui lòng nhập **số nguyên hợp lệ** cho số ngày (ví dụ: 1, 3, 7).")
        return

    days = int(raw)
    if days <= 0:
        await update.message.reply_text("Số ngày phải >= 1.")
        return
    if days > 365:
        await update.message.reply_text("Giới hạn tối đa 365 ngày.")
        return

    urls = context.user_data.get("urls", [])
    if not urls:
        await update.message.reply_text("Không tìm thấy danh sách URL trong phiên. Gửi lại file Excel hoặc dùng /indexweb.")
        context.user_data.clear()
        return

    # Tạo tên chiến dịch: TelegramName_UserID
    tg_user = update.effective_user
    display_name = tg_user.full_name or tg_user.username or "User"
    user_id = tg_user.id
    campaign_name = sanitize_campaign_name(f"{display_name}_{user_id}")

    await send_typing(context, update.effective_chat.id)
    await update.message.reply_text(
        f"Đang tạo chiến dịch:\n"
        f"• Tên: `{campaign_name}`\n"
        f"• Số ngày: {days}\n"
        f"• Số URL: {len(urls)}\n"
        f"Vui lòng đợi kết quả...",
        disable_web_page_preview=True,
    )

    # Gọi API 1hping
    try:
        timeout = aiohttp.ClientTimeout(total=180)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            result = await call_1hping_create_campaign(session, campaign_name, days, urls)
    except Exception as e:
        logger.exception("Error calling 1hping API")
        await update.message.reply_text(f"Lỗi gọi API 1hping: {e}")
        context.user_data.clear()
        return

    status = result.get("status")
    data = result.get("data")

    # Phản hồi kết quả rõ ràng
    if status and 200 <= status < 300:
        await update.message.reply_text(
            "✅ Đã tạo chiến dịch ép index thành công trên 1hping.\n"
            f"• HTTP Status: {status}\n"
            f"• Response: `{data}`",
            disable_web_page_preview=True,
        )
    else:
        await update.message.reply_text(
            "❌ Tạo chiến dịch thất bại.\n"
            f"• HTTP Status: {status}\n"
            f"• Response: `{data}`",
            disable_web_page_preview=True,
        )

    # Kết thúc phiên
    context.user_data.clear()

async def unknown(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Không hiểu lệnh. Gửi file Excel (.xlsx) hoặc dùng /indexweb <domain> hoặc /start.")

# -------------------------
# /indexweb handler
# -------------------------
async def indexweb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /indexweb <domain>
    - Quét sitemap (ưu tiên https), bỏ qua lỗi SSL.
    - Hỗ trợ sitemap index + urlset, kể cả .xml.gz.
    - Gom URL, sau đó hỏi số ngày để gửi lên 1hping.
    """
    args = context.args if hasattr(context, "args") else []
    if not args:
        await update.message.reply_text("Cú pháp: `/indexweb <domain>` (vd: `/indexweb abc.com`)", disable_web_page_preview=True)
        return

    raw_domain = args[0]
    host = _norm_domain(raw_domain)
    if not host:
        await update.message.reply_text("Domain không hợp lệ. Vui lòng thử lại, ví dụ: `/indexweb abc.com`")
        return

    await send_typing(context, update.effective_chat.id)
    await update.message.reply_text(f"Đang dò sitemap cho `{host}` (ưu tiên https, bỏ qua lỗi SSL)...", disable_web_page_preview=True)

    # Bỏ qua xác thực SSL để vẫn truy cập được nếu chứng chỉ lỗi
    timeout = aiohttp.ClientTimeout(total=300)
    connector = aiohttp.TCPConnector(ssl=False)  # BỎ QUA SSL
    try:
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            entry_points = await _discover_sitemap_entry_points(session, host)
            if not entry_points:
                await update.message.reply_text(
                    "Không tìm thấy sitemap hợp lệ (sitemap_index.xml / sitemap.xml). Kiểm tra lại domain hoặc sitemap."
                )
                return

            urls = await _collect_urls_from_sitemaps(session, entry_points, limit_depth=6)

    except Exception as e:
        logger.exception("Error while indexing web via sitemap")
        await update.message.reply_text(f"Lỗi khi quét sitemap: {e}")
        return

    if not urls:
        await update.message.reply_text("Không thu thập được URL nào từ sitemap.")
        return

    # Loại trùng lần nữa (phòng trường hợp lặp từ nhiều entry)
    seen = set()
    deduped = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            deduped.append(u)

    context.user_data["urls"] = deduped
    context.user_data["awaiting_days"] = True

    await update.message.reply_text(
        f"Đã thu thập **{len(deduped)} URL** từ sitemap `{host}`.\n"
        f"Bạn muốn chia ép trong **bao nhiêu ngày**? (nhập số nguyên, ví dụ: 1, 3, 7)"
    )

# -------------------------
# Entry
# -------------------------
def main():
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("cancel", cancel))
    app.add_handler(CommandHandler("indexweb", indexweb))  # <-- thêm lệnh mới

    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    app.add_handler(MessageHandler(filters.COMMAND, unknown))

    logger.info("Bot is starting...")
    app.run_polling(close_loop=False)

if __name__ == "__main__":
    main()
