#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import logging
import os
import re
import tempfile
import gzip
from urllib.parse import urlparse

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

# =========================
# Config & Logging
# =========================
load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
ONEHPING_API_KEY = os.getenv("ONEHPING_API_KEY", "").strip()
ONEHPING_API_URL = "https://app.1hping.com/external/api/campaign/create?culture=vi-VN"

# Cấu hình crawler
SKIP_SSL_VERIFY = os.getenv("SKIP_SSL_VERIFY", "false").lower() == "true"
CRAWLER_UA = os.getenv("CRAWLER_UA", "1hping-indexbot/1.0 (+https://app.1hping.com)")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "2000"))  # batch URL gửi mỗi campaign

if not TELEGRAM_BOT_TOKEN:
    raise RuntimeError("Missing TELEGRAM_BOT_TOKEN in environment.")
if not ONEHPING_API_KEY:
    raise RuntimeError("Missing ONEHPING_API_KEY in environment.")

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("1hping-bot")

# =========================
# Helpers chung
# =========================
def sanitize_campaign_name(name: str) -> str:
    """Loại bỏ ký tự lạ, rút gọn chiều dài cho an toàn API."""
    name = re.sub(r"\s+", " ", name).strip()
    name = re.sub(r"[^A-Za-z0-9 _\-\.\(\)\[\]]+", "", name)
    return name[:120] if len(name) > 120 else name

def _chunk(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]

async def send_typing(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    await context.bot.send_chat_action(chat_id=chat_id, action=ChatAction.TYPING)

def _short(obj, n=1200):
    s = str(obj)
    return (s[:n] + "…") if len(s) > n else s

# =========================
# Đọc Excel -> URL
# =========================
def _is_excel(doc: Document) -> bool:
    # Chỉ nhận .xlsx để tránh lỗi .xls/xlrd
    excel_mimes = {
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "application/octet-stream",
    }
    name = (doc.file_name or "").lower()
    return (doc.mime_type in excel_mimes) and name.endswith(".xlsx")

def extract_urls_from_excel(path: str) -> list[str]:
    """Đọc mọi sheet, mọi cột; gom chuỗi chứa http/https và xác thực nhanh."""
    dfs = pd.read_excel(path, sheet_name=None, header=None, dtype=str)
    urls = []
    url_like = re.compile(r"https?://[^\s<>\"\']+", re.I)
    for _, df in dfs.items():
        for val in df.to_numpy().flatten():
            if isinstance(val, str):
                for m in url_like.findall(val.strip()):
                    parsed = urlparse(m)
                    if parsed.scheme in ("http", "https") and parsed.netloc:
                        urls.append(m)
    # dedupe giữ thứ tự
    return list(dict.fromkeys(urls))

# =========================
# Call 1hping
# =========================
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
            data = {"raw": text}
        return {"status": resp.status, "data": data}

async def call_1hping_in_batches(
    session: aiohttp.ClientSession,
    campaign_name: str,
    number_of_day: int,
    urls: list[str],
    batch_size: int = BATCH_SIZE,
) -> list[tuple[str, dict]]:
    """Gọi API 1hping theo batch. Trả list (campaign_name_used, result_dict)."""
    results = []
    if not urls:
        return results
    parts = list(_chunk(urls, batch_size))
    for idx, part in enumerate(parts, start=1):
        name = campaign_name if len(parts) == 1 else f"{campaign_name}__part{idx}"
        res = await call_1hping_create_campaign(session, name, number_of_day, part)
        results.append((name, res))
    return results

# =========================
# Sitemap utilities
# =========================
def _norm_domain(raw: str) -> str:
    """Chuẩn hoá domain người dùng nhập: bỏ scheme/đường dẫn, chỉ giữ host."""
    raw = (raw or "").strip()
    if not raw:
        return ""
    if not re.match(r"^https?://", raw, flags=re.I):
        raw = "http://" + raw
    p = urlparse(raw)
    host = (p.netloc or "").strip().lower()
    host = host.split(":")[0]  # bỏ cổng
    return host

def _candidate_sitemap_urls(host: str) -> list[str]:
    """Sinh danh sách URL sitemap phổ biến để thử lần lượt (ưu tiên https)."""
    hosts = [host]
    if not host.startswith("www."):
        hosts.append("www." + host)
    cands = []
    for h in hosts:
        cands.extend([
            f"https://{h}/sitemap_index.xml",
            f"https://{h}/sitemap.xml",
            f"http://{h}/sitemap_index.xml",
            f"http://{h}/sitemap.xml",
        ])
    return cands

async def _fetch_bytes(session: aiohttp.ClientSession, url: str) -> bytes | None:
    try:
        async with session.get(url, headers={"User-Agent": CRAWLER_UA}, timeout=60) as resp:
            if resp.status != 200:
                return None
            content = await resp.read()
            ct = resp.headers.get("Content-Type", "")
            if url.lower().endswith(".gz") or "gzip" in ct or "application/gzip" in ct:
                try:
                    return gzip.decompress(content)
                except Exception:
                    return content
            return content
    except Exception as e:
        logger.warning("Fetch error %s: %s", url, e)
        return None

def _xml_findall(root: ET.Element, localname: str) -> list[ET.Element]:
    return root.findall(f".//{{*}}{localname}")

def _parse_sitemap_xml(xml_bytes: bytes) -> tuple[list[str], list[str]]:
    """
    Trả về (sitemap_links, page_urls).
    - Nếu là sitemap index: trả về list <sitemap><loc>.
    - Nếu là urlset: trả về list <url><loc>.
    """
    try:
        root = ET.fromstring(xml_bytes)
    except Exception:
        return [], []

    sitemap_links, page_urls = [], []

    for sm in _xml_findall(root, "sitemap"):
        loc_el = sm.find("{*}loc")
        if loc_el is not None and (loc := (loc_el.text or "").strip()):
            sitemap_links.append(loc)

    for u in _xml_findall(root, "url"):
        loc_el = u.find("{*}loc")
        if loc_el is not None and (loc := (loc_el.text or "").strip()):
            page_urls.append(loc)

    return sitemap_links, page_urls

async def _collect_urls_from_sitemaps(
    session: aiohttp.ClientSession,
    entry_urls: list[str],
    limit_depth: int = 6,
) -> list[str]:
    """Duyệt đệ quy sitemap index -> sitemap con -> urlset, loại trùng, giữ thứ tự."""
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

            for u in page_urls:
                if u not in seen_urls:
                    seen_urls.add(u)
                    ordered_urls.append(u)

            for c in child_sitemaps:
                if c not in seen_sitemaps:
                    next_queue.append(c)

        queue = next_queue
        depth += 1

    return ordered_urls

async def _discover_sitemap_entry_points(session: aiohttp.ClientSession, host: str) -> list[str]:
    """Thử tải các URL sitemap phổ biến, trả về entry points (index hoặc urlset)."""
    candidates = _candidate_sitemap_urls(host)
    entry_points = []

    for url in candidates:
        data = await _fetch_bytes(session, url)
        if not data:
            continue
        child_sitemaps, page_urls = _parse_sitemap_xml(data)

        if child_sitemaps:
            entry_points.append(url)
            entry_points.extend(child_sitemaps)
            break
        elif page_urls:
            entry_points.append(url)
            break

    return list(dict.fromkeys(entry_points))

# =========================
# Bot: Menu & Handlers
# =========================
HELP_TEXT = (
    "⚙️ Cú pháp lệnh:\n"
    "• /start — hướng dẫn nhanh.\n"
    "• /help — hiển thị menu cú pháp.\n"
    "• Gửi file **Excel (.xlsx)**: bot sẽ trích xuất URL và hỏi số ngày, rồi tạo campaign.\n"
    "• /indexweb <domain> — quét sitemap, gom URL, sau đó hỏi số ngày.\n"
    "   Ví dụ: `/indexweb abc.com`\n"
    "• /indexdomains [days] domain1 domain2 ... — ép index **nhiều domain** trực tiếp.\n"
    "   - Nếu token đầu là số → dùng làm số ngày, mặc định 1 ngày nếu bỏ trống.\n"
    "   Ví dụ: `/indexdomains 3 example.com abc.com`\n"
    "           `/indexdomains example.com,another.com`\n"
    "• /cancel — hủy phiên hiện tại.\n"
)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    msg = (
        "Chào bạn. Gửi **file Excel (.xlsx)** chứa list URL cần ép index.\n"
        "Hoặc dùng:\n"
        "• `/indexweb <domain>` — quét sitemap và gom URL.\n"
        "• `/indexdomains [days] domain1 domain2 ...` — ép index nhiều domain ngay.\n"
        "Sau khi có danh sách URL (từ file hoặc sitemap), bot sẽ hỏi số ngày nếu cần.\n"
        "Tên chiến dịch mặc định: `TelegramName_UserID`.\n\n"
        "Gõ /help để xem menu cú pháp."
    )
    await update.message.reply_text(msg, disable_web_page_preview=True)

async def help_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(HELP_TEXT, disable_web_page_preview=True)

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    await update.message.reply_text("Đã hủy và xóa trạng thái. Gửi lại file Excel hoặc dùng /indexweb, /indexdomains.")

# ===== Handlers: Excel -> hỏi số ngày -> gọi API =====
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
        await update.message.reply_text("Gửi file Excel (.xlsx) để bắt đầu hoặc dùng /indexweb <domain> hay /indexdomains.")
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

    try:
        timeout = aiohttp.ClientTimeout(total=180)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            results = await call_1hping_in_batches(session, campaign_name, days, urls, batch_size=BATCH_SIZE)
    except Exception as e:
        logger.exception("Error calling 1hping API")
        await update.message.reply_text(f"Lỗi gọi API 1hping: {e}")
        context.user_data.clear()
        return

    lines = [f"✅ Kết quả tạo {len(results)} campaign:"]
    for name, r in results:
        status = r.get("status")
        data = r.get("data")
        ok = (status and 200 <= status < 300)
        prefix = "• ✅" if ok else "• ❌"
        lines.append(f"{prefix} {name} — HTTP {status} — {_short(data)}")

    text = "\n".join(lines)
    if len(text) > 3500:
        with tempfile.NamedTemporaryFile("w+", delete=False, suffix=".txt") as tf:
            tf.write(text)
            tf.flush()
            fname = tf.name
        await context.bot.send_document(chat_id=update.effective_chat.id, document=open(fname, "rb"), filename="index_result.txt")
        try:
            os.remove(fname)
        except Exception:
            pass
    else:
        await update.message.reply_text(text, disable_web_page_preview=True)

    context.user_data.clear()

async def unknown(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Không hiểu lệnh. Gửi file Excel (.xlsx) hoặc dùng /indexweb, /indexdomains hoặc /help.")

# ===== /indexweb: quét sitemap một domain -> hỏi ngày =====
async def indexweb(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
    await update.message.reply_text(
        f"Đang dò sitemap cho `{host}` (ưu tiên https{' — bỏ qua SSL' if SKIP_SSL_VERIFY else ''})...",
        disable_web_page_preview=True
    )

    timeout = aiohttp.ClientTimeout(total=300)
    connector = aiohttp.TCPConnector(ssl=not SKIP_SSL_VERIFY)
    try:
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            entry_points = await _discover_sitemap_entry_points(session, host)
            if not entry_points:
                await update.message.reply_text("Không tìm thấy sitemap hợp lệ (sitemap_index.xml / sitemap.xml).")
                return
            urls = await _collect_urls_from_sitemaps(session, entry_points, limit_depth=6)
    except Exception as e:
        logger.exception("Error while indexing web via sitemap")
        await update.message.reply_text(f"Lỗi khi quét sitemap: {e}")
        return

    if not urls:
        await update.message.reply_text("Không thu thập được URL nào từ sitemap.")
        return

    # dedupe
    deduped = list(dict.fromkeys(urls))
    context.user_data["urls"] = deduped
    context.user_data["awaiting_days"] = True

    await update.message.reply_text(
        f"Đã thu thập **{len(deduped)} URL** từ sitemap `{host}`.\n"
        f"Bạn muốn chia ép trong **bao nhiêu ngày**? (nhập số nguyên, ví dụ: 1, 3, 7)"
    )

# ===== /indexdomains: ép index hàng loạt domain trực tiếp =====
def _parse_domains_raw(raw_tokens: list[str]) -> list[str]:
    out = []
    for t in raw_tokens:
        if not t:
            continue
        for piece in re.split(r"[,\n\r]+", t):
            p = piece.strip()
            if p:
                out.append(p)
    # dedupe giữ thứ tự
    seen = set()
    deduped = []
    for d in out:
        if d not in seen:
            seen.add(d)
            deduped.append(d)
    return deduped

async def indexdomains(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /indexdomains [days] domain1 domain2 ...
    - Nếu token đầu là số -> days; nếu không -> days=1.
    - Domain tách bằng dấu cách, dấu phẩy, hoặc xuống dòng.
    """
    args = context.args if hasattr(context, "args") else []
    if not args:
        await update.message.reply_text(
            "Cú pháp: `/indexdomains [days] domain1 domain2 ...`\n"
            "Ví dụ:\n"
            "`/indexdomains 3 example.com abc.com`\n"
            "`/indexdomains example.com,another.com`",
            disable_web_page_preview=True,
        )
        return

    days = 1
    tokens = list(args)
    if re.fullmatch(r"\d{1,4}", tokens[0]):
        days = int(tokens.pop(0))
        if days <= 0:
            await update.message.reply_text("Số ngày phải >= 1.")
            return
        if days > 365:
            await update.message.reply_text("Giới hạn tối đa 365 ngày.")
            return

    domains = _parse_domains_raw(tokens)
    if not domains:
        await update.message.reply_text("Không có domain hợp lệ trong tin nhắn.")
        return

    await send_typing(context, update.effective_chat.id)
    await update.message.reply_text(
        f"Nhận {len(domains)} domain. Bắt đầu quét sitemap và tạo campaign trong {days} ngày...\n"
        f"(Chạy song song tối đa 3 domain mỗi lần; SSL {'bỏ qua' if SKIP_SSL_VERIFY else 'bật kiểm tra'})",
        disable_web_page_preview=True
    )

    sem = asyncio.Semaphore(3)
    timeout = aiohttp.ClientTimeout(total=300)
    connector = aiohttp.TCPConnector(ssl=not SKIP_SSL_VERIFY)

    async def _process_domain(domain: str) -> tuple[str, dict]:
        async with sem:
            host = _norm_domain(domain)
            report: dict = {"domain": domain, "host": host, "urls_count": 0, "campaigns": [], "error": None}
            if not host:
                report["error"] = "Domain không hợp lệ"
                return domain, report

            try:
                async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
                    entry_points = await _discover_sitemap_entry_points(session, host)
                    if not entry_points:
                        report["error"] = "Không tìm thấy sitemap hợp lệ"
                        return domain, report
                    urls = await _collect_urls_from_sitemaps(session, entry_points, limit_depth=6)
            except Exception as e:
                logger.exception("Error while processing domain %s", domain)
                report["error"] = f"Lỗi khi quét sitemap: {e}"
                return domain, report

            deduped = list(dict.fromkeys(urls))
            report["urls_count"] = len(deduped)
            if not deduped:
                report["error"] = "Không thu thập được URL từ sitemap"
                return domain, report

            tg_user = update.effective_user
            display_name = tg_user.full_name or tg_user.username or "User"
            user_id = tg_user.id
            base_campaign_name = sanitize_campaign_name(f"{display_name}_{user_id}_{host}")

            try:
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    batch_results = await call_1hping_in_batches(session, base_campaign_name, days, deduped, batch_size=BATCH_SIZE)
            except Exception as e:
                logger.exception("Error calling 1hping for domain %s", domain)
                report["error"] = f"Lỗi gọi API 1hping: {e}"
                return domain, report

            for name, r in batch_results:
                report["campaigns"].append({
                    "name": name,
                    "status": r.get("status"),
                    "data_preview": _short(r.get("data"), 800),
                })
            return domain, report

    tasks = [asyncio.create_task(_process_domain(d)) for d in domains]
    results = await asyncio.gather(*tasks, return_exceptions=False)

    lines = []
    for domain, rep in results:
        if rep.get("error"):
            lines.append(f"• {domain} — ❌ {rep['error']}")
        else:
            lines.append(f"• {domain} — ✅ URLs: {rep['urls_count']} — Campaigns: {len(rep['campaigns'])}")
            for c in rep["campaigns"]:
                status = c.get("status")
                preview = c.get("data_preview") or ""
                lines.append(f"    - {c['name']} — HTTP {status} — {preview}")

    text = "\n".join(lines)
    if len(text) > 3500:
        with tempfile.NamedTemporaryFile("w+", delete=False, suffix=".txt") as tf:
            tf.write(text)
            tf.flush()
            fname = tf.name
        await context.bot.send_document(chat_id=update.effective_chat.id, document=open(fname, "rb"), filename="indexdomains_result.txt")
        try:
            os.remove(fname)
        except Exception:
            pass
    else:
        await update.message.reply_text(text, disable_web_page_preview=True)

# =========================
# Entry
# =========================
def main():
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    # Menu
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_menu))
    app.add_handler(CommandHandler("cancel", cancel))

    # Index commands
    app.add_handler(CommandHandler("indexweb", indexweb))
    app.add_handler(CommandHandler("indexdomains", indexdomains))

    # File & text flow
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    # Unknown commands
    app.add_handler(MessageHandler(filters.COMMAND, unknown))

    logger.info("Bot is starting...")
    app.run_polling(close_loop=False)

if __name__ == "__main__":
    main()
