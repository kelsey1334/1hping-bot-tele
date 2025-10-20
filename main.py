import asyncio
import logging
import os
import re
import tempfile
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
# Bot Handlers
# -------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    msg = (
        "Chào bạn. Gửi **file Excel (.xlsx)** chứa list URL cần ép index.\n"
        "Sau khi nhận file, mình sẽ hỏi bạn muốn chia trong **bao nhiêu ngày**.\n"
        "Tên chiến dịch sẽ là: `TelegramName_UserID`.\n\n"
        "Lệnh hữu ích:\n"
        "• /cancel — hủy phiên hiện tại và xóa trạng thái.\n"
    )
    await update.message.reply_text(msg, disable_web_page_preview=True)

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    await update.message.reply_text("Đã hủy và xóa trạng thái. Gửi lại file Excel nếu muốn tạo chiến dịch mới.")

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
        await update.message.reply_text("Gửi file Excel (.xlsx) để bắt đầu hoặc dùng /start.")
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
        await update.message.reply_text("Không tìm thấy danh sách URL trong phiên. Gửi lại file Excel.")
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
    await update.message.reply_text("Không hiểu lệnh. Gửi file Excel (.xlsx) hoặc dùng /start.")

# -------------------------
# Entry
# -------------------------
def main():
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("cancel", cancel))

    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    app.add_handler(MessageHandler(filters.COMMAND, unknown))

    logger.info("Bot is starting...")
    app.run_polling(close_loop=False)

if __name__ == "__main__":
    main()
