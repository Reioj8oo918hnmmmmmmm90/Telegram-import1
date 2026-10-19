"""
این فایل را فقط یک‌بار روی کامپیوتر خودت اجرا کن (نه روی Railway) تا SESSION_STRING بسازی.

    pip install telethon
    python gen_session.py
"""
from telethon.sync import TelegramClient
from telethon.sessions import StringSession

api_id = int(input("API_ID: ").strip())
api_hash = input("API_HASH: ").strip()

with TelegramClient(StringSession(), api_id, api_hash) as client:
    print("\n=== SESSION_STRING (محرمانه! مثل رمز اکانتته) ===\n")
    print(client.session.save())
