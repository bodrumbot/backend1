# ==========================================
# BODRUM BOT - PAYME CHEK PARSER + AUTO ACCEPT
# To'g'rilangan versiya - Guruh handleri fixparse_payme_receipt
# ==========================================

import os
import logging
import asyncio
from flask import app
import psycopg2
import psycopg2.extras
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
import requests
import time
import re
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo, KeyboardButton, ReplyKeyboardMarkup, ReplyKeyboardRemove, Chat
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
    JobQueue
)
from aiohttp import web
import json
import aiohttp_cors


load_dotenv()

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ==========================================
# ENVIRONMENT VARIABLES
# ==========================================

PORT = int(os.getenv("PORT", "3000"))
DATABASE_URL = (os.getenv("DATABASE_PUBLIC_URL") or 
                os.getenv("DATABASE_URL") or 
                os.getenv("DATABASE_PRIVATE_URL") or
                os.getenv("POSTGRES_URL"))

TOKEN = os.getenv("TOKEN")
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID", "")
WEBAPP_URL = os.getenv("WEBAPP_URL", "")

# ⭐ MUHIM: Payme cheklar keladigan guruh ID si
PAYME_RECEIPTS_GROUP_ID = os.getenv("PAYME_RECEIPTS_GROUP_ID", "")



# ⭐ YANGI: Guruh ID sini to'g'ri parse qilish
def parse_chat_id(chat_id_str):
    """Guruh yoki user ID sini to'g'ri formatga o'tkazish"""
    if not chat_id_str:
        return 0
    try:
        # Faqat raqamlarni olish
        chat_id = str(chat_id_str).strip()
        # Agar -100 bilan boshlansa (guruh) yoki - bilan boshlansa
        if chat_id.startswith('-100'):
            return int(chat_id)
        elif chat_id.startswith('-'):
            return int(chat_id)
        else:
            return int(chat_id)
    except Exception as e:
        logger.error(f"Chat ID parse xatosi: {e}")
        return 0

try:
    ADMIN_CHAT_ID_INT = parse_chat_id(ADMIN_CHAT_ID)
    PAYME_GROUP_ID_INT = parse_chat_id(PAYME_RECEIPTS_GROUP_ID)
except ValueError as e:
    logger.error(f"Chat ID parse xatosi: {e}")
    ADMIN_CHAT_ID_INT = 0
    PAYME_GROUP_ID_INT = 0

logger.info(f"🔧 ADMIN_CHAT_ID: {ADMIN_CHAT_ID}, parsed: {ADMIN_CHAT_ID_INT}")
logger.info(f"🔧 PAYME_RECEIPTS_GROUP_ID: {PAYME_RECEIPTS_GROUP_ID}, parsed: {PAYME_GROUP_ID_INT}")

# Global application
application = None

# ==========================================
# DATABASE FUNCTIONS
# ==========================================

# init_database() funksiyasida orders jadvalini yangilash
# init_database() funksiyasiga qo'shing (app.py dagi)

def init_database():
    """Jadval va column'larni avtomatik yaratish/yangilash"""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Orders jadvali - tg_id BIGINT ga o'zgartirish
        cur.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id SERIAL PRIMARY KEY,
                order_id VARCHAR(100) UNIQUE NOT NULL,
                name VARCHAR(255),
                phone VARCHAR(20),
                items JSONB,
                total INTEGER,
                status VARCHAR(50) DEFAULT 'pending_payment',
                payment_status VARCHAR(50) DEFAULT 'pending',
                payment_method VARCHAR(50) DEFAULT 'payme',
                location VARCHAR(255),
                tg_id BIGINT,  -- INTEGER o'rniga BIGINT
                notified BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                accepted_at TIMESTAMP,
                rejected_at TIMESTAMP,
                paid_at TIMESTAMP,
                confirmed_at TIMESTAMP,
                admin_note TEXT,
                transaction_id VARCHAR(100),
                auto_accepted BOOLEAN DEFAULT FALSE,
                initiated_from VARCHAR(50) DEFAULT 'website',
                source VARCHAR(50) DEFAULT 'website',
                payme_receipt_id VARCHAR(100),
                payme_card_mask VARCHAR(50)
            )
        """)
        
        # ⭐⭐⭐ YANGI: Mavjud tg_id ustunini BIGINT ga o'zgartirish
        cur.execute("""
            DO $$
            BEGIN
                -- Agar tg_id INTEGER bo'lsa, BIGINT ga o'zgartirish
                IF EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_name = 'orders' 
                    AND column_name = 'tg_id'
                    AND data_type = 'integer'
                ) THEN
                    ALTER TABLE orders ALTER COLUMN tg_id TYPE BIGINT;
                    RAISE NOTICE 'tg_id INTEGER -> BIGINT o''zgartirildi';
                END IF;
            END $$;
        """)
        
        # Users jadvali - tg_id BIGINT
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                tg_id BIGINT UNIQUE NOT NULL,  -- INTEGER o'rniga BIGINT
                name VARCHAR(255),
                phone VARCHAR(20),
                username VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # ⭐⭐⭐ YANGI: Mavjud users.tg_id ni ham BIGINT ga o'zgartirish
        cur.execute("""
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_name = 'users' 
                    AND column_name = 'tg_id'
                    AND data_type = 'integer'
                ) THEN
                    ALTER TABLE users ALTER COLUMN tg_id TYPE BIGINT;
                    RAISE NOTICE 'users.tg_id INTEGER -> BIGINT o''zgartirildi';
                END IF;
            END $$;
        """)
        
        conn.commit()
        cur.close()
        logger.info("✅ Database initialized successfully with BIGINT tg_id")
        return True
        
    except Exception as e:
        logger.error(f"❌ Database init error: {e}")
        return False
    finally:
        if conn:
            conn.close()


def get_db_connection():
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL not set!")
    try:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
        return conn
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        raise

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start handler"""
    user = update.effective_user
    
    # ⭐ DEBUG: Kim start bosayotganini ko'rish
    logger.info(f"🚀 /start - User ID: {user.id}, Admin ID: {ADMIN_CHAT_ID_INT}")
    
    is_admin = (user.id == ADMIN_CHAT_ID_INT)
    
    if is_admin:
        keyboard = [
            [InlineKeyboardButton("🛎️ Yangi buyurtmalar", callback_data="show_new_orders")],
            [InlineKeyboardButton("📊 Statistika", callback_data="admin_stats")],
            [InlineKeyboardButton("🍽️ Menyu ko'rish", web_app=WebAppInfo(url=WEBAPP_URL))],
            [InlineKeyboardButton("⚙️ Admin Panel", web_app=WebAppInfo(url=f"{WEBAPP_URL}/admin.html"))]
        ]
        
        welcome_text = (
            f"👋 <b>Salom, Admin {user.first_name}!</b>\n\n"
            f"🤖 <b>BODRUM</b> admin paneliga xush kelibsiz!\n\n"
            f"⏰ {datetime.now().strftime('%H:%M:%S')}"
        )
        
        await update.message.reply_text(
            welcome_text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML'
        )
        return
    
    # Oddiy foydalanuvchi
    profile = get_user_profile(user.id)
    
    if profile and profile.get('phone'):
        name = profile.get('name', 'Foydalanuvchi')
        phone = profile.get('phone', '')
        
        # Telefon formatini chiroyli qilish
        formatted_phone = phone
        if len(phone) == 9:
            formatted_phone = f"{phone[:2]} {phone[2:5]} {phone[5:7]} {phone[7:]}"
        
        keyboard = [
            [InlineKeyboardButton("🍽️ Menyuni ko'rish", web_app=WebAppInfo(url=WEBAPP_URL))]
        ]
        
        await update.message.reply_text(
            f"👋 Salom, <b>{name}</b>!\n\n"
            f"🍽️ <b>BODRUM</b> restoraniga xush kelibsiz!\n\n"
            f"📞 Telefon: +998 {formatted_phone}\n\n"
            f"🛒 Menyudan buyurtma berishingiz mumkin:",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML'
        )
    else:
        # Ro'yxatdan o'tmagan foydalanuvchi
        keyboard = ReplyKeyboardMarkup(
            [[KeyboardButton("📱 Telefon raqamni yuborish", request_contact=True)]],
            resize_keyboard=True,
            one_time_keyboard=True
        )
        
        await update.message.reply_text(
            f"👋 Salom, <b>{user.first_name}</b>!\n\n"
            f"🍽️ <b>BODRUM</b> restoraniga xush kelibsiz!\n\n"
            f"📱 Buyurtma berish uchun telefon raqamingizni yuboring:",
            reply_markup=keyboard,
            parse_mode='HTML'
        )

# ==========================================
# TELEGRAM BOT API DIRECT FUNCTIONS
# ==========================================

def send_telegram_message(chat_id: int, text: str, parse_mode: str = 'HTML', reply_markup=None) -> bool:
    """Direct API call to send message"""
    if not TOKEN:
        logger.error("❌ TOKEN not set")
        return False
    
    try:
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
        payload = {
            'chat_id': chat_id,
            'text': text,
            'parse_mode': parse_mode
        }
        if reply_markup:
            payload['reply_markup'] = json.dumps(reply_markup.to_dict() if hasattr(reply_markup, 'to_dict') else reply_markup)
        
        response = requests.post(url, json=payload, timeout=10)
        result = response.json()
        
        if result.get('ok'):
            logger.info(f"✅ Message sent to {chat_id}")
            return True
        else:
            logger.error(f"❌ Telegram API error: {result}")
            return False
    except Exception as e:
        logger.error(f"❌ send_telegram_message error: {e}")
        return False

def send_telegram_location(chat_id: int, latitude: float, longitude: float) -> bool:
    """Direct API call to send location"""
    if not TOKEN:
        return False
    
    try:
        url = f"https://api.telegram.org/bot{TOKEN}/sendLocation"
        payload = {
            'chat_id': chat_id,
            'latitude': latitude,
            'longitude': longitude
        }
        response = requests.post(url, json=payload, timeout=10)
        return response.json().get('ok', False)
    except Exception as e:
        logger.error(f"❌ send_telegram_location error: {e}")
        return False

# ==========================================
# PAYME CHEK PARSER - FAQAT ORDER ID
# ==========================================

def parse_payme_receipt(text: str) -> Optional[Dict[str, Any]]:
    """Payme chek parse - faqat ORDER ID ni oladi, qolganini ignore qiladi"""
    try:
        if not text:
            return None
        
        # Faqat ORD_ patternini qidirish - eng muhim qism
        # Format: ORD_1234567890_abcdefgh (case insensitive)
        order_match = re.search(r'ORD_[A-Za-z0-9_]+', text, re.IGNORECASE)
        
        if not order_match:
            logger.debug("❌ ORDER ID pattern topilmadi")
            return None
        
        # Order ID ni olamiz (asl holida)
        order_id = order_match.group(0)
        
        logger.info(f"✅ ORDER ID topildi: {order_id}")
        
        # Qolgan ma'lumotlarni ham olishga harakat qilamiz (agar bo'lsa)
        # Agar regex ishlamasa ham, order_id ni qaytaramiz
        
        # Summa (ixtiyoriy)
        amount = 0
        try:
            # 🇺🇿 1 000,00 сум yoki 1 000,00 so'm
            amount_match = re.search(r'[\d\s,]+\s*(?:сум|so\'m|s\'om)', text, re.IGNORECASE)
            if amount_match:
                amount_str = amount_match.group(0).replace('сум', '').replace('so\'m', '').replace('s\'om', '').replace(' ', '').replace(',', '.').strip()
                amount = int(float(amount_str))
        except:
            pass
        
        # Transaction ID (ixtiyoriy)
        transaction_id = None
        try:
            trans_match = re.search(r'[a-f0-9]{24}', text, re.IGNORECASE)
            if trans_match:
                transaction_id = trans_match.group(0)
        except:
            pass
        
        # Chek ID (ixtiyoriy)
        receipt_id = None
        try:
            # 🧾 569 yoki shunga o'xshash
            receipt_match = re.search(r'🧾\s*(\d+)', text)
            if receipt_match:
                receipt_id = receipt_match.group(1)
        except:
            pass
        
        result = {
            'order_id': order_id,
            'amount': amount,
            'card_mask': None,  # Endi shart emas
            'receipt_id': receipt_id,
            'transaction_id': transaction_id,
            'customer_name': None,  # Endi shart emas
            'raw_text': text[:100]  # Log uchun qisqa matn
        }
        
        return result
        
    except Exception as e:
        logger.error(f"❌ Chek parse xatosi: {e}")
        return None
        



# ==========================================
# PAYME GURUHIGA O'TISH VA TEKSHIRISH
# ==========================================

def get_payme_group_link(order_id: str) -> str:
    """Payme guruhiga o'tish uchun link yaratish"""
    # Guruh username yoki invite link
    # O'zingizning guruh username ni yozing
    PAYME_GROUP_USERNAME = "bodrumbota"  # O'zgartiring!
    
    # Order ID ni qidirish uchun guruhga o'tish
    # Telegram deep link orqali guruhga o'tish va qidirish
    return f"https://t.me/{PAYME_GROUP_USERNAME}?q={order_id}"

async def open_payme_group_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Payme guruhiga o'tish tugmasi handleri"""
    query = update.callback_query
    await query.answer()
    
    order_id = query.data.replace("open_payme_group_", "")
    
    # Guruhga o'tish linkini yaratish
    group_link = get_payme_group_link(order_id)
    
    # Inline keyboard bilan guruhga o'tish tugmasi
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("💳 Payme guruhiga o'tish", url=group_link)],
        [InlineKeyboardButton("🔙 Orqaga", callback_data=f"back_to_order_{order_id}")]
    ])
    
    await query.edit_message_text(
        f"💳 <b>To'lovni tekshirish</b>\n\n"
        f"🆔 Buyurtma: #{order_id[-6:]}\n\n"
        f"Payme guruhiga o'tib, quyidagi ORDER ID ni qidiring:\n"
        f"<code>{order_id}</code>\n\n"
        f"To'lov topilsa, qaytib kelib \"Qabul qilish\" ni bosing.",
        reply_markup=keyboard,
        parse_mode='HTML'
    )

async def save_categories_handler(request):
    """Kategoriyalarni saqlash (faqat admin)"""
    try:
        data = await request.json()
        categories = data.get('categories', [])
        
        # JSON faylga saqlash
        with open('categories.json', 'w', encoding='utf-8') as f:
            json.dump(categories, f, ensure_ascii=False, indent=2)
        
        return web.json_response({
            "success": True,
            "message": "Categories saved",
            "timestamp": datetime.utcnow().isoformat()
        }, headers=get_cors_headers())
        
    except Exception as e:
        logger.error(f"Save categories error: {e}")
        return web.json_response({
            "success": False,
            "error": str(e)
        }, status=500, headers=get_cors_headers())

async def get_categories_handler(request):
    """Kategoriyalarni olish - barcha mijozlar uchun"""
    try:
        import json
        import os
        
        # categories.json fayl yo'li
        categories_file = os.path.join(os.path.dirname(__file__), 'categories.json')
        
        # Fayldan kategoriyalarni o'qish
        if os.path.exists(categories_file):
            with open(categories_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                # Agar JSON da "categories" kaliti bo'lsa
                if isinstance(data, dict) and 'categories' in data:
                    categories = data['categories']
                else:
                    categories = data
        else:
            # Default kategoriyalar (agar fayl yo'q bo'lsa)
            categories = [
                {"id": "all", "name": "Все", "icon": "🍽"},
                {"id": "salad", "name": "Салаты", "icon": "🥗"},
                {"id": "soup", "name": "Супы", "icon": "🍜"},
                {"id": "bread", "name": "Хлеб", "icon": "🍞"},
                {"id": "pide", "name": "Пиде", "icon": "🫓"},
                {"id": "pizza", "name": "Пицца", "icon": "🍕"},
                {"id": "sandwich", "name": "Сендвич/Бургер", "icon": "🍔"},
                {"id": "fish", "name": "Рыба", "icon": "🐟"},
                {"id": "main", "name": "Основные блюда", "icon": "🍖"},
                {"id": "side", "name": "Гарниры", "icon": "🍟"},
                {"id": "dessert", "name": "Десерты", "icon": "🍰"},
                {"id": "fruit", "name": "Фрукты", "icon": "🍇"},
                {"id": "drink", "name": "Напитки", "icon": "🥤"}
            ]
            
            # Fayl yo'q bo'lsa, yaratib qo'yish (birinchi marta)
            try:
                with open(categories_file, 'w', encoding='utf-8') as f:
                    json.dump({"categories": categories, "updated_at": datetime.utcnow().isoformat()}, 
                             f, ensure_ascii=False, indent=2)
                logger.info(f"✅ Default categories.json yaratildi: {categories_file}")
            except Exception as e:
                logger.error(f"❌ categories.json yaratishda xato: {e}")
        
        # Timestamp ni ham qaytarish (kesh uchun)
        updated_at = datetime.utcnow().isoformat()
        
        # Agar fayldan o'qilgan bo'lsa va updated_at bo'lsa
        if os.path.exists(categories_file):
            try:
                with open(categories_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if isinstance(data, dict) and 'updated_at' in data:
                        updated_at = data['updated_at']
            except:
                pass
        
        logger.info(f"📋 Kategoriyalar so'raldi: {len(categories)} ta")
        
        return web.json_response({
            "success": True,
            "categories": categories,
            "count": len(categories),
            "timestamp": updated_at,
            "server_time": datetime.utcnow().isoformat()
        }, headers=get_cors_headers())
        
    except Exception as e:
        logger.error(f"❌ Get categories error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        
        return web.json_response({
            "success": False,
            "error": str(e),
            "categories": []  # Xatolik bo'lsa ham bo'sh massiv qaytarish
        }, status=500, headers=get_cors_headers())

async def show_order_to_admin(update: Update, context: ContextTypes.DEFAULT_TYPE, order: Dict):
    """Buyurtma ma'lumotlarini admin ga qayta ko'rsatish"""
    items = order.get('items', [])
    if isinstance(items, str):
        items = json.loads(items)
    
    items_text = "\n".join([f"• {i.get('name')} x{i.get('qty')}" for i in items]) if items else "Ma'lumot yo'q"
    
    raw_phone = order.get('phone', '')
    phone_display = format_phone_display(raw_phone)
    
    location = order.get('location')
    location_text = ""
    if location and ',' in str(location):
        try:
            lat, lng = str(location).split(',')
            lat = float(lat.strip())
            lng = float(lng.strip())
            location_text = f"\n📍 <b>Joylashuv:</b> <a href='https://maps.google.com/?q={lat},{lng}'>Xaritada ko'rish</a>"
        except:
            pass
    
    status_text = "💳 <b>TO'LOV QILINDI - QABUL QILISH KERAK!</b>"
    
    message = f"""{status_text}

🆔 Buyurtma: #{order.get('order_id', 'N/A')[-6:]}
👤 Mijoz: {order.get('name')}
📞 Telefon: {phone_display}
💵 Summa: {format_price(order.get('total', 0))} so'm
💳 Karta: {order.get('payme_card_mask', 'N/A')}
🧾 Chek ID: {order.get('payme_receipt_id', 'N/A')}
📱 Manba: {'🤖 WebApp' if order.get('source') == 'webapp' else '🌐 Sayt'}{location_text}

🍽 Mahsulotlar:
{items_text}

⏰ {datetime.now().strftime('%H:%M:%S')}

<i>⚡ To'lov muvaffaqiyatli! Buyurtmani qabul qiling yoki bekor qiling</i>"""

    keyboard = [
        [
            InlineKeyboardButton("✅ QABUL QILISH", callback_data=f"accept_{order.get('order_id')}"),
            InlineKeyboardButton("❌ BEKOR QILISH", callback_data=f"reject_{order.get('order_id')}")
        ],
        [
            InlineKeyboardButton("💳 TO'LOVNI TEKSHIRISH", callback_data=f"open_payme_group_{order.get('order_id')}")
        ]
    ]
    
    if update.callback_query:
        await update.callback_query.edit_message_text(
            message,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML'
        )
    else:
        await context.bot.send_message(
            chat_id=ADMIN_CHAT_ID_INT,
            text=message,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML'
        )

def save_user_profile(tg_id: int, name: str, phone: str, username: str = None) -> bool:
    """Foydalanuvchi profilini saqlash yoki yangilash"""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute("""
            INSERT INTO users (tg_id, name, phone, username, updated_at)
            VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (tg_id) 
            DO UPDATE SET 
                name = EXCLUDED.name,
                phone = EXCLUDED.phone,
                username = EXCLUDED.username,
                updated_at = CURRENT_TIMESTAMP
            RETURNING *
        """, (tg_id, name, phone, username))
        
        result = cur.fetchone()
        conn.commit()
        cur.close()
        
        logger.info(f"✅ Profil saqlandi: {tg_id} - {name} - {phone}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Profil saqlash xatosi: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()

def get_user_profile(tg_id: int) -> Optional[Dict[str, Any]]:
    """Foydalanuvchi profilini olish"""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute("SELECT * FROM users WHERE tg_id = %s", (tg_id,))
        result = cur.fetchone()
        cur.close()
        
        if result:
            profile = dict(result)
            for key in ['created_at', 'updated_at']:
                if profile.get(key) and hasattr(profile[key], 'isoformat'):
                    profile[key] = profile[key].isoformat()
            return profile
        return None
        
    except Exception as e:
        logger.error(f"❌ Profil olish xatosi: {e}")
        return None
    finally:
        if conn:
            conn.close()



def get_user_orders(tg_id: int) -> List[Dict[str, Any]]:
    """Foydalanuvchining barcha buyurtmalarini olish"""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT * FROM orders 
            WHERE tg_id = %s 
            ORDER BY created_at DESC
        """, (tg_id,))
        results = cur.fetchall()
        cur.close()
        
        orders = []
        for row in results:
            order_dict = dict(row)
            for key in ['created_at', 'accepted_at', 'rejected_at', 'paid_at', 'confirmed_at']:
                if order_dict.get(key) and hasattr(order_dict[key], 'isoformat'):
                    order_dict[key] = order_dict[key].isoformat()
            orders.append(order_dict)
        
        return orders
        
    except Exception as e:
        logger.error(f"❌ Buyurtmalarni olish xatosi: {e}")
        return []
    finally:
        if conn:
            conn.close()

def format_price(price: int) -> str:
    return f"{price:,}".replace(",", " ")

def format_phone_display(phone: str) -> str:
    """Telefon raqamini ko'rsatish uchun formatlash"""
    if not phone:
        return "Noma'lum"
    phone = ''.join(filter(str.isdigit, str(phone)))
    if phone.startswith('998'):
        phone = phone[3:]
    phone = phone[-9:] if len(phone) > 9 else phone
    return f"+998{phone}"

# app.py - create_order funksiyasida TUZATISH
def create_order(data: Dict) -> Optional[Dict]:
    """Yangi buyurtma yaratish"""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        items = data.get('items', [])
        if isinstance(items, list):
            items_json = json.dumps(items)
        else:
            items_json = str(items) if items else '[]'

        # ⭐⭐⭐ TO'G'RILANGAN tg_id olish
        tg_id = None
        try:
            # 1. tgId yoki tg_id dan olish
            tg_id_raw = data.get('tgId') or data.get('tg_id')
            
            # 2. Agar string bo'lsa va raqamli bo'lsa
            if tg_id_raw and str(tg_id_raw).isdigit():
                tg_id = int(tg_id_raw)
                logger.info(f"✅ tg_id integer ga o'tkazildi: {tg_id}")
            elif tg_id_raw:
                # Agar 'tg_123' formatida bo'lsa
                clean_id = str(tg_id_raw).replace('tg_', '').replace('user_', '')
                if clean_id.isdigit():
                    tg_id = int(clean_id)
                    
        except Exception as e:
            logger.error(f"❌ tg_id conversion xatosi: {e}")
            tg_id = None

        # NULL uchun to'g'ri SQL
        order_id = data.get('orderId')
        name = data.get('name', 'Mijoz')
        phone = data.get('phone', '000000000')
        total = data.get('total', 0)
        location = data.get('location')
        source = data.get('source', 'website')
        
        logger.info(f"📋 INSERT: order_id={order_id}, tg_id={tg_id} (type: {type(tg_id)})")

        cur.execute("""
            INSERT INTO orders (
                order_id, name, phone, items, total, 
                status, payment_status, payment_method, 
                location, tg_id, notified, created_at,
                source
            )
            VALUES (%s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING *
        """, (
            order_id, name, phone, items_json, total, 
            'pending_payment', 'pending', 'payme',
            location, tg_id, False, datetime.utcnow(),
            source
        ))

        result = cur.fetchone()
        conn.commit()
        cur.close()

        if result:
            order_dict = dict(result)
            # ⭐⭐⭐ MUHIM: datetime obyektlarini string ga aylantirish
            for key in ['created_at', 'accepted_at', 'rejected_at', 'paid_at', 'confirmed_at']:
                if order_dict.get(key) and hasattr(order_dict[key], 'isoformat'):
                    order_dict[key] = order_dict[key].isoformat()
            logger.info(f"✅ Buyurtma yaratildi: {order_dict.get('order_id')}")
            return order_dict
        return None

    except Exception as e:
        logger.error(f"❌ Create order ERROR: {e}")
        import traceback
        logger.error(traceback.format_exc())  # To'liq traceback
        if conn:
            conn.rollback()
        return None
    finally:
        if conn:
            conn.close()

def update_order_status(order_id: str, status: str, **kwargs) -> Optional[Dict[str, Any]]:
    """Buyurtma statusini yangilash - tg_id ni saqlab qolish bilan"""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Avval buyurtmani olish (tg_id ni saqlab qolish uchun)
        cur.execute("SELECT * FROM orders WHERE order_id = %s", (order_id,))
        existing = cur.fetchone()

        if not existing:
            logger.warning(f"⚠️ update_order_status: Buyurtma topilmadi: {order_id}")
            return None

        existing_dict = dict(existing)
        existing_tg_id = existing_dict.get('tg_id')
        logger.info(f"📊 Mavjud tg_id: {existing_tg_id}")

        update_data = {'status': status}

        # TIMESTAMP FIELDLAR
        timestamp_fields = {
            'accepted': 'accepted_at',
            'rejected': 'rejected_at', 
            'confirmed': 'confirmed_at',
            'pending_payment': None,
            'pending': None
        }

        if status in timestamp_fields and timestamp_fields[status]:
            field_name = timestamp_fields[status]
            update_data[field_name] = datetime.utcnow()

        if status == 'confirmed':
            update_data['accepted_at'] = datetime.utcnow()

        if kwargs.get('paid_at'):
            update_data['paid_at'] = kwargs.get('paid_at')

        if 'notified' in kwargs:
            update_data['notified'] = kwargs['notified']

        # Boshqa parametrlarni qo'shish (tg_id ni update qilmaslik uchun tekshiramiz)
        for key, val in kwargs.items():
            if val is not None and key not in ['paid_at', 'notified', 'accepted_at', 'confirmed_at', 'rejected_at', 'tg_id']:
                update_data[key] = val

        # SQL query yaratish
        fields = []
        values = []
        for key, val in update_data.items():
            fields.append(f"{key} = %s")
            values.append(val)
        values.append(order_id)

        query = f"UPDATE orders SET {', '.join(fields)} WHERE order_id = %s RETURNING *"
        cur.execute(query, values)
        result = cur.fetchone()
        conn.commit()
        cur.close()

        if result:
            order_dict = dict(result)
            # ⭐ MUHIM: Agar yangilangan order da tg_id yo'q bo'lsa, avvalgidan qayta tiklash
            if not order_dict.get('tg_id') and existing_tg_id:
                order_dict['tg_id'] = existing_tg_id
                # Database ni ham yangilash
                conn2 = get_db_connection()
                cur2 = conn2.cursor()
                cur2.execute("UPDATE orders SET tg_id = %s WHERE order_id = %s", (existing_tg_id, order_id))
                conn2.commit()
                cur2.close()
                conn2.close()
                logger.info(f"🔄 tg_id qayta tiklandi: {existing_tg_id}")

            logger.info(f"✅ Buyurtma yangilandi: {order_id}, status: {status}, tg_id: {order_dict.get('tg_id')}")
            return order_dict
        return None

    except Exception as e:
        logger.error(f"Update error: {e}")
        import traceback
        traceback.print_exc()
        if conn:
            conn.rollback()
        return None
    finally:
        if conn:
            conn.close()

async def get_order_handler(request):
    try:
        order_id = request.match_info['order_id']
        order = get_order(order_id)
        
        if not order:
            return web.json_response(
                {"error": "Not found"}, 
                status=404, 
                headers=get_cors_headers()
            )
        
        # ⭐⭐⭐ MUHIM: datetime obyektlarini string ga aylantirish
        response_data = {**order}
        for key in ['created_at', 'accepted_at', 'rejected_at', 'paid_at', 'confirmed_at']:
            if response_data.get(key) and hasattr(response_data[key], 'isoformat'):
                response_data[key] = response_data[key].isoformat()
        
        return web.json_response(response_data, headers=get_cors_headers())
        
    except Exception as e:
        logger.error(f"API get order error: {e}")
        return web.json_response(
            {"error": str(e)}, 
            status=500, 
            headers=get_cors_headers()
        )

async def notify_admin_payment_received(order: Dict, bot=None):
    """
    Admin ga yangi buyurtma haqida xabar (to'lov tekshirilmagan)
    """
    try:
        logger.info(f"🔔 notify_admin_new_order: {order.get('order_id')}")

        if not ADMIN_CHAT_ID_INT:
            logger.error("❌ ADMIN_CHAT_ID o'rnatilmagan!")
            return False

        if bot is None:
            global application
            if application and application.bot:
                bot = application.bot
            else:
                logger.error("❌ Bot mavjud emas!")
                return False

        # Buyurtma ma'lumotlarini olish
        items = order.get('items', [])
        if isinstance(items, str):
            items = json.loads(items)

        items_text = "\n".join([f"• {i.get('name')} x{i.get('qty')}" for i in items]) if items else "Ma'lumot yo'q"

        raw_phone = order.get('phone', '')
        phone_display = format_phone_display(raw_phone)

        customer_name = order.get('name', 'Mijoz')
        if not customer_name or customer_name == 'null':
            customer_name = 'Mijoz'

        location = order.get('location')
        location_text = ""
        location_coords = None

        if location and ',' in str(location):
            try:
                lat, lng = str(location).split(',')
                lat = float(lat.strip())
                lng = float(lng.strip())
                location_text = f"\n📍 <b>Joylashuv:</b> <a href='https://maps.google.com/?q={lat},{lng}'>Xaritada ko'rish</a>"
                location_coords = (lat, lng)
            except Exception as e:
                logger.warning(f"Joylashuv parse xatosi: {e}")
                location_text = f"\n📍 <b>Manzil:</b> {location}"
        elif location:
            location_text = f"\n📍 <b>Manzil:</b> {location}"

        source = order.get('source', 'website')
        source_icon = "🤖 WebApp" if source == 'webapp' else "🌐 Sayt"

        # ⭐ YANGI: To'lov kutilmoqda statusi
        status_text = "⏳ <b>YANGI BUYURTMA - TO'LOV KUTILMOQDA!</b>"

        admin_message = f"""{status_text}

🆔 Buyurtma: #{order.get('order_id', 'N/A')[-6:]}
👤 Mijoz: {customer_name}
📞 Telefon: {phone_display}
💵 Summa: {format_price(order.get('total', 0))} so'm
📱 Manba: {source_icon}{location_text}

🍽 Mahsulotlar:
{items_text}

⏰ {datetime.now().strftime('%H:%M:%S')}

<i>⚡ To'lovni tekshiring, keyin qabul qiling yoki bekor qiling</i>"""

        # ⭐⭐⭐ 3 TA TUGMA: Qabul, Bekor, To'lovni tekshirish
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ QABUL QILISH", callback_data=f"accept_{order.get('order_id')}"),
                InlineKeyboardButton("❌ BEKOR QILISH", callback_data=f"reject_{order.get('order_id')}")
            ],
            [
                InlineKeyboardButton("💳 TO'LOVNI TEKSHIRISH", callback_data=f"open_payme_group_{order.get('order_id')}")
            ]
        ])

        admin_sent = await bot.send_message(
            chat_id=ADMIN_CHAT_ID_INT,
            text=admin_message,
            reply_markup=keyboard,
            parse_mode='HTML'
        )

        if location_coords and admin_sent:
            try:
                await bot.send_location(
                    chat_id=ADMIN_CHAT_ID_INT,
                    latitude=location_coords[0],
                    longitude=location_coords[1]
                )
            except Exception as e:
                logger.error(f"❌ Joylashuv yuborish xatosi: {e}")

        return True

    except Exception as e:
        logger.error(f"❌ notify_admin_new_order xatosi: {e}")
        import traceback
        traceback.print_exc()
        return False

def get_cors_headers():
    return {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
        'Access-Control-Allow-Headers': '*',
        'Access-Control-Max-Age': '86400',
    }

async def options_handler(request):
    """CORS preflight so'rovlarini qaytarish"""
    return web.Response(
        status=200,
        headers=get_cors_headers(),
        body='{}'
    )

# ==========================================
# TELEGRAM BOT FUNCTIONS
# ==========================================

async def contact_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Contact qabul qilish"""
    user = update.effective_user
    contact = update.message.contact
    
    if not contact:
        return
    
    phone = contact.phone_number
    
    if phone.startswith('+'):
        phone = phone[1:]
    
    if phone.startswith('998'):
        phone = phone[3:]
    
    phone = phone[-9:] if len(phone) > 9 else phone
    
    success = save_user_profile(
        tg_id=user.id,
        name=user.first_name or "Foydalanuvchi",
        phone=phone,
        username=user.username
    )
    
    if success:
        await update.message.reply_text(
            "✅ <b>Ma'lumotlar saqlandi!</b>",
            reply_markup=ReplyKeyboardRemove(),
            parse_mode='HTML'
        )
        
        keyboard = [
            [InlineKeyboardButton("🍽️ Menyuni ko'rish", web_app=WebAppInfo(url=WEBAPP_URL))]
        ]
        
        formatted_phone = f"{phone[:2]} {phone[2:5]} {phone[5:7]} {phone[7:]}"
        
        await update.message.reply_text(
            f"👋 Salom, <b>{user.first_name}</b>!\n\n"
            f"🍽️ <b>BODRUM</b> restoraniga xush kelibsiz!\n\n"
            f"📞 Telefon: +998 {formatted_phone}\n\n"
            f"🛒 Menyudan buyurtma berishingiz mumkin:",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML'
        )
    else:
        await update.message.reply_text(
            "❌ Xatolik yuz berdi. Iltimos, qayta urinib ko'ring.",
            reply_markup=ReplyKeyboardRemove(),
            parse_mode='HTML'
        )

async def show_new_orders_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Yangi buyurtmalar ro'yxatini ko'rsatish"""
    query = update.callback_query
    await query.answer()
    
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Yangi buyurtmalar: pending_payment statusida
        cur.execute("""
            SELECT * FROM orders 
            WHERE status IN ('pending_payment', 'pending')
            AND created_at > CURRENT_TIMESTAMP - INTERVAL '24 hours'
            ORDER BY created_at DESC
        """)
        results = cur.fetchall()
        cur.close()
        
        new_orders = []
        for row in results:
            order_dict = dict(row)
            for key in ['created_at', 'accepted_at', 'rejected_at', 'paid_at', 'confirmed_at']:
                if order_dict.get(key) and hasattr(order_dict[key], 'isoformat'):
                    order_dict[key] = order_dict[key].isoformat()
            new_orders.append(order_dict)
        
    except Exception as e:
        logger.error(f"❌ Yangi buyurtmalarni olish xatosi: {e}")
        new_orders = []
    finally:
        if conn:
            conn.close()
    
    # Agar yangi buyurtma bo'lmasa
    if not new_orders:
        await query.edit_message_text(
            "📭 <b>Hozircha yangi buyurtmalar yo'q</b>\n\n"
            "Yangi buyurtmalar kelganda bu yerda ko'rinadi.",
            parse_mode='HTML'
        )
        return
    
    # Har bir buyurtma uchun xabar yuborish
    for order in new_orders:
        items = order.get('items', [])
        if isinstance(items, str):
            items = json.loads(items)
        
        items_text = "\n".join([f"• {i.get('name')} x{i.get('qty')}" for i in items]) if items else "Ma'lumot yo'q"
        
        raw_phone = order.get('phone', '')
        phone_display = format_phone_display(raw_phone)
        
        location = order.get('location')
        location_text = ""
        location_coords = None
        if location and ',' in str(location):
            try:
                lat, lng = str(location).split(',')
                lat = float(lat.strip())
                lng = float(lng.strip())
                location_text = f"\n📍 <b>Joylashuv:</b> <a href='https://maps.google.com/?q={lat},{lng}'>Xaritada ko'rish</a>"
                location_coords = (lat, lng)
            except:
                pass
        
        message = f"""🛎️ <b>YANGI BUYURTMA!</b>

🆔 Buyurtma: #{order.get('order_id', 'N/A')[-6:]}
👤 Mijoz: {order.get('name')}
📞 Telefon: {phone_display}
💵 Summa: {format_price(order.get('total', 0))} so'm
💳 To'lov: Kutilmoqda{location_text}

🍽 Mahsulotlar:
{items_text}

⏰ {order.get('created_at', datetime.now().isoformat())[:19]}

<i>⏳ To'lovni tekshiring va buyurtmani qabul qiling</i>"""

        keyboard = [
            [
                InlineKeyboardButton("✅ QABUL QILISH", callback_data=f"accept_{order.get('order_id')}"),
                InlineKeyboardButton("❌ BEKOR QILISH", callback_data=f"reject_{order.get('order_id')}")
            ],
            [
                InlineKeyboardButton("💳 TO'LOVNI TEKSHIRISH", callback_data=f"open_payme_group_{order.get('order_id')}")
            ]
        ]
        
        sent_message = await context.bot.send_message(
            chat_id=update.effective_user.id,
            text=message,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML'
        )
        
        if location_coords:
            try:
                await context.bot.send_location(
                    chat_id=update.effective_user.id,
                    latitude=location_coords[0],
                    longitude=location_coords[1],
                    reply_to_message_id=sent_message.message_id
                )
            except Exception as e:
                logger.error(f"❌ Joylashuv yuborish xatosi: {e}")
    
    # Asosiy xabarni yangilash
    await query.edit_message_text(
        f"📋 <b>{len(new_orders)} ta yangi buyurtma</b> yuborildi.\n"
        f"⏳ Ularni qabul qilish yoki bekor qilish mumkin.",
        parse_mode='HTML'
    )

def get_order(order_id: str) -> Optional[Dict[str, Any]]:
    """Buyurtmani olish - CASE INSENSITIVE va to'liq ma'lumot bilan"""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # CASE INSENSITIVE qidirish - ILIKE ishlatamiz
        cur.execute(
            "SELECT * FROM orders WHERE order_id ILIKE %s", 
            (order_id,)
        )
        result = cur.fetchone()
        cur.close()

        if result:
            order_dict = dict(result)

            # ⭐ tg_id ni tekshirish va log qilish
            tg_id = order_dict.get('tg_id')
            logger.info(f"📋 get_order: {order_id}, tg_id: {tg_id}")

            # Timestamp larni formatlash
            for key in ['created_at', 'accepted_at', 'rejected_at', 'paid_at', 'confirmed_at']:
                if order_dict.get(key) and hasattr(order_dict[key], 'isoformat'):
                    order_dict[key] = order_dict[key].isoformat()
            return order_dict
        return None

    except Exception as e:
        logger.error(f"Get order error: {e}")
        import traceback
        traceback.print_exc()
        return None
    finally:
        if conn:
            conn.close()

async def prep_time_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Admin tayyorlanish vaqtini kiritganda ishga tushadi.
    """
    user = update.effective_user

    # Faqat admin uchun tekshiruv
    if user.id != ADMIN_CHAT_ID_INT:
        return

    # Kutilayotgan state mavjudmi tekshirish
    if not context.user_data.get('awaiting_prep_time'):
        return

    order_id = context.user_data.get('accepting_order_id')
    prep_time = update.message.text.strip()

    if not order_id:
        await update.message.reply_text("❌ Xatolik: Buyurtma ID topilmadi!")
        context.user_data.pop('awaiting_prep_time', None)
        context.user_data.pop('accepting_order_id', None)
        return

    # Buyurtma ma'lumotlarini olish
    order = get_order(order_id)
    if not order:
        await update.message.reply_text("❌ Xatolik: Buyurtma topilmadi!")
        context.user_data.pop('awaiting_prep_time', None)
        context.user_data.pop('accepting_order_id', None)
        return

    # Allaqachon qabul qilinganmi tekshirish
    if order.get('status') == 'accepted':
        await update.message.reply_text("⚠️ Bu buyurtma allaqachon qabul qilingan!")
        context.user_data.pop('awaiting_prep_time', None)
        context.user_data.pop('accepting_order_id', None)
        return

    try:
        # ⭐ MUHIM: Avvalgi tg_id ni saqlab qolish
        existing_tg_id = order.get('tg_id')
        logger.info(f"📋 prep_time_handler: Mavjud tg_id = {existing_tg_id}")

        # Buyurtma statusini yangilash
        updated_order = update_order_status(
            order_id, 
            'accepted',
            admin_note=f"Tayyorlanish vaqti: {prep_time}",
            accepted_at=datetime.utcnow()
        )

        if updated_order:
            # ⭐ MUHIM: Yangilangan order dan tg_id ni olish yoki avvalgidan tiklash
            final_tg_id = updated_order.get('tg_id') or existing_tg_id
            updated_order['tg_id'] = final_tg_id
            
            logger.info(f"📋 Final tg_id: {final_tg_id}")

            # Admin ga tasdiqlash
            admin_confirm_msg = (
                "✅ <b>BUYURTMA QABUL QILINDI</b>\n\n"
                f"🆔 Buyurtma: #{order_id[-6:]}\n"
                f"👤 Mijoz: {order.get('name', 'Mijoz')}\n"
                f"⏱ <b>Tayyorlanish vaqti:</b> {prep_time}\n"
                f"💵 Summa: {format_price(order.get('total', 0))} so'm\n\n"
                f"📱 tg_id: {final_tg_id}\n"
                "📨 Mijozga xabar yuborilmoqda..."
            )
            await update.message.reply_text(admin_confirm_msg, parse_mode='HTML')

            # ⭐⭐⭐ MIJOZGA XABAR YUBORISH
            notification_sent = await notify_customer_accepted(context.bot, updated_order, prep_time)

            if notification_sent:
                await update.message.reply_text("✅ Mijozga xabar yuborildi!")
            else:
                await update.message.reply_text(
                    "⚠️ <b>Diqqat!</b>\n"
                    "Mijozga xabar yuborilmadi. Iltimos, qo'lda xabar yuboring.",
                    parse_mode='HTML'
                )
        else:
            await update.message.reply_text(
                "❌ <b>Xatolik!</b>\nBuyurtma ma'lumotlar bazasida yangilanmadi.",
                parse_mode='HTML'
            )

    except Exception as e:
        logger.error(f"❌ Buyurtma qabul qilishda xato: {e}")
        import traceback
        traceback.print_exc()
        await update.message.reply_text(
            "❌ <b>Kutilmagan xatolik yuz berdi!</b>\nIltimos, qayta urinib ko'ring.",
            parse_mode='HTML'
        )

    finally:
        # State ni tozalash
        context.user_data.pop('awaiting_prep_time', None)
        context.user_data.pop('accepting_order_id', None)

async def notify_customer_accepted(bot, order: Dict, prep_time: str):
    """
    Buyurtma qabul qilinganda mijozga xabar yuboradi.
    Tayyorlanish vaqti bilan birga yuboriladi.
    """
    # ⭐ MUHIM: Bir nechta yo'l bilan tg_id ni olish
    tg_id = order.get('tg_id') or order.get('tgId') or order.get('user_id')
    
    # String bo'lsa, int ga o'tkazish
    if tg_id and isinstance(tg_id, str):
        try:
            tg_id = int(tg_id)
        except ValueError:
            tg_id = None
    
    if not tg_id or tg_id == 0:
        logger.warning(f"⚠️ Mijoz tg_id yo'q: {order.get('order_id')}")
        return False
    
    try:
        # Xabar matnini tayyorlash
        items = order.get('items', [])
        if isinstance(items, str):
            items = json.loads(items)
        
        if items:
            items_short = ", ".join([f"{i.get('name')} x{i.get('qty')}" for i in items[:3]])
            if len(items) > 3:
                items_short += f" va yana {len(items)-3} ta"
        else:
            items_short = "Ma'lumot yo'q"
        
        current_time = datetime.now().strftime('%H:%M')
        order_id_short = str(order.get('order_id', 'N/A'))[-6:]
        total_price = format_price(order.get('total', 0))
        
        customer_message = (
            "🎉 <b>Buyurtmangiz qabul qilindi!</b>\n\n"
            f"🆔 <b>Buyurtma raqami:</b> #{order_id_short}\n"
            f"⏱ <b>Tayyorlanish vaqti:</b> {prep_time}\n"
            f"💵 <b>Summa:</b> {total_price} so'm\n\n"
            f"🍽 <b>Buyurtma:</b> {items_short}\n\n"
            "👨‍🍳 Oshxonada tayyorlanmoqda...\n"
            "🚚 Tayyor bo'lganda yetkazib beramiz!\n\n"
            "📞 Savollar bo'yicha: +998774445020\n"
            f"⏰ {current_time}"
        )
        
        # Mijozga xabar yuborish
        await bot.send_message(
            chat_id=int(tg_id),
            text=customer_message,
            parse_mode='HTML'
        )
        
        logger.info(f"✅ Mijozga qabul xabari yuborildi: {tg_id}, vaqt: {prep_time}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Mijozga xabar yuborishda xato: {e}")
        import traceback
        traceback.print_exc()
        return False

async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Barcha callback query larni qayta ishlash.
    """
    query = update.callback_query
    user = update.effective_user
    
    # Har doim callback query ga javob qaytarish
    try:
        await query.answer()
    except Exception as e:
        logger.error(f"Query answer xatosi: {e}")
    
    data = query.data
    logger.info(f"👆 Callback query: {data} from admin: {user.id}")
    
    # === TAYYORLANISH VAQTI KUTILMOQDA (Bekor qilish) ===
    if data.startswith("cancel_accept_"):
        order_id = data.replace("cancel_accept_", "")
        
        # State ni tekshirish
        if (context.user_data.get('awaiting_prep_time') and 
            context.user_data.get('accepting_order_id') == order_id):
            
            context.user_data.pop('awaiting_prep_time', None)
            context.user_data.pop('accepting_order_id', None)
            
            await query.edit_message_text(
                "❌ <b>Qabul qilish bekor qilindi</b>\n\n"
                "Yangi buyurtmalarni ko'rish uchun /start ni bosing.",
                parse_mode='HTML'
            )
            return
    
    # === STATISTIKA ===
    if data == "admin_stats":
        await show_stats(update, context)
        return
    
    # === YANGI BUYURTMALAR RO'YXATI ===
    if data == "show_new_orders":
        await show_new_orders_list(update, context)
        return
    
    # === PAYME GURUHIGA O'TISH ===
    if data.startswith("open_payme_group_"):
        order_id = data.replace("open_payme_group_", "")
        order = get_order(order_id)
        
        if not order:
            await query.edit_message_text("❌ Buyurtma topilmadi!")
            return
        
        payme_group_username = os.getenv("PAYME_GROUP_USERNAME", "bodrumbota")
        group_link = f"https://t.me/{payme_group_username}"
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("💳 Payme guruhiga o'tish", url=group_link)],
            [InlineKeyboardButton("🔙 Orqaga", callback_data=f"back_to_order_{order_id}")]
        ])
        
        await query.edit_message_text(
            "💳 <b>To'lovni tekshirish</b>\n\n"
            f"🆔 Buyurtma: #{order_id[-6:]}\n"
            f"💵 Summa: {format_price(order.get('total', 0))} so'm\n\n"
            "Quyidagi ORDER ID ni Payme guruhida qidiring:\n"
            f"<code>{order_id}</code>\n\n"
            "To'lov topilsa, qaytib kelib <b>\"Qabul qilish\"</b> ni bosing.",
            reply_markup=keyboard,
            parse_mode='HTML'
        )
        return
    
    # === ORQAGA QAYTISH ===
    if data.startswith("back_to_order_"):
        order_id = data.replace("back_to_order_", "")
        order = get_order(order_id)
        
        if not order:
            await query.edit_message_text("❌ Buyurtma topilmadi!")
            return
        
        # Buyurtma ma'lumotlarini qayta ko'rsatish
        items = order.get('items', [])
        if isinstance(items, str):
            items = json.loads(items)
        
        items_text = "\n".join([f"• {i.get('name')} x{i.get('qty')}" for i in items]) if items else "Ma'lumot yo'q"
        phone_display = format_phone_display(order.get('phone', ''))
        
        order_id_val = order.get('order_id', 'N/A')
        customer_name = order.get('name', 'Mijoz')
        total_val = order.get('total', 0)
        
        location_text = ""
        location = order.get('location')
        if location and ',' in str(location):
            try:
                lat, lng = str(location).split(',')
                location_text = f"\n📍 <b>Joylashuv:</b> <a href='https://maps.google.com/?q={lat},{lng}'>Xaritada ko'rish</a>"
            except:
                pass
        
        status_text = "⏳ <b>YANGI BUYURTMA - TO'LOV KUTILMOQDA!</b>"
        
        message = (
            f"{status_text}\n\n"
            f"🆔 Buyurtma: #{order_id_val[-6:]}\n"
            f"👤 Mijoz: {customer_name}\n"
            f"📞 Telefon: {phone_display}\n"
            f"💵 Summa: {format_price(total_val)} so'm"
            f"{location_text}\n\n"
            f"🍽 Mahsulotlar:\n{items_text}\n\n"
            f"⏰ {datetime.now().strftime('%H:%M:%S')}"
        )
        
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ QABUL QILISH", callback_data=f"accept_{order_id}"),
                InlineKeyboardButton("❌ BEKOR QILISH", callback_data=f"reject_{order_id}")
            ],
            [
                InlineKeyboardButton("💳 TO'LOVNI TEKSHIRISH", callback_data=f"open_payme_group_{order_id}")
            ]
        ])
        
        await query.edit_message_text(message, reply_markup=keyboard, parse_mode='HTML')
        return
    
    # === BUYURTMANI QABUL QILISH (Vaqt so'rash) ===
    if data.startswith("accept_"):
        order_id = data.replace("accept_", "")
        order = get_order(order_id)
        
        if not order:
            await query.edit_message_text("❌ Buyurtma topilmadi!")
            return
        
        # Allaqachon qabul qilinganmi?
        if order.get('status') == 'accepted':
            await query.answer("⚠️ Bu buyurtma allaqachon qabul qilingan!", show_alert=True)
            return
        
        # State saqlash
        context.user_data['awaiting_prep_time'] = True
        context.user_data['accepting_order_id'] = order_id
        
        # Vaqt kiritish uchun so'rov
        items = order.get('items', [])
        if isinstance(items, str):
            items = json.loads(items)
        items_text = "\n".join([f"• {i.get('name')} x{i.get('qty')}" for i in items]) if items else "Ma'lumot yo'q"
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("❌ Bekor qilish", callback_data=f"cancel_accept_{order_id}")]
        ])
        
        prompt_message = (
            "⏱ <b>BUYURTMANI QABUL QILISH</b>\n\n"
            f"🆔 Buyurtma: #{order_id[-6:]}\n"
            f"👤 Mijoz: {order.get('name', 'Mijoz')}\n"
            f"💵 Summa: {format_price(order.get('total', 0))} so'm\n\n"
            f"🍽 Mahsulotlar:\n{items_text}\n\n"
            "✍️ <b>Tayyorlanish vaqtini kiriting:</b>\n"
            "<i>Masalan:</i> <code>20 daqiqa</code>, <code>30-40 daqiqa</code>, <code>1 soat</code>"
        )
        
        await query.edit_message_text(
            prompt_message,
            reply_markup=keyboard,
            parse_mode='HTML'
        )
        return
    
    # === BUYURTMANI BEKOR QILISH ===
    if data.startswith("reject_"):
        action, order_id = data.split("_", 1)
        order = get_order(order_id)
        
        if not order:
            await query.edit_message_text("❌ Buyurtma topilmadi!")
            return
        
        # Status ni rejected ga o'zgartirish
        updated = update_order_status(order_id, 'rejected', rejected_at=datetime.utcnow())
        
        if updated:
            current_time = datetime.now().strftime('%H:%M:%S')
            # Admin ga tasdiq
            await query.edit_message_text(
                "❌ <b>BUYURTMA BEKOR QILINDI</b>\n\n"
                f"🆔 #{order_id[-6:]}\n"
                f"👤 {order.get('name', 'Mijoz')}\n"
                f"⏰ {current_time}",
                parse_mode='HTML'
            )
            
            # Mijoz ga xabar
            tg_id = order.get('tg_id')
            if tg_id and str(tg_id) not in ['0', 'None', '', 'null']:
                try:
                    await context.bot.send_message(
                        chat_id=int(tg_id),
                        text=(
                            "❌ <b>Buyurtmangiz bekor qilindi</b>\n\n"
                            f"🆔 Buyurtma: #{order_id[-6:]}\n"
                            "📞 Qo'llab-quvvatlash: +998901234567"
                        ),
                        parse_mode='HTML'
                    )
                except Exception as e:
                    logger.error(f"Mijozga bekor xabari yuborishda xato: {e}")
        else:
            await query.edit_message_text("❌ Xatolik yuz berdi!")
        
        return
    
    # === BUYURTMANI TASDIQLASH (Confirm) ===
    if data.startswith("confirm_"):
        action, order_id = data.split("_", 1)
        order = get_order(order_id)
        
        if not order:
            await query.edit_message_text("❌ Buyurtma topilmadi!")
            return
        
        updated = update_order_status(order_id, 'confirmed', confirmed_at=datetime.utcnow())
        
        if updated:
            current_time = datetime.now().strftime('%H:%M:%S')
            await query.edit_message_text(
                "✅✅ <b>BUYURTMA TASDIQLANDI</b>\n\n"
                f"🆔 #{order_id[-6:]}\n"
                f"⏰ {current_time}",
                parse_mode='HTML'
            )
            
            # Mijoz ga xabar
            tg_id = order.get('tg_id')
            if tg_id and str(tg_id) not in ['0', 'None', '', 'null']:
                try:
                    await context.bot.send_message(
                        chat_id=int(tg_id),
                        text=(
                            "✅✅ <b>Buyurtmangiz tayyor!</b>\n\n"
                            f"🆔 Buyurtma: #{order_id[-6:]}\n"
                            "🚚 Tez orada yetkazib beramiz!"
                        ),
                        parse_mode='HTML'
                    )
                except Exception as e:
                    logger.error(f"Mijozga tasdiq xabari yuborishda xato: {e}")
        else:
            await query.edit_message_text("❌ Xatolik yuz berdi!")
        
        return

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Statistika ko'rsatish"""
    user = update.effective_user
    
    if user.id != ADMIN_CHAT_ID_INT:
        await update.message.reply_text("❌ Siz admin emassiz!")
        return
    
    await show_stats(update, context)


async def show_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Statistikani ko'rsatish"""
    user = update.effective_user
    
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        today = datetime.now().strftime('%Y-%m-%d')
        
        cur.execute("SELECT COUNT(*) FROM orders WHERE status IN ('pending', 'pending_payment')")
        new_count = cur.fetchone()['count']
        
        cur.execute("SELECT COUNT(*), COALESCE(SUM(total), 0) FROM orders WHERE status = 'accepted' AND DATE(accepted_at) = %s", (today,))
        today_result = cur.fetchone()
        today_count = today_result['count']
        today_sum = today_result['coalesce'] or 0
        
        cur.execute("SELECT COUNT(*), COALESCE(SUM(total), 0) FROM orders WHERE status = 'accepted'")
        total_result = cur.fetchone()
        total_count = total_result['count']
        total_sum = total_result['coalesce'] or 0
        
        cur.close()
        conn.close()
        
        stats_text = f"""📊 <b>STATISTIKA</b>

🕐 <b>Bugun ({today}):</b>
• Buyurtmalar: {today_count} ta
• Summa: {format_price(today_sum)} so'm

⏳ <b>Kutilayotgan:</b>
• Yangi buyurtmalar: {new_count} ta

📈 <b>Jami:</b>
• Qabul qilingan: {total_count} ta
• Umumiy summa: {format_price(total_sum)} so'm

⏰ {datetime.now().strftime('%H:%M:%S')}"""
        
        keyboard = [
            [InlineKeyboardButton("🔄 Yangilash", callback_data="admin_stats")],
            [InlineKeyboardButton("🛎️ Yangi buyurtmalar", callback_data="show_new_orders")]
        ]
        
        if update.callback_query:
            await update.callback_query.edit_message_text(
                stats_text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='HTML'
            )
        else:
            await update.message.reply_text(
                stats_text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='HTML'
            )
        
    except Exception as e:
        logger.error(f"Stats error: {e}")
        text = "❌ Statistikani olishda xatolik"
        if update.callback_query:
            await update.callback_query.edit_message_text(text)
        else:
            await update.message.reply_text(text)

async def health_handler(request):
    return web.json_response({
        "status": "ok", 
        "service": "bodrum-bot",
        "timestamp": datetime.utcnow().isoformat(),
        "payme_receipt_parser": "enabled",
        "auto_accept": "enabled",
        "payme_group_id": PAYME_GROUP_ID_INT
    }, headers=get_cors_headers())

async def create_order_handler(request):
    try:
        data = await request.json()
        logger.info(f"📝 Yangi buyurtma: {data.get('orderId')}")
        
        if not data.get('name'):
            data['name'] = 'Mijoz'
        if not data.get('phone'):
            data['phone'] = '000000000'
        
        order = create_order(data)
        
        if order:
            logger.info(f"✅ Buyurtma yaratildi: {order['order_id']}")
            
            # ⭐⭐⭐ DARHOL ADMINGA XABAR YUBORISH (TO'G'RI FUNKSIYA)
            try:
                # ASINXRON XABAR YUBORISH - notify_admin_new_order chaqiriladi
                asyncio.create_task(notify_admin_new_order(order))
                logger.info(f"📨 Admin ga xabar yuborildi: {order['order_id']}")
            except Exception as e:
                logger.error(f"❌ Admin ga xabar yuborish xatosi: {e}")
            
            # Payme URL ni qaytarish
            payme_url = f"https://checkout.payme.uz/{os.getenv('PAYME_MERCHANT_ID')}?orderId={order['order_id']}&amount={order['total'] * 100}"
            
            # ⭐⭐⭐ MUHIM: datetime obyektlarini JSON serializable qilish
            response_data = {**order}
            for key in ['created_at', 'accepted_at', 'rejected_at', 'paid_at', 'confirmed_at']:
                if response_data.get(key) and hasattr(response_data[key], 'isoformat'):
                    response_data[key] = response_data[key].isoformat()
            
            response_data["message"] = "Buyurtma yaratildi. To'lovni amalga oshiring."
            response_data["payme_url"] = payme_url
            
            return web.json_response(
                response_data,
                status=201, 
                headers=get_cors_headers()
            )
        else:
            return web.json_response(
                {"error": "Failed to create order"}, 
                status=500, 
                headers=get_cors_headers()
            )
        
    except Exception as e:
        logger.error(f"API create order error: {e}")
        import traceback
        traceback.print_exc()
        return web.json_response(
            {"error": str(e)}, 
            status=500, 
            headers=get_cors_headers()
        )

async def notify_admin_new_order(order: Dict):
    """
    Admin ga YANGI BUYURTMA haqida xabar (to'lov tekshirilmagan)
    """
    try:
        logger.info(f"🔔 Yangi buyurtma admin ga: {order.get('order_id')}")

        if not ADMIN_CHAT_ID_INT:
            logger.error("❌ ADMIN_CHAT_ID o'rnatilmagan!")
            return False

        global application
        if not application or not application.bot:
            logger.error("❌ Bot mavjud emas!")
            return False
        
        bot = application.bot

        # Buyurtma ma'lumotlarini olish
        items = order.get('items', [])
        if isinstance(items, str):
            items = json.loads(items)

        items_text = "\n".join([f"• {i.get('name')} x{i.get('qty')}" for i in items]) if items else "Ma'lumot yo'q"

        raw_phone = order.get('phone', '')
        phone_display = format_phone_display(raw_phone)

        customer_name = order.get('name', 'Mijoz')
        if not customer_name or customer_name == 'null':
            customer_name = 'Mijoz'

        location = order.get('location')
        location_text = ""
        location_coords = None

        if location and ',' in str(location):
            try:
                lat, lng = str(location).split(',')
                lat = float(lat.strip())
                lng = float(lng.strip())
                location_text = f"\n📍 <b>Joylashuv:</b> <a href='https://maps.google.com/?q={lat},{lng}'>Xaritada ko'rish</a>"
                location_coords = (lat, lng)
            except Exception as e:
                logger.warning(f"Joylashuv parse xatosi: {e}")
                location_text = f"\n📍 <b>Manzil:</b> {location}"
        elif location:
            location_text = f"\n📍 <b>Manzil:</b> {location}"

        source = order.get('source', 'website')
        source_icon = "🤖 WebApp" if source == 'webapp' else "🌐 Sayt"

        # ⭐ YANGI: To'lov kutilmoqda statusi
        status_text = "⏳ <b>YANGI BUYURTMA - TO'LOV KUTILMOQDA!</b>"

        admin_message = f"""{status_text}

🆔 Buyurtma: #{order.get('order_id', 'N/A')[-6:]}
👤 Mijoz: {customer_name}
📞 Telefon: {phone_display}
💵 Summa: {format_price(order.get('total', 0))} so'm
📱 Manba: {source_icon}{location_text}

🍽 Mahsulotlar:
{items_text}

⏰ {datetime.now().strftime('%H:%M:%S')}

<i>⚡ To'lovni tekshiring, keyin qabul qiling yoki bekor qiling</i>"""

        # ⭐⭐⭐ 3 TA TUGMA: Qabul, Bekor, To'lovni tekshirish
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ QABUL QILISH", callback_data=f"accept_{order.get('order_id')}"),
                InlineKeyboardButton("❌ BEKOR QILISH", callback_data=f"reject_{order.get('order_id')}")
            ],
            [
                InlineKeyboardButton("💳 TO'LOVNI TEKSHIRISH", callback_data=f"open_payme_group_{order.get('order_id')}")
            ]
        ])

        admin_sent = await bot.send_message(
            chat_id=ADMIN_CHAT_ID_INT,
            text=admin_message,
            reply_markup=keyboard,
            parse_mode='HTML'
        )

        if location_coords and admin_sent:
            try:
                await bot.send_location(
                    chat_id=ADMIN_CHAT_ID_INT,
                    latitude=location_coords[0],
                    longitude=location_coords[1]
                )
            except Exception as e:
                logger.error(f"❌ Joylashuv yuborish xatosi: {e}")

        logger.info(f"✅ Admin ga xabar yuborildi: {order.get('order_id')}")
        return True

    except Exception as e:
        logger.error(f"❌ notify_admin_new_order xatosi: {e}")
        import traceback
        traceback.print_exc()
        return False

async def update_order_handler(request):
    """Buyurtma yangilash"""
    try:
        order_id = request.match_info['order_id']
        data = await request.json()
        
        status = data.get('status')
        payment_status = data.get('paymentStatus')
        admin_note = data.get('adminNote')
        
        updated = update_order_status(
            order_id, 
            status, 
            payment_status=payment_status,
            admin_note=admin_note
        )
        
        if updated:
            # ⭐⭐⭐ MUHIM: datetime obyektlarini string ga aylantirish
            response_data = {**updated}
            for key in ['created_at', 'accepted_at', 'rejected_at', 'paid_at', 'confirmed_at']:
                if response_data.get(key) and hasattr(response_data[key], 'isoformat'):
                    response_data[key] = response_data[key].isoformat()
            
            return web.json_response(response_data, headers=get_cors_headers())
        else:
            return web.json_response(
                {"error": "Order not found"}, 
                status=404, 
                headers=get_cors_headers()
            )
            
    except Exception as e:
        logger.error(f"Update order error: {e}")
        return web.json_response(
            {"error": str(e)}, 
            status=500, 
            headers=get_cors_headers()
        )

async def orders_list_handler(request):
    """Barcha buyurtmalarni olish - BARCHA STATUSLAR"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # ⭐⭐⭐ BARCHA STATUSLARNI OLIB QAYTARISH (filtr olib tashlandi)
        cur.execute("""
            SELECT * FROM orders 
            ORDER BY 
                CASE 
                    WHEN status = 'pending_payment' THEN 1
                    WHEN status = 'pending' THEN 2
                    WHEN status = 'accepted' THEN 3
                    WHEN status = 'confirmed' THEN 4
                    WHEN status = 'rejected' THEN 5
                    ELSE 6
                END,
                created_at DESC 
            LIMIT 200
        """)
        results = cur.fetchall()
        cur.close()
        conn.close()
        
        orders = []
        for row in results:
            order_dict = dict(row)
            # ⭐⭐⭐ MUHIM: datetime obyektlarini string ga aylantirish
            for key in ['created_at', 'accepted_at', 'rejected_at', 'paid_at', 'confirmed_at']:
                if order_dict.get(key) and hasattr(order_dict[key], 'isoformat'):
                    order_dict[key] = order_dict[key].isoformat()
            orders.append(order_dict)
        
        return web.json_response(orders, headers=get_cors_headers())
        
    except Exception as e:
        logger.error(f"Orders list error: {e}")
        return web.json_response(
            {"error": str(e)}, 
            status=500, 
            headers=get_cors_headers()
        )

async def new_orders_handler(request):
    """Yangi buyurtmalarni olish"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT * FROM orders 
            WHERE status IN ('pending', 'pending_payment', 'payment_pending') 
            ORDER BY created_at DESC
        """)
        results = cur.fetchall()
        cur.close()
        conn.close()
        
        orders = []
        for row in results:
            order_dict = dict(row)
            # ⭐⭐⭐ MUHIM: datetime obyektlarini string ga aylantirish
            for key in ['created_at', 'accepted_at', 'rejected_at', 'paid_at', 'confirmed_at']:
                if order_dict.get(key) and hasattr(order_dict[key], 'isoformat'):
                    order_dict[key] = order_dict[key].isoformat()
            orders.append(order_dict)
        
        return web.json_response(orders, headers=get_cors_headers())
        
    except Exception as e:
        logger.error(f"New orders error: {e}")
        return web.json_response(
            {"error": str(e)}, 
            status=500, 
            headers=get_cors_headers()
        )

async def update_order_handler(request):
    """Buyurtma yangilash"""
    try:
        order_id = request.match_info['order_id']
        data = await request.json()
        
        status = data.get('status')
        payment_status = data.get('paymentStatus')
        admin_note = data.get('adminNote')
        
        updated = update_order_status(
            order_id, 
            status, 
            payment_status=payment_status,
            admin_note=admin_note
        )
        
        if updated:
            return web.json_response(updated, headers=get_cors_headers())
        else:
            return web.json_response({"error": "Order not found"}, status=404, headers=get_cors_headers())
            
    except Exception as e:
        logger.error(f"Update order error: {e}")
        return web.json_response({"error": str(e)}, status=500, headers=get_cors_headers())

async def save_user_profile_api(request):
    """Foydalanuvchi profilini saqlash"""
    try:
        data = await request.json()
        tg_id_raw = data.get('tgId')
        name = data.get('name', 'Foydalanuvchi')
        phone = data.get('phone', '')
        username = data.get('username', '')
        
        print(f"💾 Profil saqlanmoqda: tg_id={tg_id_raw}, name={name}, phone={phone}")
        
        if not tg_id_raw:
            return web.json_response({
                "success": False,
                "error": "tgId required"
            }, status=400, headers=get_cors_headers())
        
        try:
            tg_id = int(tg_id_raw)
        except (ValueError, TypeError):
            return web.json_response({
                "success": False,
                "error": "Invalid tgId format"
            }, status=400, headers=get_cors_headers())
        
        if not phone or len(phone) != 9:
            return web.json_response({
                "success": False,
                "error": "Valid phone required (9 digits)"
            }, status=400, headers=get_cors_headers())
        
        success = save_user_profile(tg_id, name, phone, username)
        
        if success:
            profile = get_user_profile(tg_id)
            orders = get_user_orders(tg_id)
            
            return web.json_response({
                "success": True,
                "profile": profile,
                "orders": orders
            }, headers=get_cors_headers())
        else:
            return web.json_response({
                "success": False,
                "error": "Failed to save profile"
            }, status=500, headers=get_cors_headers())
            
    except Exception as e:
        logger.error(f"Save user profile API error: {e}")
        import traceback
        traceback.print_exc()
        return web.json_response({
            "success": False,
            "error": str(e)
        }, status=500, headers=get_cors_headers())

async def get_user_profile_api(request):
    """Foydalanuvchi profilini olish"""
    try:
        data = await request.json()
        tg_id_raw = data.get('tgId')
        
        print(f"🔍 API: Profil so'raldi, raw tg_id: {tg_id_raw}, type: {type(tg_id_raw)}")
        
        if not tg_id_raw:
            return web.json_response({
                "success": False, 
                "error": "tgId required"
            }, status=400, headers=get_cors_headers())
        
        try:
            tg_id = int(tg_id_raw)
        except (ValueError, TypeError) as e:
            print(f"❌ tgId conversion error: {e}")
            return web.json_response({
                "success": False,
                "error": "Invalid tgId format"
            }, status=400, headers=get_cors_headers())
        
        profile = get_user_profile(tg_id)
        orders = get_user_orders(tg_id)
        
        # ⭐⭐⭐ MUHIM: orders dagi datetime obyektlarini string ga aylantirish
        if orders:
            for order in orders:
                for key in ['created_at', 'accepted_at', 'rejected_at', 'paid_at', 'confirmed_at']:
                    if order.get(key) and hasattr(order[key], 'isoformat'):
                        order[key] = order[key].isoformat()
        
        # Profile dagi datetime larni ham aylantirish
        if profile:
            for key in ['created_at', 'updated_at']:
                if profile.get(key) and hasattr(profile[key], 'isoformat'):
                    profile[key] = profile[key].isoformat()
        
        print(f"✅ API: Profil: {profile is not None}, Buyurtmalar: {len(orders)}")
        
        return web.json_response({
            "success": True,
            "profile": profile,
            "orders": orders
        }, headers=get_cors_headers())
        
    except Exception as e:
        logger.error(f"Get user profile API error: {e}")
        import traceback
        traceback.print_exc()
        return web.json_response({
            "success": False,
            "error": str(e)
        }, status=500, headers=get_cors_headers())

# webhook_handler ga log qo'shing
async def webhook_handler(request):
    global application
    
    if application:
        try:
            data = await request.json()
            logger.info(f"📩 Webhook data: {data}")
            
            if 'callback_query' in data:
                logger.info(f"👆 Callback query keldi: {data['callback_query']['data']}")
            
            update = Update.de_json(data, application.bot)
            await application.process_update(update)
        except Exception as e:
            logger.error(f"Webhook processing error: {e}")
    
    return web.Response(text='OK')

async def init_webhook(app):
    global application
    
    if not TOKEN:
        logger.error("❌ TOKEN o'rnatilmagan!")
        return
    
    if not init_database():
        logger.error("❌ Database initialization failed!")
        return
    
    # Bot application yaratish
    application = Application.builder().token(TOKEN).build()
    
    # ==========================================
    # HANDLERLAR - TARTIBI MUHIM!
    # ==========================================
    
    # 1. COMMAND HANDLERS - eng muhimi bu yerda
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("stats", stats_command))    
    
    # 2. MESSAGE HANDLERS
    # Contact (telefon ulashish)
    application.add_handler(MessageHandler(
        filters.CONTACT & filters.ChatType.PRIVATE,
        contact_handler
    ))
    
    # ⭐ TAYYORLANISH VAQTI - faqat admin va specific state uchun
    # Bu handler callback dan KEYIN keladi, chunki callback query emas, matn xabar
    application.add_handler(MessageHandler(
        filters.TEXT & filters.User(user_id=ADMIN_CHAT_ID_INT) & ~filters.COMMAND,
        prep_time_handler
    ))
    
    # 3. CALLBACK QUERY HANDLER - oxirida
    application.add_handler(CallbackQueryHandler(callback_handler))
    
    # ==========================================
    # BOT NI ISHGA TUSHIRISH
    # ==========================================
    
    await application.initialize()
    await application.start()
    
    # Webhook o'rnatish
    webhook_url = os.getenv("WEBHOOK_URL", "")
    if webhook_url:
        full_webhook_url = f"{webhook_url}/webhook"
        try:
            await application.bot.set_webhook(
                url=full_webhook_url,
                allowed_updates=['message', 'callback_query', 'inline_query', 'edited_message']
            )
            logger.info(f"✅ Webhook o'rnatildi: {full_webhook_url}")
        except Exception as e:
            logger.error(f"❌ Webhook xato: {e}")
    
    logger.info(f"🤖 Bot ishga tushdi! Admin ID: {ADMIN_CHAT_ID_INT}")

async def shutdown(app):
    global application
    if application:
        try:
            await application.stop()
            await application.shutdown()
            logger.info("🛑 Bot to'xtatildi")
        except Exception as e:
            logger.error(f"Shutdown xato: {e}")

def main():
    logger.info("🔧 Bodrum Bot starting...")
    logger.info("💳 Payme chek parser + Auto accept tizimi faollashdi!")
    
    if not PORT:
        logger.error("❌ PORT o'rnatilmagan!")
        return
    
    app = web.Application()
    
    # CORS middleware
    async def cors_middleware(app, handler):
        async def middleware_handler(request):
            if request.method == 'OPTIONS':
                return web.Response(
                    status=200,
                    headers={
                        'Access-Control-Allow-Origin': '*',
                        'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
                        'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Requested-With, Cache-Control, Pragma, Accept',
                        'Access-Control-Max-Age': '86400',
                    }
                )
            
            response = await handler(request)
            response.headers['Access-Control-Allow-Origin'] = '*'
            response.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, DELETE, OPTIONS'
            response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization, X-Requested-With, Cache-Control, Pragma, Accept'
            
            return response
        
        return middleware_handler
    
    app.middlewares.append(cors_middleware)
    
    # Routes
    app.router.add_get('/', health_handler)
    app.router.add_get('/health', health_handler)
    
    # API routes
    app.router.add_get('/api/orders', orders_list_handler)
    app.router.add_get('/api/orders/new', new_orders_handler)
    app.router.add_post('/api/orders', create_order_handler)
    app.router.add_get('/api/orders/{order_id}', get_order_handler)
    app.router.add_put('/api/orders/{order_id}', update_order_handler)
    
    # User profile API
    app.router.add_post('/api/user/profile', get_user_profile_api)
    app.router.add_post('/api/user/save-profile', save_user_profile_api)
    
    # Webhook
    app.router.add_post('/webhook', webhook_handler)
    
    app.on_startup.append(init_webhook)
    app.on_cleanup.append(shutdown)
    app.router.add_get('/api/categories', get_categories_handler)
    app.router.add_post('/api/categories', save_categories_handler)
    
    logger.info(f"🚀 Server ishga tushmoqda: 0.0.0.0:{PORT}")
    logger.info(f"💳 Payme chek parser: Faol")
    logger.info(f"⚡ Auto accept: Faol")
    
    web.run_app(app, host='0.0.0.0', port=PORT)

if __name__ == "__main__":
    main()
