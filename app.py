import os
import logging
import asyncio
import json
import re
import requests
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo, KeyboardButton, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters
)
from aiohttp import web
import aiohttp
import psycopg2
import psycopg2.extras
from psycopg2 import pool
from apscheduler.schedulers.asyncio import AsyncIOScheduler

load_dotenv()

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ==========================================
# ENVIRONMENT VARIABLES
# ==========================================
PORT = int(os.getenv("PORT", "8080"))
DATABASE_URL = (os.getenv("DATABASE_PUBLIC_URL") or 
                os.getenv("DATABASE_URL") or 
                os.getenv("DATABASE_PRIVATE_URL") or
                os.getenv("POSTGRES_URL"))
TOKEN = os.getenv("TOKEN")
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID", "")
WEBAPP_URL = os.getenv("WEBAPP_URL", "")
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "")
PAYME_RECEIPTS_GROUP_ID = os.getenv("PAYME_RECEIPTS_GROUP_ID", "")
PAYME_GROUP_USERNAME = os.getenv("PAYME_GROUP_USERNAME", "bodrumbota")

# Global variables
application = None
db_pool = None
scheduler = None

# ==========================================
# DATABASE CONNECTION POOLING
# ==========================================
def init_database():
    """Database pool yaratish va jadvallarni tekshirish"""
    global db_pool
    try:
        db_pool = psycopg2.pool.SimpleConnectionPool(
            1, 20,  # min, max connections
            DATABASE_URL,
            cursor_factory=psycopg2.extras.RealDictCursor
        )
        logger.info("✅ Database pool yaratildi")
        
        # Jadvallarni yaratish
        conn = db_pool.getconn()
        cur = conn.cursor()
        
        # Orders jadvali
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
                tg_id BIGINT,
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
        
        # Users jadvali
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                tg_id BIGINT UNIQUE NOT NULL,
                name VARCHAR(255),
                phone VARCHAR(20),
                username VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Menu jadvali
        cur.execute("""
            CREATE TABLE IF NOT EXISTS menu_items (
                id SERIAL PRIMARY KEY,
                item_id INTEGER UNIQUE NOT NULL,
                name VARCHAR(255) NOT NULL,
                price INTEGER NOT NULL,
                category VARCHAR(100),
                image TEXT,
                description TEXT,
                available BOOLEAN DEFAULT TRUE,
                popular BOOLEAN DEFAULT FALSE,
                is_new BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Categories jadvali
        cur.execute("""
            CREATE TABLE IF NOT EXISTS categories (
                id VARCHAR(100) PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                icon VARCHAR(50) DEFAULT '🍽️',
                sort_order INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # ⭐ YANGI: Admin states jadvali (RAM o'rniga database)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS admin_states (
                admin_id BIGINT PRIMARY KEY,
                state_type VARCHAR(50),
                order_id VARCHAR(100),
                data JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP + INTERVAL '2 hours'
            )
        """)
        
        # Indexlar
        cur.execute("CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_orders_tg_id ON orders(tg_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_orders_created ON orders(created_at)")
        
        conn.commit()
        cur.close()
        db_pool.putconn(conn)
        
        logger.info("✅ Database initialized successfully")
        return True
        
    except Exception as e:
        logger.error(f"❌ Database init error: {e}")
        return False

def get_db_connection():
    """Database connection olish"""
    if db_pool:
        return db_pool.getconn()
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)

def put_db_connection(conn):
    """Connection ni qaytarish"""
    if db_pool and conn:
        db_pool.putconn(conn)

# ==========================================
# ADMIN STATE MANAGEMENT (Database based)
# ==========================================
def save_admin_state(admin_id: int, state_type: str, order_id: str, data: dict = None):
    """Admin holatini bazada saqlash"""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO admin_states (admin_id, state_type, order_id, data, expires_at)
            VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP + INTERVAL '2 hours')
            ON CONFLICT (admin_id) DO UPDATE SET
                state_type = EXCLUDED.state_type,
                order_id = EXCLUDED.order_id,
                data = EXCLUDED.data,
                created_at = CURRENT_TIMESTAMP,
                expires_at = EXCLUDED.expires_at
        """, (admin_id, state_type, order_id, json.dumps(data) if data else None))
        conn.commit()
        cur.close()
        logger.info(f"💾 Admin state saqlandi: {admin_id}, {state_type}, {order_id}")
    except Exception as e:
        logger.error(f"❌ State saqlash xatosi: {e}")
    finally:
        put_db_connection(conn)

def get_admin_state(admin_id: int):
    """Admin holatini olish"""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT * FROM admin_states 
            WHERE admin_id = %s AND expires_at > CURRENT_TIMESTAMP
        """, (admin_id,))
        result = cur.fetchone()
        cur.close()
        return result
    except Exception as e:
        logger.error(f"❌ State olish xatosi: {e}")
        return None
    finally:
        put_db_connection(conn)

def clear_admin_state(admin_id: int):
    """Admin holatini tozalash"""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("DELETE FROM admin_states WHERE admin_id = %s", (admin_id,))
        conn.commit()
        cur.close()
    except Exception as e:
        logger.error(f"❌ State tozalash xatosi: {e}")
    finally:
        put_db_connection(conn)

# ==========================================
# HELPER FUNCTIONS
# ==========================================
def parse_chat_id(chat_id_str):
    """Guruh yoki user ID sini to'g'ri formatga o'tkazish"""
    if not chat_id_str:
        return 0
    try:
        chat_id = str(chat_id_str).strip()
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

def get_cors_headers():
    return {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type, Authorization',
        'Access-Control-Max-Age': '86400',
    }

# ==========================================
# DATABASE OPERATIONS
# ==========================================
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

        tg_id = None
        try:
            tg_id_raw = data.get('tgId') or data.get('tg_id')
            if tg_id_raw and str(tg_id_raw).isdigit():
                tg_id = int(tg_id_raw)
            elif tg_id_raw:
                clean_id = str(tg_id_raw).replace('tg_', '').replace('user_', '')
                if clean_id.isdigit():
                    tg_id = int(clean_id)
        except Exception as e:
            logger.error(f"❌ tg_id conversion xatosi: {e}")
            tg_id = None

        order_id = data.get('orderId')
        name = data.get('name', 'Mijoz')
        phone = data.get('phone', '000000000')
        total = data.get('total', 0)
        location = data.get('location')
        source = data.get('source', 'website')
        return_url = data.get('returnUrl')

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
            for key in ['created_at', 'accepted_at', 'rejected_at', 'paid_at', 'confirmed_at']:
                if order_dict.get(key) and hasattr(order_dict[key], 'isoformat'):
                    order_dict[key] = order_dict[key].isoformat()
            logger.info(f"✅ Buyurtma yaratildi: {order_dict.get('order_id')}")
            return order_dict
        return None

    except Exception as e:
        logger.error(f"❌ Create order ERROR: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None
    finally:
        put_db_connection(conn)

def get_order(order_id: str) -> Optional[Dict[str, Any]]:
    """Buyurtmani olish"""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT * FROM orders WHERE order_id ILIKE %s", (order_id,))
        result = cur.fetchone()
        cur.close()

        if result:
            order_dict = dict(result)
            for key in ['created_at', 'accepted_at', 'rejected_at', 'paid_at', 'confirmed_at']:
                if order_dict.get(key) and hasattr(order_dict[key], 'isoformat'):
                    order_dict[key] = order_dict[key].isoformat()
            return order_dict
        return None
    except Exception as e:
        logger.error(f"Get order error: {e}")
        return None
    finally:
        put_db_connection(conn)

def update_order_status(order_id: str, status: str, **kwargs) -> Optional[Dict[str, Any]]:
    """Buyurtma statusini yangilash"""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("SELECT * FROM orders WHERE order_id = %s", (order_id,))
        existing = cur.fetchone()

        if not existing:
            return None

        update_data = {'status': status}

        timestamp_fields = {
            'accepted': 'accepted_at',
            'rejected': 'rejected_at', 
            'confirmed': 'confirmed_at',
            'pending_payment': None,
            'pending': None
        }

        if status in timestamp_fields and timestamp_fields[status]:
            update_data[timestamp_fields[status]] = datetime.utcnow()

        if status == 'confirmed':
            update_data['accepted_at'] = datetime.utcnow()

        if kwargs.get('paid_at'):
            update_data['paid_at'] = kwargs.get('paid_at')

        if 'notified' in kwargs:
            update_data['notified'] = kwargs['notified']
            
        if 'admin_note' in kwargs:
            update_data['admin_note'] = kwargs['admin_note']

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
            for key in ['created_at', 'accepted_at', 'rejected_at', 'paid_at', 'confirmed_at']:
                if order_dict.get(key) and hasattr(order_dict[key], 'isoformat'):
                    order_dict[key] = order_dict[key].isoformat()
            return order_dict
        return None

    except Exception as e:
        logger.error(f"Update error: {e}")
        return None
    finally:
        put_db_connection(conn)

def save_user_profile(tg_id: int, name: str, phone: str, username: str = None) -> bool:
    """Foydalanuvchi profilini saqlash"""
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
        """, (tg_id, name, phone, username))
        
        conn.commit()
        cur.close()
        return True
    except Exception as e:
        logger.error(f"❌ Profil saqlash xatosi: {e}")
        return False
    finally:
        put_db_connection(conn)

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
        put_db_connection(conn)

# ==========================================
# NOTIFICATION FUNCTIONS
# ==========================================
async def notify_admin_new_order(order: Dict):
    """Admin ga yangi buyurtma haqida xabar"""
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

        items = order.get('items', [])
        if isinstance(items, str):
            items = json.loads(items)

        items_text = "\n".join([f"• {i.get('name')} x{i.get('qty')}" for i in items]) if items else "Ma'lumot yo'q"
        phone_display = format_phone_display(order.get('phone', ''))
        customer_name = order.get('name', 'Mijoz')
        location = order.get('location')
        location_text = ""
        
        if location and ',' in str(location):
            try:
                lat, lng = str(location).split(',')
                location_text = f"\n📍 <b>Joylashuv:</b> <a href='https://maps.google.com/?q={lat},{lng}'>Xaritada ko'rish</a>"
            except:
                pass

        status_text = "⏳ <b>YANGI BUYURTMA - TO'LOV KUTILMOQDA!</b>"

        admin_message = f"""{status_text}

🆔 Buyurtma: #{order.get('order_id', 'N/A')[-6:]}
👤 Mijoz: {customer_name}
📞 Telefon: {phone_display}
💵 Summa: {format_price(order.get('total', 0))} so'm
📱 Manba: {'🤖 WebApp' if order.get('source') == 'webapp' else '🌐 Sayt'}{location_text}

🍽 Mahsulotlar:
{items_text}

⏰ {datetime.now().strftime('%H:%M:%S')}

<i>⚡ To'lovni tekshiring, keyin qabul qiling yoki bekor qiling</i>"""

        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ QABUL QILISH", callback_data=f"accept_{order.get('order_id')}"),
                InlineKeyboardButton("❌ BEKOR QILISH", callback_data=f"reject_{order.get('order_id')}")
            ],
            [
                InlineKeyboardButton("💳 TO'LOVNI TEKSHIRISH", callback_data=f"open_payme_group_{order.get('order_id')}")
            ]
        ])

        await bot.send_message(
            chat_id=ADMIN_CHAT_ID_INT,
            text=admin_message,
            reply_markup=keyboard,
            parse_mode='HTML'
        )
        
        logger.info(f"✅ Admin ga xabar yuborildi: {order.get('order_id')}")
        return True

    except Exception as e:
        logger.error(f"❌ notify_admin_new_order xatosi: {e}")
        import traceback
        traceback.print_exc()
        return False

async def notify_customer_accepted(bot, order: Dict, prep_time: str):
    """Buyurtma qabul qilinganda mijozga xabar"""
    tg_id = order.get('tg_id') or order.get('tgId')
    
    if tg_id and isinstance(tg_id, str):
        try:
            tg_id = int(tg_id)
        except ValueError:
            tg_id = None
    
    if not tg_id or tg_id == 0:
        logger.warning(f"⚠️ Mijoz tg_id yo'q: {order.get('order_id')}")
        return False
    
    try:
        items = order.get('items', [])
        if isinstance(items, str):
            items = json.loads(items)
        
        items_short = ", ".join([f"{i.get('name')} x{i.get('qty')}" for i in items[:3]]) if items else "Ma'lumot yo'q"
        if len(items) > 3:
            items_short += f" va yana {len(items)-3} ta"
        
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
            "📞 Savollar bo'yicha: +998882015020"
        )
        
        await bot.send_message(chat_id=int(tg_id), text=customer_message, parse_mode='HTML')
        logger.info(f"✅ Mijozga qabul xabari yuborildi: {tg_id}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Mijozga xabar yuborishda xato: {e}")
        return False

# ==========================================
# HANDLERS
# ==========================================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start handler"""
    user = update.effective_user
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
        formatted_phone = f"{phone[:2]} {phone[2:5]} {phone[5:7]} {phone[7:]}" if len(phone) == 9 else phone
        
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

async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Barcha callback query larni qayta ishlash"""
    query = update.callback_query
    user = update.effective_user
    
    try:
        await query.answer()
    except Exception as e:
        logger.error(f"Query answer xatosi: {e}")
    
    data = query.data
    logger.info(f"👆 Callback: {data} from {user.id}")
    
    # Statistika
    if data == "admin_stats":
        await show_stats(update, context)
        return
    
    # Yangi buyurtmalar
    if data == "show_new_orders":
        await show_new_orders_list(update, context)
        return
    
    # Payme guruhiga o'tish
    if data.startswith("open_payme_group_"):
        order_id = data.replace("open_payme_group_", "")
        order = get_order(order_id)
        if not order:
            await query.edit_message_text("❌ Buyurtma topilmadi!")
            return
        
        group_link = f"https://t.me/{PAYME_GROUP_USERNAME}"
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("💳 Payme guruhiga o'tish", url=group_link)],
            [InlineKeyboardButton("🔙 Orqaga", callback_data=f"back_to_order_{order_id}")]
        ])
        
        await query.edit_message_text(
            f"💳 <b>To'lovni tekshirish</b>\n\n"
            f"🆔 Buyurtma: #{order_id[-6:]}\n"
            f"💵 Summa: {format_price(order.get('total', 0))} so'm\n\n"
            f"ORDER ID: <code>{order_id}</code>",
            reply_markup=keyboard,
            parse_mode='HTML'
        )
        return
    
    # Orqaga qaytish
    if data.startswith("back_to_order_"):
        order_id = data.replace("back_to_order_", "")
        order = get_order(order_id)
        if order:
            await show_order_to_admin(update, context, order)
        return
    
    # ⭐ YANGI: Tayyorlanish vaqti tanlash (inline buttons)
    if data.startswith("prep_"):
        parts = data.split("_")
        if len(parts) >= 3:
            time_val = parts[1]
            order_id = parts[2]
            
            if time_val == "custom":
                # Custom vaqt uchun text so'rash (faqat shu holatda)
                save_admin_state(user.id, 'awaiting_custom_time', order_id)
                await query.edit_message_text(
                    f"⏱ <b>#{order_id[-6:]}</b> uchun tayyorlanish vaqtini kiriting:\n"
                    f"<i>Masalan: 25 daqiqa, 1 soat 15 daqiqa</i>",
                    parse_mode='HTML'
                )
                return
            else:
                await process_accept_with_time(update, context, order_id, f"{time_val} daqiqa")
        return
    
    # Bekor qilish
    if data.startswith("cancel_accept_"):
        order_id = data.replace("cancel_accept_", "")
        clear_admin_state(user.id)
        await query.edit_message_text(
            "❌ <b>Qabul qilish bekor qilindi</b>",
            parse_mode='HTML'
        )
        return
    
    # Buyurtma qabul qilish (vaqt tanlash tugmalari ko'rsatish)
    if data.startswith("accept_"):
        order_id = data.replace("accept_", "")
        order = get_order(order_id)
        
        if not order:
            await query.edit_message_text("❌ Buyurtma topilmadi!")
            return
        
        if order.get('status') == 'accepted':
            await query.answer("⚠️ Bu buyurtma allaqachon qabul qilingan!", show_alert=True)
            return
        
        # ⭐ Database da state saqlash (context.user_data o'rniga)
        save_admin_state(user.id, 'selecting_prep_time', order_id)
        
        # Tayyorlanish vaqti tugmalari
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("15 daqiqa", callback_data=f"prep_15_{order_id}"),
                InlineKeyboardButton("30 daqiqa", callback_data=f"prep_30_{order_id}")
            ],
            [
                InlineKeyboardButton("45 daqiqa", callback_data=f"prep_45_{order_id}"),
                InlineKeyboardButton("60 daqiqa", callback_data=f"prep_60_{order_id}")
            ],
            [
                InlineKeyboardButton("📝 Boshqa vaqt", callback_data=f"prep_custom_{order_id}")
            ],
            [
                InlineKeyboardButton("❌ Bekor qilish", callback_data=f"cancel_accept_{order_id}")
            ]
        ])
        
        phone_display = format_phone_display(order.get('phone', ''))
        
        await query.edit_message_text(
            f"⏱ <b>BUYURTMANI QABUL QILISH</b>\n\n"
            f"🆔 Buyurtma: #{order_id[-6:]}\n"
            f"👤 Mijoz: {order.get('name', 'Mijoz')}\n"
            f"📞 Telefon: {phone_display}\n"
            f"💵 Summa: {format_price(order.get('total', 0))} so'm\n\n"
            f"<b>Tayyorlanish vaqtini tanlang:</b>",
            reply_markup=keyboard,
            parse_mode='HTML'
        )
        return
    
    # Bekor qilish
    if data.startswith("reject_"):
        order_id = data.replace("reject_", "")
        order = get_order(order_id)
        
        if not order:
            await query.edit_message_text("❌ Buyurtma topilmadi!")
            return
        
        updated = update_order_status(order_id, 'rejected', rejected_at=datetime.utcnow())
        
        if updated:
            await query.edit_message_text(
                f"❌ <b>BUYURTMA BEKOR QILINDI</b>\n\n"
                f"🆔 #{order_id[-6:]}\n"
                f"⏰ {datetime.now().strftime('%H:%M:%S')}",
                parse_mode='HTML'
            )
            
            # Mijoz ga xabar
            tg_id = order.get('tg_id')
            if tg_id:
                try:
                    await context.bot.send_message(
                        chat_id=int(tg_id),
                        text=f"❌ <b>Buyurtmangiz bekor qilindi</b>\n\n🆔 Buyurtma: #{order_id[-6:]}",
                        parse_mode='HTML'
                    )
                except Exception as e:
                    logger.error(f"Mijozga bekor xabari yuborishda xato: {e}")
        return

async def process_accept_with_time(update: Update, context: ContextTypes.DEFAULT_TYPE, order_id: str, prep_time: str):
    """Buyurtma qabul qilish va mijozga xabar yuborish"""
    query = update.callback_query
    user = update.effective_user
    
    try:
        order = get_order(order_id)
        if not order:
            await query.edit_message_text("❌ Buyurtma topilmadi!")
            clear_admin_state(user.id)
            return
        
        if order.get('status') == 'accepted':
            await query.answer("Allaqachon qabul qilingan!", show_alert=True)
            clear_admin_state(user.id)
            return
        
        # Buyurtmani yangilash
        updated = update_order_status(
            order_id, 
            'accepted',
            admin_note=f"Tayyorlanish vaqti: {prep_time}",
            accepted_at=datetime.utcnow()
        )

        if updated:
            # Admin ga tasdiq
            await query.edit_message_text(
                f"✅ <b>BUYURTMA QABUL QILINDI</b>\n\n"
                f"🆔 Buyurtma: #{order_id[-6:]}\n"
                f"⏱ Tayyorlanish vaqti: {prep_time}\n"
                f"💵 Summa: {format_price(order.get('total', 0))} so'm\n\n"
                f"📨 Mijozga xabar yuborilmoqda...",
                parse_mode='HTML'
            )
            
            # Mijozga xabar
            notification_sent = await notify_customer_accepted(context.bot, updated, prep_time)
            
            if notification_sent:
                await context.bot.send_message(
                    chat_id=user.id,
                    text=f"✅ Mijozga xabar yuborildi!\n\n🆔 #{order_id[-6:]}",
                    parse_mode='HTML'
                )
            else:
                await context.bot.send_message(
                    chat_id=user.id,
                    text=f"⚠️ Mijozga xabar yuborilmadi. Qo'lda xabar yuboring.\n\n🆔 #{order_id[-6:]}",
                    parse_mode='HTML'
                )
        else:
            await query.edit_message_text("❌ Xatolik yuz berdi!", parse_mode='HTML')
            
    except Exception as e:
        logger.error(f"❌ Process accept xatosi: {e}")
        await query.edit_message_text("❌ Xatolik yuz berdi!", parse_mode='HTML')
    finally:
        clear_admin_state(user.id)

async def show_order_to_admin(update: Update, context: ContextTypes.DEFAULT_TYPE, order: Dict):
    """Buyurtma ma'lumotlarini admin ga ko'rsatish"""
    items = order.get('items', [])
    if isinstance(items, str):
        items = json.loads(items)
    
    items_text = "\n".join([f"• {i.get('name')} x{i.get('qty')}" for i in items]) if items else "Ma'lumot yo'q"
    phone_display = format_phone_display(order.get('phone', ''))
    
    location = order.get('location')
    location_text = ""
    if location and ',' in str(location):
        try:
            lat, lng = str(location).split(',')
            location_text = f"\n📍 <a href='https://maps.google.com/?q={lat},{lng}'>Xaritada ko'rish</a>"
        except:
            pass
    
    status_text = "⏳ <b>YANGI BUYURTMA - TO'LOV KUTILMOQDA!</b>"
    
    message = f"""{status_text}

🆔 Buyurtma: #{order.get('order_id', 'N/A')[-6:]}
👤 Mijoz: {order.get('name')}
📞 Telefon: {phone_display}
💵 Summa: {format_price(order.get('total', 0))} so'm
📱 Manba: {'🤖 WebApp' if order.get('source') == 'webapp' else '🌐 Sayt'}{location_text}

🍽 Mahsulotlar:
{items_text}

⏰ {datetime.now().strftime('%H:%M:%S')}"""

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

async def show_new_orders_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Yangi buyurtmalar ro'yxatini ko'rsatish"""
    query = update.callback_query
    await query.answer()
    
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT * FROM orders 
            WHERE status IN ('pending_payment', 'pending')
            AND created_at > CURRENT_TIMESTAMP - INTERVAL '24 hours'
            ORDER BY created_at DESC
            LIMIT 10
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
        put_db_connection(conn)
    
    if not new_orders:
        await query.edit_message_text(
            "📭 <b>Hozircha yangi buyurtmalar yo'q</b>",
            parse_mode='HTML'
        )
        return
    
    # Har bir buyurtma uchun alohida xabar
    for order in new_orders:
        await show_order_to_admin(update, context, order)
    
    await query.edit_message_text(
        f"📋 <b>{len(new_orders)} ta yangi buyurtma</b> yuborildi.",
        parse_mode='HTML'
    )

async def show_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Statistikani ko'rsatish"""
    user = update.effective_user
    
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        today = datetime.now().strftime('%Y-%m-%d')
        
        cur.execute("SELECT COUNT(*) FROM orders WHERE status IN ('pending', 'pending_payment')")
        new_count = cur.fetchone()['count']
        
        cur.execute("""
            SELECT COUNT(*), COALESCE(SUM(total), 0) 
            FROM orders 
            WHERE status = 'accepted' AND DATE(accepted_at) = %s
        """, (today,))
        today_result = cur.fetchone()
        
        cur.execute("""
            SELECT COUNT(*), COALESCE(SUM(total), 0) 
            FROM orders 
            WHERE status = 'accepted'
        """)
        total_result = cur.fetchone()
        
        cur.close()
        
        stats_text = f"""📊 <b>STATISTIKA</b>

🕐 <b>Bugun ({today}):</b>
• Buyurtmalar: {today_result['count']} ta
• Summa: {format_price(today_result['coalesce'])} so'm

⏳ <b>Kutilayotgan:</b>
• Yangi: {new_count} ta

📈 <b>Jami:</b>
• Qabul qilingan: {total_result['count']} ta
• Umumiy: {format_price(total_result['coalesce'])} so'm

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
    finally:
        put_db_connection(conn)

# ==========================================
# API HANDLERS
# ==========================================
async def health_handler(request):
    """Kengaytirilgan health check"""
    status = {
        "status": "ok",
        "service": "bodrum-bot",
        "timestamp": datetime.utcnow().isoformat(),
        "webhook_url": WEBHOOK_URL,
        "admin_id": ADMIN_CHAT_ID_INT
    }
    
    # Database tekshiruvi
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
        cur.close()
        put_db_connection(conn)
        status["database"] = "connected"
    except Exception as e:
        status["database"] = f"error: {str(e)}"
        status["status"] = "degraded"
    
    # Webhook tekshiruvi
    global application
    if application and application.bot:
        try:
            webhook_info = await application.bot.get_webhook_info()
            status["webhook"] = {
                "url": webhook_info.url,
                "pending_updates": webhook_info.pending_update_count
            }
        except Exception as e:
            status["webhook"] = f"error: {str(e)}"
    
    return web.json_response(status, headers=get_cors_headers())

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
            # Admin ga xabar yuborish
            asyncio.create_task(notify_admin_new_order(order))
            
            payme_url = f"https://checkout.payme.uz/{os.getenv('PAYME_MERCHANT_ID')}?orderId={order['order_id']}&amount={order['total'] * 100}"
            
            response_data = {**order}
            for key in ['created_at', 'accepted_at', 'rejected_at', 'paid_at', 'confirmed_at']:
                if response_data.get(key) and hasattr(response_data[key], 'isoformat'):
                    response_data[key] = response_data[key].isoformat()
            
            response_data["payme_url"] = payme_url
            
            return web.json_response(response_data, status=201, headers=get_cors_headers())
        else:
            return web.json_response({"error": "Failed to create order"}, status=500, headers=get_cors_headers())
        
    except Exception as e:
        logger.error(f"API create order error: {e}")
        return web.json_response({"error": str(e)}, status=500, headers=get_cors_headers())

async def orders_list_handler(request):
    """Barcha buyurtmalarni olish"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
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
        
        orders = []
        for row in results:
            order_dict = dict(row)
            for key in ['created_at', 'accepted_at', 'rejected_at', 'paid_at', 'confirmed_at']:
                if order_dict.get(key) and hasattr(order_dict[key], 'isoformat'):
                    order_dict[key] = order_dict[key].isoformat()
            orders.append(order_dict)
        
        return web.json_response(orders, headers=get_cors_headers())
        
    except Exception as e:
        logger.error(f"Orders list error: {e}")
        return web.json_response({"error": str(e)}, status=500, headers=get_cors_headers())
    finally:
        put_db_connection(conn)

async def new_orders_handler(request):
    """Yangi buyurtmalarni olish"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT * FROM orders 
            WHERE status IN ('pending', 'pending_payment') 
            ORDER BY created_at DESC
        """)
        results = cur.fetchall()
        cur.close()
        
        orders = []
        for row in results:
            order_dict = dict(row)
            for key in ['created_at', 'accepted_at', 'rejected_at', 'paid_at', 'confirmed_at']:
                if order_dict.get(key) and hasattr(order_dict[key], 'isoformat'):
                    order_dict[key] = order_dict[key].isoformat()
            orders.append(order_dict)
        
        return web.json_response(orders, headers=get_cors_headers())
        
    except Exception as e:
        logger.error(f"New orders error: {e}")
        return web.json_response({"error": str(e)}, status=500, headers=get_cors_headers())
    finally:
        put_db_connection(conn)

async def get_order_handler(request):
    try:
        order_id = request.match_info['order_id']
        order = get_order(order_id)
        
        if not order:
            return web.json_response({"error": "Not found"}, status=404, headers=get_cors_headers())
        
        response_data = {**order}
        for key in ['created_at', 'accepted_at', 'rejected_at', 'paid_at', 'confirmed_at']:
            if response_data.get(key) and hasattr(response_data[key], 'isoformat'):
                response_data[key] = response_data[key].isoformat()  # ✅ To'g'ri
        
        return web.json_response(response_data, headers=get_cors_headers())
        
    except Exception as e:
        logger.error(f"API get order error: {e}")
        return web.json_response({"error": str(e)}, status=500, headers=get_cors_headers())

async def update_order_handler(request):
    try:
        order_id = request.match_info['order_id']
        data = await request.json()
        
        status = data.get('status')
        updated = update_order_status(order_id, status)
        
        if updated:
            response_data = {**updated}
            for key in ['created_at', 'accepted_at', 'rejected_at', 'paid_at', 'confirmed_at']:
                if response_data.get(key) and hasattr(response_data[key], 'isoformat'):
                    response_data[key] = response_data[key].isoformat()
            return web.json_response(response_data, headers=get_cors_headers())
        else:
            return web.json_response({"error": "Order not found"}, status=404, headers=get_cors_headers())
            
    except Exception as e:
        logger.error(f"Update order error: {e}")
        return web.json_response({"error": str(e)}, status=500, headers=get_cors_headers())

async def webhook_handler(request):
    global application
    
    if application:
        try:
            data = await request.json()
            
            if 'callback_query' in data:
                logger.info(f"👆 Callback: {data['callback_query']['data']}")
            
            update = Update.de_json(data, application.bot)
            await application.process_update(update)
        except Exception as e:
            logger.error(f"Webhook processing error: {e}")
    
    return web.Response(text='OK')

# ==========================================
# SCHEDULER & MAINTENANCE
# ==========================================
async def verify_webhook():
    """Har 30 daqiqada webhook ni tekshirish va qayta o'rnatish"""
    global application
    if not application or not application.bot:
        return
    
    try:
        webhook_info = await application.bot.get_webhook_info()
        expected_url = f"{WEBHOOK_URL}/webhook"
        
        if webhook_info.url != expected_url:
            logger.warning(f"⚠️ Webhook noto'g'ri: {webhook_info.url}")
            await application.bot.set_webhook(
                url=expected_url,
                allowed_updates=['message', 'callback_query', 'edited_message']
            )
            logger.info("✅ Webhook qayta o'rnatildi")
            
        if webhook_info.pending_update_count > 10:
            await application.bot.delete_webhook(drop_pending_updates=True)
            await asyncio.sleep(1)
            await application.bot.set_webhook(url=expected_url)
            logger.info(f"🧹 {webhook_info.pending_update_count} ta pending update tozalandi")
            
    except Exception as e:
        logger.error(f"❌ Webhook tekshiruv xatosi: {e}")

async def self_ping():
    """Railway ni uyg'otish uchun self-ping"""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f'http://localhost:{PORT}/health') as resp:
                if resp.status == 200:
                    logger.debug("✅ Self-ping OK")
    except Exception as e:
        logger.error(f"Self-ping xato: {e}")

async def cleanup_expired_states():
    """Muddati o'tgan admin state larni tozalash"""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("DELETE FROM admin_states WHERE expires_at < CURRENT_TIMESTAMP")
        conn.commit()
        cur.close()
        logger.info("🧹 Eski admin state lar tozalandi")
    except Exception as e:
        logger.error(f"State cleanup xatosi: {e}")
    finally:
        put_db_connection(conn)

# ==========================================
# INITIALIZATION
# ==========================================
async def init_webhook(app):
    global application, scheduler
    
    if not TOKEN:
        logger.error("❌ TOKEN o'rnatilmagan!")
        return
    
    if not init_database():
        logger.error("❌ Database initialization failed!")
        return
    
    # Bot application yaratish
    application = Application.builder().token(TOKEN).build()
    
    # Handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("stats", show_stats))
    
    application.add_handler(MessageHandler(
        filters.CONTACT & filters.ChatType.PRIVATE,
        contact_handler
    ))
    
    application.add_handler(CallbackQueryHandler(callback_handler))
    
    # Initialize
    await application.initialize()
    await application.start()
    
    # Eski webhook ni tozalash
    try:
        await application.bot.delete_webhook(drop_pending_updates=True)
        await asyncio.sleep(2)
        logger.info("🧹 Eski webhook tozalandi")
    except Exception as e:
        logger.warning(f"Eski webhook ni tozalash xatosi: {e}")
    
    # Yangi webhook o'rnatish
    if WEBHOOK_URL:
        full_webhook_url = f"{WEBHOOK_URL}/webhook"
        try:
            await application.bot.set_webhook(
                url=full_webhook_url,
                allowed_updates=['message', 'callback_query', 'inline_query', 'edited_message']
            )
            logger.info(f"✅ Webhook o'rnatildi: {full_webhook_url}")
        except Exception as e:
            logger.error(f"❌ Webhook xato: {e}")
    
    # Scheduler ishga tushirish
    scheduler = AsyncIOScheduler()
    scheduler.add_job(verify_webhook, 'interval', minutes=30)
    scheduler.add_job(self_ping, 'interval', minutes=5)  # Railway uchun
    scheduler.add_job(cleanup_expired_states, 'interval', hours=1)
    scheduler.start()
    
    logger.info(f"🤖 Bot ishga tushdi! Admin ID: {ADMIN_CHAT_ID_INT}")

async def shutdown(app):
    global application, scheduler
    if scheduler:
        scheduler.shutdown()
    if application:
        try:
            await application.stop()
            await application.shutdown()
            logger.info("🛑 Bot to'xtatildi")
        except Exception as e:
            logger.error(f"Shutdown xato: {e}")

# ==========================================
# MAIN
# ==========================================
def main():
    logger.info("🔧 Bodrum Bot starting...")
    
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
                        'Access-Control-Allow-Headers': '*',
                    }
                )
            
            response = await handler(request)
            response.headers['Access-Control-Allow-Origin'] = '*'
            response.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, DELETE, OPTIONS'
            response.headers['Access-Control-Allow-Headers'] = '*'
            return response
        return middleware_handler
    
    app.middlewares.append(cors_middleware)
    
    # Routes
    app.router.add_get('/', health_handler)
    app.router.add_get('/health', health_handler)
    app.router.add_get('/api/orders', orders_list_handler)
    app.router.add_get('/api/orders/new', new_orders_handler)
    app.router.add_post('/api/orders', create_order_handler)
    app.router.add_get('/api/orders/{order_id}', get_order_handler)
    app.router.add_put('/api/orders/{order_id}', update_order_handler)
    app.router.add_post('/webhook', webhook_handler)
    
    app.on_startup.append(init_webhook)
    app.on_cleanup.append(shutdown)
    
    logger.info(f"🚀 Server: 0.0.0.0:{PORT}")
    web.run_app(app, host='0.0.0.0', port=PORT)

if __name__ == "__main__":
    main()
