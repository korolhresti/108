import asyncio
import logging
import logging.handlers
from datetime import datetime, timedelta, timezone
import json
import os
import random
import io
import base64
import time
from typing import List, Optional, Dict, Any
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode

from aiogram import Bot, Dispatcher, F, Router, types
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, BufferedInputFile
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.utils.markdown import hlink
from aiogram.client.default import DefaultBotProperties

from aiohttp import ClientSession
import psycopg
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, status, Depends, Request
from fastapi.security import APIKeyHeader
from fastapi.responses import HTMLResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from gtts import gTTS
from croniter import croniter

import web_parser
import rss_parser # Added rss_parser import

from database import get_db_pool
from datetime import datetime, timezone
from aiogram.utils.markdown import hlink

from apscheduler.schedulers.asyncio import AsyncIOScheduler

# Initialize scheduler globally
scheduler = AsyncIOScheduler()

def setup_scheduler(bot):
    # Sets up scheduled tasks for the bot.
    scheduler.add_job(fetch_and_post_news_task, 'interval', hours=24, args=[bot], id='daily_news_fetch')
    scheduler.add_job(delete_expired_news_task, 'interval', hours=5, id='delete_expired_news')
    scheduler.add_job(send_daily_digest, 'cron', hour=9, minute=0, id='send_daily_digest') # Every day at 9 AM UTC
    scheduler.start()


async def fetch_and_post_news_task(bot):
    # Fetches news from active sources and posts them.
    # This function is designed to be run as a scheduled task or manually.
    logger.info("Running fetch_and_post_news_task.")
    pool = await get_db_pool()

    async with pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("SELECT * FROM sources WHERE status = 'active'")
            sources = await cur.fetchall()
    
    if not sources:
        logger.info("No active sources found to parse.")
        return

    for source in sources:
        logger.info(f"Processing source: {source['source_name']} ({source['source_url']})")
        if not all([source.get('source_type'), source.get('source_url'), source.get('source_name')]):
            logger.warning(f"Skipping source due to missing data: {source}")
            continue

        news_items_from_source = []
        try:
            if source['source_type'] == 'rss':
                logger.info(f"Attempting to parse RSS feed: {source['source_url']}")
                try:
                    news_list = await rss_parser.parse_rss_feed(source['source_url'])
                    news_items_from_source.extend(news_list)
                    if not news_list:
                        logger.info(f"RSS parser for {source['source_url']} found no new news. Attempting web parser as fallback.")
                        parsed_article = await web_parser.parse_website(source['source_url'])
                        if parsed_article:
                            news_items_from_source.append(parsed_article)
                            logger.info(f"Web parser fallback for {source['source_url']} found news: {parsed_article.get('title', 'No Title')}")
                        else:
                            logger.info(f"Web parser fallback for {source['source_url']} found no new news.")
                    else:
                        logger.info(f"RSS parser for {source['source_url']} found {len(news_list)} news items.")
                except Exception as rss_e:
                    logger.error(f"Error parsing RSS feed {source['source_url']}: {rss_e}. Attempting web parser as fallback.", exc_info=True)
                    parsed_article = await web_parser.parse_website(source['source_url'])
                    if parsed_article:
                        news_items_from_source.append(parsed_article)
                        logger.info(f"Web parser fallback for {source['source_url']} found news: {parsed_article.get('title', 'No Title')}")
                    else:
                        logger.info(f"Web parser fallback for {source['source_url']} found no new news.")
            elif source['source_type'] == 'web':
                logger.info(f"Attempting to parse website: {source['source_url']}")
                parsed_article = await web_parser.parse_website(source['source_url'])
                if parsed_article:
                    news_items_from_source.append(parsed_article)
                    logger.info(f"Web parser for {source['source_url']} found news: {parsed_article.get('title', 'No Title')}")
                else:
                    logger.info(f"Web parser for {source['source_url']} found no new news.")
            else:
                logger.info(f"Skipping unsupported source type: {source['source_type']} for source {source['source_name']}")
                continue # Skip if source type is not supported

            added_any_news = False
            for news_data in news_items_from_source:
                if news_data:
                    # Set user_id_for_source to None for automatically parsed news so they go to 'pending' moderation
                    news_data.update({'source_id': source['id'], 'source_name': source['source_name'], 'source_type': source['source_type'], 'user_id_for_source': None})
                    added_news_item = await add_news_to_db(news_data)
                    if added_news_item:
                        await update_source_stats_publication_count(source['id'])
                        logger.info(get_message('uk', 'news_added_success', title=added_news_item.title))
                        added_any_news = True
                    else:
                        logger.info(get_message('uk', 'news_not_added', name=source['source_name']))
            
            if added_any_news:
                async with pool.connection() as conn_update:
                    async with conn_update.cursor() as cur_update:
                        await cur_update.execute("UPDATE sources SET last_parsed = CURRENT_TIMESTAMP WHERE id = %s", (source['id'],))
                        await conn_update.commit()
                logger.info(get_message('uk', 'source_last_parsed_updated', name=source['source_name']))
            else:
                logger.info(f"No new news added for source {source['source_name']} ({source['source_url']}).")

        except Exception as e:
            logger.error(get_message('uk', 'source_parsing_error', name=source.get('source_name', 'N/A'), url=source.get('source_url', 'N/A'), error=e), exc_info=True)
    
    news_to_post = await get_news_to_publish(limit=1)
    if news_to_post:
        news_item = news_to_post[0]
        await send_news_to_channel(news_item)
    else:
        logger.info("No approved news to post to channel.")


load_dotenv()

API_TOKEN = os.getenv("BOT_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")
ADMIN_API_KEY = os.getenv("ADMIN_API_KEY")
ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "").split(',') if x.strip()]
NEWS_CHANNEL_LINK = os.getenv("NEWS_CHANNEL_LINK", "-1002766273069")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")
MONOBANK_CARD_NUMBER = "4441111153021484"
HELP_BUY_CHANNEL_LINK = "https://t.me/+gT7TDOMh81M3YmY6"
HELP_SELL_BOT_LINK = "https://t.me/BigmoneycreateBot"

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

file_handler = logging.handlers.RotatingFileHandler('bot.log', maxBytes=10*1024*1024, backupCount=5)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

error_file_handler = logging.handlers.RotatingFileHandler('errors.log', maxBytes=10*1024*1024, backupCount=5)
error_file_handler.setLevel(logging.ERROR)
error_file_handler.setFormatter(formatter)
logger.addHandler(error_file_handler)

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

AI_REQUEST_LIMIT_DAILY_FREE = 3

app = FastAPI(title="Telegram AI News Bot API", version="1.0.0")
app.mount("/static", StaticFiles(directory="."), name="static")

api_key_header = APIKeyHeader(name="X-API-Key")

async def get_api_key(api_key: str = Depends(api_key_header)):
    # Dependency to validate the admin API key.
    if not ADMIN_API_KEY:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="ADMIN_API_KEY not configured.")
    if api_key is None or api_key != ADMIN_API_KEY:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or missing API key.")
    return api_key

bot = Bot(token=API_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
router = Router()
dp.include_router(router)

db_pool: Optional[AsyncConnectionPool] = None

async def get_db_pool():
    # Initializes and returns a database connection pool.
    global db_pool
    if db_pool is None:
        if not DATABASE_URL:
            logger.error("DATABASE_URL environment variable is not set.")
            raise ValueError("DATABASE_URL environment variable is not set.")
        try:
            db_pool = AsyncConnectionPool(conninfo=DATABASE_URL, min_size=1, max_size=10, open=psycopg.AsyncConnection.connect)
            async with db_pool.connection() as conn:
                await conn.execute("SELECT 1")
            logger.info("DB pool initialized successfully.")
        except Exception as e:
            logger.error(f"Failed to initialize DB pool: {e}", exc_info=True)
            raise
    return db_pool

from pydantic import BaseModel, HttpUrl

class News(BaseModel):
    # Pydantic model for a news item.
    id: Optional[int] = None
    source_id: Optional[int] = None
    title: str
    content: str
    source_url: HttpUrl
    normalized_source_url: str # Added to Pydantic model
    image_url: Optional[HttpUrl] = None
    published_at: datetime
    moderation_status: str = 'pending'
    expires_at: Optional[datetime] = None
    is_published_to_channel: Optional[bool] = False
    ai_classified_topics: Optional[List[str]] = None # Added this back for filtering

class User(BaseModel):
    # Pydantic model for a user.
    id: Optional[int] = None
    telegram_id: int
    username: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    created_at: Optional[datetime] = None
    is_admin: Optional[bool] = False
    last_active: Optional[datetime] = None
    language: Optional[str] = 'uk'
    auto_notifications: Optional[bool] = False
    digest_frequency: Optional[str] = 'daily'
    safe_mode: Optional[bool] = False
    current_feed_id: Optional[int] = None
    is_premium: Optional[bool] = False
    premium_expires_at: Optional[datetime] = None
    level: Optional[int] = 1
    badges: Optional[List[str]] = []
    inviter_id: Optional[int] = None
    view_mode: Optional[str] = 'detailed'
    premium_invite_count: Optional[int] = 0
    digest_invite_count: Optional[int] = 0
    is_pro: Optional[bool] = False
    ai_requests_today: Optional[int] = 0
    ai_last_request_date: Optional[datetime] = None

class Source(BaseModel):
    # Pydantic model for a news source.
    id: Optional[int] = None
    user_id: Optional[int] = None
    source_name: str
    source_url: HttpUrl
    normalized_source_url: str # Added to Pydantic model
    source_type: str
    status: str = 'active'
    added_at: Optional[datetime] = None
    last_parsed: Optional[datetime] = None
    parse_frequency: str = 'hourly'

MESSAGES = {
    'uk': {
        'welcome': "Привіт, {first_name}! Я ваш AI News Bot. Оберіть дію:",
        'main_menu_prompt': "Оберіть дію:",
        'help_text': ("<b>Команди:</b>\n"
                      "/start - Почати\n"
                      "/menu - Меню\n"
                      "/cancel - Скасувати\n"
                      "/my_news - Мої новини\n"
                      "/add_source - Додати джерело\n"
                      "/my_sources - Мої джерела\n"
                      "/ask_expert - Експерт\n"
                      "/invite - Запросити\n"
                      "/subscribe - Підписки\n"
                      "/donate - Донат ☕\n"
                      "<b>AI Media:</b> /ai_media_menu"),
        'action_cancelled': "Скасовано. Оберіть дію:",
        'add_source_prompt': "Надішліть URL джерела:",
        'invalid_url': "Невірний URL.",
        'source_url_not_found': "URL джерела не знайдено.",
        'source_added_success': "Джерело '{source_url}' додано!",
        'add_source_error': "Помилка додавання джерела.",
        'no_new_news': "Немає нових новин.",
        'news_not_found': "Новина не знайдена.",
        'no_more_news': "Більше новин немає.",
        'first_news': "Це перша новина.",
        'error_start_menu': "Помилка. Почніть з /menu.",
        'ai_functions_prompt': "AI-функції:",
        'ai_function_premium_only': "Лише для преміум.",
        'news_title_label': "Заголовок:",
        'news_content_label': "Зміст:",
        'published_at_label': "Опубліковано:",
        'news_progress': "Новина {current_index} з {total_news}",
        'read_source_btn': "🔗 Джерело",
        'ai_functions_btn': "🧠 AI-функції",
        'prev_btn': "⬅️ Попередня",
        'next_btn': "➡️ Далі",
        'main_menu_btn': "⬅️ Меню",
        'generating_ai_summary': "Генерую AI-резюме...",
        'ai_summary_label': "AI-резюме:",
        'select_translate_language': "Оберіть мову:",
        'translating_news': "Перекладаю...",
        'translation_label': "Переклад на {language_name}:",
        'generating_audio': "Генерую аудіо...",
        'audio_news_caption': "🔊 Новина: {title}",
        'audio_error': "Помилка генерації аудіо.",
        'ask_news_ai_prompt': "Ваше запитання:",
        'processing_question': "Обробляю...",
        'ai_response_label': "Відповідь AI:",
        'ai_news_not_found': "Новина не знайдена.",
        'ask_free_ai_prompt': "Ваше запитання до AI:",
        'extracting_entities': "Витягую сутності...",
        'entities_label': "Сутності:",
        'explain_term_prompt': "Термін для пояснення:",
        'explaining_term': "Пояснюю...",
        'term_explanation_label': "Пояснення '{term}':",
        'topics_label': "Теми:",
        'checking_facts': "Перевіряю факти...",
        'fact_check_label': "Перевірка фактів:",
        'analyzing_sentiment': "Аналізую настрій...",
        'sentiment_label': "Настрій:",
        'detecting_bias': "Виявляю упередженість...",
        'bias_label': "Упередженість:",
        'generating_audience_summary': "Генерую резюме для аудиторії...",
        'audience_summary_label': "Резюме для аудиторії:",
        'searching_historical_analogues': "Шукаю аналоги...",
        'historical_analogues_label': "Історичні аналоги:",
        'analyzing_impact': "Аналізую вплив...",
        'impact_label': "Аналіз впливу:",
        'performing_monetary_analysis': "Виконую грошовий аналіз...",
        'monetary_analysis_label': "Грошовий аналіз:",
        'bookmark_added': "Новину додано до закладок!",
        'bookmark_already_exists': "Вже в закладках.",
        'bookmark_add_error': "Помилка закладок.",
        'bookmark_removed': "Новину видалено із закладок!",
        'bookmark_not_found': "Новини немає в закладках.",
        'bookmark_remove_error': "Помилка видалення закладок.",
        'no_bookmarks': "Закладок немає.",
        'your_bookmarks_label': "Ваші закладки:",
        'report_fake_news_btn': "🚩 Повідомити фейк",
        'report_already_sent': "Репорт вже надіслано.",
        'report_sent_success': "Дякуємо! Репорт надіслано.",
        'report_action_done': "Дякуємо! Оберіть дію:",
        'user_not_identified': "Користувача не ідентифіковано.",
        'no_admin_access': "Немає доступу.",
        'loading_moderation_news': "Завантажую новини...",
        'no_pending_news': "Немає новин на модерації.",
        'moderation_news_label': "Новина на модерації ({current_index} з {total_news}):",
        'source_label': "Джерело:",
        'status_label': "Статус:",
        'approve_btn': "✅ Схвалити",
        'reject_btn': "❌ Відхилити",
        'news_approved': "Новину {news_id} схвалено!",
        'news_rejected': "Новину {news_id} відхилено!",
        'all_moderation_done': "Усі новини оброблено.",
        'no_more_moderation_news': "Більше новин немає.",
        'first_moderation_news': "Це перша новина.",
        'source_stats_label': "📊 Статистика джерел (топ-10):",
        'source_stats_entry': "{idx}. {source_name}: {count} публікацій",
        'no_source_stats': "Статистика джерел відсутня.",
        'your_invite_code': "Ваш інвайт-код: <code>{invite_code}</code>\nПоділіться: {invite_link}",
        'invite_error': "Помилка генерації коду.",
        'daily_digest_header': "📰 Ваш щоденний AI-дайджест:",
        'daily_digest_entry': "<b>{idx}. {title}</b>\n{summary}\n🔗 <a href='{source_url}'>Читати</a>\n\n",
        'no_news_for_digest': "Немає новин для дайджесту.",
        'ai_rate_limit_exceeded': "Забагато AI-запитів. Використано {count}/{limit}. Спробуйте завтра або преміум.",
        'what_new_digest_header': "👋 Привіт! Ви пропустили {count} новин. Дайджест:",
        'what_new_digest_footer': "\n\nВсі новини? Натисніть '📰 Мої новини'.",
        'cancel_btn': "Скасувати",
        'toggle_notifications_btn': "🔔 Сповіщення",
        'set_digest_frequency_btn': "🔄 Частота дайджесту",
        'toggle_safe_mode_btn': "🔒 Безпечний режим",
        'set_view_mode_btn': "👁️ Режим перегляду",
        'translate_btn': "🌐 Перекласти",
        'extract_entities_btn': "🧑‍🤝‍🧑 Сутності",
        'explain_term_btn': "❓ Пояснити",
        'listen_news_btn': "🔊 Прослухати",
        'fact_check_btn': "✅ Факт (Преміум)",
        'bias_detection_btn': "🔍 Упередженість (Преміум)",
        'audience_summary_btn': "📝 Резюме для аудиторії (Преміум)",
        'historical_analogues_btn': "📜 Аналоги (Преміум)",
        'impact_analysis_btn': "💥 Вплив (Преміум)",
        'monetary_impact_btn': "💰 Грошовий аналіз (Преміум)",
        'back_to_ai_btn': "⬅️ До AI",
        'news_channel_link_error': "Невірне посилання на канал.",
        'news_channel_link_warning': "Невірний формат посилання.",
        'news_published_success': "Новина '{title}' опублікована в каналі {identifier}.",
        'news_publish_error': "Помилка публікації '{title}': {error}",
        'source_parsing_warning': "Не вдалося спарсити з джерела: {name} ({url}).",
        'source_parsing_error': "Помилка парсингу джерела {name} ({url}): {error}",
        'no_active_sources': "Немає активних джерел.",
        'news_already_exists': "Новина з URL {url} вже існує.",
        'news_added_success': "Новина '{title}' додана.",
        'news_not_added': "Новина з джерела {name} не додана.",
        'source_last_parsed_updated': "Оновлено last_parsed для джерела {name}.",
        'deleted_expired_news': "Видалено {count} прострочених новин.",
        'no_expired_news': "Немає прострочених новин.",
        'daily_digest_no_users': "Немає користувачів для дайджесту.",
        'daily_digest_no_news': "Для користувача {user_id} немає новин для дайджесту.",
        'daily_digest_sent_success': "Дайджест надіслано користувачу {user_id}.",
        'daily_digest_send_error': "Помилка надсилання дайджесту користувачу {user_id}: {error}",
        'invite_link_label': "Посилання для запрошення",
        'source_stats_top_10': "📊 Статистика джерел (топ-10):",
        'source_stats_item': "{idx}. {source_name}: {publication_count} публікацій",
        'no_source_stats_available': "Статистика джерел відсутня.",
        'moderation_news_header': "Новина на модерації ({current_index} з {total_news}):",
        'moderation_news_approved': "Новину {news_id} схвалено!",
        'moderation_news_rejected': "Новину {news_id} відхилено!",
        'moderation_all_done': "Усі новини оброблено.",
        'moderation_no_more_news': "Більше новин немає.",
        'moderation_first_news': "Це перша новина.",
        'ask_expert_prompt': "Оберіть експерта:",
        'expert_portnikov_btn': "🕵️‍♂️ Віталій Портников",
        'expert_libsits_btn': "🧠 Ігор Лібсіц",
        'ask_expert_question_prompt': "Ваше запитання до {expert_name}:",
        'expert_response_label': "Відповідь {expert_name}:",
        'price_analysis_prompt': "💰 Аналіз цін",
        'price_analysis_generating': "Аналізую ціну...",
        'price_analysis_result': "<b>Аналіз ціни:</b>\n{result}",
        'ai_media_menu_prompt': "AI-медіа функції:",
        'youtube_to_news_btn': "▶️ YouTube → Новина",
        'create_filtered_channel_btn': "➕ Створити мій канал",
        'create_ai_media_btn': "🤖 Створити AI Медіа",
        'youtube_url_prompt': "Посилання на YouTube-відео:",
        'youtube_processing': "Обробляю YouTube...",
        'youtube_summary_label': "<b>YouTube Новина:</b>\n{summary}",
        'filtered_channel_prompt': "Назва каналу та теми (через кому):",
        'filtered_channel_creating': "Створюю канал '{channel_name}' з темами: {topics}...",
        'filtered_channel_created': "Канал '{channel_name}' 'створено'! Додайте бота як адміна, щоб він публікував новини за вашими темами.",
        'ai_media_creating': "Створюю AI-медіа...",
        'ai_media_created': "Ваше AI-медіа '{media_name}' 'створено'!",
        'analytics_menu_prompt': "Аналітика:",
        'infographics_btn': "📈 Інфографіка",
        'trust_index_btn': "⚖️ Індекс довіри",
        'long_term_connections_btn': "🔗 Зв'язки",
        'ai_prediction_btn': "🔮 AI-прогноз",
        'infographics_generating': "Генерую інфографіку...",
        'infographics_result': "<b>Інфографіка:</b>\n{result}",
        'trust_index_calculating': "Розраховую індекс довіри...",
        'trust_index_result': "<b>Індекс довіри:</b>\n{result}",
        'long_term_connections_generating': "Шукаю зв'язки...",
        'long_term_connections_result': "<b>Довгострокові зв'язки:</b>\n{result}",
        'ai_prediction_generating': "Генерую AI-прогноз...",
        'ai_prediction_result': "<b>AI-прогноз:</b>\n{result}",
        'onboarding_step_1': "Step 1: Add source '➕ Add Source'.",
        'onboarding_step_2': "Step 2: View news '📰 My News'.",
        'onboarding_step_3': "Step 3: Click '🧠 AI Functions' below news.",
        'reaction_interesting': "🔥 Interesting",
        'reaction_not_much': "😐 Not much",
        'reaction_delete': "❌ Delete",
        'reaction_saved': "Reaction saved!",
        'reaction_deleted': "News deleted.",
        'premium_granted': "Congrats! Premium access granted!",
        'digest_granted': "Congrats! Free daily AI digest granted!",
        'donate_message': "Thanks for support! Monobank card: <code>{card_number}</code> ☕",
        'my_sources_header': "Your sources:",
        'no_sources_added': "No sources added.",
        'source_item': "{idx}. {source_name} ({source_url}) - {status} [🗑️ /source_delete_{source_id}]",
        'source_deleted_success': "Source deleted.",
        'source_delete_error': "Error deleting source.",
        'subscribe_menu_prompt': "Manage subscriptions:",
        'no_subscriptions': "No topic subscriptions.",
        'your_subscriptions': "Your subscriptions: {topics}",
        'add_subscription_prompt': "Topics to subscribe (comma-separated):",
        'subscription_added': "Subscriptions '{topics}' added!",
        'subscription_removed': "Subscription '{topic}' removed.",
        'add_subscription_btn': "➕ Add Subscription",
        'remove_subscription_btn': "➖ Remove Subscription",
        'remove_subscription_prompt': "Topic to remove:",
        'subscription_not_found': "Topic '{topic}' not found.",
        'pro_tier_info': "Pro-tier: API access & extended integrations. Contact admin.",
        'help_sell_btn': "🤝 Help Sell",
        'help_buy_btn': "🛒 Help Buy",
        'help_sell_message': "Contact our sales assistant bot: {bot_link}",
        'help_buy_message': "Check the channel with best offers: {channel_link}",
        'help_btn': "❓ Help",
        'language_btn': "🌐 Language",
        'invite_friends': "👥 Invite Friends",
        'subscribe_menu': "➕ Subscriptions",
        'english_lang': "English",
        'ukrainian_lang': "Ukrainian",
        'polish_lang': "Polish",
        'german_lang': "German",
        'spanish_lang': "Spanish",
        'french_lang': "French",
        'unknown_source': "Unknown Source",
        'bookmark_add_btn': "⭐️ Bookmark",
        'action_done': "Action done.",
        'parse_now_started': "Запускаю парсинг усіх джерел. Це може зайняти деякий час.",
        'parse_now_completed': "Парсинг джерел завершено."
    }
}

def get_message(user_lang: str, key: str, **kwargs) -> str:
    # Retrieves a localized message based on the user's language and message key.
    # Falls back to Ukrainian if the user's language is not found.
    return MESSAGES.get(user_lang, MESSAGES['uk']).get(key, "").format(**kwargs)

def normalize_url(url: str) -> str:
    # Normalizes a URL to ensure consistent comparison by removing trailing slashes
    # and sorting query parameters.
    parsed = urlparse(url)
    # Remove trailing slashes from path
    path = parsed.path.rstrip('/')
    # Sort query parameters
    query_params = parse_qs(parsed.query)
    sorted_query = urlencode(sorted(query_params.items()), doseq=True)
    # Reconstruct URL without fragment and with normalized path/query
    normalized_url = urlunparse(parsed._replace(path=path, query=sorted_query, fragment=''))
    return normalized_url

async def create_or_update_user(user_data: types.User) -> User:
    # Creates a new user record or updates an existing one in the database.
    pool = await get_db_pool()
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            telegram_id = user_data.id
            username = user_data.username
            first_name = user_data.first_name
            last_name = user_data.last_name
            await cur.execute("SELECT * FROM users WHERE telegram_id = %s", (telegram_id,))
            user_record = await cur.fetchone()
            if user_record:
                await cur.execute(
                    """UPDATE users SET username = %s, first_name = %s, last_name = %s, last_active = CURRENT_TIMESTAMP WHERE telegram_id = %s RETURNING *;""",
                    (username, first_name, last_name, telegram_id)
                )
            else:
                await cur.execute(
                    """INSERT INTO users (telegram_id, username, first_name, last_name, created_at, last_active, ai_requests_today, ai_last_request_date) VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 0, CURRENT_DATE) RETURNING *;""",
                    (telegram_id, username, first_name, last_name)
                )
            return User(**await cur.fetchone())

async def get_user_by_telegram_id(telegram_id: int) -> Optional[User]:
    # Retrieves a user record from the database by their Telegram ID.
    pool = await get_db_pool()
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("SELECT * FROM users WHERE telegram_id = %s", (telegram_id,))
            user_record = await cur.fetchone()
            return User(**user_record) if user_record else None

async def update_user_premium_status(user_id: int, is_premium: bool):
    # Updates a user's premium status in the database.
    pool = await get_db_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("UPDATE users SET is_premium = %s WHERE id = %s;", (is_premium, user_id))
            await conn.commit()

async def update_user_digest_frequency(user_id: int, frequency: str):
    # Updates a user's digest frequency in the database.
    pool = await get_db_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("UPDATE users SET digest_frequency = %s WHERE id = %s;", (frequency, user_id))
            await conn.commit()

async def update_user_ai_request_count(user_id: int, count: int, last_request_date: datetime):
    # Updates a user's AI request count and last request date in the database.
    pool = await get_db_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("UPDATE users SET ai_requests_today = %s, ai_last_request_date = %s WHERE id = %s;", (count, last_request_date.date(), user_id))
            await conn.commit()

async def add_news_to_db(news_data: Dict[str, Any]) -> Optional[News]:
    # Adds a new news item to the database, or updates an existing source.
    pool = await get_db_pool()
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            normalized_source_url = normalize_url(str(news_data['source_url']))
            
            # Check if news with the same normalized source_url already exists
            await cur.execute("SELECT id FROM news WHERE normalized_source_url = %s", (normalized_source_url,))
            if await cur.fetchone():
                logger.info(f"News with URL {news_data['source_url']} (normalized: {normalized_source_url}) already exists. Skipping.")
                return None # News already exists

            # Find or create source
            # Use normalized_source_url for source lookup as well
            await cur.execute("SELECT id FROM sources WHERE normalized_source_url = %s", (normalized_source_url,))
            source_record = await cur.fetchone()
            source_id = None
            if source_record:
                source_id = source_record['id']
            else:
                user_id_for_source = news_data.get('user_id_for_source')
                parsed_url = HttpUrl(news_data['source_url'])
                source_name = parsed_url.host if parsed_url.host else 'Unknown Source'
                await cur.execute(
                    """INSERT INTO sources (user_id, source_name, source_url, normalized_source_url, source_type, added_at, last_parsed) VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) ON CONFLICT (normalized_source_url) DO UPDATE SET source_name = EXCLUDED.source_name, source_type = EXCLUDED.source_type, status = 'active', last_parsed = CURRENT_TIMESTAMP RETURNING id;""",
                    (user_id_for_source, source_name, str(news_data['source_url']), normalized_source_url, news_data.get('source_type', 'web'))
                )
                source_id = (await cur.fetchone())['id']

            # Changed logic: News from user-added sources are approved, others pending
            # If user_id_for_source is provided (meaning it's added by a user), it's approved.
            # Otherwise (from automatic parsing/YouTube generation), it's pending.
            moderation_status = 'approved' if news_data.get('user_id_for_source') is not None else 'pending'
            
            # Extract and classify topics using AI if not provided and it's a new news item
            ai_classified_topics = news_data.get('ai_classified_topics')
            if ai_classified_topics is None:
                try:
                    # Use Gemini to classify topics
                    topics_raw = await call_gemini_api(f"Класифікуй цю новину за 3-5 ключовими темами українською мовою, перелічи їх через кому: {news_data['title']}. {news_data['content']}", user_telegram_id=None) # No user_telegram_id for background task
                    if topics_raw:
                        ai_classified_topics = [t.strip().lower() for t in topics_raw.split(',') if t.strip()]
                    else:
                        ai_classified_topics = []
                except Exception as e:
                    logger.error(f"Failed to classify topics for news {news_data['title']}: {e}")
                    ai_classified_topics = [] # Default to empty list on failure

            await cur.execute(
                """INSERT INTO news (source_id, title, content, source_url, normalized_source_url, image_url, published_at, moderation_status, is_published_to_channel, ai_classified_topics) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING *;""",
                (source_id, news_data['title'], news_data['content'], str(news_data['source_url']), normalized_source_url, str(news_data.get('image_url')) if news_data.get('image_url') else None, news_data['published_at'], moderation_status, False, ai_classified_topics)
            )
            return News(**await cur.fetchone())

async def get_news_for_user(user_id: int, limit: int = 10, offset: int = 0, topics: Optional[List[str]] = None, start_datetime: Optional[datetime] = None) -> List[News]:
    # Retrieves news items for a specific user, filtering by viewed status, moderation, and topics.
    pool = await get_db_pool()
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            query = """
                SELECT n.* FROM news n
                LEFT JOIN user_news_views uv ON n.id = uv.news_id AND uv.user_id = %s
                WHERE uv.news_id IS NULL -- Only news not yet viewed by the user
                AND n.moderation_status = 'approved'
                AND (n.expires_at IS NULL OR n.expires_at > CURRENT_TIMESTAMP)
            """
            params = [user_id]
            
            if start_datetime:
                query += " AND n.published_at >= %s"
                params.append(start_datetime)
            
            if topics and len(topics) > 0: # Ensure topics list is not empty
                # Corrected operator for TEXT[] array overlap
                query += " AND n.ai_classified_topics && %s::text[]"
                params.append(topics) # Pass the list directly for TEXT[] comparison

            query += " ORDER BY n.published_at DESC LIMIT %s OFFSET %s;"
            params.extend([limit, offset])
            
            await cur.execute(query, tuple(params))
            return [News(**record) for record in await cur.fetchall()]

async def get_news_to_publish(limit: int = 1) -> List[News]:
    # Retrieves news items that are approved and not yet published to the channel.
    pool = await get_db_pool()
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("""SELECT * FROM news WHERE moderation_status = 'approved' AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP) AND is_published_to_channel = FALSE ORDER BY published_at ASC LIMIT %s;""", (limit,))
            return [News(**record) for record in await cur.fetchall()]

async def mark_news_as_published_to_channel(news_id: int):
    # Marks a news item as published to the channel.
    pool = await get_db_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""UPDATE news SET is_published_to_channel = TRUE WHERE id = %s;""", (news_id,))
            await conn.commit()
            logger.info(f"News {news_id} marked as published to channel.")

async def count_unseen_news(user_id: int) -> int:
    # Counts the number of unseen news items for a specific user.
    pool = await get_db_pool()
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("""SELECT COUNT(*) FROM news WHERE id NOT IN (SELECT news_id FROM user_news_views WHERE user_id = %s) AND moderation_status = 'approved' AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP);""", (user_id,))
            return (await cur.fetchone())['count']

async def mark_news_as_viewed(user_id: int, news_id: int):
    # Marks a news item as viewed by a user.
    pool = await get_db_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""INSERT INTO user_news_views (user_id, news_id, viewed_at) VALUES (%s, %s, CURRENT_TIMESTAMP) ON CONFLICT (user_id, news_id) DO NOTHING;""", (user_id, news_id))
            await conn.commit()

async def get_news_by_id(news_id: int) -> Optional[News]:
    # Retrieves a news item by its ID.
    pool = await get_db_pool()
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("SELECT * FROM news WHERE id = %s", (news_id,))
            news_record = await cur.fetchone()
            return News(**news_record) if news_record else None

async def get_source_by_id(source_id: int):
    # Retrieves a source by its ID.
    pool = await get_db_pool()
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("SELECT id, user_id, source_name, source_url, source_type, status, added_at FROM sources WHERE id = %s", (source_id,))
            return await cur.fetchone()

async def get_sources_by_user_id(user_id: int) -> List[Source]:
    # Retrieves all sources added by a specific user.
    pool = await get_db_pool()
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("SELECT id, user_id, source_name, source_url, normalized_source_url, source_type, status, added_at FROM sources WHERE user_id = %s ORDER BY added_at DESC;", (user_id,))
            return [Source(**record) for record in await cur.fetchall()]

async def delete_source_by_id(source_id: int, user_id: int) -> bool:
    # Deletes a source by its ID and user ID.
    pool = await get_db_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("DELETE FROM sources WHERE id = %s AND user_id = %s;", (source_id, user_id))
            await conn.commit()
            return cur.rowcount > 0

async def add_user_news_reaction(user_id: int, news_id: int, reaction_type: str):
    # Adds or updates a user's reaction to a news item.
    pool = await get_db_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """INSERT INTO user_news_reactions (user_id, news_id, reaction_type, created_at) VALUES (%s, %s, %s, CURRENT_TIMESTAMP) ON CONFLICT (user_id, news_id) DO UPDATE SET reaction_type = EXCLUDED.reaction_type, created_at = CURRENT_TIMESTAMP;""",
                (user_id, news_id, reaction_type)
            )
            await conn.commit()

async def get_user_subscriptions(user_id: int) -> List[str]:
    # Retrieves all topic subscriptions for a given user.
    pool = await get_db_pool()
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("SELECT topic FROM user_subscriptions WHERE user_id = %s;", (user_id,))
            return [row['topic'] for row in await cur.fetchall()]

async def add_user_subscription(user_id: int, topic: str):
    # Adds a new topic subscription for a user.
    pool = await get_db_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("INSERT INTO user_subscriptions (user_id, topic, subscribed_at) VALUES (%s, %s, CURRENT_TIMESTAMP) ON CONFLICT (user_id, topic) DO NOTHING;", (user_id, topic))
            await conn.commit()

async def remove_user_subscription(user_id: int, topic: str):
    # Removes a topic subscription for a user.
    pool = await get_db_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("DELETE FROM user_subscriptions WHERE user_id = %s AND topic = %s;", (user_id, topic))
            await conn.commit()

class AddSourceStates(StatesGroup):
    # States for adding a new source.
    waiting_for_url = State()

class NewsBrowse(StatesGroup):
    # States for browsing news.
    Browse_news = State()
    news_index = State()
    news_ids = State()
    last_message_id = State()

class AIAssistant(StatesGroup):
    # States for AI assistant functionalities.
    waiting_for_question = State()
    waiting_for_news_id_for_ai = State()
    waiting_for_term_to_explain = State()
    waiting_for_translate_language = State()
    waiting_for_free_question = State()
    waiting_for_youtube_url = State()
    waiting_for_expert_question = State()
    expert_type = State()
    waiting_for_price_analysis_input = State()
    waiting_for_filtered_channel_details = State()
    waiting_for_ai_media_name = State()

class ModerationStates(StatesGroup):
    # States for news moderation.
    browsing_pending_news = State()

class SubscriptionStates(StatesGroup):
    # States for managing user subscriptions.
    waiting_for_topics_to_add = State()
    waiting_for_topic_to_remove = State()

def get_main_menu_keyboard(user_lang: str) -> InlineKeyboardMarkup:
    # Generates the main menu keyboard.
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text=get_message(user_lang, 'help_btn'), callback_data="help_menu"), InlineKeyboardButton(text=get_message(user_lang, 'language_btn'), callback_data="language_menu"))
    builder.row(InlineKeyboardButton(text=get_message(user_lang, 'help_buy_btn'), callback_data="help_buy"), InlineKeyboardButton(text=get_message(user_lang, 'help_sell_btn'), callback_data="help_sell"))
    builder.row(InlineKeyboardButton(text=get_message(user_lang, 'my_news'), callback_data="my_news"), InlineKeyboardButton(text=get_message(user_lang, 'add_source'), callback_data="add_source"))
    builder.row(InlineKeyboardButton(text=get_message(user_lang, 'ask_expert'), callback_data="ask_expert"), InlineKeyboardButton(text=get_message(user_lang, 'ai_media_menu'), callback_data="ai_media_menu"))
    builder.row(InlineKeyboardButton(text=get_message(user_lang, 'invite_friends'), callback_data="invite_friends"), InlineKeyboardButton(text=get_message(user_lang, 'subscribe_menu'), callback_data="subscribe_menu"))
    builder.row(InlineKeyboardButton(text=get_message(user_lang, 'donate'), callback_data="donate"))
    return builder.as_markup()

def get_news_reactions_keyboard(news_id: int, user_lang: str) -> InlineKeyboardMarkup:
    # Generates the news reaction keyboard.
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text=get_message(user_lang, 'reaction_interesting'), callback_data=f"react_news_interesting_{news_id}"),
        InlineKeyboardButton(text=get_message(user_lang, 'reaction_not_much'), callback_data=f"react_news_not_much_{news_id}"),
        InlineKeyboardButton(text=get_message(user_lang, 'reaction_delete'), callback_data=f"react_news_delete_{news_id}")
    )
    return builder.as_markup()

def get_ai_news_functions_keyboard(news_id: int, user_lang: str, page: int = 0) -> InlineKeyboardMarkup:
    # Generates the AI news functions keyboard.
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text=get_message(user_lang, 'translate_btn'), callback_data=f"translate_select_lang_{news_id}"))
    builder.row(InlineKeyboardButton(text=get_message(user_lang, 'listen_news_btn'), callback_data=f"listen_news_{news_id}"))
    builder.row(InlineKeyboardButton(text=get_message(user_lang, 'extract_entities_btn'), callback_data=f"extract_entities_{news_id}"))
    builder.row(InlineKeyboardButton(text=get_message(user_lang, 'explain_term_btn'), callback_data=f"explain_term_{news_id}"))
    builder.row(InlineKeyboardButton(text=get_message(user_lang, 'fact_check_btn'), callback_data=f"fact_check_news_{news_id}"))
    builder.row(InlineKeyboardButton(text=get_message(user_lang, 'bookmark_add_btn'), callback_data=f"bookmark_news_add_{news_id}"), InlineKeyboardButton(text=get_message(user_lang, 'report_fake_news_btn'), callback_data=f"report_fake_news_{news_id}"))
    builder.row(InlineKeyboardButton(text=get_message(user_lang, 'main_menu_btn'), callback_data="main_menu"))
    return builder.as_markup()

def get_translate_language_keyboard(news_id: int, user_lang: str) -> InlineKeyboardMarkup:
    # Generates the language selection keyboard for translation.
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text=get_message(user_lang, 'english_lang'), callback_data=f"translate_to_en_{news_id}"), InlineKeyboardButton(text=get_message(user_lang, 'ukrainian_lang'), callback_data=f"translate_to_uk_{news_id}"))
    builder.row(InlineKeyboardButton(text=get_message(user_lang, 'polish_lang'), callback_data=f"translate_to_pl_{news_id}"), InlineKeyboardButton(text=get_message(user_lang, 'german_lang'), callback_data=f"translate_to_de_{news_id}"))
    builder.row(InlineKeyboardButton(text=get_message(user_lang, 'spanish_lang'), callback_data=f"translate_to_es_{news_id}"), InlineKeyboardButton(text=get_message(user_lang, 'french_lang'), callback_data=f"translate_to_fr_{news_id}"))
    builder.row(InlineKeyboardButton(text=get_message(user_lang, 'back_to_ai_btn'), callback_data=f"ai_news_functions_menu_{news_id}"))
    return builder.as_markup()

def get_expert_selection_keyboard(user_lang: str) -> InlineKeyboardMarkup:
    # Generates the expert selection keyboard.
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text=get_message(user_lang, 'expert_portnikov_btn'), callback_data="ask_expert_portnikov"))
    builder.row(InlineKeyboardButton(text=get_message(user_lang, 'expert_libsits_btn'), callback_data="ask_expert_libsits"))
    builder.row(InlineKeyboardButton(text=get_message(user_lang, 'main_menu_btn'), callback_data="main_menu"))
    return builder.as_markup()

def get_ai_media_menu_keyboard(user_lang: str) -> InlineKeyboardMarkup:
    # Generates the AI media menu keyboard.
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text=get_message(user_lang, 'price_analysis_prompt'), callback_data="price_analysis_menu"), InlineKeyboardButton(text=get_message(user_lang, 'ask_expert'), callback_data="ask_expert"))
    builder.row(InlineKeyboardButton(text=get_message(user_lang, 'youtube_to_news_btn'), callback_data="youtube_to_news"), InlineKeyboardButton(text=get_message(user_lang, 'create_filtered_channel_btn'), callback_data="create_filtered_channel"))
    builder.row(InlineKeyboardButton(text=get_message(user_lang, 'create_ai_media_btn'), callback_data="create_ai_media"))
    builder.row(InlineKeyboardButton(text=get_message(user_lang, 'main_menu_btn'), callback_data="main_menu")) # Back to main menu
    return builder.as_markup()

def get_analytics_menu_keyboard(user_lang: str) -> InlineKeyboardMarkup:
    # Generates the analytics menu keyboard.
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text=get_message(user_lang, 'infographics_btn'), callback_data="infographics"))
    builder.row(InlineKeyboardButton(text=get_message(user_lang, 'trust_index_btn'), callback_data="trust_index"))
    builder.row(InlineKeyboardButton(text=get_message(user_lang, 'long_term_connections_btn'), callback_data="long_term_connections"))
    builder.row(InlineKeyboardButton(text=get_message(user_lang, 'ai_prediction_btn'), callback_data="ai_prediction"))
    builder.row(InlineKeyboardButton(text=get_message(user_lang, 'back_to_ai_btn'), callback_data="ai_media_menu"))
    return builder.as_markup()

def get_price_analysis_keyboard(user_lang: str) -> InlineKeyboardMarkup:
    # Generates the price analysis keyboard.
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text=get_message(user_lang, 'price_analysis_prompt'), callback_data="init_price_analysis"))
    builder.row(InlineKeyboardButton(text=get_message(user_lang, 'help_sell_btn'), callback_data="help_sell"))
    builder.row(InlineKeyboardButton(text=get_message(user_lang, 'help_buy_btn'), callback_data="help_buy"))
    builder.row(InlineKeyboardButton(text=get_message(user_lang, 'back_to_ai_btn'), callback_data="ai_media_menu"))
    return builder.as_markup()

def get_subscription_menu_keyboard(user_lang: str) -> InlineKeyboardMarkup:
    # Generates the subscription menu keyboard.
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text=get_message(user_lang, 'add_subscription_btn'), callback_data="add_subscription"))
    builder.row(InlineKeyboardButton(text=get_message(user_lang, 'remove_subscription_btn'), callback_data="remove_subscription"))
    builder.row(InlineKeyboardButton(text=get_message(user_lang, 'main_menu_btn'), callback_data="main_menu"))
    return builder.as_markup()

@router.message(CommandStart())
async def command_start_handler(message: Message, state: FSMContext):
    # Handles the /start command, creates/updates user, and shows welcome message/onboarding.
    await state.clear()
    user = await create_or_update_user(message.from_user)
    user_lang = user.language if user else 'uk'

    if message.text and len(message.text.split()) > 1:
        invite_code = message.text.split()[1]
        await handle_invite_code(user.id, invite_code, user_lang, message.chat.id)
    
    if user and (datetime.now(timezone.utc) - user.created_at).total_seconds() < 60:
        onboarding_messages = [
            get_message(user_lang, 'welcome', first_name=message.from_user.first_name),
            get_message(user_lang, 'onboarding_step_1'),
            get_message(user_lang, 'onboarding_step_2'),
            get_message(user_lang, 'onboarding_step_3')
        ]
        for msg_text in onboarding_messages:
            await message.answer(msg_text)
    else:
        if user:
            last_active_dt = user.last_active.replace(tzinfo=timezone.utc) if user.last_active else datetime.now(timezone.utc)
            time_since_last_active = datetime.now(timezone.utc) - last_active_dt
            if time_since_last_active > timedelta(days=2):
                unseen_count = await count_unseen_news(user.id)
                if unseen_count > 0:
                    await message.answer(get_message(user_lang, 'what_new_digest_header', count=unseen_count))
                    # For digest, summarize recent news, not necessarily from start of day
                    news_for_digest = await get_news_for_user(user.id, limit=3)
                    digest_text = ""
                    for i, news_item in enumerate(news_for_digest):
                        # Use Gemini for a brief summary for the digest
                        summary = await call_gemini_api(f"Зроби коротке резюме новини українською мовою: {news_item.content}", user_telegram_id=message.from_user.id)
                        digest_text += get_message(user_lang, 'daily_digest_entry', idx=i+1, title=news_item.title, summary=summary, source_url=news_item.source_url)
                        await mark_news_as_viewed(user.id, news_item.id)
                    if digest_text:
                        await message.answer(digest_text + get_message(user_lang, 'what_new_digest_footer'), parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    await message.answer(get_message(user_lang, 'welcome', first_name=message.from_user.first_name), reply_markup=get_main_menu_keyboard(user_lang))

@router.message(Command("menu"))
async def command_menu_handler(message: Message, state: FSMContext):
    # Handles the /menu command, clearing current state and showing the main menu.
    await state.clear()
    user = await create_or_update_user(message.from_user)
    user_lang = user.language if user else 'uk'
    await message.answer(get_message(user_lang, 'main_menu_prompt'), reply_markup=get_main_menu_keyboard(user_lang))

@router.message(Command("cancel"))
async def command_cancel_handler(message: Message, state: FSMContext):
    # Handles the /cancel command, clearing current state and returning to the main menu.
    await state.clear()
    user = await get_user_by_telegram_id(message.from_user.id)
    user_lang = user.language if user else 'uk'
    await message.answer(get_message(user_lang, 'action_cancelled'), reply_markup=get_main_menu_keyboard(user_lang))

@router.callback_query(F.data == "main_menu")
async def callback_main_menu(callback: CallbackQuery, state: FSMContext):
    # Handles callback for returning to the main menu.
    await state.clear()
    user = await get_user_by_telegram_id(callback.from_user.id)
    user_lang = user.language if user else 'uk'
    await callback.message.edit_text(get_message(user_lang, 'main_menu_prompt'), reply_markup=get_main_menu_keyboard(user_lang))
    await callback.answer()

@router.callback_query(F.data == "help_menu")
async def callback_help_menu(callback: CallbackQuery, state: FSMContext):
    # Handles callback for displaying help information.
    user = await get_user_by_telegram_id(callback.from_user.id)
    user_lang = user.language if user else 'uk'
    await callback.message.edit_text(get_message(user_lang, 'help_text'), parse_mode=ParseMode.HTML, reply_markup=get_main_menu_keyboard(user_lang))
    await callback.answer()

@router.callback_query(F.data == "add_source")
async def callback_add_source(callback: CallbackQuery, state: FSMContext):
    # Handles callback for initiating the add source process.
    user = await get_user_by_telegram_id(callback.from_user.id)
    user_lang = user.language if user else 'uk'
    await callback.message.edit_text(get_message(user_lang, 'add_source_prompt'), reply_markup=InlineKeyboardBuilder().add(InlineKeyboardButton(text=get_message(user_lang, 'cancel_btn'), callback_data="cancel_action")).as_markup())
    await state.set_state(AddSourceStates.waiting_for_url)
    await callback.answer()

@router.message(AddSourceStates.waiting_for_url)
async def process_source_url(message: Message, state: FSMContext):
    # Processes the URL provided by the user for adding a new source.
    user = await get_user_by_telegram_id(message.from_user.id)
    user_lang = user.language if user else 'uk'
    source_url = message.text
    if not (source_url.startswith("http://") or source_url.startswith("https://")):
        await message.answer(get_message(user_lang, 'invalid_url'))
        return
    try:
        normalized_url = normalize_url(source_url)
        pool = await get_db_pool()
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                parsed_url = HttpUrl(source_url)
                source_name = parsed_url.host if parsed_url.host else get_message(user_lang, 'unknown_source')
                await cur.execute(
                    """INSERT INTO sources (user_id, source_name, source_url, normalized_source_url, source_type, added_at, last_parsed) VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) ON CONFLICT (normalized_source_url) DO UPDATE SET source_name = EXCLUDED.source_name, source_type = EXCLUDED.source_type, status = 'active', last_parsed = CURRENT_TIMESTAMP RETURNING id;""",
                    (user.id, source_name, source_url, normalized_url, 'web') # Default to 'web' type for user-added sources
                )
                await conn.commit()
        await message.answer(get_message(user_lang, 'source_added_success', source_url=source_url), reply_markup=get_main_menu_keyboard(user_lang))
    except Exception as e:
        logger.error(f"Error adding source '{source_url}': {e}", exc_info=True)
        await message.answer(get_message(user_lang, 'add_source_error'), reply_markup=get_main_menu_keyboard(user_lang))
    await state.clear()

@router.callback_query(F.data == "my_sources")
async def handle_my_sources_command(callback: CallbackQuery, state: FSMContext):
    # Handles the 'my_sources' callback, displaying a list of user's added sources.
    user = await get_user_by_telegram_id(callback.from_user.id)
    user_lang = user.language if user else 'uk'
    sources = await get_sources_by_user_id(user.id)
    
    if not sources:
        await callback.message.edit_text(get_message(user_lang, 'no_sources_added'), reply_markup=get_main_menu_keyboard(user_lang))
        await callback.answer()
        return

    response_text = get_message(user_lang, 'my_sources_header') + "\n\n"
    for idx, source in enumerate(sources):
        response_text += get_message(user_lang, 'source_item', idx=idx+1, source_name=source.source_name, source_url=source.source_url, status=source.status, source_id=source.id) + "\n"
    
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text=get_message(user_lang, 'main_menu_btn'), callback_data="main_menu"))
    
    await callback.message.edit_text(response_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True, reply_markup=builder.as_markup())
    await callback.answer()

@router.message(F.text.startswith("/source_delete_"))
async def handle_delete_source_command(message: Message):
    # Handles the command to delete a specific source.
    try:
        source_id = int(message.text.split('_')[-1])
        user = await get_user_by_telegram_id(message.from_user.id)
        user_lang = user.language if user else 'uk'
        
        if await delete_source_by_id(source_id, user.id):
            await message.answer(get_message(user_lang, 'source_deleted_success'), reply_markup=get_main_menu_keyboard(user_lang))
        else:
            await message.answer(get_message(user_lang, 'source_delete_error'), reply_markup=get_main_menu_keyboard(user_lang))
    except ValueError:
        user = await get_user_by_telegram_id(message.from_user.id)
        user_lang = user.language if user else 'uk'
        await message.answer(get_message(user_lang, 'source_delete_error'), reply_markup=get_main_menu_keyboard(user_lang))
    except Exception as e:
        logger.error(f"Error handling delete source command: {e}", exc_info=True)
        user = await get_user_by_telegram_id(message.from_user.id)
        user_lang = user.language if user else 'uk'
        await message.answer(get_message(user_lang, 'source_delete_error'), reply_markup=get_main_menu_keyboard(user_lang))

@router.callback_query(F.data == "my_news")
async def handle_my_news_command(callback: CallbackQuery, state: FSMContext):
    # Handles the 'my_news' callback, fetching and displaying news for the user.
    user = await get_user_by_telegram_id(callback.from_user.id)
    user_lang = user.language if user else 'uk'
    user_subscriptions = await get_user_subscriptions(user.id)

    # Get news from the beginning of the current day
    start_of_today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    all_news_ids = [n.id for n in await get_news_for_user(user.id, limit=100, offset=0, topics=user_subscriptions if user_subscriptions else None, start_datetime=start_of_today)]

    if not all_news_ids:
        await callback.message.edit_text(get_message(user_lang, 'no_new_news'), reply_markup=get_main_menu_keyboard(user_lang))
        await callback.answer()
        return
    
    current_state_data = await state.get_data()
    last_message_id = current_state_data.get('last_message_id')
    if last_message_id:
        try: await bot.delete_message(chat_id=callback.message.chat.id, message_id=last_message_id)
        except Exception as e: logger.warning(f"Failed to delete previous message {last_message_id}: {e}")
    
    await state.update_data(current_news_index=0, news_ids=all_news_ids)
    await state.set_state(NewsBrowse.Browse_news)
    
    await send_news_to_user(callback.message.chat.id, all_news_ids[0], 0, len(all_news_ids), state)
    await callback.answer()

@router.callback_query(NewsBrowse.Browse_news, F.data == "next_news")
async def handle_next_news_command(callback: CallbackQuery, state: FSMContext):
    # Handles the 'next_news' callback, displaying the next news item.
    user = await get_user_by_telegram_id(callback.from_user.id)
    user_lang = user.language if user else 'uk'
    user_data = await state.get_data()
    news_ids = user_data.get("news_ids", [])
    current_index = user_data.get("current_news_index", 0)
    last_message_id = user_data.get('last_message_id')

    if not news_ids or current_index + 1 >= len(news_ids):
        await callback.message.edit_text(get_message(user_lang, 'no_more_news'), reply_markup=get_main_menu_keyboard(user_lang))
        await callback.answer()
        return
    
    next_index = current_index + 1
    await state.update_data(current_news_index=next_index)
    
    if last_message_id:
        try: await bot.delete_message(chat_id=callback.message.chat.id, message_id=last_message_id)
        except Exception as e: logger.warning(f"Failed to delete previous message {last_message_id}: {e}")
    
    await send_news_to_user(callback.message.chat.id, news_ids[next_index], next_index, len(news_ids), state)
    await callback.answer()

@router.callback_query(NewsBrowse.Browse_news, F.data == "prev_news")
async def handle_prev_news_command(callback: CallbackQuery, state: FSMContext):
    # Handles the 'prev_news' callback, displaying the previous news item.
    user = await get_user_by_telegram_id(callback.from_user.id)
    user_lang = user.language if user else 'uk'
    user_data = await state.get_data()
    news_ids = user_data.get("news_ids", [])
    current_index = user_data.get("current_news_index", 0)
    last_message_id = user_data.get('last_message_id')

    if not news_ids or current_index <= 0:
        await callback.message.edit_text(get_message(user_lang, 'first_news'), reply_markup=get_main_menu_keyboard(user_lang))
        await callback.answer()
        return
    
    prev_index = current_index - 1
    await state.update_data(current_news_index=prev_index)
    
    if last_message_id:
        try: await bot.delete_message(chat_id=callback.message.chat.id, message_id=last_message_id)
        except Exception as e: logger.warning(f"Failed to delete previous message {last_message_id}: {e}")
    
    await send_news_to_user(callback.message.chat.id, news_ids[prev_index], prev_index, len(news_ids), state)
    await callback.answer()

async def send_news_to_user(chat_id: int, news_id: int, current_index: int, total_news: int, state: FSMContext):
    # Sends a news item to the user's chat.
    news_item = await get_news_by_id(news_id)
    user = await get_user_by_telegram_id(chat_id)
    user_lang = user.language if user else 'uk'

    if not news_item:
        await bot.send_message(chat_id, get_message(user_lang, 'news_not_found'))
        return
    
    source_info = await get_source_by_id(news_item.source_id)
    source_name = source_info['source_name'] if source_info else get_message(user_lang, 'unknown_source')

    text = (
        f"<b>{get_message(user_lang, 'news_title_label')}</b> {news_item.title}\n\n"
        f"<b>{get_message(user_lang, 'news_content_label')}</b>\n{news_item.content}\n\n"
        f"{get_message(user_lang, 'published_at_label')} {news_item.published_at.strftime('%d.%m.%Y %H:%M')}\n"
        f"{get_message(user_lang, 'news_progress', current_index=current_index + 1, total_news=total_news)}\n\n"
    )

    keyboard_builder = InlineKeyboardBuilder()
    keyboard_builder.row(InlineKeyboardButton(text=get_message(user_lang, 'read_source_btn'), url=str(news_item.source_url)))
    keyboard_builder.row(InlineKeyboardButton(text=get_message(user_lang, 'ai_functions_btn'), callback_data=f"ai_news_functions_menu_{news_item.id}"))
    # Use builder.row() instead of builder.row_width() for consistent button layout
    keyboard_builder.row(*get_news_reactions_keyboard(news_item.id, user_lang).inline_keyboard[0])
    
    nav_buttons = []
    if current_index > 0:
        nav_buttons.append(InlineKeyboardButton(text=get_message(user_lang, 'prev_btn'), callback_data="prev_news"))
    if current_index < total_news - 1:
        nav_buttons.append(InlineKeyboardButton(text=get_message(user_lang, 'next_btn'), callback_data="next_news"))
    
    if nav_buttons:
        keyboard_builder.row(*nav_buttons)
    
    keyboard_builder.row(InlineKeyboardButton(text=get_message(user_lang, 'main_menu_btn'), callback_data="main_menu"))

    msg = None
    if news_item.image_url:
        try:
            msg = await bot.send_photo(chat_id=chat_id, photo=str(news_item.image_url), caption=text, reply_markup=keyboard_builder.as_markup(), parse_mode=ParseMode.HTML)
        except Exception as e:
            logger.warning(f"Failed to send photo for news {news_id} from URL {news_item.image_url}: {e}. Sending with placeholder.")
            placeholder_image_url = "https://placehold.co/600x400/CCCCCC/000000?text=No+Image"
            msg = await bot.send_photo(chat_id=chat_id, photo=placeholder_image_url, caption=text + f"\n(Original image URL: {news_item.image_url})", reply_markup=keyboard_builder.as_markup(), parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    else:
        placeholder_image_url = "https://placehold.co/600x400/CCCCCC/000000?text=No+Image"
        msg = await bot.send_photo(chat_id=chat_id, photo=placeholder_image_url, caption=text, reply_markup=keyboard_builder.as_markup(), parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    
    if msg:
        await state.update_data(last_message_id=msg.message_id)
    
    if user:
        await mark_news_as_viewed(user.id, news_item.id)


async def call_gemini_api(prompt: str, user_telegram_id: Optional[int] = None, chat_history: Optional[List[Dict]] = None, image_data: Optional[str] = None) -> Optional[str]:
    # Calls the Gemini API to generate text or analyze images.
    # Includes rate limiting for non-premium users.
    if not GEMINI_API_KEY:
        return "AI is not available. Please configure GEMINI_API_KEY."

    if user_telegram_id:
        user = await get_user_by_telegram_id(user_telegram_id)
        if user and not user.is_premium and not user.is_pro:
            today = datetime.now(timezone.utc).date()
            if user.ai_last_request_date and user.ai_last_request_date.date() != today:
                await update_user_ai_request_count(user.id, 0, datetime.now(timezone.utc))
                user.ai_requests_today = 0
            
            if user.ai_requests_today >= AI_REQUEST_LIMIT_DAILY_FREE:
                return get_message(user.language if user else 'uk', 'ai_rate_limit_exceeded', count=user.ai_requests_today, limit=AI_REQUEST_LIMIT_DAILY_FREE)
            
            await update_user_ai_request_count(user.id, user.ai_requests_today + 1, datetime.now(timezone.utc))

    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={GEMINI_API_KEY}"
    headers = {"Content-Type": "application/json"}
    
    contents = []
    if chat_history:
        for entry in chat_history:
            contents.append({"role": entry["role"], "parts": [{"text": entry["text"]}]})
    
    parts = [{"text": prompt}]
    if image_data:
        parts.append({"inlineData": {"mimeType": "image/jpeg", "data": image_data}})
    
    contents.append({"role": "user", "parts": parts})

    payload = {"contents": contents, "generationConfig": {"temperature": 0.7, "topK": 40, "topP": 0.95, "maxOutputTokens": 1000}}
    
    try:
        async with ClientSession() as session:
            async with session.post(url, headers=headers, json=payload) as response:
                if response.status == 429:
                    logger.warning("Gemini API rate limit exceeded.")
                    return "Too many AI requests. Please try again later."
                response.raise_for_status()
                data = await response.json()
                if data and data.get("candidates"):
                    return data["candidates"][0]["content"]["parts"][0]["text"]
                logger.error(f"Gemini API response missing candidates: {data}")
                return "Failed to get AI response."
    except Exception as e:
        logger.error(f"Error calling Gemini API: {e}", exc_info=True)
        return "An error occurred with AI. Please try again later."

async def check_premium_access(user_telegram_id: int) -> bool:
    # Checks if a user has premium or pro access.
    user = await get_user_by_telegram_id(user_telegram_id)
    return user and (user.is_premium or user.is_pro)

@router.callback_query(F.data.startswith("ai_news_functions_menu_"))
async def handle_ai_news_functions_menu(callback: CallbackQuery):
    # Displays the AI news functions menu for a specific news item.
    parts = callback.data.split('_')
    news_id = int(parts[-1])
    page = int(parts[-2]) if len(parts) > 4 else 0 # This page parameter is now mostly vestigial
    
    user = await get_user_by_telegram_id(callback.from_user.id)
    user_lang = user.language if user else 'uk'
    
    await callback.message.edit_text(get_message(user_lang, 'ai_functions_prompt'), reply_markup=get_ai_news_functions_keyboard(news_id, user_lang, page))
    await callback.answer()

@router.callback_query(F.data.startswith("translate_select_lang_"))
async def handle_translate_select_language(callback: CallbackQuery, state: FSMContext):
    # Handles the selection of a language for news translation.
    news_id = int(callback.data.split('_')[3])
    user = await get_user_by_telegram_id(callback.from_user.id)
    user_lang = user.language if user else 'uk'
    await state.update_data(news_id_for_translate=news_id)
    await callback.message.edit_text(get_message(user_lang, 'select_translate_language'), reply_markup=get_translate_language_keyboard(news_id, user_lang))
    await state.set_state(AIAssistant.waiting_for_translate_language)
    await callback.answer()

@router.callback_query(AIAssistant.waiting_for_translate_language, F.data.startswith("translate_to_"))
async def handle_translate_to_language(callback: CallbackQuery, state: FSMContext):
    # Handles the translation of a news item to the selected language.
    parts = callback.data.split('_')
    lang_code = parts[2]
    news_id = int(parts[3])
    user = await get_user_by_telegram_id(callback.from_user.id)
    user_lang = user.language if user else 'uk'
    language_names = {"en": get_message(user_lang, 'english_lang'), "pl": get_message(user_lang, 'polish_lang'), "de": get_message(user_lang, 'german_lang'), "es": get_message(user_lang, 'spanish_lang'), "fr": get_message(user_lang, 'french_lang'), "uk": get_message(user_lang, 'ukrainian_lang')}
    language_name = language_names.get(lang_code, "selected language")
    news_item = await get_news_by_id(news_id)
    
    if not news_item:
        await callback.answer(get_message(user_lang, 'news_not_found'), show_alert=True)
        await state.clear()
        return
    
    await callback.message.edit_text(get_message(user_lang, 'translating_news', language_name=language_name))
    translation = await call_gemini_api(f"Переклади цю новину на {language_name} мовою: {news_item.content}", user_telegram_id=callback.from_user.id)
    
    await callback.message.edit_text(get_message(user_lang, 'translation_label', language_name=language_name, translation=translation), reply_markup=get_ai_news_functions_keyboard(news_id, user_lang))
    await state.clear()
    await callback.answer()

@router.callback_query(F.data.startswith("listen_news_"))
async def handle_listen_news(callback: CallbackQuery):
    # Generates and sends an audio version of a news item.
    news_id = int(callback.data.split('_')[2])
    news_item = await get_news_by_id(news_id)
    user = await get_user_by_telegram_id(callback.from_user.id)
    user_lang = user.language if user else 'uk'
    
    if not news_item:
        await callback.answer(get_message(user_lang, 'news_not_found'), show_alert=True)
        return
    
    await callback.message.edit_text(get_message(user_lang, 'generating_audio'))
    try:
        text_to_speak = f"{news_item.title}. {news_item.content}"
        tts = gTTS(text=text_to_speak, lang=user_lang) 
        audio_buffer = io.BytesIO()
        await asyncio.to_thread(tts.write_to_fp, audio_buffer)
        audio_buffer.seek(0)
        
        await bot.send_voice(chat_id=callback.message.chat.id, voice=BufferedInputFile(audio_buffer.getvalue(), filename=f"news_{news_id}.mp3"), caption=get_message(user_lang, 'audio_news_caption', title=news_item.title), reply_markup=get_ai_news_functions_keyboard(news_id, user_lang))
        await callback.message.delete()
    except Exception as e:
        logger.error(f"Error generating or sending audio for news {news_id}: {e}", exc_info=True)
        await callback.message.edit_text(get_message(user_lang, 'audio_error'), reply_markup=get_ai_news_functions_keyboard(news_id, user_lang))
    await callback.answer()

@router.callback_query(F.data.startswith("extract_entities_"))
async def handle_extract_entities(callback: CallbackQuery):
    # Extracts key entities from a news item using AI.
    # Requires premium access.
    news_id = int(callback.data.split('_')[2])
    news_item = await get_news_by_id(news_id)
    user = await get_user_by_telegram_id(callback.from_user.id)
    user_lang = user.language if user else 'uk'
    
    if not news_item:
        await callback.answer(get_message(user_lang, 'news_not_found'), show_alert=True)
        return
    if not await check_premium_access(callback.from_user.id):
        await callback.answer(get_message(user_lang, 'ai_function_premium_only'), show_alert=True)
        return
    
    await callback.message.edit_text(get_message(user_lang, 'extracting_entities'))
    entities = await call_gemini_api(f"Витягни ключові сутності з новини українською. Перелічи їх через кому: {news_item.content}", user_telegram_id=callback.from_user.id)
    
    await callback.message.edit_text(get_message(user_lang, 'entities_label') + f"\n{entities}", reply_markup=get_ai_news_functions_keyboard(news_id, user_lang))
    await callback.answer()

@router.callback_query(F.data.startswith("explain_term_"))
async def handle_explain_term(callback: CallbackQuery, state: FSMContext):
    # Prompts the user to provide a term to explain within the context of a news item.
    # Requires premium access.
    news_id = int(callback.data.split('_')[2])
    news_item = await get_news_by_id(news_id)
    user = await get_user_by_telegram_id(callback.from_user.id)
    user_lang = user.language if user else 'uk'
    
    if not news_item:
        await callback.answer(get_message(user_lang, 'news_not_found'), show_alert=True)
        return
    if not await check_premium_access(callback.from_user.id):
        await callback.answer(get_message(user_lang, 'ai_function_premium_only'), show_alert=True)
        return
    
    await state.update_data(waiting_for_news_id_for_ai=news_id)
    await callback.message.edit_text(get_message(user_lang, 'explain_term_prompt'), reply_markup=InlineKeyboardBuilder().add(InlineKeyboardButton(text=get_message(user_lang, 'cancel_btn'), callback_data="cancel_action")).as_markup())
    await state.set_state(AIAssistant.waiting_for_term_to_explain)
    await callback.answer()

@router.message(AIAssistant.waiting_for_term_to_explain)
async def process_term_explanation(message: Message, state: FSMContext):
    # Processes the term provided by the user and generates an explanation using AI.
    term = message.text
    user_data = await state.get_data()
    news_id = user_data.get("waiting_for_news_id_for_ai")
    user = await get_user_by_telegram_id(message.from_user.id)
    user_lang = user.language if user else 'uk'
    
    if not news_id:
        await message.answer(get_message(user_lang, 'ai_news_not_found'), reply_markup=get_main_menu_keyboard(user_lang))
        await state.clear()
        return
    
    news_item = await get_news_by_id(news_id)
    if not news_item:
        await message.answer(get_message(user_lang, 'news_not_found'), reply_markup=get_main_menu_keyboard(user_lang))
        await state.clear()
        return
    
    await message.answer(get_message(user_lang, 'explaining_term'))
    explanation = await call_gemini_api(f"Поясни термін '{term}' у контексті новини українською: {news_item.content}", user_telegram_id=message.from_user.id)
    
    await message.answer(get_message(user_lang, 'term_explanation_label', term=term) + f"\n{explanation}", reply_markup=get_ai_news_functions_keyboard(news_id, user_lang))
    await state.clear()

@router.callback_query(F.data.startswith("fact_check_news_"))
async def handle_fact_check_news(callback: CallbackQuery):
    # Performs a fact-check on a news item using AI.
    # Requires premium access.
    news_id = int(callback.data.split('_')[3])
    news_item = await get_news_by_id(news_id)
    user = await get_user_by_telegram_id(callback.from_user.id)
    user_lang = user.language if user else 'uk'
    
    if not news_item:
        await callback.answer(get_message(user_lang, 'news_not_found'), show_alert=True)
        return
    if not await check_premium_access(callback.from_user.id):
        await callback.answer(get_message(user_lang, 'ai_function_premium_only'), show_alert=True)
        return
    
    await callback.message.edit_text(get_message(user_lang, 'checking_facts'))
    fact_check = await call_gemini_api(f"Виконай перевірку фактів для новини українською. Вкажи неточності або маніпуляції. Якщо є, наведи джерела: {news_item.content}", user_telegram_id=callback.from_user.id)
    
    await callback.message.edit_text(get_message(user_lang, 'fact_check_label') + f"\n{fact_check}", reply_markup=get_ai_news_functions_keyboard(news_id, user_lang))
    await callback.answer()

@router.callback_query(F.data.startswith("bookmark_news_"))
async def handle_bookmark_news(callback: CallbackQuery, state: FSMContext):
    # Handles adding or removing a news item from user bookmarks.
    parts = callback.data.split('_')
    action = parts[2]
    news_id = int(parts[3])
    user = await get_user_by_telegram_id(callback.from_user.id)
    user_lang = user.language if user else 'uk'
    
    if not user:
        await callback.answer(get_message(user_lang, 'user_not_identified'), show_alert=True)
        return
    
    pool = await get_db_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            if action == 'add':
                try:
                    await cur.execute("""INSERT INTO bookmarks (user_id, news_id, bookmarked_at) VALUES (%s, %s, CURRENT_TIMESTAMP) ON CONFLICT (user_id, news_id) DO NOTHING;""", (user.id, news_id))
                    if cur.rowcount > 0:
                        await callback.answer(get_message(user_lang, 'bookmark_added'), show_alert=True)
                    else:
                        await callback.answer(get_message(user_lang, 'bookmark_already_exists'), show_alert=True)
                except Exception as e:
                    logger.error(f"Error adding bookmark for user {user.id}, news {news_id}: {e}", exc_info=True)
                    await callback.answer(get_message(user_lang, 'bookmark_add_error'), show_alert=True)
            elif action == 'remove':
                try:
                    await cur.execute("DELETE FROM bookmarks WHERE user_id = %s AND news_id = %s;", (user.id, news_id))
                    if cur.rowcount > 0:
                        await callback.answer(get_message(user_lang, 'bookmark_removed'), show_alert=True)
                    else:
                        await callback.answer(get_message(user_lang, 'bookmark_not_found'), show_alert=True)
                except Exception as e:
                    logger.error(f"Error removing bookmark for user {user.id}, news {news_id}: {e}", exc_info=True)
                    await callback.answer(get_message(user_lang, 'bookmark_remove_error'), show_alert=True)
            await conn.commit()
    
    current_state_data = await state.get_data()
    last_message_id = current_state_data.get('last_message_id')
    if last_message_id:
        try:
            await bot.edit_message_reply_markup(chat_id=callback.message.chat.id, message_id=last_message_id, reply_markup=get_ai_news_functions_keyboard(news_id, user_lang))
        except Exception as e:
            logger.warning(f"Failed to edit message reply markup {last_message_id} after bookmark action: {e}")
    else:
        await callback.message.edit_text(get_message(user_lang, 'action_done'), reply_markup=get_main_menu_keyboard(user_lang))
    await callback.answer()

@router.callback_query(F.data.startswith("report_fake_news_"))
async def handle_report_fake_news(callback: CallbackQuery):
    # Handles reporting a news item as fake news.
    news_id = int(callback.data.split('_')[3])
    user = await get_user_by_telegram_id(callback.from_user.id)
    user_lang = user.language if user else 'uk'
    
    if not user:
        await callback.answer(get_message(user_lang, 'user_not_identified'), show_alert=True)
        return
    
    pool = await get_db_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT 1 FROM reports WHERE user_id = %s AND target_type = 'news' AND target_id = %s;", (user.id, news_id))
            if await cur.fetchone():
                await callback.answer(get_message(user_lang, 'report_already_sent'), show_alert=True)
                return
            
            await cur.execute("""INSERT INTO reports (user_id, target_type, target_id, reason, created_at, status) VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP, 'pending');""", (user.id, 'news', news_id, 'Fake news report'))
            await conn.commit()
    
    await callback.answer(get_message(user_lang, 'report_sent_success'), show_alert=True)
    await callback.message.edit_text(get_message(user_lang, 'report_action_done'), reply_markup=get_ai_news_functions_keyboard(news_id, user_lang))

async def send_news_to_channel(news_item: News):
    # Sends a news item to the configured Telegram channel.
    # Summarizes content if too long.
    if not NEWS_CHANNEL_LINK:
        logger.warning("NEWS_CHANNEL_LINK is not configured. Skipping channel post.")
        return
    
    channel_identifier = NEWS_CHANNEL_LINK
    
    display_content = news_item.content
    if len(display_content) > 250:
        summary_prompt = f"Скороти цей текст до 250 символів, зберігаючи суть, українською мовою: {display_content}"
        ai_summary = await call_gemini_api(summary_prompt)
        if ai_summary:
            display_content = ai_summary
            if len(display_content) > 247:
                display_content = display_content[:247] + "..."
        else:
            display_content = display_content[:247] + "..."

    text = (
        f"<b>Нова новина:</b> {news_item.title}\n\n"
        f"{display_content}\n\n"
        f"🔗 <a href='{news_item.source_url}'>Читати повністю</a>\n"
        f"Опубліковано: {news_item.published_at.strftime('%d.%m.%Y %H:%M')}"
    )
    
    try:
        if news_item.image_url:
            try:
                # Attempt to send photo. If it fails, log and send as text.
                await bot.send_photo(chat_id=channel_identifier, photo=str(news_item.image_url), caption=text, parse_mode=ParseMode.HTML)
            except Exception as photo_e:
                logger.error(f"Failed to send photo for news {news_item.id} to channel {channel_identifier} from URL {news_item.image_url}: {photo_e}. Sending message without photo.", exc_info=True)
                await bot.send_message(chat_id=channel_identifier, text=text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
        else:
            await bot.send_message(chat_id=channel_identifier, text=text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
        
        logger.info(get_message('uk', 'news_published_success', title=news_item.title, identifier=channel_identifier))
        await mark_news_as_published_to_channel(news_item.id)
    except Exception as e:
        logger.error(get_message('uk', 'news_publish_error', title=news_item.title, identifier=channel_identifier, error=e), exc_info=True)

async def delete_expired_news_task():
    # Deletes news items that have passed their expiration date.
    pool = await get_db_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("DELETE FROM news WHERE expires_at IS NOT NULL AND expires_at < CURRENT_TIMESTAMP;")
            deleted_count = cur.rowcount
            await conn.commit()
            if deleted_count > 0:
                logger.info(get_message('uk', 'deleted_expired_news', count=deleted_count))
            else:
                logger.info(get_message('uk', 'no_expired_news'))

async def send_daily_digest():
    # Sends a daily news digest to users who have auto-notifications enabled.
    logger.info("Running send_daily_digest task.")
    pool = await get_db_pool()
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("SELECT id, telegram_id, language FROM users WHERE auto_notifications = TRUE AND digest_frequency = 'daily';")
            users_for_digest = await cur.fetchall()
    
    if not users_for_digest:
        logger.info(get_message('uk', 'daily_digest_no_users'))
        return
    
    for user_data in users_for_digest:
        user_db_id = user_data['id']
        user_telegram_id = user_data['telegram_id']
        user_lang = user_data['language']
        
        news_items = await get_news_for_user(user_db_id, limit=5)
        if not news_items:
            logger.info(get_message('uk', 'daily_digest_no_news', user_id=user_telegram_id))
            continue
        
        digest_text = get_message(user_lang, 'daily_digest_header') + "\n\n"
        for i, news_item in enumerate(news_items):
            summary = await call_gemini_api(f"Зроби коротке резюме новини українською мовою: {news_item.content}", user_telegram_id=user_telegram_id)
            digest_text += get_message(user_lang, 'daily_digest_entry', idx=i+1, title=news_item.title, summary=summary, source_url=news_item.source_url)
            await mark_news_as_viewed(user_db_id, news_item.id)
        
        try:
            await bot.send_message(chat_id=user_telegram_id, text=digest_text, reply_markup=get_main_menu_keyboard(user_lang), parse_mode=ParseMode.HTML, disable_web_page_preview=True)
            logger.info(get_message('uk', 'daily_digest_sent_success', user_id=user_telegram_id))
        except Exception as e:
            logger.error(get_message('uk', 'daily_digest_send_error', user_id=user_telegram_id, error=e), exc_info=True)

async def generate_invite_code() -> str:
    # Generates a random 8-character invite code.
    return ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=8))

async def create_invite(inviter_user_db_id: int) -> Optional[str]:
    # Creates a new invite code for a user.
    pool = await get_db_pool()
    async with pool.connection() as conn:
        # Explicitly set row_factory for this cursor to ensure dictionary return
        async with conn.cursor(row_factory=dict_row) as cur:
            invite_code = await generate_invite_code()
            try:
                await cur.execute("""INSERT INTO invitations (inviter_user_id, invite_code, created_at, status) VALUES (%s, %s, CURRENT_TIMESTAMP, 'pending') RETURNING invite_code;""", (inviter_user_db_id, invite_code))
                await conn.commit()
                return invite_code
            except Exception as e:
                logger.error(f"Error creating invite for user {inviter_user_db_id}: {e}", exc_info=True)
                return None

async def handle_invite_code(new_user_db_id: int, invite_code: str, user_lang: str, chat_id: int):
    # Handles the processing of an invite code when a new user starts the bot.
    # Grants premium/digest benefits to the inviter if criteria are met.
    pool = await get_db_pool()
    async with pool.connection() as conn:
        # Explicitly set row_factory for this cursor to ensure dictionary return
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("""SELECT id, inviter_user_id FROM invitations WHERE invite_code = %s AND status = 'pending' AND used_at IS NULL;""", (invite_code,))
            invite_record = await cur.fetchone()
            if invite_record:
                invite_id = invite_record['id']
                inviter_user_db_id = invite_record['inviter_user_id']
                await cur.execute("UPDATE invitations SET used_at = CURRENT_TIMESTAMP, status = 'accepted', invitee_telegram_id = %s WHERE id = %s;", (new_user_db_id, invite_id))
                
                await cur.execute("UPDATE users SET premium_invite_count = premium_invite_count + 1, digest_invite_count = digest_invite_count + 1 WHERE id = %s RETURNING premium_invite_count, digest_invite_count;", (inviter_user_db_id,))
                inviter_updated_counts = await cur.fetchone()

                if inviter_updated_counts:
                    if inviter_updated_counts['premium_invite_count'] >= 5:
                        await update_user_premium_status(inviter_user_db_id, True)
                        inviter_telegram_user = await get_user_by_telegram_id(inviter_user_db_id)
                        if inviter_telegram_user:
                            await bot.send_message(chat_id=inviter_telegram_user.telegram_id, text=get_message(user_lang, 'premium_granted'))
                    
                    if inviter_updated_counts['digest_invite_count'] >= 10:
                        await update_user_digest_frequency(inviter_user_db_id, 'daily')
                        inviter_telegram_user = await get_user_by_telegram_id(inviter_user_db_id)
                        if inviter_telegram_user:
                            await bot.send_message(chat_id=inviter_telegram_user.telegram_id, text=get_message(user_lang, 'digest_granted'))
                
                await conn.commit()
            else:
                logger.info(f"Invite code {invite_code} not found or already used.")

@router.callback_query(F.data == "invite_friends")
async def command_invite_handler(callback: CallbackQuery):
    # Handles the 'invite_friends' callback, generating and displaying an invite code.
    user = await get_user_by_telegram_id(callback.from_user.id)
    user_lang = user.language if user else 'uk'
    if not user:
        user = await create_or_update_user(callback.from_user)
    
    invite_code = await create_invite(user.id)
    if invite_code:
        # Await bot.get_me() to get the bot's username
        bot_info = await bot.get_me()
        invite_link = f"https://t.me/{bot_info.username}?start={invite_code}"
        await callback.message.edit_text(get_message(user_lang, 'your_invite_code', invite_code=invite_code, invite_link=hlink(get_message(user_lang, 'invite_link_label'), invite_link)), parse_mode=ParseMode.HTML, disable_web_page_preview=False, reply_markup=get_main_menu_keyboard(user_lang))
    else:
        await callback.message.edit_text(get_message(user_lang, 'invite_error'), reply_markup=get_main_menu_keyboard(user_lang))
    await callback.answer()

async def update_source_stats_publication_count(source_id: int):
    # Updates the publication count for a given source in source_stats.
    pool = await get_db_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""INSERT INTO source_stats (source_id, publication_count, last_updated) VALUES (%s, 1, CURRENT_TIMESTAMP) ON CONFLICT (source_id) DO UPDATE SET publication_count = source_stats.publication_count + 1, last_updated = CURRENT_TIMESTAMP;""", (source_id,))
            await conn.commit()

@router.callback_query(F.data == "ask_expert")
async def handle_ask_expert(callback: CallbackQuery):
    # Handles the 'ask_expert' callback, displaying expert selection options.
    user = await get_user_by_telegram_id(callback.from_user.id)
    user_lang = user.language if user else 'uk'
    await callback.message.edit_text(get_message(user_lang, 'ask_expert_prompt'), reply_markup=get_expert_selection_keyboard(user_lang))
    await callback.answer()

@router.callback_query(F.data.startswith("ask_expert_"))
async def handle_expert_selection(callback: CallbackQuery, state: FSMContext):
    # Handles the selection of an expert and prompts for a question.
    expert_type = callback.data.replace("ask_expert_", "")
    user = await get_user_by_telegram_id(callback.from_user.id)
    user_lang = user.language if user else 'uk'
    expert_name = "Віталій Портников" if expert_type == "portnikov" else "Ігор Лібсіц"
    await state.update_data(expert_type=expert_type)
    await callback.message.edit_text(get_message(user_lang, 'ask_expert_question_prompt', expert_name=expert_name), reply_markup=InlineKeyboardBuilder().add(InlineKeyboardButton(text=get_message(user_lang, 'cancel_btn'), callback_data="cancel_action")).as_markup())
    await state.set_state(AIAssistant.waiting_for_expert_question)
    await callback.answer()

@router.message(AIAssistant.waiting_for_expert_question)
async def process_expert_question(message: Message, state: FSMContext):
    # Processes the user's question and generates an AI response from the selected expert.
    user_question = message.text
    user_data = await state.get_data()
    expert_type = user_data.get("expert_type")
    user = await get_user_by_telegram_id(message.from_user.id)
    user_lang = user.language if user else 'uk'
    expert_name = "Віталій Портников" if expert_type == "portnikov" else "Ігор Лібсіц"
    
    prompt = ""
    if expert_type == "portnikov":
        prompt = f"Відповідай як відомий український журналіст Віталій Портников, аналізуючи політичні та соціальні події: {user_question}"
    elif expert_type == "libsits":
        prompt = f"Відповідай як відомий український економіст Ігор Лібсіц, аналізуючи економічні тенденції та їх наслідки: {user_question}"
    
    await message.answer(get_message(user_lang, 'processing_question'))
    ai_response = await call_gemini_api(prompt, user_telegram_id=message.from_user.id)
    
    await message.answer(get_message(user_lang, 'expert_response_label', expert_name=expert_name) + f"\n{ai_response}", reply_markup=get_main_menu_keyboard(user_lang))
    await state.clear()

@router.callback_query(F.data == "price_analysis_menu")
async def handle_price_analysis_menu(callback: CallbackQuery):
    # Displays the price analysis menu.
    user = await get_user_by_telegram_id(callback.from_user.id)
    user_lang = user.language if user else 'uk'
    await callback.message.edit_text(get_message(user_lang, 'price_analysis_prompt'), reply_markup=get_price_analysis_keyboard(user_lang))
    await callback.answer()

@router.callback_query(F.data == "init_price_analysis")
async def handle_init_price_analysis(callback: CallbackQuery, state: FSMContext):
    # Initiates the price analysis process, prompting for user input.
    user = await get_user_by_telegram_id(callback.from_user.id)
    user_lang = user.language if user else 'uk'
    await callback.message.edit_text(get_message(user_lang, 'price_analysis_prompt'), reply_markup=InlineKeyboardBuilder().add(InlineKeyboardButton(text=get_message(user_lang, 'cancel_btn'), callback_data="cancel_action")).as_markup())
    await state.set_state(AIAssistant.waiting_for_price_analysis_input)
    await callback.answer()

@router.message(AIAssistant.waiting_for_price_analysis_input)
async def process_price_analysis_input(message: Message, state: FSMContext):
    # Processes user input (text and/or image) for price analysis.
    # Uses Google Search and Gemini API for analysis.
    user_input = message.text
    user = await get_user_by_telegram_id(message.from_user.id)
    user_lang = user.language if user else 'uk'
    
    image_data_base64 = None
    if message.photo:
        file_id = message.photo[-1].file_id
        file = await bot.get_file(file_id)
        file_path = file.file_path
        image_bytes = io.BytesIO()
        await bot.download_file(file_path, destination=image_bytes)
        image_data_base66 = base64.b64encode(image_bytes.getvalue()).decode('utf-8')
        logger.info(f"Received image for price analysis. Size: {len(image_data_base64)} bytes.")

    await message.answer(get_message(user_lang, 'price_analysis_generating'))
    
    search_query = f"ціна {user_input} купити Україна"
    if image_data_base64:
        search_query = f"розпізнати товар та ціна {user_input} купити Україна"

    # Using google_search tool to find relevant information
    search_results = await asyncio.to_thread(google_search.search, queries=[search_query, f"price {user_input} buy Ukraine"])
    
    price_context = []
    for res_set in search_results:
        if res_set.results:
            for res in res_set.results:
                if res.snippet:
                    price_context.append(res.snippet)
    
    context_text = "\n\n".join(price_context[:3])

    prompt = f"Проаналізуй опис товару '{user_input}' та, якщо є, зображення. Використай наступну інформацію з пошуку: {context_text}. Розрахуй приблизну ціну в UAH, запропонуй можливі місця придбання та вкажи фактори, що впливають на ціну, та можливі аналоги. Будь максимально точним."
    price_analysis_result = await call_gemini_api(prompt, user_telegram_id=message.from_user.id, image_data=image_data_base64)
    
    await message.answer(get_message(user_lang, 'price_analysis_result', result=price_analysis_result), reply_markup=get_ai_media_menu_keyboard(user_lang))
    await state.clear()

@router.callback_query(F.data == "help_sell")
async def handle_help_sell(callback: CallbackQuery):
    # Provides information on how to sell.
    user = await get_user_by_telegram_id(callback.from_user.id)
    user_lang = user.language if user else 'uk'
    await callback.message.edit_text(
        get_message(user_lang, 'help_sell_message', bot_link=hlink("BigmoneycreateBot", HELP_SELL_BOT_LINK)),
        parse_mode=ParseMode.HTML,
        reply_markup=get_price_analysis_keyboard(user_lang)
    )
    await callback.answer()

@router.callback_query(F.data == "help_buy")
async def handle_help_buy(callback: CallbackQuery):
    # Provides information on how to buy.
    user = await get_user_by_telegram_id(callback.from_user.id)
    user_lang = user.language if user else 'uk'
    await callback.message.edit_text(
        get_message(user_lang, 'help_buy_message', channel_link=hlink("канал", HELP_BUY_CHANNEL_LINK)),
        parse_mode=ParseMode.HTML,
        reply_markup=get_price_analysis_keyboard(user_lang)
    )
    await callback.answer()

@router.callback_query(F.data == "ai_media_menu")
async def handle_ai_media_menu(callback: CallbackQuery):
    # Displays the AI media menu.
    user = await get_user_by_telegram_id(callback.from_user.id)
    user_lang = user.language if user else 'uk'
    await callback.message.edit_text(get_message(user_lang, 'ai_media_menu_prompt'), reply_markup=get_ai_media_menu_keyboard(user_lang))
    await callback.answer()

@router.callback_query(F.data == "youtube_to_news")
async def handle_youtube_to_news(callback: CallbackQuery, state: FSMContext):
    # Initiates the YouTube to news conversion process.
    user = await get_user_by_telegram_id(callback.from_user.id)
    user_lang = user.language if user else 'uk'
    await callback.message.edit_text(get_message(user_lang, 'youtube_url_prompt'), reply_markup=InlineKeyboardBuilder().add(InlineKeyboardButton(text=get_message(user_lang, 'cancel_btn'), callback_data="cancel_action")).as_markup())
    await state.set_state(AIAssistant.waiting_for_youtube_url)
    await callback.answer()

@router.message(AIAssistant.waiting_for_youtube_url)
async def process_youtube_url(message: Message, state: FSMContext):
    # Processes a YouTube URL, generates a news summary, and adds it to the database.
    youtube_url = message.text
    user = await get_user_by_telegram_id(message.from_user.id)
    user_lang = user.language if user else 'uk'
    
    if not (youtube_url.startswith("http://") or youtube_url.startswith("https://")) or "youtube.com" not in youtube_url:
        await message.answer(get_message(user_lang, 'invalid_url'))
        return
    
    await message.answer(get_message(user_lang, 'youtube_processing'))
    
    search_query = f"YouTube video summary {youtube_url}"
    # Using google_search tool to find relevant information
    search_results = await asyncio.to_thread(google_search.search, queries=[search_query, f"YouTube {youtube_url} transcript summary"])
    
    transcript_context = []
    for res_set in search_results:
        if res_set.results:
            for res in res_set.results:
                if res.snippet:
                    transcript_context.append(res.snippet)
    
    context_text = "\n\n".join(transcript_context[:2])
    
    prompt = f"На основі цієї інформації про YouTube відео, згенеруй новину українською мовою, включаючи заголовок, короткий зміст та аналітику. Інформація: {context_text}"
    ai_news_content = await call_gemini_api(prompt, user_telegram_id=message.from_user.id)
    
    title = ai_news_content.split('\n')[0] if ai_news_content and '\n' in ai_news_content else "YouTube Відео Новина"
    
    video_id = None
    if 'v=' in youtube_url:
        video_id = youtube_url.split('v=')[-1].split('&')[0]
    elif 'youtu.be/' in youtube_url:
        video_id = youtube_url.split('youtu.be/')[-1].split('?')[0]

    image_url = f"https://img.youtube.com/vi/{video_id}/0.jpg" if video_id else "https://placehold.co/600x400/FF0000/FFFFFF?text=YouTube+Video"

    youtube_news_data = {
        "title": title,
        "content": ai_news_content,
        "source_url": youtube_url,
        "image_url": image_url,
        "published_at": datetime.now(timezone.utc),
        "source_type": "youtube",
        "user_id_for_source": None # This will make it pending for moderation
    }
    added_news = await add_news_to_db(youtube_news_data)
    if added_news:
        await message.answer(get_message(user_lang, 'youtube_summary_label', summary=ai_news_content), reply_markup=get_ai_media_menu_keyboard(user_lang))
    else:
        await message.answer(get_message(user_lang, 'add_source_error'), reply_markup=get_ai_media_menu_keyboard(user_lang))
    await state.clear()

@router.callback_query(F.data == "create_filtered_channel")
async def handle_create_filtered_channel(callback: CallbackQuery, state: FSMContext):
    # Initiates the process of creating a filtered channel.
    user = await get_user_by_telegram_id(callback.from_user.id)
    user_lang = user.language if user else 'uk'
    await callback.message.edit_text(get_message(user_lang, 'filtered_channel_prompt'), reply_markup=InlineKeyboardBuilder().add(InlineKeyboardButton(text=get_message(user_lang, 'cancel_btn'), callback_data="cancel_action")).as_markup())
    await state.set_state(AIAssistant.waiting_for_filtered_channel_details)
    await callback.answer()

@router.message(AIAssistant.waiting_for_filtered_channel_details)
async def process_filtered_channel_details(message: Message, state: FSMContext):
    # Processes details for creating a filtered channel and adds user subscriptions.
    details = message.text.split(',')
    if len(details) < 2:
        await message.answer(get_message('uk', 'filtered_channel_prompt'))
        return
    
    channel_name = details[0].strip()
    topics = [t.strip().lower() for t in details[1:] if t.strip()]
    user = await get_user_by_telegram_id(message.from_user.id)
    user_lang = user.language if user else 'uk'
    
    await message.answer(get_message(user_lang, 'filtered_channel_creating', channel_name=channel_name, topics=', '.join(topics)))
    
    for topic in topics:
        await add_user_subscription(user.id, topic)

    await message.answer(get_message(user_lang, 'filtered_channel_created', channel_name=channel_name), reply_markup=get_ai_media_menu_keyboard(user_lang))
    await state.clear()

@router.callback_query(F.data == "create_ai_media")
async def handle_create_ai_media(callback: CallbackQuery, state: FSMContext):
    # Initiates the process of creating an AI media.
    user = await get_user_by_telegram_id(callback.from_user.id)
    user_lang = user.language if user else 'uk'
    await callback.message.edit_text(get_message(user_lang, 'ai_media_creating'), reply_markup=InlineKeyboardBuilder().add(InlineKeyboardButton(text=get_message(user_lang, 'cancel_btn'), callback_data="cancel_action")).as_markup())
    await state.set_state(AIAssistant.waiting_for_ai_media_name)
    await callback.answer()

@router.message(AIAssistant.waiting_for_ai_media_name)
async def process_ai_media_name(message: Message, state: FSMContext):
    # Processes the name for the AI media and provides a confirmation message.
    media_name = message.text.strip()
    user = await get_user_by_telegram_id(message.from_user.id)
    user_lang = user.language if user else 'uk'
    
    await message.answer(get_message(user_lang, 'ai_media_creating'))
    
    mock_media_description = f"Ваше AI-медіа '{media_name}' тепер генерує AI-тексти, публікує їх у власному каналі, надає аналітику та щоденні дайджести."
    
    await message.answer(get_message(user_lang, 'ai_media_created', media_name=media_name) + f"\n\n{mock_media_description}", reply_markup=get_ai_media_menu_keyboard(user_lang))
    await state.clear()

@router.callback_query(F.data == "analytics_menu")
async def handle_analytics_menu(callback: CallbackQuery):
    # Displays the analytics menu.
    user = await get_user_by_telegram_id(callback.from_user.id)
    user_lang = user.language if user else 'uk'
    await callback.message.edit_text(get_message(user_lang, 'analytics_menu_prompt'), reply_markup=get_analytics_menu_keyboard(user_lang))
    await callback.answer()

@router.callback_query(F.data == "infographics")
async def handle_infographics(callback: CallbackQuery):
    # Generates a description of an infographic based on current trends.
    user = await get_user_by_telegram_id(callback.from_user.id)
    user_lang = user.language if user else 'uk'
    
    await callback.message.edit_text(get_message(user_lang, 'infographics_generating'))
    
    search_queries = [
        "latest global economic data trends",
        "recent technology adoption rates",
        "social media usage statistics 2024",
        "новини економіки статистика",
        "тренди технологій"
    ]
    # Using google_search tool to find relevant information
    search_results = await asyncio.to_thread(google_search.search, queries=search_queries)
    
    data_context = []
    for res_set in search_results:
        if res_set.results:
            for res in res_set.results:
                if res.snippet:
                    data_context.append(res.snippet)
    
    context_text = "\n\n".join(data_context[:5])
    
    prompt = f"На основі цих даних, опиши інфографіку, що візуалізує ключові тренди та взаємозв'язки. Опис має бути детальний, ніби ти пояснюєш, що зображено на інфографіці, які дані використано та які висновки можна зробити. Використовуй дані: {context_text}"
    infographics_description = await call_gemini_api(prompt, user_telegram_id=callback.from_user.id)
    
    await callback.message.edit_text(get_message(user_lang, 'infographics_result', result=infographics_description), reply_markup=get_analytics_menu_keyboard(user_lang))
    await callback.answer()

@router.callback_query(F.data == "trust_index")
async def handle_trust_index(callback: CallbackQuery):
    # Calculates and describes a 'Trust Index' for news sources.
    user = await get_user_by_telegram_id(callback.from_user.id)
    user_lang = user.language if user else 'uk'
    
    await callback.message.edit_text(get_message(user_lang, 'trust_index_calculating'))
    
    search_queries = [
        "fact-checking news sources reputation",
        "media bias ratings",
        "новини про довіру до ЗМІ",
        "репутація новинних агенцій"
    ]
    # Using google_search tool to find relevant information
    search_results = await asyncio.to_thread(google_search.search, queries=search_queries)
    
    trust_context = []
    for res_set in search_results:
        if res_set.results:
            for res in res_set.results:
                if res.snippet:
                    trust_context.append(res.snippet)
    
    context_text = "\n\n".join(trust_context[:5])

    prompt = f"На основі загальновідомої інформації та даних про репутацію джерел, опиши 'Індекс довіри до джерел'. Включи приклади, які джерела вважаються більш/менш надійними та чому. Використовуй контекст: {context_text}"
    trust_index_result = await call_gemini_api(prompt, user_telegram_id=callback.from_user.id)
    
    await callback.message.edit_text(get_message(user_lang, 'trust_index_result', result=trust_index_result), reply_markup=get_analytics_menu_keyboard(user_lang))
    await callback.answer()

@router.callback_query(F.data == "long_term_connections")
async def handle_long_term_connections(callback: CallbackQuery):
    # Identifies and describes long-term connections between events.
    user = await get_user_by_telegram_id(callback.from_user.id)
    user_lang = user.language if user else 'uk'
    
    await callback.message.edit_text(get_message(user_lang, 'long_term_connections_generating'))
    
    search_queries = [
        "historical events influencing current economy",
        "long term political trends Ukraine",
        "історичні передумови сучасних подій",
        "взаємозв'язок світових криз"
    ]
    # Using google_search tool to find relevant information
    search_results = await asyncio.to_thread(google_search.search, queries=search_queries)
    
    historical_context = []
    for res_set in search_results:
        if res_set.results:
            for res in res_set.results:
                if res.snippet:
                    historical_context.append(res.snippet)
    
    context_text = "\n\n".join(historical_context[:5])

    prompt = f"Знайди довгострокові зв'язки між подіями та опиши їх. Наприклад, як минулі економічні рішення впливають на поточну ситуацію. Використовуй контекст: {context_text}"
    connections_result = await call_gemini_api(prompt, user_telegram_id=callback.from_user.id)
    
    await callback.message.edit_text(get_message(user_lang, 'long_term_connections_result', result=connections_result), reply_markup=get_analytics_menu_keyboard(user_lang))
    await callback.answer()

@router.callback_query(F.data == "ai_prediction")
async def handle_ai_prediction(callback: CallbackQuery):
    # Generates an AI prediction based on current events and analysis.
    user = await get_user_by_telegram_id(callback.from_user.id)
    user_lang = user.language if user else 'uk'
    
    await callback.message.edit_text(get_message(user_lang, 'ai_prediction_generating'))
    
    search_queries = [
        "future of AI development predictions",
        "global economic forecasts next 5 years",
        "climate change impact predictions",
        "прогнози розвитку технологій",
        "майбутнє світової економіки"
    ]
    # Using google_search tool to find relevant information
    search_results = await asyncio.to_thread(google_search.search, queries=search_queries)
    
    prediction_context = []
    for res_set in search_results:
        if res_set.results:
            for res in res_set.results:
                if res.snippet:
                    prediction_context.append(res.snippet)
    
    context_text = "\n\n".join(prediction_context[:5])

    prompt = f"Зроби прогноз 'Що буде далі' на основі поточних подій та аналітики. Прогноз має бути реалістичним і обґрунтованим. Використовуй контекст: {context_text}"
    prediction_result = await call_gemini_api(prompt, user_telegram_id=callback.from_user.id)
    
    await callback.message.edit_text(get_message(user_lang, 'ai_prediction_result', result=prediction_result), reply_markup=get_analytics_menu_keyboard(user_lang))
    await callback.answer()

@router.callback_query(F.data == "donate")
async def handle_donate_command(callback: CallbackQuery):
    # Handles the 'donate' callback, displaying donation information.
    user = await get_user_by_telegram_id(callback.from_user.id)
    user_lang = user.language if user else 'uk'
    
    await callback.message.edit_text(
        get_message(user_lang, 'donate_message', card_number=MONOBANK_CARD_NUMBER),
        parse_mode=ParseMode.HTML,
        reply_markup=get_main_menu_keyboard(user_lang)
    )
    await callback.answer()

@router.callback_query(F.data == "subscribe_menu")
async def handle_subscribe_menu(callback: CallbackQuery):
    # Displays the subscription management menu.
    user = await get_user_by_telegram_id(callback.from_user.id)
    user_lang = user.language if user else 'uk'
    
    subscriptions = await get_user_subscriptions(user.id)
    
    response_text = get_message(user_lang, 'subscribe_menu_prompt') + "\n\n"
    if subscriptions:
        response_text += get_message(user_lang, 'your_subscriptions', topics=", ".join(subscriptions))
    else:
        response_text += get_message(user_lang, 'no_subscriptions')
        
    await callback.message.edit_text(response_text, reply_markup=get_subscription_menu_keyboard(user_lang))
    await callback.answer()

@router.callback_query(F.data == "add_subscription")
async def handle_add_subscription(callback: CallbackQuery, state: FSMContext):
    # Initiates the process of adding new topic subscriptions.
    user = await get_user_by_telegram_id(callback.from_user.id)
    user_lang = user.language if user else 'uk'
    await callback.message.edit_text(get_message(user_lang, 'add_subscription_prompt'), reply_markup=InlineKeyboardBuilder().add(InlineKeyboardButton(text=get_message(user_lang, 'cancel_btn'), callback_data="cancel_action")).as_markup())
    await state.set_state(SubscriptionStates.waiting_for_topics_to_add)
    await callback.answer()

@router.message(SubscriptionStates.waiting_for_topics_to_add)
async def process_topics_to_add(message: Message, state: FSMContext):
    # Processes the topics provided by the user for subscription.
    topics_raw = message.text
    topics = [t.strip().lower() for t in topics_raw.split(',') if t.strip()]
    user = await get_user_by_telegram_id(message.from_user.id)
    user_lang = user.language if user else 'uk'
    
    for topic in topics:
        await add_user_subscription(user.id, topic)
    
    await message.answer(get_message(user_lang, 'subscription_added', topics=", ".join(topics)), reply_markup=get_subscription_menu_keyboard(user_lang))
    await state.clear()

@router.callback_query(F.data == "remove_subscription")
async def handle_remove_subscription(callback: CallbackQuery, state: FSMContext):
    # Initiates the process of removing a topic subscription.
    user = await get_user_by_telegram_id(callback.from_user.id)
    user_lang = user.language if user else 'uk'
    await callback.message.edit_text(get_message(user_lang, 'remove_subscription_prompt'), reply_markup=InlineKeyboardBuilder().add(InlineKeyboardButton(text=get_message(user_lang, 'cancel_btn'), callback_data="cancel_action")).as_markup())
    await state.set_state(SubscriptionStates.waiting_for_topic_to_remove)
    await callback.answer()

@router.message(SubscriptionStates.waiting_for_topic_to_remove)
async def process_topic_to_remove(message: Message, state: FSMContext):
    # Processes the topic provided by the user for removal from subscriptions.
    topic_to_remove = message.text.strip().lower()
    user = await get_user_by_telegram_id(message.from_user.id)
    user_lang = user.language if user else 'uk'
    
    subscriptions = await get_user_subscriptions(user.id)
    if topic_to_remove in subscriptions:
        await remove_user_subscription(user.id, topic_to_remove)
        await message.answer(get_message(user_lang, 'subscription_removed', topic=topic_to_remove), reply_markup=get_subscription_menu_keyboard(user_lang))
    else:
        await message.answer(get_message(user_lang, 'subscription_not_found', topic=topic_to_remove), reply_markup=get_subscription_menu_keyboard(user_lang))
    await state.clear()

@router.message(Command("parse_now"))
async def command_parse_now_handler(message: Message):
    # Manually triggers the news parsing and posting task.
    user = await get_user_by_telegram_id(message.from_user.id)
    user_lang = user.language if user else 'uk'
    
    await message.answer(get_message(user_lang, 'parse_now_started'))
    asyncio.create_task(fetch_and_post_news_task(bot))
    await message.answer(get_message(user_lang, 'parse_now_completed'), reply_markup=get_main_menu_keyboard(user_lang))


@app.on_event("startup")
async def on_startup():
    # Startup event handler for the FastAPI application.
    # Sets webhook and starts the custom scheduler.
    webhook_url = os.getenv("WEBHOOK_URL")
    if not webhook_url:
        render_external_url = os.getenv("RENDER_EXTERNAL_HOSTNAME")
        if render_external_url:
            webhook_url = f"https://{render_external_url}/telegram_webhook"
        else:
            logger.error("WEBHOOK_URL not defined. Webhook will not be set.")
    try:
        logger.info(f"Attempting to set webhook to: {webhook_url}")
        await bot.set_webhook(url=webhook_url)
        logger.info(f"Webhook successfully set to {webhook_url}")
    except Exception as e:
        logger.error(f"Error setting webhook: {e}", exc_info=True)
    
    # Setup the APScheduler jobs here after bot is initialized
    setup_scheduler(bot)
    logger.info("FastAPI app started.")

@app.on_event("shutdown")
async def on_shutdown():
    # Shutdown event handler for the FastAPI application.
    # Closes database pool and bot session.
    if db_pool:
        await db_pool.close()
    logger.info("DB pool closed.")
    await bot.session.close()
    logger.info("FastAPI app shut down.")

@app.post("/telegram_webhook")
async def telegram_webhook(request: Request):
    # Endpoint for Telegram webhook updates.
    try:
        update = await request.json()
        aiogram_update = types.Update.model_validate(update, context={"bot": bot})
        await dp.feed_update(bot, aiogram_update)
    except Exception as e:
        logger.error(f"Error processing Telegram webhook: {e}", exc_info=True)
    return {"ok": True}

@app.post("/")
async def root_webhook(request: Request):
    # Root endpoint for Telegram webhook updates (fallback).
    try:
        update = await request.json()
        aiogram_update = types.Update.model_validate(update, context={"bot": bot})
        await dp.feed_update(bot, aiogram_update)
    except Exception as e:
        logger.error(f"Error processing Telegram webhook at root path: {e}", exc_info=True)
    return {"ok": True}

@app.get("/healthz", response_class=PlainTextResponse)
async def healthz():
    # Health check endpoint.
    return "OK"

@app.get("/ping", response_class=PlainTextResponse)
async def ping():
    # Ping endpoint.
    return "pong"

@app.get("/", response_class=HTMLResponse)
async def read_root():
    # Serves the index.html file.
    with open("index.html", "r", encoding="utf-8") as f:
        return f.read()

@app.get("/dashboard", response_class=HTMLResponse)
async def read_dashboard(api_key: str = Depends(get_api_key)):
    # Serves the dashboard.html file, protected by API key.
    with open("dashboard.html", "r", encoding="utf-8") as f:
        return f.read()

@app.get("/users", response_class=HTMLResponse)
async def read_users(api_key: str = Depends(get_api_key), limit: int = 10, offset: int = 0):
    # Retrieves a list of users for the admin dashboard.
    pool = await get_db_pool()
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("SELECT id, telegram_id, username, first_name, last_name, created_at, is_admin, last_active, language, auto_notifications, digest_frequency, safe_mode, current_feed_id, is_premium, premium_expires_at, level, badges, inviter_id, view_mode, premium_invite_count, digest_invite_count, is_pro, ai_requests_today, ai_last_request_date FROM users ORDER BY created_at DESC LIMIT %s OFFSET %s;", (limit, offset))
            return {"users": await cur.fetchall()}

@app.get("/reports", response_class=HTMLResponse)
async def read_reports(api_key: str = Depends(get_api_key), limit: int = 10, offset: int = 0):
    # Retrieves a list of reports for the admin dashboard.
    pool = await get_db_pool()
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            query = "SELECT r.*, u.username, u.first_name, n.title as news_title, n.source_url as news_source_url FROM reports r LEFT JOIN users u ON r.user_id = u.id LEFT JOIN news n ON r.target_id = n.id WHERE r.target_type = 'news' ORDER BY r.created_at DESC LIMIT %s OFFSET %s;"
            await cur.execute(query, (limit, offset))
            return await cur.fetchall()

@app.get("/api/admin/sources")
async def get_admin_sources(api_key: str = Depends(get_api_key), limit: int = 100, offset: int = 0):
    # Retrieves a list of sources for the admin dashboard.
    pool = await get_db_pool()
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("SELECT id, user_id, source_name, source_url, normalized_source_url, source_type, status, added_at, last_parsed, parse_frequency FROM sources ORDER BY added_at DESC LIMIT %s OFFSET %s;", (limit, offset))
            return [Source(**s) for s in await cur.fetchall()]

@app.get("/api/admin/stats")
async def get_admin_stats(api_key: str = Depends(get_api_key)):
    # Retrieves general statistics for the admin dashboard.
    pool = await get_db_pool()
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("SELECT COUNT(*) AS total_users FROM users;")
            total_users = (await cur.fetchone())['total_users']
            await cur.execute("SELECT COUNT(*) AS total_news FROM news;")
            total_news = (await cur.fetchone())['total_news']
            await cur.execute("SELECT COUNT(DISTINCT telegram_id) AS active_users_count FROM users WHERE last_active >= NOW() - INTERVAL '24 hours';")
            active_users_count = (await cur.fetchone())['active_users_count']
            return {"total_users": total_users, "total_news": total_news, "active_users_count": active_users_count}

@app.get("/api/admin/news")
async def get_admin_news(api_key: str = Depends(api_key_header), limit: int = 10, offset: int = 0, status: Optional[str] = None):
    # Retrieves a list of news items for the admin dashboard, with optional status filtering.
    pool = await get_db_pool()
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            query = "SELECT n.*, s.source_name FROM news n JOIN sources s ON n.source_id = s.id"
            params = []
            if status:
                query += " WHERE n.moderation_status = %s"
                params.append(status)
            query += " ORDER BY n.published_at DESC LIMIT %s OFFSET %s;"
            params.extend([limit, offset])
            await cur.execute(query, tuple(params))
            return await cur.fetchall()

@app.get("/api/admin/news/counts_by_status")
async def get_news_counts_by_status(api_key: str = Depends(api_key_header)):
    # Retrieves the count of news items grouped by moderation status.
    pool = await get_db_pool()
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("SELECT moderation_status, COUNT(*) FROM news GROUP BY moderation_status;")
            return {row['moderation_status']: row['count'] for row in await cur.fetchall()}

@app.put("/api/admin/news/{news_id}")
async def update_admin_news(news_id: int, news: News, api_key: str = Depends(api_key_header)):
    # Updates a specific news item in the database.
    pool = await get_db_pool()
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            # Ensure ai_classified_topics is passed as a list for TEXT[] column
            params = [news.source_id, news.title, news.content, str(news.source_url), normalize_url(str(news.source_url)), str(news.image_url) if news.image_url else None, news.published_at, news.moderation_status, news.expires_at, news.is_published_to_channel, news.ai_classified_topics, news_id]
            await cur.execute("""UPDATE news SET source_id = %s, title = %s, content = %s, source_url = %s, normalized_source_url = %s, image_url = %s, published_at = %s, moderation_status = %s, expires_at = %s, is_published_to_channel = %s, ai_classified_topics = %s WHERE id = %s RETURNING *;""", tuple(params))
            updated_rec = await cur.fetchone()
            if not updated_rec:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="News not found.")
            return News(**updated_rec).__dict__

@app.delete("/api/admin/news/{news_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_admin_news_api(news_id: int, api_key: str = Depends(api_key_header)):
    # Deletes a specific news item from the database.
    pool = await get_db_pool()
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("DELETE FROM news WHERE id = %s", (news_id,))
            if cur.rowcount == 0:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="News not found.")
            return

if __name__ == "__main__":
    import uvicorn
    if not os.getenv("WEBHOOK_URL"):
        logger.info("WEBHOOK_URL not set. Running bot in polling mode.")
        async def start_polling():
            # Initialize bot here for polling mode
            polling_bot = Bot(token=API_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
            # Setup APScheduler jobs for polling mode
            setup_scheduler(polling_bot)
            await dp.start_polling(polling_bot)
        asyncio.run(start_polling())
    uvicorn.run(app, host="0.0.0.0", port=8000)
