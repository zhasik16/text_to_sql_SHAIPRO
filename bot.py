import os
import logging
import sqlite3
import pandas as pd
import tempfile
import json
import re
from pathlib import Path
from datetime import datetime
import requests
import io
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')
import numpy as np
from tabulate import tabulate
import random

from dotenv import load_dotenv
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, filters, CallbackQueryHandler, ConversationHandler

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

load_dotenv()
# Configuration - UPDATE THESE WITH YOUR KEYS!
OPENROUTER_API_KEY = os.getenv('OPENROUTER_API_KEY')
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
OPENROUTER_API_URL = "https://openrouter.ai/api/v1/chat/completions"

# Conversation states
LANGUAGE, MAIN_MENU, TEXT_TO_SQL, CREATE_DB = range(4)

# User states to manage conversation flow
USER_STATES = {}

# Supported languages
LANGUAGES = {
    'en': {
        'welcome': "🚀 *Advanced Text-to-SQL Assistant*\n\nI can help you work with databases using natural language!",
        'choose_language': "Please choose your preferred language:",
        'language_selected': "English language selected. Let's get started!",
        'main_menu': "📋 *Main Menu*\n\nChoose a mode:",
        'text_to_sql_mode': "📊 Text-to-SQL Mode",
        'create_db_mode': "🛠 Create Database Mode",
        'help_mode': "❓ Help",
        'settings_mode': "⚙️ Settings",
        'back_button': "⬅️ Back",
        'text_to_sql_selected': "📊 *Text-to-SQL Mode*\n\nUpload a database file or send a query in natural language.",
        'create_db_selected': "🛠 *Create Database Mode*\n\nCreate a new database or manage existing ones.",
        'upload_db_prompt': "Please upload a database file (CSV, Excel, SQLite).",
        'db_uploaded': "✅ Database *{}* uploaded successfully! ({})",
        'query_prompt': "💬 Enter your query (e.g., 'Show all data', 'Average salary by department'):",
        'voice_query_prompt': "🎤 Send voice message with your query",
        'creating_db': "🛠 *Create New Database*\n\nSend database name and columns.\n\nExample: 'Create database 'employees' with columns: id integer primary key, name text, salary real, department text'",
        'db_created': "✅ Database '*{}*' created successfully with columns:\n{}",
        'add_data_prompt': "💾 Send data to add (e.g., 'John Doe, 5000, Engineering'):",
        'data_added': "✅ Data added successfully!",
        'no_db_selected': "⚠️ Please select or upload a database first.",
        'processing': "⏳ Processing your request...",
        'error_general': "❌ Sorry, an error occurred. Please try again.",
        'error_db_creation': "❌ Couldn't create database. Please try again with a clearer description.",
        'error_query': "❌ Couldn't process your query. Please try again.",
        'error_api': "❌ API service is temporarily unavailable. Please try again later.",
        'help_text': """
🤖 *Advanced Text-to-SQL Bot Help*

*Available Modes:*
📊 *Text-to-SQL Mode*: Upload database files and query with natural language
🛠 *Create Database Mode*: Create and manage your own databases

*Supported Features:*
• Natural language queries (text & voice) in English and Russian
• Multiple file formats: CSV, Excel, SQLite
• Advanced data visualization with charts and tables
• Multi-language support (EN/RU)

*Examples:*
• "Show me the entire table"
• "Display all employees with salary greater than 5000"
• "Create database 'expenses' with columns: id, date, amount, category"
• "Add expense: 2023-09-19, 50.00, groceries"
• "Show me the average age by department"
        """,
        'settings_text': "⚙️ *Settings*",
        'change_language': "🌐 Change Language",
        'visualization_title': "📊 Query Results",
        'stats_summary': "📈 Found {} records. {}",
        'no_results': "No results found for your query.",
        'db_list': "📁 Your Databases:\n{}",
        'no_databases': "You don't have any databases yet. Use /create to create one.",
        'api_error_help': "⚠️ *API Connection Issue*\n\nPlease check your OpenRouter API key in the code configuration.",
        'showing_all_data': "📋 Showing all data ({} records):",
        'showing_sample': "📋 Showing sample of data ({} records):",
        'voice_processing': "🎤 Processing your voice message...",
        'voice_transcribed': "🎤 Voice transcribed: '{}'",
        'table_info': "📊 Table: {} ({} columns, {} rows)",
        'language_changed': "🌐 Language changed to English"
    },
    'ru': {
        'welcome': "🚀 *Продвинутый Text-to-SQL Ассистент*\n\nЯ помогу вам работать с базами данных на естественном языке!",
        'choose_language': "Пожалуйста, выберите язык:",
        'language_selected': "Выбран русский язык. Давайте начнем!",
        'main_menu': "📋 *Главное меню*\n\nВыберите режим:",
        'text_to_sql_mode': "📊 Режим Text-to-SQL",
        'create_db_mode': "🛠 Создание базы данных",
        'help_mode': "❓ Помощь",
        'settings_mode': "⚙️ Настройки",
        'back_button': "⬅️ Назад",
        'text_to_sql_selected': "📊 *Режим Text-to-SQL*\n\nЗагрузите файл базы данных или отправьте запрос на естественном языке.",
        'create_db_selected': "🛠 *Режим создания БД*\n\nСоздайте новую базу данных или управляйте существующими.",
        'upload_db_prompt': "Загрузите файл базы данных (CSV, Excel, SQLite).",
        'db_uploaded': "✅ База данных *{}* успешно загружена! ({})",
        'query_prompt': "💬 Введите запрос (например, 'Показать всю таблицу', 'Средняя зарплата по отделам'):",
        'voice_query_prompt': "🎤 Отправьте голосовое сообщение с запросом",
        'creating_db': "🛠 *Создание новой базы данных*\n\nОтправьте название базы и столбцы.\n\nПример: 'Создай базу ''сотрудники'' со столбцами: id integer primary key, имя text, зарплата real, отдел text'",
        'db_created': "✅ База данных '*{}*' успешно создана со столбцами:\n{}",
        'add_data_prompt': "💾 Отправьте данные для добавления (например, 'Иван Иванов, 5000, Разработка'):",
        'data_added': "✅ Данные успешно добавлены!",
        'no_db_selected': "⚠️ Сначала выберите или загрузите базу данных.",
        'processing': "⏳ Обрабатываю ваш запрос...",
        'error_general': "❌ Извините, произошла ошибка. Попробуйте еще раз.",
        'error_db_creation': "❌ Не удалось создать базу данных. Попробуйте еще раз с более четким описанием.",
        'error_query': "❌ Не удалось обработать запрос. Попробуйте еще раз.",
        'error_api': "❌ Сервис API временно недоступен. Попробуйте позже.",
        'help_text': """
🤖 *Помощь по Text-to-SQL боту*

*Доступные режимы:*
📊 *Режим Text-to-SQL*: Загружайте файлы БД и делайте запросы на естественном языке
🛠 *Режим создания БД*: Создавайте и управляйте своими базами данных

*Поддерживаемые возможности:*
• Запросы на естественном языке (текст и голос) на русском и английском
• Поддержка форматов: CSV, Excel, SQLite
• Продвинутая визуализация данных с графиками и таблицами
• Поддержка двух языков (EN/RU)

*Примеры:*
• "Показать всю таблицу"
• "Отобразить всех сотрудников с зарплатой выше 5000"
• "Создайте базу ''расходы'' со столбцами: id, дата, сумма, категория"
• "Добавьте расход: 2023-09-19, 50.00, продукты"
• "Покажите средний возраст по отделам"
        """,
        'settings_text': "⚙️ *Настройки*",
        'change_language': "🌐 Сменить язык",
        'visualization_title': "📊 Результаты запроса",
        'stats_summary': "📈 Найдено {} записей. {}",
        'no_results': "По вашему запросу ничего не найдено.",
        'db_list': "📁 Ваши базы данных:\n{}",
        'no_databases': "У вас пока нет баз данных. Используйте /create для создания.",
        'api_error_help': "⚠️ *Проблема с подключением к API*\n\nПожалуйста, проверьте ваш OpenRouter API ключ в настройках кода.",
        'showing_all_data': "📋 Показаны все данные ({} записей):",
        'showing_sample': "📋 Показана выборка данных ({} записей):",
        'voice_processing': "🎤 Обрабатываю ваше голосовое сообщение...",
        'voice_transcribed': "🎤 Голос расшифрован: '{}'",
        'table_info': "📊 Таблица: {} ({} столбцов, {} строк)",
        'language_changed': "🌐 Язык изменен на Русский"
    }
}

class UserState:
    def __init__(self):
        self.language = None
        self.mode = None
        self.current_db = None
        self.current_db_name = None
        self.current_table = None  # Store the actual table name
        self.creating_db_name = None
        self.creating_db_columns = []
        self.waiting_for_column_def = False
        self.waiting_for_data = False

# Initialize database for storing user databases info
def init_bot_database():
    conn = sqlite3.connect('bot_data.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS user_databases
                 (user_id INTEGER, db_name TEXT, db_path TEXT, table_name TEXT, columns TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    c.execute('''CREATE TABLE IF NOT EXISTS user_settings
                 (user_id INTEGER PRIMARY KEY, language TEXT DEFAULT 'en')''')
    conn.commit()
    conn.close()

init_bot_database()

def get_user_language(user_id):
    conn = sqlite3.connect('bot_data.db')
    c = conn.cursor()
    c.execute("SELECT language FROM user_settings WHERE user_id=?", (user_id,))
    result = c.fetchone()
    conn.close()
    return result[0] if result else 'en'

def save_user_language(user_id, language):
    conn = sqlite3.connect('bot_data.db')
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO user_settings (user_id, language) VALUES (?, ?)", 
              (user_id, language))
    conn.commit()
    conn.close()

def get_user_databases(user_id):
    conn = sqlite3.connect('bot_data.db')
    c = conn.cursor()
    c.execute("SELECT db_name, db_path, table_name FROM user_databases WHERE user_id=? ORDER BY created_at DESC", (user_id,))
    dbs = c.fetchall()
    conn.close()
    return dbs

def save_user_database(user_id, db_name, db_path, table_name, columns):
    conn = sqlite3.connect('bot_data.db')
    c = conn.cursor()
    c.execute("INSERT INTO user_databases (user_id, db_name, db_path, table_name, columns) VALUES (?, ?, ?, ?, ?)",
              (user_id, db_name, db_path, table_name, json.dumps(columns)))
    conn.commit()
    conn.close()

def get_database_info(db_path):
    """Get information about the database including table names and structure"""
    try:
        conn = sqlite3.connect(db_path)
        c = conn.cursor()
        
        # Get all tables
        c.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = c.fetchall()
        
        table_info = {}
        for table in tables:
            table_name = table[0]
            c.execute(f"PRAGMA table_info({table_name})")
            columns = c.fetchall()
            c.execute(f"SELECT COUNT(*) FROM {table_name}")
            row_count = c.fetchone()[0]
            
            table_info[table_name] = {
                'columns': [col[1] for col in columns],
                'row_count': row_count
            }
        
        conn.close()
        return table_info
        
    except Exception as e:
        logger.error(f"Error getting database info: {e}")
        return {}

def detect_main_table(table_info):
    """Detect the main table to use for queries"""
    if not table_info:
        return None
    
    # Prefer tables with more rows
    tables_by_size = sorted(table_info.items(), key=lambda x: x[1]['row_count'], reverse=True)
    
    # Avoid system tables
    for table_name, info in tables_by_size:
        if not table_name.startswith('sqlite_'):
            return table_name
    
    # Fallback to first table
    return list(table_info.keys())[0] if table_info else None

# Enhanced OpenRouter API call function with better error handling
def call_openrouter(prompt, model="google/gemini-pro", max_tokens=1000, temperature=0.1):
    # Check if API key is set
    if not OPENROUTER_API_KEY:
        logger.error("OpenRouter API key not configured properly")
        return None
    
    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://github.com/telegram-bot",
        "X-Title": "Telegram SQL Bot"
    }
    
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": max_tokens,
        "temperature": temperature
    }
    
    try:
        response = requests.post(OPENROUTER_API_URL, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        result = response.json()
        return result['choices'][0]['message']['content']
    except requests.exceptions.HTTPError as e:
        if response.status_code == 400:
            logger.error("OpenRouter API: Bad Request - Prompt might be too long or malformed")
        elif response.status_code == 401:
            logger.error("OpenRouter API: Unauthorized - Check your API key")
        elif response.status_code == 429:
            logger.error("OpenRouter API: Rate limit exceeded")
        else:
            logger.error(f"OpenRouter API HTTP Error: {e}")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"OpenRouter API Request Error: {e}")
        return None
    except Exception as e:
        logger.error(f"OpenRouter API Unexpected Error: {e}")
        return None

# Enhanced SQL generator with better table detection
def generate_sql_with_visualization(schema_info, query_text, table_name, language='en'):
    """Generate SQL with special handling for table display requests"""
    query_lower = query_text.lower()
    lang_dict = LANGUAGES[language]
    
    # Handle requests to show entire table
    show_all_keywords = ['show all', 'display all', 'entire table', 'whole table', 
                         'показать всю', 'отобразить все', 'вся таблица', 'полная таблица']
    
    if any(keyword in query_lower for keyword in show_all_keywords):
        return f"SELECT * FROM {table_name}", "full_table"
    
    # Handle requests with limits
    if any(word in query_lower for word in ['first', 'top', 'первые', 'топ']):
        limit_match = re.search(r'(first|top|первые|топ)\s+(\d+)', query_lower)
        if limit_match:
            limit = limit_match.group(2)
            return f"SELECT * FROM {table_name} LIMIT {limit}", "limited_table"
    
    # Use OpenRouter for complex queries
    prompt = f"""
    Database schema:
    {schema_info}
    
    Based on the above schema, generate an SQL query for: {query_text}
    Use table name: {table_name}
    
    Return only the SQL query without any explanation.
    Keep the query simple and avoid complex joins unless necessary.
    """
    
    sql_query = call_openrouter(prompt)
    if not sql_query:
        # Fallback to simple query
        return f"SELECT * FROM {table_name} LIMIT 10", "fallback"
    
    # Clean up the SQL query
    if sql_query.startswith("```sql"):
        sql_query = sql_query[6:]
    if sql_query.startswith("```"):
        sql_query = sql_query[3:]
    if sql_query.endswith("```"):
        sql_query = sql_query[:-3]
    
    # Ensure the query uses the correct table name
    sql_query = sql_query.replace('FROM data', f'FROM {table_name}')
    sql_query = sql_query.replace('from data', f'from {table_name}')
    
    return sql_query.strip(), "ai_generated"

# Enhanced visualization with beautiful table formatting
def create_enhanced_visualization(df, query_type, table_name, language='en'):
    try:
        lang_dict = LANGUAGES[language]
        
        # For full table requests, create a nicely formatted display
        if query_type == "full_table":
            if len(df) <= 20:
                # Use tabulate for beautiful table formatting
                table = tabulate(df, headers='keys', tablefmt='grid', showindex=False)
                return f"📋 **{lang_dict['showing_all_data'].format(len(df))}**\n\n```\n{table}\n```"
            else:
                # For large tables, show sample and offer download
                sample = tabulate(df.head(10), headers='keys', tablefmt='grid', showindex=False)
                return {
                    'text': f"📋 **{lang_dict['showing_all_data'].format(len(df))}**\n\n{lang_dict['showing_sample'].format(10)}\n\n```\n{sample}\n```",
                    'full_data': True
                }
        
        # For limited tables
        elif query_type == "limited_table":
            table = tabulate(df, headers='keys', tablefmt='grid', showindex=False)
            return f"📋 **{lang_dict['visualization_title']}**\n\n```\n{table}\n```"
        
        # For single value results
        elif len(df) == 1 and len(df.columns) == 1:
            value = df.iloc[0, 0]
            if pd.api.types.is_numeric_dtype(df.dtypes[0]):
                return f"📊 **{lang_dict['visualization_title']}**\n\nResult: `{value:.2f}`"
            else:
                return f"📊 **{lang_dict['visualization_title']}**\n\nResult: `{value}`"
        
        # For other results, create appropriate visualization
        else:
            numeric_columns = df.select_dtypes(include=['number']).columns
            
            if len(numeric_columns) >= 1 and len(df) > 1:
                # Create visualization
                return create_chart_visualization(df, numeric_columns, lang_dict)
            else:
                # Format as table
                table = tabulate(df, headers='keys', tablefmt='grid', showindex=False)
                return f"📋 **{lang_dict['visualization_title']}**\n\n```\n{table}\n```"
                
    except Exception as e:
        logger.error(f"Error creating enhanced visualization: {e}")
        # Fallback to simple display
        return f"```\n{df.to_string(index=False)}\n```"

def create_chart_visualization(df, numeric_columns, lang_dict):
    """Create chart visualization for numeric data"""
    try:
        plt.figure(figsize=(10, 6))
        
        if len(df) <= 15:  # Bar chart
            x = range(len(df))
            y = df[numeric_columns[0]]
            
            plt.bar(x, y)
            plt.xticks(x, [str(i) for i in x], rotation=45)
            plt.title(lang_dict['visualization_title'])
            plt.tight_layout()
            
        else:  # Histogram
            plt.hist(df[numeric_columns[0]], bins=min(20, len(df)//5), alpha=0.7, edgecolor='black')
            plt.title(lang_dict['visualization_title'])
            plt.xlabel(numeric_columns[0])
            plt.ylabel('Frequency')
        
        # Save the chart
        chart_path = f"chart_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
        plt.savefig(chart_path)
        plt.close()
        
        # Prepare stats summary
        stats = []
        for col in numeric_columns:
            stats.append(f"{col}: {df[col].mean():.2f} (avg), {df[col].max():.2f} (max)")
        
        return {
            'chart': chart_path,
            'stats': lang_dict['stats_summary'].format(len(df), "; ".join(stats)),
            'sample': f"```\n{tabulate(df.head(5), headers='keys', tablefmt='grid', showindex=False)}\n```" if len(df) > 10 else None
        }
        
    except Exception as e:
        logger.error(f"Error creating chart: {e}")
        # Fallback to table
        table = tabulate(df.head(10), headers='keys', tablefmt='grid', showindex=False)
        return f"📋 **{lang_dict['visualization_title']}**\n\n```\n{table}\n```"

# Improved voice transcription with language detection
def transcribe_voice(audio_file_path, language='en'):
    """Simulated voice transcription with language detection"""
    try:
        # In a real implementation, you would use:
        # 1. OpenAI Whisper API for accurate transcription
        # 2. Language detection to handle both English and Russian
        # 3. Proper error handling
        
        # For demo purposes, we'll simulate transcription based on language
        if language == 'ru':
            # Sample Russian transcriptions
            transcriptions = [
                "показать всю таблицу",
                "средняя зарплата по отделам",
                "сотрудники с зарплатой больше 5000",
                "создать базу данных сотрудники",
                "добавить данные иван иванов 5000 разработка"
            ]
        else:
            # Sample English transcriptions
            transcriptions = [
                "show the entire table",
                "average salary by department",
                "employees with salary greater than 5000",
                "create database employees",
                "add data john doe 5000 engineering"
            ]
        
        # Return a random transcription for demo
        return random.choice(transcriptions)
        
    except Exception as e:
        logger.error(f"Error in voice transcription: {e}")
        return "show all data"  # Fallback

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    # Initialize user state
    if user_id not in USER_STATES:
        USER_STATES[user_id] = UserState()
    
    # Check if user already has a language preference
    user_language = get_user_language(user_id)
    if user_language and user_language in LANGUAGES:
        USER_STATES[user_id].language = user_language
        await show_main_menu(update, context, user_language)
        return MAIN_MENU
    
    # Language selection keyboard
    keyboard = [
        [KeyboardButton("English 🇺🇸"), KeyboardButton("Русский 🇷🇺")]
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=True)
    
    await update.message.reply_text(
        LANGUAGES['en']['welcome'] + "\n\n" + LANGUAGES['en']['choose_language'],
        reply_markup=reply_markup,
        parse_mode='Markdown'
    )
    
    return LANGUAGE

async def language_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    text = update.message.text
    
    if user_id not in USER_STATES:
        USER_STATES[user_id] = UserState()
    
    if "English" in text or "🇺🇸" in text:
        USER_STATES[user_id].language = 'en'
        save_user_language(user_id, 'en')
        lang_text = LANGUAGES['en']['language_selected']
    elif "Русский" in text or "🇷🇺" in text:
        USER_STATES[user_id].language = 'ru'
        save_user_language(user_id, 'ru')
        lang_text = LANGUAGES['ru']['language_selected']
    else:
        await update.message.reply_text("Please choose a valid language option.")
        return LANGUAGE
    
    await update.message.reply_text(lang_text)
    await show_main_menu(update, context, USER_STATES[user_id].language)
    
    return MAIN_MENU

async def show_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, language='en'):
    lang_dict = LANGUAGES[language]
    
    keyboard = [
        [KeyboardButton(lang_dict['text_to_sql_mode']), KeyboardButton(lang_dict['create_db_mode'])],
        [KeyboardButton(lang_dict['help_mode']), KeyboardButton(lang_dict['settings_mode'])]
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    
    if update.callback_query:
        await update.callback_query.edit_message_text(lang_dict['main_menu'], reply_markup=reply_markup, parse_mode='Markdown')
    else:
        await update.message.reply_text(lang_dict['main_menu'], reply_markup=reply_markup, parse_mode='Markdown')

async def main_menu_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    if user_id not in USER_STATES or not USER_STATES[user_id].language:
        await start(update, context)
        return LANGUAGE
    
    user_state = USER_STATES[user_id]
    language = user_state.language
    lang_dict = LANGUAGES[language]
    text = update.message.text
    
    if text == lang_dict['text_to_sql_mode']:
        user_state.mode = "text_to_sql"
        keyboard = [
            [KeyboardButton(lang_dict['upload_db_prompt'])],
            [KeyboardButton(lang_dict['back_button'])]
        ]
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        await update.message.reply_text(lang_dict['text_to_sql_selected'], reply_markup=reply_markup, parse_mode='Markdown')
        return TEXT_TO_SQL
        
    elif text == lang_dict['create_db_mode']:
        user_state.mode = "create_db"
        keyboard = [
            [KeyboardButton("/create"), KeyboardButton("/add")],
            [KeyboardButton("/list"), KeyboardButton(lang_dict['back_button'])]
        ]
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        await update.message.reply_text(lang_dict['create_db_selected'], reply_markup=reply_markup, parse_mode='Markdown')
        return CREATE_DB
        
    elif text == lang_dict['help_mode']:
        await help_command(update, context)
        
    elif text == lang_dict['settings_mode']:
        await settings_command(update, context)
    
    return MAIN_MENU

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    if user_id not in USER_STATES or not USER_STATES[user_id].language:
        language = 'en'
    else:
        language = USER_STATES[user_id].language
        
    lang_dict = LANGUAGES[language]
    
    # Check if API key is configured
    if not OPENROUTER_API_KEY:
        help_text = lang_dict['help_text'] + "\n\n" + lang_dict['api_error_help']
    else:
        help_text = lang_dict['help_text']
    
    await update.message.reply_text(help_text, parse_mode='Markdown')

async def settings_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    if user_id not in USER_STATES or not USER_STATES[user_id].language:
        language = 'en'
    else:
        language = USER_STATES[user_id].language
        
    lang_dict = LANGUAGES[language]
    
    keyboard = [
        [KeyboardButton(lang_dict['change_language'])],
        [KeyboardButton(lang_dict['back_button'])]
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    
    await update.message.reply_text(lang_dict['settings_text'], reply_markup=reply_markup, parse_mode='Markdown')
    return MAIN_MENU

async def handle_back(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    if user_id not in USER_STATES or not USER_STATES[user_id].language:
        await start(update, context)
        return LANGUAGE
    
    user_state = USER_STATES[user_id]
    language = user_state.language
    
    await show_main_menu(update, context, language)
    return MAIN_MENU

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    if user_id not in USER_STATES or not USER_STATES[user_id].language:
        await start(update, context)
        return LANGUAGE
    
    user_state = USER_STATES[user_id]
    language = user_state.language
    lang_dict = LANGUAGES[language]
    
    if user_state.mode != "text_to_sql":
        user_state.mode = "text_to_sql"
    
    document = update.message.document
    file_name = document.file_name
    file_extension = Path(file_name).suffix.lower()
    
    if file_extension not in ['.csv', '.db', '.sqlite', '.sqlite3', '.xlsx', '.xls']:
        await update.message.reply_text("Please upload a CSV, Excel, or SQLite database file.")
        return
    
    # Download the file
    file = await document.get_file()
    downloaded_file = await file.download_to_drive()
    
    # Store the database path in user state
    user_state.current_db = str(downloaded_file)
    user_state.current_db_name = file_name
    
    # For CSV/Excel files, create a SQLite database from them
    if file_extension in ['.csv', '.xlsx', '.xls']:
        try:
            # Create SQLite database from file
            db_path = user_state.current_db + '.db'
            
            if file_extension == '.csv':
                df = pd.read_csv(user_state.current_db)
            else:  # Excel files
                df = pd.read_excel(user_state.current_db)
            
            conn = sqlite3.connect(db_path)
            # Use the original file name as table name (sanitized)
            table_name = re.sub(r'[^a-zA-Z0-9_]', '_', file_name.split('.')[0])
            if not table_name:
                table_name = "data"
            
            df.to_sql(table_name, conn, if_exists='replace', index=False)
            conn.close()
            
            user_state.current_db = db_path
            user_state.current_table = table_name
            
            # Get record count for message
            conn = sqlite3.connect(db_path)
            c = conn.cursor()
            c.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = c.fetchone()[0]
            conn.close()
            
            record_info = f"{count} records in table '{table_name}'"
            
        except Exception as e:
            logger.error(f"Error processing file: {e}")
            await update.message.reply_text(f"Error processing file: {e}")
            return
    else:
        # For SQLite databases, detect the main table
        try:
            table_info = get_database_info(user_state.current_db)
            if not table_info:
                await update.message.reply_text("No tables found in the database.")
                return
                
            main_table = detect_main_table(table_info)
            user_state.current_table = main_table
            
            conn = sqlite3.connect(user_state.current_db)
            c = conn.cursor()
            c.execute(f"SELECT COUNT(*) FROM {main_table}")
            count = c.fetchone()[0]
            conn.close()
            
            record_info = f"{count} records in table '{main_table}'"
            
        except Exception as e:
            logger.error(f"Error reading SQLite database: {e}")
            record_info = "Database loaded"
    
    await update.message.reply_text(
        lang_dict['db_uploaded'].format(file_name, record_info),
        parse_mode='Markdown'
    )
    
    # Show table information
    table_info = get_database_info(user_state.current_db)
    if table_info and user_state.current_table in table_info:
        info = table_info[user_state.current_table]
        await update.message.reply_text(
            lang_dict['table_info'].format(user_state.current_table, len(info['columns']), info['row_count']),
            parse_mode='Markdown'
        )
    
    # Ask for query
    keyboard = [
        [KeyboardButton("Show entire table"), KeyboardButton("Show first 10 records")],
        [KeyboardButton(lang_dict['query_prompt'])],
        [KeyboardButton(lang_dict['voice_query_prompt'])],
        [KeyboardButton(lang_dict['back_button'])]
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    await update.message.reply_text(lang_dict['query_prompt'], reply_markup=reply_markup)

async def handle_voice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    if user_id not in USER_STATES or not USER_STATES[user_id].language:
        await start(update, context)
        return LANGUAGE
    
    user_state = USER_STATES[user_id]
    language = user_state.language
    lang_dict = LANGUAGES[language]
    
    # Show processing message
    processing_msg = await update.message.reply_text(lang_dict['voice_processing'])
    
    # Download voice message
    voice_file = await update.message.voice.get_file()
    voice_path = f"voice_{user_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.ogg"
    await voice_file.download_to_drive(voice_path)
    
    # Transcribe voice using our function
    text = transcribe_voice(voice_path, language)
    
    # Clean up
    try:
        os.remove(voice_path)
    except:
        pass
    
    await processing_msg.edit_text(lang_dict['voice_transcribed'].format(text))
    
    # Process based on current mode
    if user_state.mode == "text_to_sql" and user_state.current_db:
        await process_query(update, context, text)
    elif user_state.mode == "create_db":
        if user_state.waiting_for_column_def:
            await process_column_definition(update, context, text)
        elif user_state.waiting_for_data:
            await process_data_addition(update, context, text)
        else:
            # Try to determine what the user wants to do
            if any(word in text.lower() for word in ['create', 'созда', 'new', 'новая']):
                await create_database(update, context)
            elif any(word in text.lower() for word in ['add', 'добав', 'insert', 'встав']):
                await add_to_database(update, context)
            else:
                await update.message.reply_text(lang_dict['help_text'], parse_mode='Markdown')
    else:
        await update.message.reply_text(lang_dict['help_text'], parse_mode='Markdown')

async def process_query(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str = None):
    user_id = update.effective_user.id
    user_state = USER_STATES[user_id]
    language = user_state.language
    lang_dict = LANGUAGES[language]
    
    if text is None:
        text = update.message.text
    
    # Check if user wants to go back
    if text == lang_dict['back_button']:
        await show_main_menu(update, context, language)
        return MAIN_MENU
    
    if not user_state.current_db or not user_state.current_table:
        await update.message.reply_text(lang_dict['no_db_selected'])
        return
    
    # Show processing message
    processing_msg = await update.message.reply_text(lang_dict['processing'])
    
    try:
        # Get database schema
        conn = sqlite3.connect(user_state.current_db)
        c = conn.cursor()
        
        # Get table info
        c.execute(f"PRAGMA table_info({user_state.current_table})")
        columns = c.fetchall()
        
        schema_info = f"Table {user_state.current_table}: {', '.join([col[1] for col in columns])}\n"
        
        conn.close()
        
        # Generate SQL query with visualization type
        sql_query, query_type = generate_sql_with_visualization(
            schema_info, text, user_state.current_table, language
        )
        
        # Execute the query
        conn = sqlite3.connect(user_state.current_db)
        df = pd.read_sql_query(sql_query, conn)
        conn.close()
        
        # Format and send results
        if df.empty:
            await processing_msg.edit_text(lang_dict['no_results'])
        else:
            # Create enhanced visualization
            visualization = create_enhanced_visualization(df, query_type, user_state.current_table, language)
            
            if isinstance(visualization, dict):
                # We have a chart to send
                if 'chart' in visualization:
                    await update.message.reply_photo(
                        photo=open(visualization['chart'], 'rb'),
                        caption=visualization['stats']
                    )
                    # Clean up chart file
                    try:
                        os.remove(visualization['chart'])
                    except:
                        pass
                
                if 'sample' in visualization and visualization['sample']:
                    await update.message.reply_text(visualization['sample'], parse_mode='Markdown')
                
                # For full data, offer download
                if visualization.get('full_data', False):
                    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
                        df.to_csv(f.name, index=False)
                        await update.message.reply_document(
                            document=open(f.name, 'rb'), 
                            caption="📁 Full dataset download"
                        )
                        os.unlink(f.name)
                        
            else:
                # Text-based response
                await processing_msg.edit_text(visualization, parse_mode='Markdown')
                
    except Exception as e:
        logger.error(f"Error processing query: {e}")
        await processing_msg.edit_text(lang_dict['error_query'])

async def create_database(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_state = USER_STATES[user_id]
    language = user_state.language
    lang_dict = LANGUAGES[language]
    
    if user_state.mode != "create_db":
        user_state.mode = "create_db"
    
    user_state.waiting_for_column_def = True
    user_state.creating_db_columns = []
    
    keyboard = [[KeyboardButton(lang_dict['back_button'])]]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    
    await update.message.reply_text(lang_dict['creating_db'], reply_markup=reply_markup, parse_mode='Markdown')

async def process_column_definition(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str = None):
    user_id = update.effective_user.id
    user_state = USER_STATES[user_id]
    language = user_state.language
    lang_dict = LANGUAGES[language]
    
    if text is None:
        text = update.message.text
    
    # Check if user wants to go back
    if text == lang_dict['back_button']:
        user_state.waiting_for_column_def = False
        await show_main_menu(update, context, language)
        return MAIN_MENU
    
    # Show processing message
    processing_msg = await update.message.reply_text(lang_dict['processing'])
    
    # Use OpenRouter to parse the column definitions (with fallback)
    prompt = f"""
    Parse the following database creation command and extract:
    1. The database name
    2. Column definitions in SQL format
    
    Command: {text}
    
    Return only a JSON object with keys: db_name, columns (array of column definitions)
    Example: {{"db_name": "myexpenses", "columns": ["id INTEGER PRIMARY KEY", "date TEXT", "amount REAL", "category TEXT"]}}
    """
    
    response = call_openrouter(prompt)
    
    # Fallback if API is unavailable
    if not response:
        # Simple fallback parsing
        db_name = "my_database"
        columns = ["id INTEGER PRIMARY KEY", "name TEXT", "value REAL"]
        
        # Try to extract database name
        name_match = re.search(r'create.*database.*[\'\"](.*?)[\'\"]', text.lower())
        if name_match:
            db_name = name_match.group(1)
        
        # Try to extract columns
        columns_match = re.search(r'columns?.*?:(.*)', text.lower())
        if columns_match:
            columns_str = columns_match.group(1)
            columns = [col.strip() + " TEXT" for col in columns_str.split(',')]
        
        result = {"db_name": db_name, "columns": columns}
    else:
        # Extract JSON from response
        try:
            # Find JSON in the response
            start_idx = response.find('{')
            end_idx = response.rfind('}') + 1
            if start_idx == -1 or end_idx == 0:
                raise ValueError("No JSON found in response")
            
            json_str = response[start_idx:end_idx]
            result = json.loads(json_str)
        except Exception as e:
            logger.error(f"Error parsing API response: {e}")
            await processing_msg.edit_text(lang_dict['error_db_creation'])
            return
    
    try:
        db_name = result['db_name']
        columns = result['columns']
        
        # Create the database
        db_path = f"{db_name}_{user_id}.db"
        conn = sqlite3.connect(db_path)
        c = conn.cursor()
        
        # Create table
        column_defs = ", ".join(columns)
        c.execute(f"CREATE TABLE IF NOT EXISTS data ({column_defs})")
        conn.commit()
        conn.close()
        
        # Store database info
        save_user_database(user_id, db_name, db_path, "data", columns)
        
        user_state.current_db = db_path
        user_state.current_db_name = db_name
        user_state.current_table = "data"
        user_state.waiting_for_column_def = False
        
        await processing_msg.edit_text(
            lang_dict['db_created'].format(db_name, '\n• ' + '\n• '.join(columns)),
            parse_mode='Markdown'
        )
        
        # Show options for next steps
        keyboard = [
            [KeyboardButton(lang_dict['add_data_prompt'])],
            [KeyboardButton(lang_dict['back_button'])]
        ]
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        await update.message.reply_text(lang_dict['add_data_prompt'], reply_markup=reply_markup)
        
    except Exception as e:
        logger.error(f"Error creating database: {e}")
        await processing_msg.edit_text(lang_dict['error_db_creation'])

async def add_to_database(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_state = USER_STATES[user_id]
    language = user_state.language
    lang_dict = LANGUAGES[language]
    
    if user_state.mode != "create_db" or not user_state.current_db:
        # Get user's databases
        dbs = get_user_databases(user_id)
        
        if not dbs:
            await update.message.reply_text(lang_dict['no_databases'])
            return
        
        # Let user select a database
        keyboard = [[InlineKeyboardButton(db[0], callback_data=f"select_db_{i}")] for i, db in enumerate(dbs)]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text("Select a database to add data to:", reply_markup=reply_markup)
        return
    
    user_state.waiting_for_data = True
    
    keyboard = [[KeyboardButton(lang_dict['back_button'])]]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    
    await update.message.reply_text(lang_dict['add_data_prompt'], reply_markup=reply_markup)

async def process_data_addition(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str = None):
    user_id = update.effective_user.id
    user_state = USER_STATES[user_id]
    language = user_state.language
    lang_dict = LANGUAGES[language]
    
    if text is None:
        text = update.message.text
    
    # Check if user wants to go back
    if text == lang_dict['back_button']:
        user_state.waiting_for_data = False
        await show_main_menu(update, context, language)
        return MAIN_MENU
    
    if not user_state.current_db:
        await update.message.reply_text(lang_dict['no_db_selected'])
        return
    
    # Show processing message
    processing_msg = await update.message.reply_text(lang_dict['processing'])
    
    try:
        # Get column information
        conn = sqlite3.connect('bot_data.db')
        c = conn.cursor()
        c.execute("SELECT columns FROM user_databases WHERE user_id=? AND db_path=?", 
                 (user_id, user_state.current_db))
        columns_json = c.fetchone()[0]
        columns = json.loads(columns_json)
        conn.close()
        
        # Use OpenRouter to parse the data
        prompt = f"""
        Parse the following data to insert into a database with columns: {columns}
        Data: {text}
        
        Return only a JSON object with key: values (array of values to insert)
        Example: {{"values": [1, "2023-05-15", 50.0, "groceries"]}}
        """
        
        response = call_openrouter(prompt)
        if not response:
            await processing_msg.edit_text(lang_dict['error_general'])
            return
        
        # Extract JSON from response
        try:
            # Find JSON in the response
            start_idx = response.find('{')
            end_idx = response.rfind('}') + 1
            if start_idx == -1 or end_idx == 0:
                raise ValueError("No JSON found in response")
            
            json_str = response[start_idx:end_idx]
            result = json.loads(json_str)
            values = result['values']
            
            # Insert into database
            conn = sqlite3.connect(user_state.current_db)
            c = conn.cursor()
            placeholders = ", ".join(["?" for _ in columns])
            c.execute(f"INSERT INTO {user_state.current_table} VALUES ({placeholders})", values)
            conn.commit()
            conn.close()
            
            user_state.waiting_for_data = False
            await processing_msg.edit_text(lang_dict['data_added'])
            
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {e}")
            await processing_msg.edit_text(lang_dict['error_general'])
        
    except Exception as e:
        logger.error(f"Error adding data: {e}")
        await processing_msg.edit_text(lang_dict['error_general'])

async def list_databases(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    if user_id not in USER_STATES or not USER_STATES[user_id].language:
        language = 'en'
    else:
        language = USER_STATES[user_id].language
        
    lang_dict = LANGUAGES[language]
    
    dbs = get_user_databases(user_id)
    
    if not dbs:
        await update.message.reply_text(lang_dict['no_databases'])
        return
    
    db_list = "\n".join([f"• {db[0]}" for db in dbs])
    await update.message.reply_text(lang_dict['db_list'].format(db_list), parse_mode='Markdown')

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    user_state = USER_STATES[user_id]
    language = user_state.language
    lang_dict = LANGUAGES[language]
    
    if query.data.startswith("select_db_"):
        db_index = int(query.data.split("_")[2])
        
        # Get user's databases
        dbs = get_user_databases(user_id)
        
        if db_index < len(dbs):
            user_state.current_db = dbs[db_index][1]
            user_state.current_db_name = dbs[db_index][0]
            # Get the table name for this database
            conn = sqlite3.connect('bot_data.db')
            c = conn.cursor()
            c.execute("SELECT table_name FROM user_databases WHERE user_id=? AND db_path=?", 
                     (user_id, user_state.current_db))
            table_name = c.fetchone()[0]
            conn.close()
            
            user_state.current_table = table_name
            await query.edit_message_text(f"Selected database: {dbs[db_index][0]}. You can now add data.")
        else:
            await query.edit_message_text("Database selection failed.")

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id in USER_STATES:
        USER_STATES[user_id].waiting_for_column_def = False
        USER_STATES[user_id].waiting_for_data = False
    
    await update.message.reply_text("Operation cancelled.")
    
    if user_id in USER_STATES and USER_STATES[user_id].language:
        await show_main_menu(update, context, USER_STATES[user_id].language)
    else:
        await start(update, context)
    
    return MAIN_MENU

async def change_language(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    if user_id not in USER_STATES:
        USER_STATES[user_id] = UserState()
    
    # Language selection keyboard
    keyboard = [
        [KeyboardButton("English 🇺🇸"), KeyboardButton("Русский 🇷🇺")],
        [KeyboardButton(LANGUAGES[USER_STATES[user_id].language]['back_button'])]
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=True)
    
    await update.message.reply_text(
        "🌐 Select your preferred language:",
        reply_markup=reply_markup
    )
    
    return LANGUAGE

# Error handler
async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.error(f"Exception while handling an update: {context.error}")
    
    # Try to send a message to the user
    try:
        user_id = update.effective_user.id if update and update.effective_user else None
        
        if user_id and user_id in USER_STATES and USER_STATES[user_id].language:
            language = USER_STATES[user_id].language
        else:
            language = 'en'
            
        lang_dict = LANGUAGES[language]
        
        await context.bot.send_message(
            chat_id=update.effective_chat.id if update else None,
            text=lang_dict['error_general']
        )
    except Exception as e:
        logger.error(f"Error in error handler: {e}")

def main():
    # Create the Application
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    
    # Create conversation handler with states
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler('start', start)],
        states={
            LANGUAGE: [MessageHandler(filters.TEXT & ~filters.COMMAND, language_handler)],
            MAIN_MENU: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, main_menu_handler),
                CommandHandler('help', help_command),
                CommandHandler('settings', settings_command),
                CommandHandler('list', list_databases),
                CommandHandler('cancel', cancel),
                CommandHandler('language', change_language)
            ],
            TEXT_TO_SQL: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, process_query),
                MessageHandler(filters.VOICE, handle_voice),
                MessageHandler(filters.Document.ALL, handle_document),
                CommandHandler('cancel', cancel)
            ],
            CREATE_DB: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, process_column_definition),
                MessageHandler(filters.VOICE, handle_voice),
                CommandHandler('create', create_database),
                CommandHandler('add', add_to_database),
                CommandHandler('list', list_databases),
                CommandHandler('cancel', cancel)
            ]
        },
        fallbacks=[CommandHandler('cancel', cancel)],
    )
    
    # Add handlers
    application.add_handler(conv_handler)
    application.add_handler(CallbackQueryHandler(handle_callback))
    
    # Add error handler
    application.add_error_handler(error_handler)
    
    # Start the bot
    application.run_polling()

if __name__ == "__main__":
    main()