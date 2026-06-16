import discord
from discord.ext import commands, tasks
from discord import app_commands
from discord.ext.commands import cooldown, BucketType
from datetime import datetime, timedelta, timezone
import json
import os
import re
import asyncio
from typing import List, Optional, Dict, Any, Tuple
from discord.ui import Button, View, Modal, TextInput
from functools import wraps
from collections import deque
import logging
import threading
import socket
import random

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TOKEN = os.getenv("DISCORD_TOKEN")

if not TOKEN:
    print("Токен не найден!")
    exit(1)

intents = discord.Intents.all()
bot = commands.Bot(command_prefix='!', intents=intents, owner_id=1490421579597746186)

bot.remove_command('help')

MAX_HISTORY_PER_USER = 100
MAX_JOIN_HISTORY = 200
LOCKDOWN_DURATION = 35 * 60
WARN1_EXPIRE_DAYS = 3
WARN2_EXPIRE_DAYS = 6
MAX_WARNS = 3
MUTE_ON_MAX_WARNS_HOURS = 10
MAX_CLEAR_MESSAGES = 500
MAX_SLOWMODE_SECONDS = 21600
MAX_TEMPBAN_DAYS = 7
MAX_USER_TICKETS = 3
AUTO_SAVE_INTERVAL = 300
RATE_LIMIT_COOLDOWN = 3
SPAM_THRESHOLD = 4
MENTION_THRESHOLD = 4
RAID_SUSPICION_THRESHOLD = 5
RAID_ACTIVATE_THRESHOLD = 15
RAID_HARD_THRESHOLD = 20
LOCKDOWN_SLOWMODE = 600

WARNS_FILE = 'warns.json'
LOGS_FILE = 'logs.json'
SPAM_COUNT_FILE = 'spam_count.json'
COMMANDS_ACCESS_FILE = 'commands_access.json'
RAID_CONFIG_FILE = 'raid_config.json'
ROLE_PERMISSIONS_FILE = 'role_permissions.json'
CHANNEL_BACKUP_FILE = 'channel_backup.json'
TICKETS_FILE = 'tickets.json'
SUPPORT_CONFIG_FILE = 'support_config.json'
SUPPORT_ADMINS_FILE = 'support_admins.json'
VERIFY_ROLES_FILE = 'verify_roles.json'
LOCKDOWN_FILE = 'lockdown.json'
TIMERS_FILE = 'timers.json'
COINS_FILE = 'coins.json'
WORK_COOLDOWN_FILE = 'work_cooldown.json'
MESSAGE_STATS_FILE = 'message_stats.json'
TEMPROLE_FILE = 'temprole.json'

PROFESSIONS = [
    ("Врач", 50000, 300000),
    ("Программист", 80000, 600000),
    ("Учитель", 30000, 120000),
    ("Инженер", 50000, 250000),
    ("Адвокат", 40000, 350000),
    ("Бухгалтер", 45000, 180000),
    ("Архитектор", 60000, 300000),
    ("Дизайнер", 50000, 250000),
    ("Маркетолог", 50000, 280000),
    ("Повар", 40000, 180000),
    ("Электрик", 45000, 150000),
    ("Сантехник", 40000, 140000),
    ("Водитель", 40000, 130000),
    ("Строитель", 45000, 180000),
    ("Психолог", 35000, 200000),
    ("Переводчик", 40000, 220000),
    ("Финансист", 60000, 400000),
    ("Агроном", 45000, 180000),
    ("Журналист", 35000, 180000),
    ("Пожарный", 40000, 120000),
    ("Вор", 1000, 100000),
    ("Прокурор", 25000, 180000),
    ("Президент авио компании", 250000, 600000),
    ("Генерал армии", 100000, 450000)
]

def load_json(file, default):
    if os.path.exists(file):
        try:
            with open(file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Ошибка загрузки {file}: {e}")
            return default
    return default

def save_json(file, data):
    try:
        with open(file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
    except Exception as e:
        logger.error(f"Ошибка сохранения {file}: {e}")

warns = load_json(WARNS_FILE, {})
logs_config = load_json(LOGS_FILE, {})
spam_count = load_json(SPAM_COUNT_FILE, {})
commands_access = load_json(COMMANDS_ACCESS_FILE, {})
raid_config = load_json(RAID_CONFIG_FILE, {})
role_permissions = load_json(ROLE_PERMISSIONS_FILE, {})
channel_backup = load_json(CHANNEL_BACKUP_FILE, {})
tickets_data = load_json(TICKETS_FILE, {
    'ticket_counter': 0,
    'complaint_counter': 0,
    'tickets': {},
    'complaints': {},
    'archived_tickets': {},
    'archived_complaints': {}
})
support_config = load_json(SUPPORT_CONFIG_FILE, {})
support_admins = load_json(SUPPORT_ADMINS_FILE, {})
verify_roles_data = load_json(VERIFY_ROLES_FILE, {})
lockdown_data = load_json(LOCKDOWN_FILE, {})
timers_data = load_json(TIMERS_FILE, {})
coins_data = load_json(COINS_FILE, {})
work_cooldown = load_json(WORK_COOLDOWN_FILE, {})
message_stats = load_json(MESSAGE_STATS_FILE, {})
temprole_data = load_json(TEMPROLE_FILE, {})

ticket_counter_lock = threading.Lock()
complaint_counter_lock = threading.Lock()
data_save_lock = threading.Lock()

last_moderator_cache: Dict[str, Tuple[int, datetime]] = {}
CACHE_TTL = 60

message_history: Dict[str, deque] = {}
join_history: Dict[int, deque] = {}
raid_active: Dict[int, bool] = {}
raid_level: Dict[int, int] = {}
suspicion_active: Dict[int, bool] = {}

START_TIME = datetime.now()

def save_warns():
    with data_save_lock:
        save_json(WARNS_FILE, warns)

def save_logs_config():
    with data_save_lock:
        save_json(LOGS_FILE, logs_config)

def save_spam_count():
    with data_save_lock:
        save_json(SPAM_COUNT_FILE, spam_count)

def save_commands_access():
    with data_save_lock:
        save_json(COMMANDS_ACCESS_FILE, commands_access)

def save_raid_config():
    with data_save_lock:
        save_json(RAID_CONFIG_FILE, raid_config)

def save_role_permissions():
    with data_save_lock:
        save_json(ROLE_PERMISSIONS_FILE, role_permissions)

def save_channel_backup():
    with data_save_lock:
        save_json(CHANNEL_BACKUP_FILE, channel_backup)

def save_tickets():
    with data_save_lock:
        save_json(TICKETS_FILE, tickets_data)

def save_support_config():
    with data_save_lock:
        save_json(SUPPORT_CONFIG_FILE, support_config)

def save_support_admins():
    with data_save_lock:
        save_json(SUPPORT_ADMINS_FILE, support_admins)

def save_verify_roles():
    with data_save_lock:
        save_json(VERIFY_ROLES_FILE, verify_roles_data)

def save_lockdown():
    with data_save_lock:
        save_json(LOCKDOWN_FILE, lockdown_data)

def save_timers():
    with data_save_lock:
        save_json(TIMERS_FILE, timers_data)

def save_coins():
    with data_save_lock:
        save_json(COINS_FILE, coins_data)

def save_work_cooldown():
    with data_save_lock:
        save_json(WORK_COOLDOWN_FILE, work_cooldown)

def save_message_stats():
    with data_save_lock:
        save_json(MESSAGE_STATS_FILE, message_stats)

def save_temprole():
    with data_save_lock:
        save_json(TEMPROLE_FILE, temprole_data)

def format_ticket_number(num: int) -> str:
    return f"Т-{str(num // 100).zfill(2)}-{str(num % 100).zfill(2)}"

def format_complaint_number(num: int) -> str:
    return f"Ж-{str(num // 100).zfill(2)}-{str(num % 100).zfill(2)}"

def can_target(user: discord.Member, target: discord.Member) -> bool:
    if user.guild_permissions.administrator:
        return True
    if target.guild_permissions.administrator:
        return False
    if target == user.guild.owner:
        return False
    if user.top_role <= target.top_role:
        return False
    return True

def can_mute_target(user: discord.Member, target: discord.Member) -> bool:
    if user.guild_permissions.administrator:
        return True
    if target.guild_permissions.administrator:
        return False
    if target == user.guild.owner:
        return False
    if user.top_role <= target.top_role:
        return False
    return True

def can_manage_support(user: discord.Member, guild_id: int) -> bool:
    if user.guild_permissions.administrator:
        return True
    guild_id_str = str(guild_id)
    if guild_id_str in support_admins:
        for role_id in support_admins[guild_id_str].get('roles', []):
            role = user.guild.get_role(int(role_id))
            if role and role in user.roles:
                return True
    return False

def can_manage_role(user: discord.Member, target_role: discord.Role, bot_member: discord.Member) -> bool:
    if user.guild_permissions.administrator:
        return True
    if target_role >= bot_member.top_role:
        return False
    if target_role >= user.top_role:
        return False
    guild_id_str = str(user.guild.id)
    if guild_id_str not in role_permissions:
        return False
    if str(user.id) in role_permissions[guild_id_str].get('users', {}):
        if str(target_role.id) in role_permissions[guild_id_str]['users'][str(user.id)]:
            return True
    for role in user.roles:
        if str(role.id) in role_permissions[guild_id_str].get('roles', {}):
            if str(target_role.id) in role_permissions[guild_id_str]['roles'][str(role.id)]:
                return True
    return False

def has_command_access(user: discord.Member, command_name: str) -> bool:
    if user.guild_permissions.administrator:
        return True
    guild_id_str = str(user.guild.id)
    if guild_id_str in commands_access:
        if command_name in commands_access[guild_id_str]:
            access = commands_access[guild_id_str][command_name]
            if str(user.id) in access.get('users', []):
                return True
            for role in user.roles:
                if str(role.id) in access.get('roles', []):
                    return True
    return False

def is_night_time(guild_id: int = None) -> bool:
    current_hour = datetime.now(timezone.utc).hour
    return 0 <= current_hour < 6

def is_new_account(created_at: datetime) -> int:
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=timezone.utc)
    account_age = datetime.now(timezone.utc) - created_at
    return account_age.days

def get_support_channel(guild):
    if str(guild.id) in support_config:
        channel_id = support_config[str(guild.id)].get('ticket_channel')
        if channel_id:
            return guild.get_channel(channel_id)
    return None

def get_moderation_channel(guild):
    if str(guild.id) in support_config:
        channel_id = support_config[str(guild.id)].get('complaint_channel')
        if channel_id:
            return guild.get_channel(channel_id)
    return None

def has_link(text: str) -> bool:
    url_pattern = r'(https?://[^\s]+|www\.[^\s]+|discord\.gg/[^\s]+|discord\.com/invite/[^\s]+)'
    return re.search(url_pattern, text.lower()) is not None

def is_valid_emoji(emoji: str) -> bool:
    emoji_pattern = re.compile(r'^[\U0001F600-\U0001F64F\U0001F300-\U0001F5FF\U0001F680-\U0001F6FF\U0001F1E0-\U0001F1FF\u2600-\u26FF\u2700-\u27BF\U0001F900-\U0001F9FF\U0001FA70-\U0001FAFF]+$')
    if emoji_pattern.match(emoji):
        return True
    custom_pattern = re.compile(r'^<a?:[a-zA-Z0-9_]+:[0-9]+>$')
    if custom_pattern.match(emoji):
        return True
    if len(emoji) == 1 and ord(emoji) > 255:
        return True
    return False

def get_time_seconds(value: int, unit: str) -> int:
    multipliers = {"seconds": 1, "minutes": 60, "hours": 3600, "days": 86400}
    return value * multipliers.get(unit, 1)

def can_create_ticket(user_id: int, guild_id: int) -> Tuple[bool, int]:
    active_tickets = 0
    for ticket_data in tickets_data['tickets'].values():
        if ticket_data.get('user_id') == user_id and ticket_data.get('status') in ['open', 'in_progress']:
            active_tickets += 1
    return active_tickets < MAX_USER_TICKETS, active_tickets

def cleanup_old_history():
    now = datetime.now()
    for user_id in list(message_history.keys()):
        history = message_history[user_id]
        while history and (now - history[0]['time']).total_seconds() > 60:
            history.popleft()

def cleanup_old_join_history(guild_id: int):
    now = datetime.now()
    if guild_id in join_history:
        history = join_history[guild_id]
        while history and (now - history[0]).total_seconds() > 300:
            history.popleft()

def get_cached_moderator(guild_id: int, target_id: int, action: str) -> Optional[discord.Member]:
    cache_key = f"{guild_id}_{target_id}_{action}"
    if cache_key in last_moderator_cache:
        moderator_id, timestamp = last_moderator_cache[cache_key]
        if (datetime.now() - timestamp).total_seconds() < CACHE_TTL:
            guild = bot.get_guild(guild_id)
            if guild:
                return guild.get_member(moderator_id)
    return None

def cache_moderator(guild_id: int, target_id: int, action: str, moderator_id: int):
    cache_key = f"{guild_id}_{target_id}_{action}"
    last_moderator_cache[cache_key] = (moderator_id, datetime.now())

def get_balance(user_id: int) -> int:
    return coins_data.get(str(user_id), 0)

def add_balance(user_id: int, amount: int):
    coins_data[str(user_id)] = get_balance(user_id) + amount
    save_coins()

def remove_balance(user_id: int, amount: int) -> bool:
    if get_balance(user_id) >= amount:
        coins_data[str(user_id)] = get_balance(user_id) - amount
        save_coins()
        return True
    return False

def get_user_message_stats(user_id: int) -> Dict:
    return message_stats.get(str(user_id), {
        'total': 0,
        'month': 0,
        'week': 0,
        'last_month_reset': datetime.now().isoformat(),
        'last_week_reset': datetime.now().isoformat()
    })

def update_message_stats(user_id: int):
    stats = get_user_message_stats(user_id)
    stats['total'] += 1
    stats['month'] += 1
    stats['week'] += 1
    message_stats[str(user_id)] = stats
    save_message_stats()

def get_user_punishments(guild_id: int, user_id: int) -> List[Dict]:
    punishments = []
    guild_id_str = str(guild_id)
    user_id_str = str(user_id)
    
    if guild_id_str in warns and user_id_str in warns[guild_id_str]:
        for warn in warns[guild_id_str][user_id_str]:
            punishments.append({
                'type': 'warn',
                'reason': warn['reason'],
                'date': warn['date'],
                'moderator': warn['moderator'],
                'id': warn['id']
            })
    
    return punishments

async def send_log(guild_id: int, embed: discord.Embed):
    if str(guild_id) in logs_config:
        channel_id = logs_config[str(guild_id)]
        channel = bot.get_channel(channel_id)
        if channel:
            try:
                await channel.send(embed=embed)
            except Exception as e:
                logger.error(f"Ошибка отправки лога: {e}")

async def send_dm(user: discord.User, title: str, reason: str, guild_name: str, warn_count: int = None):
    try:
        embed = discord.Embed(title=title, color=discord.Color.red(), timestamp=datetime.now())
        embed.add_field(name="Сервер", value=guild_name, inline=False)
        if warn_count is not None:
            embed.add_field(name='Предупреждений', value=f'{warn_count}/3', inline=False)
        embed.add_field(name='Причина', value=reason, inline=False)
        await user.send(embed=embed)
    except:
        pass

async def backup_channel(channel) -> bool:
    if isinstance(channel, discord.TextChannel):
        backup_data = {
            'name': channel.name, 'type': 'text', 'position': channel.position,
            'topic': channel.topic, 'slowmode_delay': channel.slowmode_delay,
            'is_nsfw': channel.is_nsfw(), 'category_id': channel.category.id if channel.category else None,
            'permissions': [], 'messages': []
        }
        for target, overwrite in channel.overwrites.items():
            perm_data = {
                'target_type': 'role' if isinstance(target, discord.Role) else 'member',
                'target_id': target.id,
                'target_name': target.name if isinstance(target, discord.Role) else str(target),
                'allow': overwrite.pair()[0].value,
                'deny': overwrite.pair()[1].value
            }
            backup_data['permissions'].append(perm_data)
        try:
            async for message in channel.history(limit=200):
                msg_data = {
                    'id': message.id, 'author_id': message.author.id, 'author_name': str(message.author),
                    'content': message.content, 'created_at': message.created_at.isoformat(),
                    'attachments': [a.url for a in message.attachments]
                }
                backup_data['messages'].append(msg_data)
        except:
            pass
        channel_backup[str(channel.id)] = backup_data
        save_channel_backup()
        return True
    return False

async def restore_channel(guild, channel_id: int):
    if str(channel_id) not in channel_backup:
        return None
    backup = channel_backup[str(channel_id)]
    category = None
    if backup.get('category_id'):
        category = guild.get_channel(backup['category_id'])
    try:
        if backup['type'] == 'text':
            new_channel = await guild.create_text_channel(
                name=backup['name'], category=category, position=backup['position'],
                topic=backup.get('topic', ''), slowmode_delay=backup.get('slowmode_delay', 0),
                nsfw=backup.get('is_nsfw', False)
            )
            for perm_data in backup.get('permissions', []):
                try:
                    if perm_data['target_type'] == 'role':
                        target = guild.get_role(perm_data['target_id'])
                    else:
                        target = guild.get_member(perm_data['target_id'])
                    if target:
                        allow = discord.Permissions(perm_data['allow'])
                        deny = discord.Permissions(perm_data['deny'])
                        await new_channel.set_permissions(target, overwrite=discord.PermissionOverwrite.from_pair(allow, deny))
                except:
                    pass
            for msg_data in reversed(backup.get('messages', [])):
                try:
                    await new_channel.send(msg_data['content'])
                    await asyncio.sleep(0.2)
                except:
                    pass
            return new_channel
    except Exception as e:
        logger.error(f'Ошибка восстановления канала: {e}')
        return None

async def schedule_unmute(guild_id: int, user_id: int, until: datetime):
    timer_key = f"unmute_{guild_id}_{user_id}"
    timers_data[timer_key] = {'type': 'unmute', 'guild_id': guild_id, 'user_id': user_id, 'until': until.isoformat()}
    save_timers()
    now = datetime.now(timezone.utc)
    wait_seconds = (until - now).total_seconds()
    if wait_seconds > 0:
        await asyncio.sleep(wait_seconds)
        if timer_key in timers_data:
            del timers_data[timer_key]
            save_timers()
            guild = bot.get_guild(guild_id)
            if guild:
                user = guild.get_member(user_id)
                if user and user.timed_out_until:
                    try:
                        await user.timeout(None)
                    except:
                        pass

async def schedule_unwarn(guild_id: int, user_id: int, warn_id: int, warn_number: int, expires_at: str):
    timer_key = f"unwarn_{guild_id}_{user_id}_{warn_id}"
    timers_data[timer_key] = {'type': 'unwarn', 'guild_id': guild_id, 'user_id': user_id, 'warn_id': warn_id, 'warn_number': warn_number, 'expires_at': expires_at}
    save_timers()
    
    if warn_number == 1:
        days = WARN1_EXPIRE_DAYS
    else:
        days = WARN2_EXPIRE_DAYS
    
    await asyncio.sleep(days * 24 * 60 * 60)
    
    if timer_key in timers_data:
        del timers_data[timer_key]
        save_timers()
        guild_id_str = str(guild_id)
        user_id_str = str(user_id)
        if guild_id_str in warns and user_id_str in warns[guild_id_str]:
            user_warns = warns[guild_id_str][user_id_str]
            warn_to_remove = None
            for warn in user_warns:
                if warn['id'] == warn_id:
                    warn_to_remove = warn
                    break
            if warn_to_remove:
                user_warns.remove(warn_to_remove)
                for i, warn in enumerate(user_warns, 1):
                    warn['id'] = i
                if not user_warns:
                    del warns[guild_id_str][user_id_str]
                save_warns()

async def schedule_temprole(guild_id: int, user_id: int, role_id: int, duration_seconds: int):
    timer_key = f"temprole_{guild_id}_{user_id}_{role_id}"
    temprole_data[timer_key] = {
        'type': 'temprole',
        'guild_id': guild_id,
        'user_id': user_id,
        'role_id': role_id,
        'until': (datetime.now() + timedelta(seconds=duration_seconds)).isoformat()
    }
    save_temprole()
    
    await asyncio.sleep(duration_seconds)
    
    if timer_key in temprole_data:
        del temprole_data[timer_key]
        save_temprole()
        guild = bot.get_guild(guild_id)
        if guild:
            user = guild.get_member(user_id)
            role = guild.get_role(role_id)
            if user and role and role in user.roles:
                try:
                    await user.remove_roles(role, reason="Временная роль истекла")
                except:
                    pass

async def add_warn_and_check(guild_id: int, user_id: int, moderator_id: int, reason: str, guild_name: str, is_auto: bool = False):
    if not reason or not reason.strip():
        reason = "Не указана"
    user_id_str = str(user_id)
    guild_id_str = str(guild_id)
    if guild_id_str not in warns:
        warns[guild_id_str] = {}
    if user_id_str not in warns[guild_id_str]:
        warns[guild_id_str][user_id_str] = []
    warn_count = len(warns[guild_id_str][user_id_str])
    if warn_count >= MAX_WARNS:
        return False, warn_count
    warn_id = warn_count + 1
    warn_number = warn_count + 1
    
    if warn_number == 1:
        days = WARN1_EXPIRE_DAYS
    else:
        days = WARN2_EXPIRE_DAYS
    
    expires_at = (datetime.now() + timedelta(days=days)).isoformat()
    warn_data = {'id': warn_id, 'reason': reason, 'moderator': moderator_id, 'date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'expires_at': expires_at, 'warn_number': warn_number}
    warns[guild_id_str][user_id_str].append(warn_data)
    save_warns()
    asyncio.create_task(schedule_unwarn(guild_id, user_id, warn_id, warn_number, expires_at))
    new_warn_count = len(warns[guild_id_str][user_id_str])
    guild = bot.get_guild(guild_id)
    user = guild.get_member(user_id) if guild else None
    moderator = guild.get_member(moderator_id) if guild else None
    if user:
        await send_dm(user, 'ВЫ ПОЛУЧИЛИ ПРЕДУПРЕЖДЕНИЕ', reason, guild_name, new_warn_count)
    if moderator and not is_auto:
        log_embed = discord.Embed(title='ВЫДАНО ПРЕДУПРЕЖДЕНИЕ', color=discord.Color.orange(), timestamp=datetime.now())
        if user:
            log_embed.set_thumbnail(url=user.avatar.url if user.avatar else user.default_avatar.url)
            log_embed.add_field(name='Участник', value=f'{user.mention}\n`{user}`', inline=True)
        log_embed.add_field(name='Модератор', value=f'{moderator.mention}\n`{moderator}`', inline=True)
        log_embed.add_field(name='Предупреждений', value=f'**{new_warn_count}/3**', inline=True)
        log_embed.add_field(name='Причина', value=f'```{reason}```', inline=False)
        log_embed.set_footer(text=f'ID предупреждения: {warn_id} | Снимется через {days} дней')
        await send_log(guild_id, log_embed)
    if new_warn_count >= MAX_WARNS:
        timeout_until = discord.utils.utcnow() + timedelta(hours=MUTE_ON_MAX_WARNS_HOURS)
        try:
            await user.edit(timed_out_until=timeout_until, reason=f'{MAX_WARNS}/3 предупреждений: {reason}')
        except:
            try:
                await user.timeout(timeout_until, reason=f'{MAX_WARNS}/3 предупреждений: {reason}')
            except:
                pass
        await send_dm(user, 'ВЫ ЗАМУЧЕНЫ АВТОМАТИЧЕСКИ', f'{MAX_WARNS}/3 предупреждений. Причина: {reason}', guild_name, MAX_WARNS)
        warns[guild_id_str][user_id_str] = []
        save_warns()
        log_mute_embed = discord.Embed(title='АВТОМАТИЧЕСКИЙ МУТ', color=discord.Color.dark_red(), timestamp=datetime.now())
        if user:
            log_mute_embed.add_field(name='Участник', value=f'{user.mention}\n`{user}`', inline=True)
        log_mute_embed.add_field(name='Длительность', value=f'{MUTE_ON_MAX_WARNS_HOURS} часов', inline=True)
        log_mute_embed.add_field(name='Причина', value='Достигнуто 3 предупреждения', inline=False)
        await send_log(guild_id, log_mute_embed)
        return True, new_warn_count
    return False, new_warn_count

async def activate_lockdown(guild_id: int, reason: str):
    guild = bot.get_guild(guild_id)
    if not guild:
        return
    raid_active[guild_id] = True
    lockdown_data[str(guild_id)] = {'active': True, 'start_time': datetime.now().isoformat(), 'reason': reason, 'original_perms': {}}
    save_lockdown()
    for channel in guild.channels:
        if isinstance(channel, discord.TextChannel):
            is_admin_channel = False
            for target, overwrite in channel.overwrites.items():
                if isinstance(target, discord.Role) and target.permissions.administrator:
                    is_admin_channel = True
                    break
                elif isinstance(target, discord.Member) and target.guild_permissions.administrator:
                    is_admin_channel = True
                    break
            if is_admin_channel or channel.is_news():
                continue
            try:
                overwrite = channel.overwrites_for(guild.default_role)
                lockdown_data[str(guild_id)]['original_perms'][str(channel.id)] = {'send_messages': overwrite.send_messages, 'add_reactions': overwrite.add_reactions}
                overwrite.send_messages = False
                overwrite.add_reactions = False
                await channel.set_permissions(guild.default_role, overwrite=overwrite)
                await channel.edit(slowmode_delay=LOCKDOWN_SLOWMODE)
            except:
                pass
    for channel in guild.voice_channels:
        try:
            overwrite = channel.overwrites_for(guild.default_role)
            lockdown_data[str(guild_id)]['original_perms'][str(channel.id)] = {'connect': overwrite.connect}
            overwrite.connect = False
            await channel.set_permissions(guild.default_role, overwrite=overwrite)
        except:
            pass
    try:
        invites = await guild.invites()
        for invite in invites:
            try:
                await invite.delete()
            except:
                pass
        lockdown_data[str(guild_id)]['invites_deleted'] = True
        save_lockdown()
    except:
        pass
    embed = discord.Embed(title='РЕЖИМ LOCKDOWN АКТИВИРОВАН', color=discord.Color.dark_red(), timestamp=datetime.now())
    embed.add_field(name='Причина', value=reason, inline=False)
    embed.add_field(name='Длительность', value='35 минут', inline=False)
    await send_log(guild_id, embed)
    await asyncio.sleep(LOCKDOWN_DURATION)
    if raid_active.get(guild_id, False):
        await deactivate_lockdown(guild_id)

async def deactivate_lockdown(guild_id: int):
    guild = bot.get_guild(guild_id)
    if not guild:
        return
    raid_active[guild_id] = False
    if str(guild_id) in lockdown_data:
        config = lockdown_data[str(guild_id)]
        for channel_id, perms in config.get('original_perms', {}).items():
            channel = guild.get_channel(int(channel_id))
            if channel:
                try:
                    overwrite = channel.overwrites_for(guild.default_role)
                    if 'send_messages' in perms:
                        overwrite.send_messages = perms['send_messages']
                    if 'add_reactions' in perms:
                        overwrite.add_reactions = perms['add_reactions']
                    if 'connect' in perms:
                        overwrite.connect = perms['connect']
                    await channel.set_permissions(guild.default_role, overwrite=overwrite)
                    await channel.edit(slowmode_delay=0)
                except:
                    pass
        lockdown_data.pop(str(guild_id))
        save_lockdown()
    embed = discord.Embed(title='РЕЖИМ LOCKDOWN ОТКЛЮЧЕН', color=discord.Color.green(), timestamp=datetime.now())
    await send_log(guild_id, embed)

async def check_raid(guild_id: int):
    current_time = datetime.now()
    if guild_id not in join_history:
        join_history[guild_id] = deque(maxlen=MAX_JOIN_HISTORY)
    cleanup_old_join_history(guild_id)
    joins_15s = len([t for t in join_history[guild_id] if (current_time - t).total_seconds() < 15])
    joins_60s = len([t for t in join_history[guild_id] if (current_time - t).total_seconds() < 60])
    joins_120s = len([t for t in join_history[guild_id] if (current_time - t).total_seconds() < 120])
    if guild_id not in raid_active:
        raid_active[guild_id] = False
        raid_level[guild_id] = 0
        suspicion_active[guild_id] = False
    night_multiplier = 1.5 if is_night_time(guild_id) else 1
    suspicion_threshold = int(RAID_SUSPICION_THRESHOLD * night_multiplier)
    if not raid_active[guild_id]:
        if joins_15s >= suspicion_threshold:
            if not suspicion_active[guild_id]:
                suspicion_active[guild_id] = True
                embed = discord.Embed(title='ПОДОЗРЕНИЕ НА РЕЙД', color=discord.Color.orange(), timestamp=datetime.now())
                embed.add_field(name='Статистика', value=f'{joins_15s} человек за 15 секунд', inline=False)
                await send_log(guild_id, embed)
                guild = bot.get_guild(guild_id)
                if guild:
                    for channel in guild.text_channels:
                        try:
                            if channel.permissions_for(guild.me).manage_channels:
                                await channel.edit(slowmode_delay=5)
                        except:
                            pass
        if joins_60s >= RAID_ACTIVATE_THRESHOLD:
            raid_active[guild_id] = True
            raid_level[guild_id] = 2
            await activate_lockdown(guild_id, 'рейд')
            embed = discord.Embed(title='ОБНАРУЖЕН РЕЙД!', color=discord.Color.red(), timestamp=datetime.now())
            embed.add_field(name='Статистика', value=f'{joins_60s} человек за 60 секунд', inline=False)
            await send_log(guild_id, embed)
        elif joins_120s >= RAID_HARD_THRESHOLD:
            raid_active[guild_id] = True
            raid_level[guild_id] = 3
            await activate_lockdown(guild_id, 'жесткий рейд')
            guild = bot.get_guild(guild_id)
            if guild:
                bot_member = guild.me
                for member in guild.members:
                    if member.id == bot_member.id:
                        continue
                    account_days = (datetime.now(timezone.utc) - member.created_at.replace(tzinfo=timezone.utc)).days
                    if account_days < 1:
                        try:
                            await member.ban(reason='Анти-рейд: аккаунт младше 1 дня')
                        except:
                            pass
                    elif account_days < 7:
                        try:
                            await member.kick(reason='Анти-рейд: аккаунт младше 7 дней')
                        except:
                            pass
            embed = discord.Embed(title='ЖЕСТКИЙ РЕЙД ОБНАРУЖЕН!', color=discord.Color.dark_red(), timestamp=datetime.now())
            embed.add_field(name='Статистика', value=f'{joins_120s} человек за 120 секунд', inline=False)
            await send_log(guild_id, embed)

async def restore_lockdown_state():
    for guild_id_str, data in lockdown_data.items():
        if data.get('active', False):
            start_time = datetime.fromisoformat(data['start_time'])
            elapsed = (datetime.now() - start_time).total_seconds()
            if elapsed < LOCKDOWN_DURATION:
                remaining = LOCKDOWN_DURATION - elapsed
                raid_active[int(guild_id_str)] = True
                asyncio.create_task(delayed_deactivate(int(guild_id_str), remaining))
            else:
                await deactivate_lockdown(int(guild_id_str))
    
    for timer_key, timer_data in list(timers_data.items()):
        if timer_data.get('type') == 'unwarn':
            expires_at = datetime.fromisoformat(timer_data['expires_at'])
            if datetime.now() >= expires_at:
                guild_id = timer_data['guild_id']
                user_id = timer_data['user_id']
                warn_id = timer_data['warn_id']
                del timers_data[timer_key]
                save_timers()
                
                guild_id_str = str(guild_id)
                user_id_str = str(user_id)
                if guild_id_str in warns and user_id_str in warns[guild_id_str]:
                    user_warns = warns[guild_id_str][user_id_str]
                    warn_to_remove = None
                    for warn in user_warns:
                        if warn['id'] == warn_id:
                            warn_to_remove = warn
                            break
                    if warn_to_remove:
                        user_warns.remove(warn_to_remove)
                        for i, warn in enumerate(user_warns, 1):
                            warn['id'] = i
                        if not user_warns:
                            del warns[guild_id_str][user_id_str]
                        save_warns()
            else:
                remaining = (expires_at - datetime.now()).total_seconds()
                if remaining > 0:
                    guild_id = timer_data['guild_id']
                    user_id = timer_data['user_id']
                    warn_id = timer_data['warn_id']
                    warn_number = timer_data.get('warn_number', 1)
                    asyncio.create_task(schedule_unwarn(guild_id, user_id, warn_id, warn_number, timer_data['expires_at']))
        elif timer_data.get('type') == 'unmute':
            until = datetime.fromisoformat(timer_data['until'])
            if datetime.now(timezone.utc) >= until:
                guild_id = timer_data['guild_id']
                user_id = timer_data['user_id']
                del timers_data[timer_key]
                save_timers()
                guild = bot.get_guild(guild_id)
                if guild:
                    user = guild.get_member(user_id)
                    if user and user.timed_out_until:
                        try:
                            await user.timeout(None)
                        except:
                            pass
            else:
                guild_id = timer_data['guild_id']
                user_id = timer_data['user_id']
                until = datetime.fromisoformat(timer_data['until'])
                asyncio.create_task(schedule_unmute(guild_id, user_id, until))
    
    for timer_key, timer_data in list(temprole_data.items()):
        if timer_data.get('type') == 'temprole':
            until = datetime.fromisoformat(timer_data['until'])
            if datetime.now() >= until:
                guild_id = timer_data['guild_id']
                user_id = timer_data['user_id']
                role_id = timer_data['role_id']
                del temprole_data[timer_key]
                save_temprole()
                guild = bot.get_guild(guild_id)
                if guild:
                    user = guild.get_member(user_id)
                    role = guild.get_role(role_id)
                    if user and role and role in user.roles:
                        try:
                            await user.remove_roles(role, reason="Временная роль истекла")
                        except:
                            pass
            else:
                remaining = (until - datetime.now()).total_seconds()
                if remaining > 0:
                    guild_id = timer_data['guild_id']
                    user_id = timer_data['user_id']
                    role_id = timer_data['role_id']
                    asyncio.create_task(schedule_temprole(guild_id, user_id, role_id, remaining))

async def delayed_deactivate(guild_id: int, delay: float):
    await asyncio.sleep(delay)
    await deactivate_lockdown(guild_id)

@tasks.loop(minutes=5)
async def auto_save_all():
    save_warns()
    save_tickets()
    save_spam_count()
    save_commands_access()
    save_role_permissions()
    save_lockdown()
    save_timers()
    save_coins()
    save_work_cooldown()
    save_message_stats()
    save_temprole()
    cleanup_old_history()

@tasks.loop(minutes=5)
async def cleanup_history_task():
    cleanup_old_history()

async def reset_monthly_stats():
    now = datetime.now()
    for user_id, stats in message_stats.items():
        last_reset = datetime.fromisoformat(stats.get('last_month_reset', now.isoformat()))
        if (now - last_reset).days >= 30:
            stats['month'] = 0
            stats['last_month_reset'] = now.isoformat()
    save_message_stats()

async def reset_weekly_stats():
    now = datetime.now()
    for user_id, stats in message_stats.items():
        last_reset = datetime.fromisoformat(stats.get('last_week_reset', now.isoformat()))
        if (now - last_reset).days >= 7:
            stats['week'] = 0
            stats['last_week_reset'] = now.isoformat()
    save_message_stats()

class RoleButtonView(View):
    def __init__(self, roles_data: list, message_id: int):
        super().__init__(timeout=None)
        self.roles_data = roles_data
        self.message_id = message_id
        seen_pairs = set()
        for item in roles_data:
            pair_key = (item['role_id'], item['emoji'])
            if pair_key in seen_pairs:
                continue
            seen_pairs.add(pair_key)
            button = Button(label=item['label'], style=discord.ButtonStyle.primary, emoji=item['emoji'])
            button.callback = self.create_callback(item['role_id'])
            self.add_item(button)
    def create_callback(self, role_id):
        async def callback(interaction: discord.Interaction):
            role = interaction.guild.get_role(role_id)
            if role:
                if role in interaction.user.roles:
                    await interaction.user.remove_roles(role)
                    await interaction.response.send_message(f"Роль {role.mention} снята!", ephemeral=True)
                else:
                    await interaction.user.add_roles(role)
                    await interaction.response.send_message(f"Вам выдана роль {role.mention}!", ephemeral=True)
            else:
                await interaction.response.send_message("Роль не найдена", ephemeral=True)
        return callback

class SupportModal(Modal):
    def __init__(self):
        super().__init__(title="📨 Создание обращения в поддержку")
        self.description = TextInput(label="📝 Подробное описание", placeholder="Подробно опишите свой вопрос...", style=discord.TextStyle.paragraph, required=True, max_length=1500)
        self.add_item(self.description)
        self.attachments = TextInput(label="🔗 Доказательства", placeholder="Ссылки на скриншоты или сообщения (не обязательно)", required=False, max_length=500)
        self.add_item(self.attachments)
    
    async def on_submit(self, interaction: discord.Interaction):
        if interaction.user.bot:
            await interaction.response.send_message("❌ Боты не могут создавать обращения", ephemeral=True)
            return
        can_create, active_count = can_create_ticket(interaction.user.id, interaction.guild.id)
        if not can_create:
            await interaction.response.send_message(f"❌ У вас уже есть {active_count} активных обращений. Максимум - {MAX_USER_TICKETS}", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        async with asyncio.Lock():
            with ticket_counter_lock:
                tickets_data['ticket_counter'] += 1
                ticket_num = tickets_data['ticket_counter']
            ticket_id = format_ticket_number(ticket_num)
            tickets_data['tickets'][ticket_id] = {
                'user_id': interaction.user.id, 'user_name': str(interaction.user),
                'topic': "Без темы", 'description': self.description.value,
                'attachments': self.attachments.value, 'status': 'open',
                'created_at': datetime.now().isoformat(), 'has_response': False, 'messages': []
            }
            save_tickets()
        embed = discord.Embed(
            title=f"🎫 {ticket_id}",
            description=f"**📌 Тема:** Без темы\n**🟢 Статус:** Ожидает рассмотрения\n**👤 Создатель:** {interaction.user.mention}\n**📅 Создан:** <t:{int(datetime.now().timestamp())}:F>",
            color=discord.Color.orange(),
            timestamp=datetime.now()
        )
        embed.add_field(name="📝 Описание", value=self.description.value[:1024], inline=False)
        if self.attachments.value:
            embed.add_field(name="🔗 Доказательства", value=self.attachments.value, inline=False)
        embed.set_footer(text=f"ID: {ticket_id}")
        support_channel = get_support_channel(interaction.guild)
        if support_channel:
            view = TicketControlView(ticket_id, interaction.user.id, interaction.guild.id, is_complaint=False)
            await support_channel.send(embed=embed, view=view)
            await interaction.followup.send(f"✅ Обращение **{ticket_id}** создано!", ephemeral=True)
        else:
            await interaction.followup.send("❌ Канал для тикетов не настроен. Используйте `!setup-support`", ephemeral=True)

class ComplaintModal(Modal):
    def __init__(self, target_user: discord.Member = None):
        super().__init__(title="⚠️ Подача жалобы")
        self.target_user_id = TextInput(label="🆔 Дискорд юз нарушителя", placeholder="Уажите дискорд юз нарушителя через @", required=True, max_length=20)
        self.add_item(self.target_user_id)
        self.reason = TextInput(label="📋 Причина жалобы", placeholder="Укажите пункты правил через запятую которые были нарушены", required=True, max_length=30)
        self.add_item(self.reason)
        self.description = TextInput(label="📝 Подробное описание", placeholder="Подробно опишите ситуацию которая произошла на сервере", style=discord.TextStyle.paragraph, required=True, max_length=1500)
        self.add_item(self.description)
        self.evidence = TextInput(label="🔗 Доказательства", placeholder="Ссылка на сообщение, или ссылка на скриншоты нарушения (Imgur/Yapix)", required=False, max_length=500)
        self.add_item(self.evidence)
        if target_user:
            self.target_user_id.default = str(target_user.id)
    async def on_submit(self, interaction: discord.Interaction):
        if interaction.user.bot:
            await interaction.response.send_message("❌ Боты не могут создавать жалобы", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        async with asyncio.Lock():
            with complaint_counter_lock:
                tickets_data['complaint_counter'] += 1
                complaint_num = tickets_data['complaint_counter']
            complaint_id = format_complaint_number(complaint_num)
            target_id = self.target_user_id.value
            target_user = None
            if target_id.isdigit():
                target_user = interaction.guild.get_member(int(target_id))
            tickets_data['complaints'][complaint_id] = {
                'user_id': interaction.user.id, 'user_name': str(interaction.user),
                'target_id': int(target_id) if target_id.isdigit() else None,
                'target_name': target_user.name if target_user else target_id,
                'reason': self.reason.value, 'description': self.description.value,
                'evidence': self.evidence.value, 'status': 'open',
                'created_at': datetime.now().isoformat(), 'has_response': False, 'messages': []
            }
            save_tickets()
        embed = discord.Embed(
            title=f"⚠️ {complaint_id}",
            description=f"**👤 Жалоба на:** {target_user.mention if target_user else target_id}\n**📋 Причина:** {self.reason.value}\n**🟡 Статус:** Рассматривается\n**👤 Жалобщик:** {interaction.user.mention}",
            color=discord.Color.red(),
            timestamp=datetime.now()
        )
        embed.add_field(name="📝 Описание", value=self.description.value[:1024], inline=False)
        if self.evidence.value:
            embed.add_field(name="🔗 Доказательства", value=self.evidence.value, inline=False)
        embed.set_footer(text=f"ID: {complaint_id}")
        mod_channel = get_moderation_channel(interaction.guild)
        if mod_channel:
            view = TicketControlView(complaint_id, interaction.user.id, interaction.guild.id, is_complaint=True)
            await mod_channel.send(embed=embed, view=view)
            await interaction.followup.send(f"✅ Жалоба **{complaint_id}** передана модераторам!", ephemeral=True)
        else:
            await interaction.followup.send("❌ Канал для жалоб не настроен. Используйте `!setup-support`", ephemeral=True)

class SupportView(View):
    def __init__(self, guild_id: int):
        super().__init__(timeout=None)
        self.guild_id = guild_id
    
    @discord.ui.button(label="Обратиться в поддержку", style=discord.ButtonStyle.primary, emoji="📨")
    async def support_button(self, interaction: discord.Interaction, button: Button):
        if interaction.user.bot:
            await interaction.response.send_message("❌ Боты не могут создавать обращения", ephemeral=True)
            return
        modal = SupportModal()
        await interaction.response.send_modal(modal)
    
    @discord.ui.button(label="Пожаловаться на пользователя", style=discord.ButtonStyle.danger, emoji="⚠️")
    async def complaint_button(self, interaction: discord.Interaction, button: Button):
        if interaction.user.bot:
            await interaction.response.send_message("❌ Боты не могут создавать жалобы", ephemeral=True)
            return
        modal = ComplaintModal()
        await interaction.response.send_modal(modal)
    
    @discord.ui.button(label="Мои обращения", style=discord.ButtonStyle.secondary, emoji="📊")
    async def my_tickets_button(self, interaction: discord.Interaction, button: Button):
        user_tickets = []
        for ticket_id, ticket_data in tickets_data['tickets'].items():
            if ticket_data.get('user_id') == interaction.user.id and ticket_data.get('status') in ['open', 'in_progress']:
                user_tickets.append((ticket_id, ticket_data, 'ticket'))
        for complaint_id, complaint_data in tickets_data['complaints'].items():
            if complaint_data.get('user_id') == interaction.user.id and complaint_data.get('status') in ['open', 'in_progress']:
                user_tickets.append((complaint_id, complaint_data, 'complaint'))
        
        if not user_tickets:
            await interaction.response.send_message("📭 У вас нет активных обращений", ephemeral=True)
            return
        
        embed = discord.Embed(title=f"📋 АКТИВНЫЕ ОБРАЩЕНИЯ ({len(user_tickets)})", color=discord.Color.blue())
        for item_id, item_data, item_type in user_tickets:
            status_text = "🟡 Ожидает" if item_data.get('status') == 'open' else "🟢 В работе"
            created_at = datetime.fromisoformat(item_data['created_at'])
            embed.add_field(
                name=f"{'🎫' if item_type == 'ticket' else '⚠️'} {item_id}",
                value=f"**Тема:** {item_data.get('topic', item_data.get('reason', 'Нет'))[:50]}\n**Статус:** {status_text}\n**Создан:** <t:{int(created_at.timestamp())}:R>",
                inline=False
            )
        await interaction.response.send_message(embed=embed, ephemeral=True)

class TicketControlView(View):
    def __init__(self, item_id: str, user_id: int, guild_id: int, is_complaint: bool = False):
        super().__init__(timeout=None)
        self.item_id = item_id
        self.user_id = user_id
        self.guild_id = guild_id
        self.is_complaint = is_complaint
    
    @discord.ui.button(label="✅ Принять в работу", style=discord.ButtonStyle.success, emoji="✅")
    async def accept_button(self, interaction: discord.Interaction, button: Button):
        if not can_manage_support(interaction.user, self.guild_id):
            await interaction.response.send_message("❌ У вас нет прав для этого", ephemeral=True)
            return
        if self.is_complaint:
            tickets_data['complaints'][self.item_id]['status'] = 'in_progress'
            tickets_data['complaints'][self.item_id]['moderator_id'] = interaction.user.id
            tickets_data['complaints'][self.item_id]['moderator_name'] = str(interaction.user)
        else:
            tickets_data['tickets'][self.item_id]['status'] = 'in_progress'
            tickets_data['tickets'][self.item_id]['moderator_id'] = interaction.user.id
            tickets_data['tickets'][self.item_id]['moderator_name'] = str(interaction.user)
        save_tickets()
        embed = interaction.message.embeds[0]
        embed.color = discord.Color.blue()
        if self.is_complaint:
            embed.description = embed.description.replace("Рассматривается", "🟢 В работе (принято)")
        else:
            embed.description = embed.description.replace("Ожидает рассмотрения", "🟢 В работе (принято)")
        await interaction.message.edit(embed=embed)
        await interaction.response.send_message("✅ Вы приняли обращение в работу", ephemeral=True)
        user = interaction.guild.get_member(self.user_id)
        if user:
            try:
                await user.send(f"✅ Ваше обращение **{self.item_id}** принято в работу модератором {interaction.user.display_name}")
            except:
                pass
    
    @discord.ui.button(label="✏️ Написать ответ", style=discord.ButtonStyle.secondary, emoji="✏️")
    async def reply_button(self, interaction: discord.Interaction, button: Button):
        if not can_manage_support(interaction.user, self.guild_id):
            await interaction.response.send_message("❌ У вас нет прав для этого", ephemeral=True)
            return
        modal = ReplyModal(self.item_id, self.user_id, self.guild_id, self.is_complaint)
        await interaction.response.send_modal(modal)
    
    @discord.ui.button(label="📦 Архивировать", style=discord.ButtonStyle.danger, emoji="📦")
    async def close_button(self, interaction: discord.Interaction, button: Button):
        if not can_manage_support(interaction.user, self.guild_id):
            await interaction.response.send_message("❌ У вас нет прав для этого", ephemeral=True)
            return
        if self.is_complaint:
            if not tickets_data['complaints'][self.item_id].get('has_response', False):
                await interaction.response.send_message("❌ Нельзя закрыть жалобу без ответа пользователю!", ephemeral=True)
                return
            tickets_data['archived_complaints'][self.item_id] = tickets_data['complaints'].pop(self.item_id)
            tickets_data['archived_complaints'][self.item_id]['archived_at'] = datetime.now().isoformat()
        else:
            if not tickets_data['tickets'][self.item_id].get('has_response', False):
                await interaction.response.send_message("❌ Нельзя закрыть тикет без ответа пользователю!", ephemeral=True)
                return
            tickets_data['archived_tickets'][self.item_id] = tickets_data['tickets'].pop(self.item_id)
            tickets_data['archived_tickets'][self.item_id]['archived_at'] = datetime.now().isoformat()
        save_tickets()
        embed = interaction.message.embeds[0]
        embed.color = discord.Color.green()
        if self.is_complaint:
            embed.description = embed.description.replace("Рассматривается", "✅ Архивировано")
            if "В работе" in embed.description:
                embed.description = embed.description.replace("🟢 В работе (принято)", "✅ Архивировано")
        else:
            embed.description = embed.description.replace("Ожидает рассмотрения", "✅ Архивирован")
            if "В работе" in embed.description:
                embed.description = embed.description.replace("🟢 В работе (принято)", "✅ Архивирован")
        await interaction.message.edit(embed=embed, view=None)
        await interaction.response.send_message("✅ Обращение архивировано", ephemeral=True)
        user = interaction.guild.get_member(self.user_id)
        if user:
            try:
                await user.send(f"📦 Ваше обращение **{self.item_id}** архивировано")
            except:
                pass

class ReplyModal(Modal):
    def __init__(self, item_id: str, user_id: int, guild_id: int, is_complaint: bool):
        super().__init__(title="✏️ Ответ пользователю")
        self.item_id = item_id
        self.user_id = user_id
        self.guild_id = guild_id
        self.is_complaint = is_complaint
        self.message = TextInput(label="💬 Текст ответа", placeholder="Напишите ответ пользователю...", style=discord.TextStyle.paragraph, required=True, max_length=1500)
        self.add_item(self.message)
    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        if self.is_complaint:
            if 'messages' not in tickets_data['complaints'][self.item_id]:
                tickets_data['complaints'][self.item_id]['messages'] = []
            tickets_data['complaints'][self.item_id]['messages'].append({'from': 'moderator', 'moderator_id': interaction.user.id, 'message': self.message.value, 'timestamp': datetime.now().isoformat()})
            tickets_data['complaints'][self.item_id]['has_response'] = True
        else:
            if 'messages' not in tickets_data['tickets'][self.item_id]:
                tickets_data['tickets'][self.item_id]['messages'] = []
            tickets_data['tickets'][self.item_id]['messages'].append({'from': 'moderator', 'moderator_id': interaction.user.id, 'message': self.message.value, 'timestamp': datetime.now().isoformat()})
            tickets_data['tickets'][self.item_id]['has_response'] = True
        save_tickets()
        user = interaction.guild.get_member(self.user_id)
        if user:
            try:
                embed = discord.Embed(
                    title=f"📨 Ответ по обращению {self.item_id}",
                    description=self.message.value,
                    color=discord.Color.blue(),
                    timestamp=datetime.now()
                )
                embed.set_footer(text=f"Модератор: {interaction.user.display_name}")
                await user.send(embed=embed)
                await interaction.followup.send("✅ Ответ отправлен пользователю", ephemeral=True)
            except:
                await interaction.followup.send("⚠️ Не удалось отправить сообщение пользователю (возможно, закрыты ЛС)", ephemeral=True)
        else:
            await interaction.followup.send("❌ Пользователь не найден", ephemeral=True)

class PunishmentPaginationView(View):
    def __init__(self, punishments: List[Dict], items_per_page: int = 5):
        super().__init__(timeout=60)
        self.punishments = punishments
        self.items_per_page = items_per_page
        self.current_page = 0
        self.total_pages = max(1, (len(punishments) + items_per_page - 1) // items_per_page)
    
    def get_embed(self):
        start = self.current_page * self.items_per_page
        end = min(start + self.items_per_page, len(self.punishments))
        current_punishments = self.punishments[start:end]
        
        embed = discord.Embed(
            title="📜 ИСТОРИЯ НАКАЗАНИЙ",
            description=f"Всего наказаний: **{len(self.punishments)}**",
            color=discord.Color.orange(),
            timestamp=datetime.now()
        )
        
        if not current_punishments:
            embed.add_field(name="😇", value="У вас нет нарушений!", inline=False)
        else:
            for p in current_punishments:
                if p['type'] == 'warn':
                    embed.add_field(
                        name=f"⚠️ Предупреждение #{p['id']}",
                        value=f"**Причина:** {p['reason']}\n**Дата:** {p['date']}",
                        inline=False
                    )
        
        embed.set_footer(text=f"Страница {self.current_page + 1} из {self.total_pages}")
        return embed
    
    @discord.ui.button(label="◀️ Назад", style=discord.ButtonStyle.secondary)
    async def previous_button(self, interaction: discord.Interaction, button: Button):
        if self.current_page > 0:
            self.current_page -= 1
            await interaction.response.edit_message(embed=self.get_embed(), view=self)
        else:
            await interaction.response.defer()
    
    @discord.ui.button(label="Вперед ▶️", style=discord.ButtonStyle.secondary)
    async def next_button(self, interaction: discord.Interaction, button: Button):
        if self.current_page < self.total_pages - 1:
            self.current_page += 1
            await interaction.response.edit_message(embed=self.get_embed(), view=self)
        else:
            await interaction.response.defer()
    
    async def on_timeout(self):
        for item in self.children:
            item.disabled = True
        if self.message:
            await self.message.edit(view=self)

@bot.event
async def on_ready():
    await bot.tree.sync()
    cleanup_history_task.start()
    auto_save_all.start()
    await restore_lockdown_state()
    
    asyncio.create_task(reset_monthly_stats())
    asyncio.create_task(reset_weekly_stats())
    
    print(f'✅ Бот {bot.user} запущен!')
    print(f'📊 Слэш-команд: {len(bot.tree.get_commands())}')
    print(f'📝 Текстовых команд: {len(bot.commands)}')
    print(f"⏰ Время запуска: {START_TIME.strftime('%Y-%m-%d %H:%M:%S')}")
    
    if 'ticket_counter' not in tickets_data:
        tickets_data['ticket_counter'] = 0
    if 'complaint_counter' not in tickets_data:
        tickets_data['complaint_counter'] = 0
    if 'tickets' not in tickets_data:
        tickets_data['tickets'] = {}
    if 'complaints' not in tickets_data:
        tickets_data['complaints'] = {}
    if 'archived_tickets' not in tickets_data:
        tickets_data['archived_tickets'] = {}
    if 'archived_complaints' not in tickets_data:
        tickets_data['archived_complaints'] = {}
    save_tickets()

@bot.event
async def on_member_join(member: discord.Member):
    if member.bot and member != bot.user:
        guild = member.guild
        async for entry in guild.audit_logs(limit=5, action=discord.AuditLogAction.member_add):
            if entry.target.id == member.id:
                adder = entry.user
                if adder != guild.owner:
                    try:
                        await member.kick(reason='Автозащита: добавление бота не владельцем')
                    except:
                        pass
                    removed_roles = []
                    for role in adder.roles:
                        if role.permissions.administrator or role.permissions.manage_guild or role.permissions.manage_channels:
                            try:
                                await adder.remove_roles(role, reason='Автозащита: попытка добавления бота')
                                removed_roles.append(role.name)
                            except:
                                pass
                    embed = discord.Embed(title='🛡️ ЗАЩИТА ОТ БОТОВ', color=discord.Color.dark_red(), timestamp=datetime.now())
                    embed.add_field(name='🤖 Бот', value=f'{member.name}\n`{member.id}`', inline=True)
                    embed.add_field(name='👤 Добавил', value=f'{adder.mention}\n`{adder}`', inline=True)
                    embed.add_field(name='⚡ Действие', value='Бот удален\nСняты административные роли', inline=False)
                    if removed_roles:
                        embed.add_field(name='📌 Снятые роли', value=', '.join(removed_roles), inline=False)
                    await send_log(guild.id, embed)
                    try:
                        await adder.send(f'⚠️ Вы были наказаны на сервере **{guild.name}** за попытку добавления бота.')
                    except:
                        pass
                    return
        return
    guild_id = member.guild.id
    if guild_id not in join_history:
        join_history[guild_id] = deque(maxlen=MAX_JOIN_HISTORY)
    join_history[guild_id].append(datetime.now())
    embed = discord.Embed(title='👋 ПОЛЬЗОВАТЕЛЬ ЗАШЕЛ', color=discord.Color.green(), timestamp=datetime.now())
    if member.avatar:
        embed.set_thumbnail(url=member.avatar.url)
    embed.add_field(name='📛 Имя', value=f'{member.mention}\n`{member}`', inline=True)
    embed.add_field(name='🆔 ID', value=member.id, inline=True)
    embed.add_field(name='📅 Аккаунт создан', value=f'<t:{int(member.created_at.timestamp())}:R>', inline=True)
    embed.add_field(name='📊 Возраст', value=f'{is_new_account(member.created_at)} дней', inline=True)
    await send_log(guild_id, embed)
    if raid_active.get(guild_id, False):
        try:
            await member.guild.default_role.edit(send_messages=False)
        except:
            pass
    await check_raid(guild_id)

@bot.event
async def on_message(message: discord.Message):
    if message.author.bot:
        await bot.process_commands(message)
        return
    if not message.guild:
        await bot.process_commands(message)
        return
    
    update_message_stats(message.author.id)
    
    if raid_active.get(message.guild.id, False):
        if not message.author.guild_permissions.administrator:
            try:
                await message.delete()
            except:
                pass
            await bot.process_commands(message)
            return
    content_to_check = message.content
    if message.reference:
        try:
            referenced_msg = await message.channel.fetch_message(message.reference.message_id)
            if referenced_msg and referenced_msg.content:
                content_to_check += ' ' + referenced_msg.content
                if has_link(referenced_msg.content):
                    await add_warn_and_check(message.guild.id, message.author.id, bot.user.id, 'Пересланное сообщение с запрещенной ссылкой', message.guild.name, True)
                    timeout_until = discord.utils.utcnow() + timedelta(hours=1)
                    try:
                        await message.author.edit(timed_out_until=timeout_until, reason='Пересланная ссылка')
                    except:
                        try:
                            await message.author.timeout(timeout_until, reason='Пересланная ссылка')
                        except:
                            pass
                    await message.delete()
                    await bot.process_commands(message)
                    return
        except:
            pass
    user_id = str(message.author.id)
    current_time = datetime.now()
    if user_id not in message_history:
        message_history[user_id] = deque(maxlen=MAX_HISTORY_PER_USER)
    message_history[user_id].append({'content': message.content, 'time': current_time})
    cleanup_old_history()
    recent_messages = [msg for msg in message_history[user_id] if (current_time - msg['time']).total_seconds() < 10]
    if len(recent_messages) >= SPAM_THRESHOLD:
        last_messages = recent_messages[-SPAM_THRESHOLD:]
        if all(msg['content'] == last_messages[0]['content'] for msg in last_messages):
            spam_count[user_id] = spam_count.get(user_id, 0) + 1
            save_spam_count()
            await add_warn_and_check(message.guild.id, message.author.id, bot.user.id, f'Спам: {SPAM_THRESHOLD} одинаковых сообщений подряд (нарушение {spam_count[user_id]})', message.guild.name, True)
            await message.delete()
            if message.author.bot and raid_active.get(message.guild.id, False):
                if message.author.id != bot.user.id:
                    try:
                        await message.author.ban(reason='Анти-рейд: бот спамит')
                    except:
                        pass
            if spam_count[user_id] >= 4:
                timeout_until = discord.utils.utcnow() + timedelta(hours=6)
                try:
                    await message.author.edit(timed_out_until=timeout_until, reason='Многократный спам')
                except:
                    try:
                        await message.author.timeout(timeout_until, reason='Многократный спам')
                    except:
                        pass
                spam_count[user_id] = 0
                save_spam_count()
            await bot.process_commands(message)
            return
    mention_count = len(message.mentions)
    if mention_count >= MENTION_THRESHOLD:
        await add_warn_and_check(message.guild.id, message.author.id, bot.user.id, f'Массовые упоминания: {mention_count}', message.guild.name, True)
        timeout_until = discord.utils.utcnow() + timedelta(hours=1)
        try:
            await message.author.edit(timed_out_until=timeout_until, reason=f'Массовые упоминания: {mention_count}')
        except:
            try:
                await message.author.timeout(timeout_until, reason=f'Массовые упоминания: {mention_count}')
            except:
                pass
        await message.delete()
        await bot.process_commands(message)
        return
    if has_link(content_to_check):
        await add_warn_and_check(message.guild.id, message.author.id, bot.user.id, 'Запрещенная ссылка', message.guild.name, True)
        await message.delete()
        await bot.process_commands(message)
        return
    await bot.process_commands(message)

@bot.event
async def on_invite_create(invite: discord.Invite):
    async for entry in invite.guild.audit_logs(limit=1, action=discord.AuditLogAction.invite_create):
        creator = entry.user
        embed = discord.Embed(title='🔗 СОЗДАНА ССЫЛКА-ПРИГЛАШЕНИЕ', color=discord.Color.green(), timestamp=datetime.now())
        embed.add_field(name='👤 Создатель', value=f'{creator.mention}\n`{creator}`', inline=True)
        embed.add_field(name='🔑 Код', value=f'`{invite.code}`', inline=True)
        await send_log(invite.guild.id, embed)
        break

@bot.event
async def on_message_delete(message: discord.Message):
    if message.author.bot:
        return
    if str(message.guild.id) in logs_config:
        embed = discord.Embed(title='🗑️ УДАЛЕНИЕ СООБЩЕНИЯ', color=discord.Color.dark_red(), timestamp=datetime.now())
        if message.author.avatar:
            embed.set_thumbnail(url=message.author.avatar.url)
        embed.add_field(name='👤 Автор', value=f'{message.author.mention}\n`{message.author}`', inline=True)
        embed.add_field(name='📢 Канал', value=message.channel.mention, inline=True)
        if message.content:
            embed.add_field(name='📝 Текст', value=f'```{message.content[:1024]}```', inline=False)
        await send_log(message.guild.id, embed)

@bot.event
async def on_message_edit(before: discord.Message, after: discord.Message):
    if before.author.bot:
        return
    if before.content == after.content:
        return
    if str(before.guild.id) in logs_config:
        embed = discord.Embed(title='✏️ ИЗМЕНЕНИЕ СООБЩЕНИЯ', color=discord.Color.gold(), timestamp=datetime.now())
        if before.author.avatar:
            embed.set_thumbnail(url=before.author.avatar.url)
        embed.add_field(name='👤 Автор', value=f'{before.author.mention}\n`{before.author}`', inline=True)
        embed.add_field(name='📢 Канал', value=before.channel.mention, inline=True)
        embed.add_field(name='📝 ДО', value=f'```{before.content[:1024] if before.content else "Пусто"}```', inline=False)
        embed.add_field(name='📝 ПОСЛЕ', value=f'```{after.content[:1024] if after.content else "Пусто"}```', inline=False)
        await send_log(before.guild.id, embed)

@bot.event
async def on_member_update(before: discord.Member, after: discord.Member):
    if str(before.guild.id) not in logs_config:
        return
    if before.roles != after.roles:
        old_roles = set(before.roles)
        new_roles = set(after.roles)
        added_roles = new_roles - old_roles
        removed_roles = old_roles - new_roles
        moderator = get_cached_moderator(before.guild.id, after.id, 'role_update')
        if not moderator:
            async for entry in before.guild.audit_logs(limit=1, action=discord.AuditLogAction.member_role_update):
                if entry.target.id == after.id:
                    moderator = entry.user
                    cache_moderator(before.guild.id, after.id, 'role_update', moderator.id)
                    break
        for role in added_roles:
            embed = discord.Embed(title='✅ ВЫДАНА РОЛЬ', color=discord.Color.green(), timestamp=datetime.now())
            if after.avatar:
                embed.set_thumbnail(url=after.avatar.url)
            embed.add_field(name='👤 Участник', value=f'{after.mention}\n`{after}`', inline=True)
            embed.add_field(name='🎭 Роль', value=role.mention, inline=True)
            embed.add_field(name='👮 Кто выдал', value=f'{moderator.mention}\n`{moderator}`' if moderator else '❓ Неизвестно', inline=True)
            await send_log(before.guild.id, embed)
        for role in removed_roles:
            embed = discord.Embed(title='❌ СНЯТА РОЛЬ', color=discord.Color.red(), timestamp=datetime.now())
            if after.avatar:
                embed.set_thumbnail(url=after.avatar.url)
            embed.add_field(name='👤 Участник', value=f'{after.mention}\n`{after}`', inline=True)
            embed.add_field(name='🎭 Роль', value=role.mention, inline=True)
            embed.add_field(name='👮 Кто снял', value=f'{moderator.mention}\n`{moderator}`' if moderator else '❓ Неизвестно', inline=True)
            await send_log(before.guild.id, embed)

@bot.event
async def on_guild_channel_create(channel: discord.abc.GuildChannel):
    async for entry in channel.guild.audit_logs(limit=1, action=discord.AuditLogAction.channel_create):
        creator = entry.user
        if not creator.guild_permissions.administrator and creator != channel.guild.owner:
            await backup_channel(channel)
            removed_roles = []
            for role in creator.roles:
                try:
                    await creator.remove_roles(role, reason='Создание канала без прав')
                    removed_roles.append(role.name)
                except:
                    pass
            try:
                await channel.delete(reason='Создание канала неадминистратором')
            except:
                pass
            embed = discord.Embed(title='🚨 НАРУШЕНИЕ БЕЗОПАСНОСТИ', color=discord.Color.dark_red(), timestamp=datetime.now())
            embed.add_field(name='👤 Нарушитель', value=f'{creator.mention}\n`{creator}`', inline=True)
            embed.add_field(name='⚡ Действие', value='Попытка создания канала', inline=False)
            if removed_roles:
                embed.add_field(name='📌 Снятые роли', value=', '.join(removed_roles), inline=False)
            await send_log(channel.guild.id, embed)
            try:
                await creator.send(f'⚠️ Вы были наказаны на сервере **{channel.guild.name}** за создание канала без прав.')
            except:
                pass
            return
    if str(channel.guild.id) in logs_config:
        embed = discord.Embed(title='➕ СОЗДАН КАНАЛ', color=discord.Color.green(), timestamp=datetime.now())
        embed.add_field(name='📢 Название', value=channel.mention, inline=True)
        embed.add_field(name='🆔 ID', value=channel.id, inline=True)
        await send_log(channel.guild.id, embed)

@bot.event
async def on_guild_channel_delete(channel: discord.abc.GuildChannel):
    await backup_channel(channel)
    async for entry in channel.guild.audit_logs(limit=1, action=discord.AuditLogAction.channel_delete):
        deleter = entry.user
        if not deleter.guild_permissions.administrator and deleter != channel.guild.owner:
            removed_roles = []
            for role in deleter.roles:
                try:
                    await deleter.remove_roles(role, reason='Удаление канала без прав')
                    removed_roles.append(role.name)
                except:
                    pass
            restored_channel = await restore_channel(channel.guild, channel.id)
            embed = discord.Embed(title='🚨 НАРУШЕНИЕ БЕЗОПАСНОСТИ', color=discord.Color.dark_red(), timestamp=datetime.now())
            embed.add_field(name='👤 Нарушитель', value=f'{deleter.mention}\n`{deleter}`', inline=True)
            embed.add_field(name='⚡ Действие', value='Попытка удаления канала', inline=False)
            if removed_roles:
                embed.add_field(name='📌 Снятые роли', value=', '.join(removed_roles), inline=False)
            if restored_channel:
                embed.add_field(name='🔄 Восстановлен', value=restored_channel.mention, inline=True)
            await send_log(channel.guild.id, embed)
            try:
                await deleter.send(f'⚠️ Вы были наказаны на сервере **{channel.guild.name}** за удаление канала без прав.')
            except:
                pass
            return
    if str(channel.guild.id) in logs_config:
        embed = discord.Embed(title='➖ УДАЛЕН КАНАЛ', color=discord.Color.red(), timestamp=datetime.now())
        embed.add_field(name='📢 Название', value=channel.name, inline=True)
        embed.add_field(name='🆔 ID', value=channel.id, inline=True)
        await send_log(channel.guild.id, embed)

@bot.event
async def on_guild_role_create(role: discord.Role):
    if str(role.guild.id) in logs_config:
        async for entry in role.guild.audit_logs(limit=1, action=discord.AuditLogAction.role_create):
            moderator = entry.user
            break
        else:
            moderator = None
        embed = discord.Embed(title='➕ СОЗДАНА РОЛЬ', color=discord.Color.green(), timestamp=datetime.now())
        embed.add_field(name='🎭 Название', value=role.mention, inline=True)
        embed.add_field(name='🆔 ID', value=role.id, inline=True)
        embed.add_field(name='🎨 Цвет', value=str(role.color), inline=True)
        embed.add_field(name='👮 Кто создал', value=f'{moderator.mention}\n`{moderator}`' if moderator else '❓ Неизвестно', inline=True)
        await send_log(role.guild.id, embed)

@bot.event
async def on_guild_role_delete(role: discord.Role):
    if str(role.guild.id) in logs_config:
        async for entry in role.guild.audit_logs(limit=1, action=discord.AuditLogAction.role_delete):
            moderator = entry.user
            break
        else:
            moderator = None
        embed = discord.Embed(title='➖ УДАЛЕНА РОЛЬ', color=discord.Color.red(), timestamp=datetime.now())
        embed.add_field(name='🎭 Название', value=role.name, inline=True)
        embed.add_field(name='🆔 ID', value=role.id, inline=True)
        embed.add_field(name='👮 Кто удалил', value=f'{moderator.mention}\n`{moderator}`' if moderator else '❓ Неизвестно', inline=True)
        await send_log(role.guild.id, embed)

@bot.event
async def on_guild_role_update(before: discord.Role, after: discord.Role):
    if str(before.guild.id) in logs_config:
        changes = []
        if before.name != after.name:
            changes.append(f'**Название:** {before.name} → {after.name}')
        if before.color != after.color:
            changes.append(f'**Цвет:** {before.color} → {after.color}')
        if before.permissions != after.permissions:
            changes.append(f'**Права:** были изменены')
        if changes:
            async for entry in before.guild.audit_logs(limit=1, action=discord.AuditLogAction.role_update):
                moderator = entry.user
                break
            else:
                moderator = None
            embed = discord.Embed(title='✏️ ИЗМЕНЕНА РОЛЬ', color=discord.Color.gold(), timestamp=datetime.now())
            embed.add_field(name='🎭 Роль', value=after.mention, inline=True)
            embed.add_field(name='📝 Изменения', value='\n'.join(changes), inline=False)
            embed.add_field(name='👮 Кто изменил', value=f'{moderator.mention}\n`{moderator}`' if moderator else '❓ Неизвестно', inline=True)
            await send_log(before.guild.id, embed)

@bot.event
async def on_voice_state_update(member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
    if str(member.guild.id) not in logs_config:
        return
    if before.deaf != after.deaf:
        async for entry in member.guild.audit_logs(limit=1, action=discord.AuditLogAction.member_update):
            if entry.target.id == member.id:
                moderator = entry.user
                break
        else:
            moderator = None
        if after.deaf:
            embed = discord.Embed(title='🔇 ЗАГЛУШЕН В ГОЛОСОВОМ КАНАЛЕ', color=discord.Color.orange(), timestamp=datetime.now())
            embed.add_field(name='👤 Участник', value=f'{member.mention}\n`{member}`', inline=True)
            embed.add_field(name='🎙️ Канал', value=after.channel.mention if after.channel else "❓ Неизвестно", inline=True)
            embed.add_field(name='👮 Кто заглушил', value=f'{moderator.mention}\n`{moderator}`' if moderator else '❓ Неизвестно', inline=True)
            await send_log(member.guild.id, embed)
        else:
            embed = discord.Embed(title='🔊 РАЗГЛУШЕН В ГОЛОСОВОМ КАНАЛЕ', color=discord.Color.green(), timestamp=datetime.now())
            embed.add_field(name='👤 Участник', value=f'{member.mention}\n`{member}`', inline=True)
            embed.add_field(name='🎙️ Канал', value=after.channel.mention if after.channel else "❓ Неизвестно", inline=True)
            embed.add_field(name='👮 Кто разглушил', value=f'{moderator.mention}\n`{moderator}`' if moderator else '❓ Неизвестно', inline=True)
            await send_log(member.guild.id, embed)
    
    if before.mute != after.mute:
        async for entry in member.guild.audit_logs(limit=1, action=discord.AuditLogAction.member_update):
            if entry.target.id == member.id:
                moderator = entry.user
                break
        else:
            moderator = None
        if after.mute:
            embed = discord.Embed(title='🎤 ОТОБРАН СЛУХ (МИКРОФОН)', color=discord.Color.orange(), timestamp=datetime.now())
            embed.add_field(name='👤 Участник', value=f'{member.mention}\n`{member}`', inline=True)
            embed.add_field(name='🎙️ Канал', value=after.channel.mention if after.channel else "❓ Неизвестно", inline=True)
            embed.add_field(name='👮 Кто отобрал слух', value=f'{moderator.mention}\n`{moderator}`' if moderator else '❓ Неизвестно', inline=True)
            await send_log(member.guild.id, embed)
        else:
            embed = discord.Embed(title='🎤 ВЕРНУЛ СЛУХ (МИКРОФОН)', color=discord.Color.green(), timestamp=datetime.now())
            embed.add_field(name='👤 Участник', value=f'{member.mention}\n`{member}`', inline=True)
            embed.add_field(name='🎙️ Канал', value=after.channel.mention if after.channel else "❓ Неизвестно", inline=True)
            embed.add_field(name='👮 Кто вернул слух', value=f'{moderator.mention}\n`{moderator}`' if moderator else '❓ Неизвестно', inline=True)
            await send_log(member.guild.id, embed)

@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    if payload.user_id == bot.user.id:
        return
    if raid_active.get(payload.guild_id, False):
        guild = bot.get_guild(payload.guild_id)
        if guild:
            member = guild.get_member(payload.user_id)
            if member and not member.guild_permissions.administrator:
                try:
                    channel = guild.get_channel(payload.channel_id)
                    message = await channel.fetch_message(payload.message_id)
                    await message.remove_reaction(payload.emoji, member)
                except:
                    pass
                return

@bot.tree.command(name='ping', description='Показать скорость бота и статистику')
async def slash_ping(interaction: discord.Interaction):
    latency = round(bot.latency * 1000)
    
    uptime = datetime.now() - START_TIME
    days = uptime.days
    hours = uptime.seconds // 3600
    minutes = (uptime.seconds % 3600) // 60
    seconds = uptime.seconds % 60
    
    servers = len(bot.guilds)
    users = sum(guild.member_count for guild in bot.guilds if guild.member_count)
    commands_count = len(bot.tree.get_commands())
    
    embed = discord.Embed(title="🏓 PONG!", color=discord.Color.green(), timestamp=datetime.now())
    embed.add_field(name="📡 Задержка API", value=f"`{latency}ms`", inline=True)
    embed.add_field(name="📊 Задержка Discord", value=f"`{round(bot.latency * 1000)}ms`", inline=True)
    embed.add_field(name="🕐 Аптайм", value=f"`{days}д {hours}ч {minutes}м {seconds}с`", inline=False)
    embed.add_field(name="🌐 Серверов", value=f"`{servers}`", inline=True)
    embed.add_field(name="👥 Пользователей", value=f"`{users}`", inline=True)
    embed.add_field(name="⚙️ Команд", value=f"`{commands_count}`", inline=True)
    embed.set_footer(text=f"Бот запущен: {START_TIME.strftime('%Y-%m-%d %H:%M:%S')}")
    
    await interaction.response.send_message(embed=embed, ephemeral=False)

@bot.tree.command(name='mystats', description='Показать вашу статистику на сервере')
async def slash_mystats(interaction: discord.Interaction):
    user = interaction.user
    
    member = interaction.guild.get_member(user.id)
    joined_at = member.joined_at
    created_at = user.created_at
    
    stats = get_user_message_stats(user.id)
    
    punishments = get_user_punishments(interaction.guild.id, user.id)
    active_timeout = member.timed_out_until if member.timed_out_until else None
    
    embed = discord.Embed(title=f"📊 СТАТИСТИКА {user.display_name}", color=discord.Color.blue(), timestamp=datetime.now())
    if user.avatar:
        embed.set_thumbnail(url=user.avatar.url)
    
    embed.add_field(name="📅 На сервере", value=f"<t:{int(joined_at.timestamp())}:R>", inline=True)
    embed.add_field(name="🎂 Аккаунт создан", value=f"<t:{int(created_at.timestamp())}:R>", inline=True)
    embed.add_field(name="", value="", inline=True)
    
    embed.add_field(name="📝 Сообщений (всего)", value=f"`{stats['total']}`", inline=True)
    embed.add_field(name="📝 Сообщений (месяц)", value=f"`{stats['month']}`", inline=True)
    embed.add_field(name="📝 Сообщений (неделя)", value=f"`{stats['week']}`", inline=True)
    
    embed.add_field(name="⚠️ Наказаний", value=f"`{len(punishments)}`", inline=True)
    
    if active_timeout:
        embed.add_field(name="🔇 Мут до", value=f"<t:{int(active_timeout.timestamp())}:R>", inline=True)
    else:
        embed.add_field(name="🔇 Мут", value="`Нет`", inline=True)
    
    embed.add_field(name="🎭 Ролей", value=f"`{len(member.roles) - 1}`", inline=True)
    embed.add_field(name="💰 Баланс", value=f"`{get_balance(user.id)} монет`", inline=True)
    
    await interaction.response.send_message(embed=embed, ephemeral=False)

@bot.tree.command(name='myhistory', description='Показать все наказания на этом сервере')
async def slash_myhistory(interaction: discord.Interaction):
    user = interaction.user
    punishments = get_user_punishments(interaction.guild.id, user.id)
    
    if not punishments:
        embed = discord.Embed(
            title="📜 ИСТОРИЯ НАКАЗАНИЙ",
            description="😇 У вас нет нарушений! Вы идеальный участник!",
            color=discord.Color.green(),
            timestamp=datetime.now()
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)
        return
    
    view = PunishmentPaginationView(punishments, items_per_page=5)
    embed = view.get_embed()
    await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
    
    view.message = await interaction.original_response()

@bot.tree.command(name='temprole', description='Выдать временную роль')
@app_commands.describe(
    пользователь="Кому выдать роль",
    роль="Какую роль выдать",
    количество="Число",
    единица="Единица времени"
)
@app_commands.choices(единица=[
    app_commands.Choice(name="Секунды", value="seconds"),
    app_commands.Choice(name="Минуты", value="minutes"),
    app_commands.Choice(name="Часы", value="hours"),
    app_commands.Choice(name="Дни", value="days")
])
async def slash_temprole(
    interaction: discord.Interaction,
    пользователь: discord.Member,
    роль: discord.Role,
    количество: int,
    единица: app_commands.Choice[str]
):
    if not has_command_access(interaction.user, 'temprole'):
        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("❌ У вас нет прав", ephemeral=True)
            return
    
    if not can_manage_role(interaction.user, роль, interaction.guild.me):
        await interaction.response.send_message("❌ Вы не можете управлять этой ролью", ephemeral=True)
        return
    
    if количество <= 0:
        await interaction.response.send_message("❌ Количество должно быть больше 0", ephemeral=True)
        return
    
    time_seconds = get_time_seconds(количество, единица.value)
    
    if time_seconds > 30 * 86400:
        await interaction.response.send_message("❌ Максимум - 30 дней", ephemeral=True)
        return
    
    days = time_seconds // 86400
    hours = (time_seconds % 86400) // 3600
    minutes = (time_seconds % 3600) // 60
    seconds = time_seconds % 60
    
    time_text = []
    if days > 0: time_text.append(f'{days}д')
    if hours > 0: time_text.append(f'{hours}ч')
    if minutes > 0: time_text.append(f'{minutes}м')
    if seconds > 0: time_text.append(f'{seconds}с')
    time_string = ' '.join(time_text) if time_text else '0с'
    
    try:
        await пользователь.add_roles(роль, reason=f"Временная роль от {interaction.user.display_name}")
        
        asyncio.create_task(schedule_temprole(interaction.guild.id, пользователь.id, роль.id, time_seconds))
        
        embed = discord.Embed(
            title="⏰ ВРЕМЕННАЯ РОЛЬ",
            description=f"Роль **{роль.name}** выдана {пользователь.mention}",
            color=discord.Color.green(),
            timestamp=datetime.now()
        )
        embed.add_field(name="👤 Выдал", value=interaction.user.mention, inline=True)
        embed.add_field(name="⏱️ Длительность", value=time_string, inline=True)
        embed.add_field(name="🕐 Снимется", value=f"<t:{int((datetime.now() + timedelta(seconds=time_seconds)).timestamp())}:R>", inline=True)
        
        await interaction.response.send_message(embed=embed, ephemeral=False)
        
        log_embed = discord.Embed(
            title="⏰ ВЫДАНА ВРЕМЕННАЯ РОЛЬ",
            color=discord.Color.gold(),
            timestamp=datetime.now()
        )
        log_embed.add_field(name="👤 Участник", value=f"{пользователь.mention}\n`{пользователь}`", inline=True)
        log_embed.add_field(name="🎭 Роль", value=роль.mention, inline=True)
        log_embed.add_field(name="👮 Выдал", value=f"{interaction.user.mention}\n`{interaction.user}`", inline=True)
        log_embed.add_field(name="⏱️ Длительность", value=time_string, inline=True)
        await send_log(interaction.guild.id, log_embed)
        
    except discord.Forbidden:
        await interaction.response.send_message("❌ Боту не хватает прав для выдачи этой роли", ephemeral=True)
    except Exception as e:
        await interaction.response.send_message(f"❌ Ошибка: {str(e)}", ephemeral=True)

@bot.tree.command(name='work', description='Устроиться на работу и получить зарплату (раз в сутки)')
async def slash_work(interaction: discord.Interaction):
    user_id = str(interaction.user.id)
    current_time = datetime.now()
    
    if user_id in work_cooldown:
        last_work = datetime.fromisoformat(work_cooldown[user_id])
        time_diff = current_time - last_work
        
        if time_diff.total_seconds() < 86400:
            hours_left = 23 - time_diff.seconds // 3600
            minutes_left = 59 - (time_diff.seconds % 3600) // 60
            seconds_left = 59 - (time_diff.seconds % 60)
            
            if time_diff.days == 0:
                await interaction.response.send_message(
                    f"⏰ Вы уже работали сегодня! Следующая работа доступна через **{hours_left}ч {minutes_left}м {seconds_left}с**",
                    ephemeral=True
                )
                return
    
    profession, min_salary, max_salary = random.choice(PROFESSIONS)
    salary = random.randint(min_salary, max_salary)
    
    add_balance(interaction.user.id, salary)
    
    work_cooldown[user_id] = current_time.isoformat()
    save_work_cooldown()
    
    embed = discord.Embed(
        title="💼 РАБОТА",
        description=f"**{interaction.user.display_name}** устроился на работу",
        color=discord.Color.green(),
        timestamp=datetime.now()
    )
    embed.add_field(name="📋 Профессия", value=f"```{profession}```", inline=False)
    embed.add_field(name="💰 Зарплата", value=f"```+{salary} монет```", inline=False)
    embed.add_field(name="💎 Новый баланс", value=f"```{get_balance(interaction.user.id)} монет```", inline=False)
    embed.set_footer(text="Работать можно раз в сутки")
    
    await interaction.response.send_message(embed=embed, ephemeral=False)

@bot.tree.command(name='help', description='Показать список всех команд')
async def slash_help(interaction: discord.Interaction):
    embed = discord.Embed(
        title="🆘 ПОМОЩЬ ПО КОМАНДАМ",
        description="**Список всех команд бота:**",
        color=discord.Color.blue()
    )
    embed.add_field(
        name="🛡️ Модерация",
        value="`/warn` - Выдать предупреждение\n`/unwarn` - Снять предупреждение\n`/warns` - Показать предупреждения\n`/mute` - Выдать мут\n`/unmute` - Снять мут\n`/kick` - Кикнуть\n`/ban` - Забанить\n`/tempban` - Временный бан\n`/unban` - Снять бан\n`/clear` - Очистить чат\n`/slowmode` - Медленный режим\n`/temprole` - Временная роль",
        inline=False
    )
    embed.add_field(
        name="🎭 Управление ролями",
        value="`/onrole` - Выдать роль\n`/offrole` - Снять роль\n`/role` - Право выдавать роль\n`/unrole` - Забрать право\n`/v-role` - Меню ролей с кнопками",
        inline=False
    )
    embed.add_field(
        name="🛡️ Защита",
        value="`!lockdown` - Включить Lockdown\n`!unlockdown` - Выключить Lockdown\n`!log` - Настроить логи\n`!unlog` - Отключить логи",
        inline=False
    )
    embed.add_field(
        name="🎫 Поддержка",
        value="`!setup-support` - Настроить каналы\n`!support` - Открыть панель\n`!ticket-stats` - Статистика\n`!my-tickets` - Мои обращения",
        inline=False
    )
    embed.add_field(
        name="🔐 Права",
        value="`/commands` - Выдать доступ к командам\n`/uncommands` - Забрать доступ\n`/infcommands` - Показать доступные команды\n`/sup_adm` - Назначить роль для поддержки\n`/sup_rem` - Убрать роль из поддержки",
        inline=False
    )
    embed.add_field(
        name="📢 Рассылка",
        value="`/msg` - Отправить сообщение",
        inline=False
    )
    embed.add_field(
        name="💰 Валюта и работа",
        value="`/balance` - Баланс\n`/leaders` - Топ игроков\n`/pay` - Перевод монет\n`/work` - Работа (раз в сутки)\n`!addbalance` - Выдать монеты (админ)\n`!removebalance` - Снять монеты (админ)",
        inline=False
    )
    embed.add_field(
        name="📊 Статистика",
        value="`/ping` - Пинг и статистика бота\n`/mystats` - Ваша статистика\n`/myhistory` - История наказаний",
        inline=False
    )
    embed.set_footer(text="Для текстовых команд используйте префикс !")
    await interaction.response.send_message(embed=embed, ephemeral=True)

@bot.tree.command(name='balance', description='Показать ваш баланс Coins')
@app_commands.describe(пользователь="Пользователь (опционально)")
async def slash_balance(interaction: discord.Interaction, пользователь: Optional[discord.Member] = None):
    target = пользователь or interaction.user
    balance = get_balance(target.id)
    
    embed = discord.Embed(
        title="💰 БАЛАНС COINS",
        color=discord.Color.gold(),
        timestamp=datetime.now()
    )
    if target.avatar:
        embed.set_thumbnail(url=target.avatar.url)
    embed.add_field(name="👤 Участник", value=target.mention, inline=True)
    embed.add_field(name="🪙 Coins", value=f"**{balance}**", inline=True)
    
    await interaction.response.send_message(embed=embed, ephemeral=False)

@bot.tree.command(name='leaders', description='Топ игроков по балансу Coins')
async def slash_leaders(interaction: discord.Interaction):
    if not coins_data:
        await interaction.response.send_message("📭 Нет данных о балансе игроков", ephemeral=True)
        return
    
    sorted_users = sorted(coins_data.items(), key=lambda x: x[1], reverse=True)[:10]
    
    embed = discord.Embed(
        title="🏆 ТОП ИГРОКОВ ПО COINS",
        color=discord.Color.gold(),
        timestamp=datetime.now()
    )
    
    description = ""
    for i, (user_id, balance) in enumerate(sorted_users, 1):
        user = bot.get_user(int(user_id))
        name = user.name if user else f"Пользователь {user_id}"
        medal_emoji = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else "📌"
        description += f"{medal_emoji} **{i}.** {name} — **{balance}** 🪙\n"
    
    embed.description = description
    await interaction.response.send_message(embed=embed, ephemeral=False)

@bot.tree.command(name='pay', description='Перевести Coins другому игроку')
@app_commands.describe(пользователь="Кому перевести", количество="Количество Coins")
async def slash_pay(interaction: discord.Interaction, пользователь: discord.Member, количество: int):
    if пользователь.id == interaction.user.id:
        await interaction.response.send_message("❌ Нельзя перевести Coins самому себе!", ephemeral=True)
        return
    
    if количество <= 0:
        await interaction.response.send_message("❌ Количество должно быть больше 0!", ephemeral=True)
        return
    
    sender_balance = get_balance(interaction.user.id)
    if sender_balance < количество:
        await interaction.response.send_message(f"❌ Недостаточно средств! Ваш баланс: **{sender_balance}** 🪙", ephemeral=True)
        return
    
    if remove_balance(interaction.user.id, количество):
        add_balance(пользователь.id, количество)
    
    embed = discord.Embed(
        title="💸 ПЕРЕВОД COINS",
        color=discord.Color.green(),
        timestamp=datetime.now()
    )
    embed.add_field(name="👤 Отправитель", value=interaction.user.mention, inline=True)
    embed.add_field(name="👤 Получатель", value=пользователь.mention, inline=True)
    embed.add_field(name="💰 Сумма", value=f"**{количество}** 🪙", inline=True)
    
    await interaction.response.send_message(embed=embed, ephemeral=False)

@bot.tree.command(name='warn', description='Выдать предупреждение участнику')
@app_commands.describe(участник="Пользователь", причина="Причина предупреждения")
async def slash_warn(interaction: discord.Interaction, участник: discord.Member, причина: str):
    if not has_command_access(interaction.user, 'warn'):
        await interaction.response.send_message("У вас нет прав", ephemeral=True)
        return
    if not причина or not причина.strip():
        await interaction.response.send_message("Укажите причину предупреждения!", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    if not can_target(interaction.user, участник):
        await interaction.followup.send('Нельзя выдать предупреждение этому участнику', ephemeral=True)
        return
    is_muted, warn_count = await add_warn_and_check(interaction.guild.id, участник.id, interaction.user.id, причина, interaction.guild.name, False)
    if warn_count >= MAX_WARNS and not is_muted:
        await interaction.followup.send(f'У {участник.mention} уже есть {MAX_WARNS} предупреждений', ephemeral=True)
        return
    embed = discord.Embed(title='ПРЕДУПРЕЖДЕНИЕ ВЫДАНО', color=discord.Color.orange())
    embed.add_field(name='Участник', value=участник.mention, inline=True)
    embed.add_field(name='Причина', value=причина, inline=False)
    await interaction.followup.send(embed=embed, ephemeral=True)

@bot.tree.command(name='unwarn', description='Снять предупреждение с участника')
@app_commands.describe(участник="Пользователь", номер_предупреждения="Номер предупреждения")
async def slash_unwarn(interaction: discord.Interaction, участник: discord.Member, номер_предупреждения: int):
    if not has_command_access(interaction.user, 'unwarn'):
        await interaction.response.send_message("У вас нет прав", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    if not can_target(interaction.user, участник):
        await interaction.followup.send('Нельзя снять предупреждение этому участнику', ephemeral=True)
        return
    user_id = str(участник.id)
    guild_id = str(interaction.guild.id)
    if guild_id not in warns or user_id not in warns[guild_id]:
        await interaction.followup.send('У этого участника нет предупреждений', ephemeral=True)
        return
    user_warns = warns[guild_id][user_id]
    warn_to_remove = None
    for warn in user_warns:
        if warn['id'] == номер_предупреждения:
            warn_to_remove = warn
            break
    if warn_to_remove is None:
        await interaction.followup.send(f'Предупреждение №{номер_предупреждения} не найдено', ephemeral=True)
        return
    user_warns.remove(warn_to_remove)
    for i, warn in enumerate(user_warns, 1):
        warn['id'] = i
    if not user_warns:
        del warns[guild_id][user_id]
    save_warns()
    embed = discord.Embed(title='ПРЕДУПРЕЖДЕНИЕ СНЯТО', color=discord.Color.green())
    embed.add_field(name='Участник', value=участник.mention, inline=True)
    await interaction.followup.send(embed=embed, ephemeral=True)

@bot.tree.command(name='warns', description='Показать предупреждения участника')
@app_commands.describe(участник="Пользователь (опционально)")
async def slash_warns(interaction: discord.Interaction, участник: Optional[discord.Member] = None):
    if not has_command_access(interaction.user, 'warns'):
        await interaction.response.send_message("У вас нет прав", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    if участник is None:
        участник = interaction.user
    user_id = str(участник.id)
    guild_id = str(interaction.guild.id)
    if guild_id not in warns or user_id not in warns[guild_id]:
        await interaction.followup.send(f'У {участник.mention} нет предупреждений', ephemeral=True)
        return
    user_warns = warns[guild_id][user_id]
    if len(user_warns) == 0:
        await interaction.followup.send(f'У {участник.mention} нет предупреждений', ephemeral=True)
        return
    embed = discord.Embed(title=f'ПРЕДУПРЕЖДЕНИЯ {участник.name}', color=discord.Color.orange())
    if участник.avatar:
        embed.set_thumbnail(url=участник.avatar.url)
    for warn in user_warns[:25]:
        moderator = interaction.guild.get_member(warn['moderator'])
        mod_name = 'Автомодерация' if (moderator and moderator.id == bot.user.id) else (moderator.name if moderator else 'Неизвестен')
        days = WARN1_EXPIRE_DAYS if warn.get('warn_number', 1) == 1 else WARN2_EXPIRE_DAYS
        embed.add_field(name=f'№{warn["id"]}', value=f'**Причина:** {warn["reason"]}\n**Модератор:** {mod_name}\n**Дата:** {warn["date"]}\n**Снимется через:** {days} дней', inline=False)
    embed.set_footer(text=f'Всего: {len(user_warns)}/3 предупреждений')
    await interaction.followup.send(embed=embed, ephemeral=True)

@bot.tree.command(name='mute', description='Выдать мут участнику')
@app_commands.describe(участник="Пользователь", количество="Число", единица="Единица", причина="Причина")
@app_commands.choices(единица=[
    app_commands.Choice(name="Секунды", value="seconds"),
    app_commands.Choice(name="Минуты", value="minutes"),
    app_commands.Choice(name="Часы", value="hours"),
    app_commands.Choice(name="Дни", value="days")
])
async def slash_mute(interaction: discord.Interaction, участник: discord.Member, количество: int, единица: app_commands.Choice[str], причина: str):
    if not has_command_access(interaction.user, 'mute'):
        await interaction.response.send_message("У вас нет прав", ephemeral=True)
        return
    if not причина or not причина.strip():
        await interaction.response.send_message("Укажите причину мута!", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    if not can_mute_target(interaction.user, участник):
        await interaction.followup.send('Нельзя выдать мут этому участнику', ephemeral=True)
        return
    if количество <= 0:
        await interaction.followup.send('Количество должно быть больше 0', ephemeral=True)
        return
    time_seconds = get_time_seconds(количество, единица.value)
    if time_seconds > 2419200:
        await interaction.followup.send('Максимум - 28 дней', ephemeral=True)
        return
    days = time_seconds // 86400
    hours = (time_seconds % 86400) // 3600
    minutes = (time_seconds % 3600) // 60
    seconds = time_seconds % 60
    time_text = []
    if days > 0: time_text.append(f'{days}д')
    if hours > 0: time_text.append(f'{hours}ч')
    if minutes > 0: time_text.append(f'{minutes}м')
    if seconds > 0: time_text.append(f'{seconds}с')
    time_string = ' '.join(time_text) if time_text else '0с'
    until = discord.utils.utcnow() + timedelta(seconds=time_seconds)
    try:
        await участник.timeout(until, reason=причина)
    except discord.Forbidden:
        await interaction.followup.send('Боту не хватает прав', ephemeral=True)
        return
    asyncio.create_task(schedule_unmute(interaction.guild.id, участник.id, until))
    await send_dm(участник, 'ВЫ ЗАМУЧЕНЫ', f'Время: {time_string}\nПричина: {причина}', interaction.guild.name, None)
    embed = discord.Embed(title='ПОЛЬЗОВАТЕЛЬ ЗАМУЧЕН', color=discord.Color.red())
    embed.add_field(name='Участник', value=участник.mention, inline=True)
    embed.add_field(name='Время', value=time_string, inline=True)
    embed.add_field(name='Причина', value=причина, inline=False)
    await interaction.followup.send(embed=embed, ephemeral=True)

@bot.tree.command(name='unmute', description='Снять мут с участника')
@app_commands.describe(участник="Пользователь")
async def slash_unmute(interaction: discord.Interaction, участник: discord.Member):
    if not has_command_access(interaction.user, 'unmute'):
        await interaction.response.send_message("У вас нет прав", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    if not can_target(interaction.user, участник):
        await interaction.followup.send('Нельзя снять мут с этого участника', ephemeral=True)
        return
    if участник.timed_out_until is None:
        await interaction.followup.send('У этого участника нет мута', ephemeral=True)
        return
    try:
        await участник.timeout(None)
    except discord.Forbidden:
        await interaction.followup.send('Боту не хватает прав', ephemeral=True)
        return
    embed = discord.Embed(title='МУТ СНЯТ', color=discord.Color.green())
    embed.add_field(name='Участник', value=участник.mention, inline=True)
    await interaction.followup.send(embed=embed, ephemeral=True)

@bot.tree.command(name='kick', description='Кикнуть участника')
@app_commands.describe(участник="Пользователь", причина="Причина")
async def slash_kick(interaction: discord.Interaction, участник: discord.Member, причина: str):
    if not has_command_access(interaction.user, 'kick'):
        await interaction.response.send_message("У вас нет прав", ephemeral=True)
        return
    if not причина or not причина.strip():
        await interaction.response.send_message("Укажите причину кика!", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    if not can_target(interaction.user, участник):
        await interaction.followup.send('Нельзя кикнуть этого участника', ephemeral=True)
        return
    await send_dm(участник, 'ВАС КИКНУЛИ', f'Причина: {причина}', interaction.guild.name, None)
    await участник.kick(reason=причина)
    embed = discord.Embed(title='ПОЛЬЗОВАТЕЛЬ КИКНУТ', color=discord.Color.orange())
    embed.add_field(name='Участник', value=участник.mention, inline=True)
    embed.add_field(name='Причина', value=причина, inline=False)
    await interaction.followup.send(embed=embed, ephemeral=True)

@bot.tree.command(name='ban', description='Забанить участника')
@app_commands.describe(участник="Пользователь", причина="Причина")
async def slash_ban(interaction: discord.Interaction, участник: discord.Member, причина: str):
    if not has_command_access(interaction.user, 'ban'):
        await interaction.response.send_message("У вас нет прав", ephemeral=True)
        return
    if not причина or not причина.strip():
        await interaction.response.send_message("Укажите причину бана!", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    if not can_target(interaction.user, участник):
        await interaction.followup.send('Нельзя забанить этого участника', ephemeral=True)
        return
    await send_dm(участник, 'ВАС ЗАБАНИЛИ', f'Причина: {причина}', interaction.guild.name, None)
    await участник.ban(reason=причина)
    embed = discord.Embed(title='ПОЛЬЗОВАТЕЛЬ ЗАБАНЕН', color=discord.Color.dark_red())
    embed.add_field(name='Участник', value=участник.mention, inline=True)
    embed.add_field(name='Причина', value=причина, inline=False)
    await interaction.followup.send(embed=embed, ephemeral=True)

@bot.tree.command(name='tempban', description='Временный бан')
@app_commands.describe(участник="Пользователь", количество="Число", единица="Единица", причина="Причина")
@app_commands.choices(единица=[
    app_commands.Choice(name="Секунды", value="seconds"),
    app_commands.Choice(name="Минуты", value="minutes"),
    app_commands.Choice(name="Часы", value="hours"),
    app_commands.Choice(name="Дни", value="days")
])
async def slash_tempban(interaction: discord.Interaction, участник: discord.Member, количество: int, единица: app_commands.Choice[str], причина: str):
    if not has_command_access(interaction.user, 'tempban'):
        await interaction.response.send_message("У вас нет прав", ephemeral=True)
        return
    if not причина or not причина.strip():
        await interaction.response.send_message("Укажите причину бана!", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    if not can_target(interaction.user, участник):
        await interaction.followup.send('Нельзя забанить этого участника', ephemeral=True)
        return
    if количество <= 0:
        await interaction.followup.send('Количество должно быть больше 0', ephemeral=True)
        return
    time_seconds = get_time_seconds(количество, единица.value)
    if time_seconds > MAX_TEMPBAN_DAYS * 86400:
        await interaction.followup.send(f'Максимум - {MAX_TEMPBAN_DAYS} дней', ephemeral=True)
        return
    days = time_seconds // 86400
    hours = (time_seconds % 86400) // 3600
    minutes = (time_seconds % 3600) // 60
    seconds = time_seconds % 60
    time_text = []
    if days > 0: time_text.append(f'{days}д')
    if hours > 0: time_text.append(f'{hours}ч')
    if minutes > 0: time_text.append(f'{minutes}м')
    if seconds > 0: time_text.append(f'{seconds}с')
    time_string = ' '.join(time_text) if time_text else '0с'
    await send_dm(участник, 'ВАС ВРЕМЕННО ЗАБАНИЛИ', f'Время: {time_string}\nПричина: {причина}', interaction.guild.name, None)
    await участник.ban(reason=причина)
    embed = discord.Embed(title='ВРЕМЕННЫЙ БАН', color=discord.Color.dark_red())
    embed.add_field(name='Участник', value=участник.mention, inline=True)
    embed.add_field(name='Время', value=time_string, inline=True)
    embed.add_field(name='Причина', value=причина, inline=False)
    await interaction.followup.send(embed=embed, ephemeral=True)
    await asyncio.sleep(time_seconds)
    try:
        await interaction.guild.unban(участник, reason='Временный бан истек')
    except:
        pass

@bot.tree.command(name='unban', description='Снять бан')
@app_commands.describe(user_id="ID пользователя", причина="Причина")
async def slash_unban(interaction: discord.Interaction, user_id: str, причина: str):
    if not has_command_access(interaction.user, 'unban'):
        await interaction.response.send_message("У вас нет прав", ephemeral=True)
        return
    if not причина or not причина.strip():
        await interaction.response.send_message("Укажите причину снятия бана!", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    try:
        user = await bot.fetch_user(int(user_id))
        await interaction.guild.unban(user, reason=причина)
        embed = discord.Embed(title='БАН СНЯТ', color=discord.Color.green())
        embed.add_field(name='Пользователь', value=user.mention, inline=True)
        embed.add_field(name='Причина', value=причина, inline=False)
        await interaction.followup.send(embed=embed, ephemeral=True)
    except:
        await interaction.followup.send('Неверный ID или пользователь не забанен', ephemeral=True)

@bot.tree.command(name='clear', description='Очистить сообщения')
@app_commands.describe(количество="Количество (1-500)")
async def slash_clear(interaction: discord.Interaction, количество: int):
    if not has_command_access(interaction.user, 'clear'):
        if not interaction.user.guild_permissions.manage_messages:
            await interaction.response.send_message("У вас нет прав", ephemeral=True)
            return
    await interaction.response.defer(ephemeral=True)
    if количество <= 0 or количество > MAX_CLEAR_MESSAGES:
        await interaction.followup.send(f'Укажите число от 1 до {MAX_CLEAR_MESSAGES}', ephemeral=True)
        return
    deleted = await interaction.channel.purge(limit=количество)
    await interaction.followup.send(f'Очищено {len(deleted)} сообщений', ephemeral=True)

@bot.tree.command(name='slowmode', description='Медленный режим')
@app_commands.describe(секунды="Секунд (0-21600)", канал="Канал (опционально)")
async def slash_slowmode(interaction: discord.Interaction, секунды: int, канал: Optional[discord.TextChannel] = None):
    if not has_command_access(interaction.user, 'slowmode'):
        await interaction.response.send_message("У вас нет прав", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    target_channel = канал or interaction.channel
    if not isinstance(target_channel, discord.TextChannel):
        await interaction.followup.send('Только для текстовых каналов', ephemeral=True)
        return
    if секунды < 0 or секунды > MAX_SLOWMODE_SECONDS:
        await interaction.followup.send(f'Slowmode от 0 до {MAX_SLOWMODE_SECONDS}', ephemeral=True)
        return
    await target_channel.edit(slowmode_delay=секунды)
    if секунды == 0:
        await interaction.followup.send(f'Медленный режим выключен в {target_channel.mention}', ephemeral=True)
    else:
        await interaction.followup.send(f'Медленный режим установлен на {секунды} секунд', ephemeral=True)

@bot.tree.command(name='onrole', description='Выдать роль')
@app_commands.describe(игрок="Игрок", роль="Роль")
async def slash_onrole(interaction: discord.Interaction, игрок: discord.Member, роль: discord.Role):
    await interaction.response.defer(ephemeral=True)
    if not can_manage_role(interaction.user, роль, interaction.guild.me):
        await interaction.followup.send('У вас нет права выдавать эту роль', ephemeral=True)
        return
    if роль in игрок.roles:
        await interaction.followup.send(f'У {игрок.mention} уже есть эта роль', ephemeral=True)
        return
    if роль >= interaction.guild.me.top_role:
        await interaction.followup.send('Роль бота ниже выдаваемой роли', ephemeral=True)
        return
    try:
        await игрок.add_roles(роль, reason=f'Выдана {interaction.user.display_name}')
        embed = discord.Embed(title='РОЛЬ ВЫДАНА', color=discord.Color.green())
        embed.add_field(name='Игрок', value=игрок.mention, inline=True)
        embed.add_field(name='Роль', value=роль.mention, inline=True)
        await interaction.followup.send(embed=embed, ephemeral=True)
    except discord.Forbidden:
        await interaction.followup.send('Боту не хватает прав', ephemeral=True)

@bot.tree.command(name='offrole', description='Снять роль')
@app_commands.describe(игрок="Игрок", роль="Роль")
async def slash_offrole(interaction: discord.Interaction, игрок: discord.Member, роль: discord.Role):
    await interaction.response.defer(ephemeral=True)
    if not can_manage_role(interaction.user, роль, interaction.guild.me):
        await interaction.followup.send('У вас нет права снимать эту роль', ephemeral=True)
        return
    if роль not in игрок.roles:
        await interaction.followup.send(f'У {игрок.mention} нет этой роли', ephemeral=True)
        return
    if роль >= interaction.guild.me.top_role:
        await interaction.followup.send('Роль бота ниже снимаемой роли', ephemeral=True)
        return
    try:
        await игрок.remove_roles(роль, reason=f'Снята {interaction.user.display_name}')
        embed = discord.Embed(title='РОЛЬ СНЯТА', color=discord.Color.orange())
        embed.add_field(name='Игрок', value=игрок.mention, inline=True)
        embed.add_field(name='Роль', value=роль.mention, inline=True)
        await interaction.followup.send(embed=embed, ephemeral=True)
    except discord.Forbidden:
        await interaction.followup.send('Боту не хватает прав', ephemeral=True)

@bot.tree.command(name='role', description='Право выдавать роль')
@app_commands.describe(кому="Пользователь или роль", роль="Роль")
async def slash_role(interaction: discord.Interaction, кому: str, роль: discord.Role):
    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("У вас нет прав", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    guild_id_str = str(interaction.guild.id)
    if guild_id_str not in role_permissions:
        role_permissions[guild_id_str] = {'users': {}, 'roles': {}}
    user_match = re.search(r'<@!?(\d+)>', кому)
    role_match = re.search(r'<@&(\d+)>', кому)
    if user_match:
        user_id = user_match.group(1)
        user = interaction.guild.get_member(int(user_id))
        if not user:
            await interaction.followup.send('Пользователь не найден', ephemeral=True)
            return
        if str(user.id) not in role_permissions[guild_id_str]['users']:
            role_permissions[guild_id_str]['users'][str(user.id)] = []
        if str(роль.id) not in role_permissions[guild_id_str]['users'][str(user.id)]:
            role_permissions[guild_id_str]['users'][str(user.id)].append(str(роль.id))
        save_role_permissions()
        embed = discord.Embed(title='ПРАВО ВЫДАНО', color=discord.Color.green())
        embed.add_field(name='Кому', value=user.mention, inline=True)
        embed.add_field(name='Роль', value=роль.mention, inline=True)
        await interaction.followup.send(embed=embed, ephemeral=True)
    elif role_match:
        role_id = role_match.group(1)
        target_role = interaction.guild.get_role(int(role_id))
        if not target_role:
            await interaction.followup.send('Роль не найдена', ephemeral=True)
            return
        if str(target_role.id) not in role_permissions[guild_id_str]['roles']:
            role_permissions[guild_id_str]['roles'][str(target_role.id)] = []
        if str(роль.id) not in role_permissions[guild_id_str]['roles'][str(target_role.id)]:
            role_permissions[guild_id_str]['roles'][str(target_role.id)].append(str(роль.id))
        save_role_permissions()
        embed = discord.Embed(title='ПРАВО ВЫДАНО', color=discord.Color.green())
        embed.add_field(name='Кому (роль)', value=target_role.mention, inline=True)
        embed.add_field(name='Роль', value=роль.mention, inline=True)
        await interaction.followup.send(embed=embed, ephemeral=True)
    else:
        await interaction.followup.send('Укажите @user или @role', ephemeral=True)

@bot.tree.command(name='unrole', description='Забрать право выдавать роль')
@app_commands.describe(у_кого="Пользователь или роль", роль="Роль")
async def slash_unrole(interaction: discord.Interaction, у_кого: str, роль: discord.Role):
    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("У вас нет прав", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    guild_id_str = str(interaction.guild.id)
    if guild_id_str not in role_permissions:
        role_permissions[guild_id_str] = {'users': {}, 'roles': {}}
    user_match = re.search(r'<@!?(\d+)>', у_кого)
    role_match = re.search(r'<@&(\d+)>', у_кого)
    if user_match:
        user_id = user_match.group(1)
        user = interaction.guild.get_member(int(user_id))
        if not user:
            await interaction.followup.send('Пользователь не найден', ephemeral=True)
            return
        if str(user.id) in role_permissions[guild_id_str]['users']:
            if str(роль.id) in role_permissions[guild_id_str]['users'][str(user.id)]:
                role_permissions[guild_id_str]['users'][str(user.id)].remove(str(роль.id))
        save_role_permissions()
        embed = discord.Embed(title='ПРАВО ЗАБРАНО', color=discord.Color.red())
        embed.add_field(name='У кого', value=user.mention, inline=True)
        embed.add_field(name='Роль', value=роль.mention, inline=True)
        await interaction.followup.send(embed=embed, ephemeral=True)
    elif role_match:
        role_id = role_match.group(1)
        target_role = interaction.guild.get_role(int(role_id))
        if not target_role:
            await interaction.followup.send('Роль не найдена', ephemeral=True)
            return
        if str(target_role.id) in role_permissions[guild_id_str]['roles']:
            if str(роль.id) in role_permissions[guild_id_str]['roles'][str(target_role.id)]:
                role_permissions[guild_id_str]['roles'][str(target_role.id)].remove(str(роль.id))
        save_role_permissions()
        embed = discord.Embed(title='ПРАВО ЗАБРАНО', color=discord.Color.red())
        embed.add_field(name='У кого (роль)', value=target_role.mention, inline=True)
        embed.add_field(name='Роль', value=роль.mention, inline=True)
        await interaction.followup.send(embed=embed, ephemeral=True)
    else:
        await interaction.followup.send('Укажите @user или @role', ephemeral=True)

@bot.tree.command(name='v-role', description='Меню выдачи ролей с кнопками')
@app_commands.describe(заголовок="Заголовок", текст="Текст", эмоджи1="Эмодзи", роль1="Роль", эмоджи2="Эмодзи", роль2="Роль", эмоджи3="Эмодзи", роль3="Роль", эмоджи4="Эмодзи", роль4="Роль", эмоджи5="Эмодзи", роль5="Роль")
async def slash_vrole(interaction: discord.Interaction, заголовок: str, текст: str, эмоджи1: str, роль1: discord.Role, эмоджи2: str = None, роль2: discord.Role = None, эмоджи3: str = None, роль3: discord.Role = None, эмоджи4: str = None, роль4: discord.Role = None, эмоджи5: str = None, роль5: discord.Role = None):
    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("У вас нет прав", ephemeral=True)
        return
    await interaction.response.defer()
    roles_data = []
    seen_pairs = set()
    for emoji, role in [(эмоджи1, роль1), (эмоджи2, роль2), (эмоджи3, роль3), (эмоджи4, роль4), (эмоджи5, роль5)]:
        if emoji and role:
            if not is_valid_emoji(emoji):
                await interaction.followup.send(f"Неверный эмодзи: {emoji}", ephemeral=True)
                return
            pair_key = (role.id, emoji)
            if pair_key not in seen_pairs:
                seen_pairs.add(pair_key)
                roles_data.append({'emoji': emoji, 'role_id': role.id, 'label': role.name})
    if not roles_data:
        await interaction.followup.send("Укажите хотя бы одну пару", ephemeral=True)
        return
    embed = discord.Embed(title=заголовок, description=текст, color=discord.Color.blue())
    view = RoleButtonView(roles_data, 0)
    message = await interaction.channel.send(embed=embed, view=view)
    await interaction.followup.send(f"Панель создана: {message.jump_url}", ephemeral=True)

@bot.tree.command(name='commands', description='Выдать доступ к командам')
@app_commands.describe(команды="Список команд", пользователь="Пользователь", роль="Роль")
async def slash_commands(interaction: discord.Interaction, команды: str, пользователь: discord.Member = None, роль: discord.Role = None):
    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("У вас нет прав", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    guild_id_str = str(interaction.guild.id)
    if guild_id_str not in commands_access:
        commands_access[guild_id_str] = {}
    command_list = [cmd.strip() for cmd in команды.split(',')]
    if пользователь:
        for command_name in command_list:
            if command_name not in commands_access[guild_id_str]:
                commands_access[guild_id_str][command_name] = {'users': [], 'roles': []}
            if str(пользователь.id) not in commands_access[guild_id_str][command_name]['users']:
                commands_access[guild_id_str][command_name]['users'].append(str(пользователь.id))
        save_commands_access()
        embed = discord.Embed(title='ДОСТУП ВЫДАН', color=discord.Color.green())
        embed.add_field(name='Пользователь', value=пользователь.mention, inline=True)
        embed.add_field(name='Команды', value=f'/{", /".join(command_list)}', inline=False)
        await interaction.followup.send(embed=embed, ephemeral=True)
    elif роль:
        for command_name in command_list:
            if command_name not in commands_access[guild_id_str]:
                commands_access[guild_id_str][command_name] = {'users': [], 'roles': []}
            if str(роль.id) not in commands_access[guild_id_str][command_name]['roles']:
                commands_access[guild_id_str][command_name]['roles'].append(str(роль.id))
        save_commands_access()
        embed = discord.Embed(title='ДОСТУП ВЫДАН', color=discord.Color.green())
        embed.add_field(name='Роль', value=роль.mention, inline=True)
        embed.add_field(name='Команды', value=f'/{", /".join(command_list)}', inline=False)
        await interaction.followup.send(embed=embed, ephemeral=True)
    else:
        await interaction.followup.send('Укажите пользователя или роль', ephemeral=True)

@bot.tree.command(name='uncommands', description='Забрать доступ к командам')
@app_commands.describe(команды="Список команд", пользователь="Пользователь", роль="Роль")
async def slash_uncommands(interaction: discord.Interaction, команды: str, пользователь: discord.Member = None, роль: discord.Role = None):
    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("У вас нет прав", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    guild_id_str = str(interaction.guild.id)
    if guild_id_str not in commands_access:
        commands_access[guild_id_str] = {}
    command_list = [cmd.strip() for cmd in команды.split(',')]
    if пользователь:
        for command_name in command_list:
            if command_name in commands_access[guild_id_str]:
                if str(пользователь.id) in commands_access[guild_id_str][command_name].get('users', []):
                    commands_access[guild_id_str][command_name]['users'].remove(str(пользователь.id))
        save_commands_access()
        embed = discord.Embed(title='ДОСТУП УДАЛЕН', color=discord.Color.red())
        embed.add_field(name='Пользователь', value=пользователь.mention, inline=True)
        embed.add_field(name='Команды', value=f'/{", /".join(command_list)}', inline=False)
        await interaction.followup.send(embed=embed, ephemeral=True)
    elif роль:
        for command_name in command_list:
            if command_name in commands_access[guild_id_str]:
                if str(роль.id) in commands_access[guild_id_str][command_name].get('roles', []):
                    commands_access[guild_id_str][command_name]['roles'].remove(str(роль.id))
        save_commands_access()
        embed = discord.Embed(title='ДОСТУП УДАЛЕН', color=discord.Color.red())
        embed.add_field(name='Роль', value=роль.mention, inline=True)
        embed.add_field(name='Команды', value=f'/{", /".join(command_list)}', inline=False)
        await interaction.followup.send(embed=embed, ephemeral=True)
    else:
        await interaction.followup.send('Укажите пользователя или роль', ephemeral=True)

@bot.tree.command(name='infcommands', description='Показать доступные команды')
@app_commands.describe(игрок="Игрок", роль="Роль")
async def slash_infcommands(interaction: discord.Interaction, игрок: discord.Member = None, роль: discord.Role = None):
    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("У вас нет прав", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    guild_id_str = str(interaction.guild.id)
    if guild_id_str not in commands_access:
        commands_access[guild_id_str] = {}
    available_commands = []
    if игрок:
        target_name = игрок.display_name
        for cmd_name, access_data in commands_access[guild_id_str].items():
            if cmd_name == 'all':
                continue
            if str(игрок.id) in access_data.get('users', []):
                available_commands.append(cmd_name)
                continue
            for role in игрок.roles:
                if str(role.id) in access_data.get('roles', []):
                    available_commands.append(cmd_name)
                    break
        if 'all' in commands_access[guild_id_str]:
            if str(игрок.id) in commands_access[guild_id_str]['all'].get('users', []):
                for cmd_name in commands_access[guild_id_str].keys():
                    if cmd_name != 'all' and cmd_name not in available_commands:
                        available_commands.append(cmd_name)
            for role in игрок.roles:
                if str(role.id) in commands_access[guild_id_str]['all'].get('roles', []):
                    for cmd_name in commands_access[guild_id_str].keys():
                        if cmd_name != 'all' and cmd_name not in available_commands:
                            available_commands.append(cmd_name)
    elif роль:
        target_name = роль.name
        for cmd_name, access_data in commands_access[guild_id_str].items():
            if cmd_name == 'all':
                continue
            if str(роль.id) in access_data.get('roles', []):
                available_commands.append(cmd_name)
        if 'all' in commands_access[guild_id_str]:
            if str(роль.id) in commands_access[guild_id_str]['all'].get('roles', []):
                for cmd_name in commands_access[guild_id_str].keys():
                    if cmd_name != 'all' and cmd_name not in available_commands:
                        available_commands.append(cmd_name)
    else:
        await interaction.followup.send('Укажите игрока или роль', ephemeral=True)
        return
    if not available_commands:
        embed = discord.Embed(title=f'КОМАНДЫ ДЛЯ {target_name}', description='Нет доступных команд', color=discord.Color.orange())
    else:
        embed = discord.Embed(title=f'КОМАНДЫ ДЛЯ {target_name}', color=discord.Color.green())
        commands_list = '\n'.join([f'`/{cmd}`' for cmd in sorted(available_commands)])
        embed.add_field(name=f'Всего: {len(available_commands)}', value=commands_list[:1000], inline=False)
    await interaction.followup.send(embed=embed, ephemeral=True)

@bot.tree.command(name='sup_adm', description='Назначить роль для поддержки')
@app_commands.describe(роль="Роль")
async def slash_sup_adm(interaction: discord.Interaction, роль: discord.Role):
    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("У вас нет прав", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    guild_id_str = str(interaction.guild.id)
    if guild_id_str not in support_admins:
        support_admins[guild_id_str] = {'roles': []}
    if str(роль.id) not in support_admins[guild_id_str]['roles']:
        support_admins[guild_id_str]['roles'].append(str(роль.id))
        save_support_admins()
        embed = discord.Embed(title='РОЛЬ НАЗНАЧЕНА', color=discord.Color.green())
        embed.add_field(name='Роль', value=роль.mention, inline=True)
        await interaction.followup.send(embed=embed, ephemeral=True)
    else:
        await interaction.followup.send('Роль уже имеет доступ', ephemeral=True)

@bot.tree.command(name='sup_rem', description='Убрать роль из поддержки')
@app_commands.describe(роль="Роль")
async def slash_sup_rem(interaction: discord.Interaction, роль: discord.Role):
    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("У вас нет прав", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    guild_id_str = str(interaction.guild.id)
    if guild_id_str in support_admins:
        if str(роль.id) in support_admins[guild_id_str].get('roles', []):
            support_admins[guild_id_str]['roles'].remove(str(роль.id))
            save_support_admins()
            embed = discord.Embed(title='РОЛЬ УБРАНА', color=discord.Color.red())
            embed.add_field(name='Роль', value=роль.mention, inline=True)
            await interaction.followup.send(embed=embed, ephemeral=True)
        else:
            await interaction.followup.send('Роль не имеет доступа', ephemeral=True)
    else:
        await interaction.followup.send('Нет настроенных ролей', ephemeral=True)

@bot.tree.command(name='msg', description='Отправить сообщение')
@app_commands.describe(каналы="Каналы через #", сообщение="Текст", заголовок="Заголовок", тег="Роль", фото="Изображение")
async def slash_msg(interaction: discord.Interaction, каналы: str, сообщение: str, заголовок: str = None, тег: discord.Role = None, фото: discord.Attachment = None):
    if not has_command_access(interaction.user, 'msg'):
        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("У вас нет прав", ephemeral=True)
            return
    await interaction.response.defer(ephemeral=True)
    channel_mentions = re.findall(r'<#(\d+)>', каналы)
    channels_to_send = []
    for channel_id_str in channel_mentions:
        channel = interaction.guild.get_channel(int(channel_id_str))
        if channel and isinstance(channel, discord.TextChannel):
            channels_to_send.append(channel)
    if not channels_to_send:
        await interaction.followup.send('Не найдено каналов', ephemeral=True)
        return
    if заголовок:
        embed = discord.Embed(title=заголовок, description=сообщение, color=discord.Color.blue())
    else:
        embed = discord.Embed(description=сообщение, color=discord.Color.blue())
    if фото:
        embed.set_image(url=фото.url)
    success_count = 0
    allowed_mentions = discord.AllowedMentions(everyone=False, roles=[тег] if тег else [])
    for channel in channels_to_send:
        try:
            if тег:
                await channel.send(embed=embed, allowed_mentions=allowed_mentions)
                await channel.send(тег.mention, allowed_mentions=allowed_mentions)
            else:
                await channel.send(embed=embed, allowed_mentions=allowed_mentions)
            success_count += 1
        except Exception as e:
            logger.error(f"Ошибка: {e}")
    await interaction.followup.send(f'Отправлено в {success_count} каналов', ephemeral=True)

@bot.command(name='clear')
async def text_clear(ctx, количество: int):
    if not has_command_access(ctx.author, 'clear'):
        if not ctx.author.guild_permissions.manage_messages:
            await ctx.send("У вас нет прав", delete_after=5)
            return
    if количество <= 0 or количество > MAX_CLEAR_MESSAGES:
        await ctx.send(f'Укажите число от 1 до {MAX_CLEAR_MESSAGES}', delete_after=5)
        return
    deleted = await ctx.channel.purge(limit=количество + 1)
    msg = await ctx.send(f'Очищено {len(deleted) - 1} сообщений')
    await msg.delete(delay=3)

@bot.command(name='slowmode')
async def text_slowmode(ctx, секунды: int, канал: discord.TextChannel = None):
    if not has_command_access(ctx.author, 'slowmode'):
        await ctx.send("У вас нет прав", delete_after=5)
        return
    target_channel = канал or ctx.channel
    if not isinstance(target_channel, discord.TextChannel):
        await ctx.send('Только для текстовых каналов', delete_after=5)
        return
    if секунды < 0 or секунды > MAX_SLOWMODE_SECONDS:
        await ctx.send(f'Slowmode от 0 до {MAX_SLOWMODE_SECONDS}', delete_after=5)
        return
    await target_channel.edit(slowmode_delay=секунды)
    if секунды == 0:
        await ctx.send(f'Медленный режим выключен в {target_channel.mention}', delete_after=5)
    else:
        await ctx.send(f'Медленный режим установлен на {секунды} секунд', delete_after=5)

@bot.command(name='lockdown')
async def text_lockdown(ctx):
    if not ctx.author.guild_permissions.administrator:
        await ctx.send("У вас нет прав", delete_after=5)
        return
    if raid_active.get(ctx.guild.id, False):
        await ctx.send('Lockdown уже активен', delete_after=5)
        return
    await activate_lockdown(ctx.guild.id, 'ручное включение')
    await ctx.send('РЕЖИМ LOCKDOWN ВКЛЮЧЕН')

@bot.command(name='unlockdown')
async def text_unlockdown(ctx):
    if not ctx.author.guild_permissions.administrator:
        await ctx.send("У вас нет прав", delete_after=5)
        return
    if not raid_active.get(ctx.guild.id, False):
        await ctx.send('Lockdown не активен', delete_after=5)
        return
    await deactivate_lockdown(ctx.guild.id)
    await ctx.send('РЕЖИМ LOCKDOWN ОТКЛЮЧЕН')

@bot.command(name='log')
async def text_log(ctx, канал: discord.TextChannel):
    if not ctx.author.guild_permissions.administrator:
        await ctx.send("У вас нет прав", delete_after=5)
        return
    logs_config[str(ctx.guild.id)] = канал.id
    save_logs_config()
    await ctx.send(f'Логи настроены на {канал.mention}')

@bot.command(name='unlog')
async def text_unlog(ctx):
    if not ctx.author.guild_permissions.administrator:
        await ctx.send("У вас нет прав", delete_after=5)
        return
    if str(ctx.guild.id) in logs_config:
        del logs_config[str(ctx.guild.id)]
        save_logs_config()
        await ctx.send('Логи отключены')
    else:
        await ctx.send('Логи не были настроены', delete_after=5)

@bot.command(name='setup-support')
async def text_setup_support(ctx, канал: discord.TextChannel = None):
    if канал is None:
        await ctx.send("Использование: `!setup-support #канал`", delete_after=5)
        return
    if not ctx.author.guild_permissions.administrator:
        await ctx.send("У вас нет прав", delete_after=5)
        return
    support_config[str(ctx.guild.id)] = {'ticket_channel': канал.id, 'complaint_channel': канал.id}
    save_support_config()
    await ctx.send(f'✅ Система поддержки настроена в {канал.mention}')
    panel_embed = discord.Embed(
        title="🛡️ ТЕХНИЧЕСКАЯ ПОДДЕРЖКА",
        description="**Выберите действие:**\n📨 - Создать обращение\n⚠️ - Пожаловаться на пользователя\n📊 - Мои обращения",
        color=discord.Color.blue()
    )
    view = SupportView(ctx.guild.id)
    await канал.send(embed=panel_embed, view=view)

@bot.command(name='support')
async def text_support(ctx):
    if not ctx.author.guild_permissions.administrator:
        await ctx.send("У вас нет прав", delete_after=5)
        return
    embed = discord.Embed(
        title="🛡️ ТЕХНИЧЕСКАЯ ПОДДЕРЖКА",
        description="**Выберите действие:**\n📨 - Создать обращение\n⚠️ - Пожаловаться на пользователя\n📊 - Мои обращения",
        color=discord.Color.blue()
    )
    view = SupportView(ctx.guild.id)
    await ctx.send(embed=embed, view=view)

@bot.command(name='ticket-stats')
async def text_ticket_stats(ctx):
    if not can_manage_support(ctx.author, ctx.guild.id):
        await ctx.send("❌ У вас нет прав", delete_after=5)
        return
    open_tickets = sum(1 for t in tickets_data['tickets'].values() if t.get('status') == 'open')
    in_progress = sum(1 for t in tickets_data['tickets'].values() if t.get('status') == 'in_progress')
    closed_tickets = sum(1 for t in tickets_data['archived_tickets'].values())
    open_complaints = sum(1 for c in tickets_data['complaints'].values() if c.get('status') == 'open')
    in_progress_complaints = sum(1 for c in tickets_data['complaints'].values() if c.get('status') == 'in_progress')
    closed_complaints = sum(1 for c in tickets_data['archived_complaints'].values())
    embed = discord.Embed(title="📊 СТАТИСТИКА ПОДДЕРЖКИ", color=discord.Color.blue())
    embed.add_field(name="🎫 ТИКЕТЫ", value=f"🟡 Открытых: {open_tickets}\n🟢 В работе: {in_progress}\n📦 Архивировано: {closed_tickets}\n📊 Всего: {tickets_data['ticket_counter']}", inline=True)
    embed.add_field(name="⚠️ ЖАЛОБЫ", value=f"🟡 Открытых: {open_complaints}\n🟢 В работе: {in_progress_complaints}\n📦 Архивировано: {closed_complaints}\n📊 Всего: {tickets_data['complaint_counter']}", inline=True)
    await ctx.send(embed=embed)

@bot.command(name='my-tickets')
async def text_my_tickets(ctx):
    user_tickets = []
    for ticket_id, ticket_data in tickets_data['tickets'].items():
        if ticket_data.get('user_id') == ctx.author.id and ticket_data.get('status') in ['open', 'in_progress']:
            user_tickets.append((ticket_id, ticket_data, 'ticket'))
    for complaint_id, complaint_data in tickets_data['complaints'].items():
        if complaint_data.get('user_id') == ctx.author.id and complaint_data.get('status') in ['open', 'in_progress']:
            user_tickets.append((complaint_id, complaint_data, 'complaint'))
    if not user_tickets:
        await ctx.send("📭 У вас нет активных обращений")
        return
    embed = discord.Embed(title=f"📋 АКТИВНЫЕ ОБРАЩЕНИЯ ({len(user_tickets)})", color=discord.Color.blue())
    for item_id, item_data, item_type in user_tickets:
        status_text = "🟡 Ожидает" if item_data.get('status') == 'open' else "🟢 В работе"
        created_at = datetime.fromisoformat(item_data['created_at'])
        embed.add_field(
            name=f"{'🎫' if item_type == 'ticket' else '⚠️'} {item_id}",
            value=f"**Тема:** {item_data.get('topic', item_data.get('reason', 'Нет'))[:50]}\n**Статус:** {status_text}\n**Создан:** <t:{int(created_at.timestamp())}:R>",
            inline=False
        )
    await ctx.send(embed=embed)

@bot.command(name='addbalance')
async def text_add_balance(ctx, пользователь: discord.Member, количество: int):
    if not ctx.author.guild_permissions.administrator:
        await ctx.send("❌ У вас нет прав!", delete_after=5)
        return
    if количество <= 0:
        await ctx.send("❌ Количество должно быть больше 0!", delete_after=5)
        return
    add_balance(пользователь.id, количество)
    new_balance = get_balance(пользователь.id)
    embed = discord.Embed(title="💰 ВЫДАЧА COINS", color=discord.Color.green(), timestamp=datetime.now())
    embed.add_field(name="👤 Админ", value=ctx.author.mention, inline=True)
    embed.add_field(name="👤 Игрок", value=пользователь.mention, inline=True)
    embed.add_field(name="➕ Добавлено", value=f"+{количество} 🪙", inline=True)
    embed.add_field(name="💎 Новый баланс", value=f"{new_balance} 🪙", inline=True)
    await ctx.send(embed=embed)
    try:
        await пользователь.send(f"✅ Вам начислено **{количество}** 🪙 на сервере **{ctx.guild.name}**!\nВаш баланс: **{new_balance}** 🪙")
    except:
        pass

@bot.command(name='removebalance')
async def text_remove_balance(ctx, пользователь: discord.Member, количество: int):
    if not ctx.author.guild_permissions.administrator:
        await ctx.send("❌ У вас нет прав!", delete_after=5)
        return
    if количество <= 0:
        await ctx.send("❌ Количество должно быть больше 0!", delete_after=5)
        return
    current_balance = get_balance(пользователь.id)
    if current_balance < количество:
        await ctx.send(f"❌ У игрока {пользователь.mention} недостаточно средств! Баланс: **{current_balance}** 🪙", delete_after=5)
        return
    remove_balance(пользователь.id, количество)
    new_balance = get_balance(пользователь.id)
    embed = discord.Embed(title="💰 СНЯТИЕ COINS", color=discord.Color.red(), timestamp=datetime.now())
    embed.add_field(name="👤 Админ", value=ctx.author.mention, inline=True)
    embed.add_field(name="👤 Игрок", value=пользователь.mention, inline=True)
    embed.add_field(name="➖ Снято", value=f"-{количество} 🪙", inline=True)
    embed.add_field(name="💎 Новый баланс", value=f"{new_balance} 🪙", inline=True)
    await ctx.send(embed=embed)
    try:
        await пользователь.send(f"⚠️ С вашего счета снято **{количество}** 🪙 на сервере **{ctx.guild.name}**!\nВаш баланс: **{new_balance}** 🪙")
    except:
        pass

bot.run(TOKEN)