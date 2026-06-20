"""
🤖 BOT v7.4 - PRODUCTION READY (ALL BUGS FIXED & OPTIMIZED)

MAJOR FIXES in v7.4:
  ✅ Fix QQ88 stuck code (clear dedup + runtime cache)
  ✅ BOT_START_TIME set NGAY ĐẦU (trước preload)
  ✅ catch_up=False + handler priority=10
  ✅ Filter chat không trong config
  ✅ Worker count tăng 8→16, queue 200→500
  ✅ Message worker ultra fast (get_nowait)
  ✅ Double-check BOT_START_TIME trong handler + worker
"""

import asyncio
import csv
import json
import re
import time
import random
import traceback
import tempfile
import shutil
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

from telethon import TelegramClient, events
from telethon.tl.types import MessageEntitySpoiler
from telethon.network import ConnectionTcpAbridged
from playwright.async_api import async_playwright

from config import Config
from logger_setup import logger
from code_validator import CodeValidator
from image_code_extractor import get_image_extractor
from database import init_database
from rate_limiter import init_anti_detection
from monitoring import init_monitoring
from features import print_version_info, get_shutdown_handler, send_alert_to_user

# ============================================================
# DOMAIN-SPECIFIC SUBMIT BUTTON SELECTORS
# ============================================================
SUBMIT_BUTTON_SELECTORS = {
    "mm88code.com":    "img.submit-btn, .submit-button-container img, .submit-btn",
    "llwincode.com":   'img[src*="btnnhancode" i], img[alt*="nhan" i]',
    "xx88code.com":    'button[aria-label="Nhận code"], button[aria-label*="Nhan code" i]',
    "o8code.com":      ".modal-submit-btn",
    "new88b.today":    'button[aria-label*="Kiểm tra" i]',
    "tangquaqq88.com": 'button[aria-label*="Kiểm tra" i]',
    "uy88code.org":    "#casinoSubmit",
    "mmoocode.shop":   "#casinoSubmit",
}

# ============================================================
# BOT STATE
# ============================================================
class BotState:
    def __init__(self):
        self.playwright_instance = None
        self.connected_browsers = {}
        self.account_pages = {}
        self.context_locks = {}
        self.is_running = True
        self.cf_verified = {}
        self.submission_count = {}
        self._input_cache: dict = {}
        self._input_cache_ttl = 20.0
        self._site_code_seen: dict = {}
        self._page_urls: dict = {}
        self.handler_registered = False
        self._last_cleanup_time = time.time()
        self._pending_image_msgs: dict = {}
        self._PENDING_IMAGE_TTL: float = getattr(Config, 'PENDING_IMAGE_TTL', 180.0)
        self._tab_fail_count: dict = {}
        self._TAB_FAIL_THRESHOLD: int = getattr(Config, "TAB_FAIL_THRESHOLD", 5)
        # NEW: manual CF verification events (key -> asyncio.Event)
        self._manual_cf_events: dict = {}


bot_state = BotState()

# ✅ SET BOT_START_TIME NGAY TẬP ĐẦU (trước khi nhận tin)
BOT_START_TIME = datetime.now(timezone.utc)

# Telegram client - optimized for non-blocking
client = TelegramClient(
    Config.SESSION_NAME,
    Config.API_ID,
    Config.API_HASH,
    device_model="Desktop Bot",
    system_version="Windows 10",
    app_version="1.0",
    connection=ConnectionTcpAbridged,
    connection_retries=5,
    retry_delay=1,
    auto_reconnect=True,
    use_ipv6=False,
    flood_sleep_threshold=60,
    receive_updates=True,
    sequential_updates=False,
)

# Global state
_systems = None
message_queue: asyncio.Queue = None
message_workers: list = []
_history_queue: asyncio.Queue = None
_history_writer_task = None
_submit_semaphore: asyncio.Semaphore | None = None
_domain_semaphores: dict = {}
_active_submit_tasks: set[asyncio.Task] = set()

# ============================================================
# HELPERS & UTILITIES
# ============================================================

def normalize_domain(url: str) -> str:
    parsed = urlparse(url or "")
    domain = parsed.netloc or parsed.path
    return domain.lower().replace("www.", "").strip("/")


def select_random_code(codes: list) -> str:
    if not codes:
        return None
    return random.choice(codes)


def _today_str() -> str:
    return datetime.now().strftime("%Y-%m-%d")


# Code history logging
CODE_HISTORY_DIR = Path("logs/code_history")
CODE_HISTORY_DIR.mkdir(parents=True, exist_ok=True)


def _write_history_row(row: dict):
    try:
        fieldnames = [
            "time", "event_type", "channel", "site", "account", "code",
            "source", "status", "telegram_delay", "submit_elapsed",
            "message", "screenshot",
        ]
        csv_path = CODE_HISTORY_DIR / f"code_history_{_today_str()}.csv"
        jsonl_path = CODE_HISTORY_DIR / f"code_history_{_today_str()}.jsonl"
        
        write_header = not csv_path.exists()
        with csv_path.open("a", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if write_header:
                writer.writeheader()
            writer.writerow(row)
        
        with jsonl_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    except Exception as e:
        logger.debug(f"⚠️ Cannot write code history: {e}")


async def _history_writer_loop():
    global _history_queue
    while True:
        try:
            row = await _history_queue.get()
            if row is None:
                break
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, _write_history_row, row)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.debug(f"⚠️ history_writer_loop error: {e}")
        finally:
            try:
                _history_queue.task_done()
            except Exception:
                pass


def start_history_writer():
    global _history_queue, _history_writer_task
    _history_queue = asyncio.Queue(maxsize=2000)
    _history_writer_task = asyncio.create_task(_history_writer_loop())
    logger.info("✅ Background history writer started")


# bring_to_front helper that respects SKIP_BRING_TO_FRONT
async def maybe_bring_to_front(page):
    if getattr(Config, "SKIP_BRING_TO_FRONT", False):
        logger.debug("ℹ️ SKIP_BRING_TO_FRONT=true -> bỏ qua bring_to_front()")
        return
    try:
        await page.bring_to_front()
    except Exception:
        pass


def get_submit_semaphore() -> asyncio.Semaphore:
    global _submit_semaphore
    if _submit_semaphore is None:
        limit = max(1, int(getattr(Config, "MAX_CONCURRENT_SUBMITS", 2)))
        _submit_semaphore = asyncio.Semaphore(limit)
    return _submit_semaphore


def get_domain_semaphore(domain: str) -> asyncio.Semaphore:
    global _domain_semaphores
    if domain not in _domain_semaphores:
        limit = max(1, int(getattr(Config, "MAX_CONCURRENT_SUBMITS_PER_DOMAIN", 2)))
        _domain_semaphores[domain] = asyncio.Semaphore(limit)
    return _domain_semaphores[domain]


def append_code_history(
    event_type: str,
    code: str = "",
    target_url: str = "",
    account: str = "",
    channel: str = "",
    source: str = "",
    status: str = "",
    telegram_delay=None,
    submit_elapsed=None,
    message: str = "",
    screenshot: str = "",
):
    try:
        row = {
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "event_type": event_type,
            "channel": channel or "",
            "site": normalize_domain(target_url),
            "account": account or "",
            "code": str(code or ""),
            "source": source or "",
            "status": status or "",
            "telegram_delay": "" if telegram_delay is None else f"{float(telegram_delay):.2f}",
            "submit_elapsed": "" if submit_elapsed is None else f"{float(submit_elapsed):.2f}",
            "message": str(message or "").replace("\n", " ")[:300],
            "screenshot": str(screenshot or ""),
        }
        if _history_queue is not None:
            try:
                _history_queue.put_nowait(row)
            except asyncio.QueueFull:
                logger.debug("⚠️ History queue full")
        else:
            _write_history_row(row)
        return row
    except Exception as e:
        logger.debug(f"⚠️ Cannot enqueue code history: {e}")
        return None


# ============================================================
# DEDUPLICATION
# ============================================================

def _prune_site_code_seen():
    ttl = float(getattr(Config, "SITE_CODE_DEDUP_TTL", 10.0))
    now = time.time()
    expired = [k for k, ts in bot_state._site_code_seen.items() if now - ts > ttl]
    for k in expired:
        del bot_state._site_code_seen[k]


async def _cleanup_scheduler():
    while bot_state.is_running:
        try:
            await asyncio.sleep(float(getattr(Config, "INPUT_CACHE_CLEANUP_INTERVAL", 300)))
            _prune_site_code_seen()
            expired = await _cleanup_pending_images()
            if expired:
                logger.info(f"🧹 Cleaned {expired} expired pending image(s)")
            else:
                logger.debug("🧹 Cleanup done")
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.debug(f"⚠️ Cleanup error: {e}")


def is_site_code_duplicate(domain: str, code: str) -> bool:
    ttl = float(getattr(Config, "SITE_CODE_DEDUP_TTL", 10.0))
    now = time.time()
    _prune_site_code_seen()
    key = (domain, code.upper())
    seen_at = bot_state._site_code_seen.get(key)
    if seen_at is not None and now - seen_at < ttl:
        return True
    bot_state._site_code_seen[key] = now
    return False


def build_daily_summary():
    try:
        csv_path = CODE_HISTORY_DIR / f"code_history_{_today_str()}.csv"
        if not csv_path.exists():
            return None
        
        summary = {}
        with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("event_type") != "RESULT":
                    continue
                key = (row.get("site", ""), row.get("account", ""))
                if key not in summary:
                    summary[key] = {"SUCCESS": 0, "FAILED": 0, "UNKNOWN": 0}
                status = row.get("status") or "UNKNOWN"
                summary[key].setdefault(status, 0)
                summary[key][status] += 1
        
        out_path = CODE_HISTORY_DIR / f"daily_summary_{_today_str()}.csv"
        with out_path.open("w", newline="", encoding="utf-8-sig") as f:
            fieldnames = ["date", "site", "account", "success", "failed", "unknown", "total"]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for (site, account), counts in sorted(summary.items()):
                s = counts.get("SUCCESS", 0)
                fa = counts.get("FAILED", 0)
                u = counts.get("UNKNOWN", 0)
                writer.writerow({
                    "date": _today_str(),
                    "site": site,
                    "account": account,
                    "success": s,
                    "failed": fa,
                    "unknown": u,
                    "total": s + fa + u,
                })
        logger.info(f"📒 Daily summary: {out_path}")
        return str(out_path)
    except Exception as e:
        logger.warning(f"⚠️ Cannot create daily summary: {e}")
        return None


def measure_telegram_delay_fast(msg_timestamp) -> float | None:
    try:
        if msg_timestamp.tzinfo is None:
            msg_timestamp = msg_timestamp.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - msg_timestamp).total_seconds()
    except Exception:
        return None


def build_unique_account_targets():
    items = []
    seen_domains = set()
    
    sorted_channels = sorted(
        Config.CHANNEL_CONFIG.items(),
        key=lambda item: item[1].get("priority", 999),
    )
    
    for chat_id, channel_config in sorted_channels:
        target_url = channel_config["url"]
        domain = normalize_domain(target_url)
        
        if domain in seen_domains:
            continue
        seen_domains.add(domain)
        
        accounts = channel_config.get("accounts", [])
        if not accounts:
            continue
        
        first_account = sorted(accounts, key=lambda a: a.get("priority", 999))[0]
        
        if domain == "mm88code.com" and len(accounts) >= 2:
            sorted_accounts = sorted(accounts, key=lambda a: a.get("priority", 999))
            for acc in sorted_accounts:
                items.append({
                    "chat_id": chat_id,
                    "channel_name": channel_config.get("name", ""),
                    "target_url": target_url,
                    "domain": domain,
                    "key": f"{domain}|{acc['username']}",
                    "port": get_user_port(acc["username"]),
                    "accounts": [acc],
                })
            continue
        
        port = get_user_port(first_account["username"])
        
        items.append({
            "chat_id": chat_id,
            "channel_name": channel_config.get("name", ""),
            "target_url": target_url,
            "domain": domain,
            "key": domain,
            "port": port,
            "accounts": sorted(accounts, key=lambda a: a.get("priority", 999)),
        })
    
    return items


def get_user_port(user: str) -> int:
    for port, users_list in getattr(Config, "CDP_CONNECTIONS", {}).items():
        if user in users_list:
            return int(port)
    return 9222


def get_default_account_for_domain(domain_key: str) -> str | None:
    if "|" in domain_key:
        domain, user = domain_key.split("|", 1)
        return user
    domain = domain_key
    for chat_id, cfg in Config.CHANNEL_CONFIG.items():
        if normalize_domain(cfg["url"]) == domain:
            accounts = cfg.get("accounts", [])
            if accounts:
                return sorted(accounts, key=lambda a: a.get("priority", 999))[0]["username"]
    return None


# ============================================================
# BROWSER INITIALIZATION
# ============================================================

async def verify_telegram_session():
    logger.info("\n" + "=" * 70)
    logger.info("🔐 VERIFYING TELEGRAM SESSION...")
    try:
        me = await client.get_me()
        dc_id = client.session.dc_id
        dc_names = {1: "DC1 Miami 🇺🇸", 2: "DC2 Amsterdam 🇳🇱", 3: "DC3 Miami 🇺🇸", 4: "DC4 Amsterdam 🇳🇱", 5: "DC5 Singapore 🇸🇬"}
        dc_label = dc_names.get(dc_id, f"DC{dc_id} Unknown")
        logger.info(f"✅ SESSION VALID! @{me.username} (ID: {me.id})")
        logger.info(f"📡 Telegram DC: {dc_label} — {'✅ Tốt cho VN' if dc_id == 5 else '⚠️ Xa VN, có thể delay'}")
        return True
    except Exception as e:
        logger.error(f"❌ SESSION ERROR: {e}")
        return False


async def verify_channels_and_get_ids():
    logger.info("\n" + "=" * 70)
    logger.info("📡 VERIFYING CHANNELS...")
    valid_channels = {}
    my_dialogs = {dialog.id: dialog async for dialog in client.iter_dialogs()}
    
    for chat_id, channel_config in Config.CHANNEL_CONFIG.items():
        if chat_id in my_dialogs:
            logger.info(f"✅ VALID: {channel_config['name']}")
            valid_channels[chat_id] = channel_config
        else:
            logger.warning(f"❌ NOT JOINED: {channel_config['name']}")
    
    return valid_channels


async def init_systems():
    print_version_info()
    db = init_database(Config.DATABASE_PATH)
    anti_det = init_anti_detection()
    _, _, perf_mon = init_monitoring()
    
    bot_state.playwright_instance = await async_playwright().start()
    get_shutdown_handler().setup(bot_state)
    
    start_history_writer()
    
    return {
        "db": db,
        "anti_detection": anti_det,
        "performance_monitor": perf_mon,
    }


async def safe_is_visible(element) -> bool:
    try:
        return await element.is_visible()
    except Exception:
        return False


def _invalidate_input_cache(key: str):
    bot_state._input_cache.pop(key, None)


async def find_input_fields(page, cache_key: str = None):
    now = time.time()
    if cache_key:
        cached = bot_state._input_cache.get(cache_key)
        if cached:
            username_input, code_input, cache_time = cached
            if now - cache_time < bot_state._input_cache_ttl:
                try:
                    if code_input:
                        visible = await code_input.is_visible()
                        if visible:
                            return username_input, code_input
                    _invalidate_input_cache(cache_key)
                except Exception:
                    _invalidate_input_cache(cache_key)
    username_input = None
    code_input = None
    username_selectors = [
        "#account-code", "#username-input", "#ten_tai_khoan",
        "input#username", "input[name='username']",
        "input[placeholder*='người dùng' i]",
        "input[placeholder*='tên' i]",
        "input[placeholder*='tài' i]",
        "input[placeholder*='tài khoản' i]",
        "input[placeholder*='user' i]",
        "input[placeholder*='đăng nhập' i]",
        "input[name='ten_tai_khoan']", "input[id='username']",
        "input[type='text']",
    ]
    code_selectors = [
        "#promo-code", "#giftcode-input",
        "input[autocomplete='one-time-code']",
        "input#code", "input[name='code']",
        "input[placeholder*='mã code' i]",
        "input[placeholder*='code' i]",
        "input[placeholder*='mã' i]",
        "input[name='giftcode']",
        "input[id='code']",
        "input[id*='code' i]",
        "input[id*='promo' i]",
    ]
    try:
        for selector in username_selectors:
            try:
                element = await page.query_selector(selector)
                if element and await safe_is_visible(element):
                    username_input = element
                    break
            except Exception:
                pass
        for selector in code_selectors:
            try:
                element = await page.query_selector(selector)
                if element and await safe_is_visible(element):
                    code_input = element
                    break
            except Exception:
                pass
        if not username_input or not code_input:
            inputs = await page.query_selector_all(
                "input:not([type='hidden']):not([type='checkbox'])"
                ":not([type='radio']):not([type='submit'])"
            )
            visible_inputs = []
            for inp in inputs:
                if await safe_is_visible(inp):
                    visible_inputs.append(inp)
            
            if len(visible_inputs) >= 2:
                if not username_input:
                    username_input = visible_inputs[0]
                if not code_input:
                    code_input = visible_inputs[1]
            elif len(visible_inputs) == 1:
                if not code_input:
                    code_input = visible_inputs[0]
    except Exception as e:
        logger.debug(f"⚠️ Error finding input fields: {e}")
    if cache_key and code_input:
        bot_state._input_cache[cache_key] = (username_input, code_input, now)
    return username_input, code_input


# ============================================================
# SCROLL TO INPUT FIELDS
# ============================================================

async def scroll_to_input_fields(page):
    try:
        found = await page.evaluate("""
            () => {
                const inputs = document.querySelectorAll('input[type="text"], input:not([type="hidden"])');
                if (inputs.length > 0) {
                    const firstInput = inputs[0];
                    firstInput.scrollIntoView({behavior: 'smooth', block: 'center'});
                    firstInput.focus();
                    return true;
                }
                return false;
            }
        """)
        await asyncio.sleep(0.1)
        if found:
            logger.debug("✅ Scrolled to input fields")
        else:
            logger.warning("⚠️ scroll_to_input_fields: không tìm thấy input nào trên trang")
        return found
    except Exception as e:
        logger.debug(f"⚠️ Scroll error: {e}")
        return False


async def get_input_value(input_element) -> str:
    try:
        return (await input_element.input_value(timeout=1000)).strip()
    except Exception:
        return ""


# ============================================================
# SUBMIT BUTTON CLICKING
# ============================================================

async def click_submit_fast(page, domain: str = "") -> bool:
    domain_sel = SUBMIT_BUTTON_SELECTORS.get(domain)
    if domain_sel:
        try:
            clicked = await page.evaluate(f"""
                async () => {{
                    const deadline = Date.now() + 600;
                    while (Date.now() < deadline) {{
                        const btn = document.querySelector('{domain_sel}');
                        if (btn && !btn.disabled) {{
                            const rect = btn.getBoundingClientRect();
                            if (rect.width > 0 && rect.height > 0) {{
                                btn.click();
                                return true;
                            }}
                        }}
                        await new Promise(r => setTimeout(r, 100));
                    }}
                    const btn = document.querySelector('{domain_sel}');
                    if (btn) {{ btn.click(); return true; }}
                    return false;
                }}
            """)
            if clicked:
                logger.debug(f"✅ Clicked domain-specific button: {domain}")
                return True
        except Exception:
            pass
    try:
        clicked = await page.evaluate("""
            () => {
                const keywords = [
                    'kiểm tra ngay', 'kiem tra ngay',
                    'kiểm tra', 'kiem tra',
                    'nhận code', 'nhan code',
                    'nhận ngay', 'nhan ngay',
                    'áp dụng', 'ap dung',
                    'đổi code', 'doi code',
                    'nạp code', 'nap code',
                    'gửi', 'gui',
                    'submit', 'apply'
                ];
                const EXCLUDE = /menu|nav|home|close|cancel|toggle|hamburger|back|trở về|huỷ|hủy|đóng|xác thực|xac thuc|verify|check/i;
                const els = [...document.querySelectorAll(
                    'button, a[role="button"], div[role="button"], span[role="button"], input[type="button"], input[type="submit"]'
                )];
                for (const kw of keywords) {
                    for (const el of els) {
                        if (el.disabled) continue;
                        const aria = (el.getAttribute('aria-label') || '').toLowerCase();
                        const img = el.querySelector('img[alt]');
                        const imgAlt = img ? (img.getAttribute('alt') || '').toLowerCase() : '';
                        const txt = (el.innerText || el.textContent || el.value || '').toLowerCase().trim();
                        if (EXCLUDE.test(aria + txt)) continue;
                        if ([txt, aria, imgAlt].some(s => s && s.includes(kw))) {
                            const rect = el.getBoundingClientRect();
                            if (rect.width > 0 && rect.height > 0) {
                                el.click();
                                return true;
                            }
                        }
                    }
                }
                return false;
            }
        """)
        if clicked:
            return True
    except Exception:
        pass
    generic_selectors = [
        "button[type='submit']",
        "input[type='submit']",
        ".btn-submit",
        ".apply-btn",
        ".submit-btn",
        "[class*='submit' i]",
        "[class*='apply' i]",
    ]
    for sel in generic_selectors:
        try:
            el = await page.query_selector(sel)
            if el and await safe_is_visible(el):
                await page.evaluate("el => el.click()", el)
                return True
        except Exception:
            pass
    try:
        await page.keyboard.press("Enter")
        return True
    except Exception:
        return False


# ============================================================
# CLOUDFLARE HANDLING
# ============================================================

async def handle_cloudflare_popup(page) -> bool:
    try:
        modal_visible = False
        for sel in ["text=Mã xác thực", "text=MÃ XÁC THỰC", "h3:has-text('xác thực')"]:
            try:
                el = await page.query_selector(sel)
                if el and await safe_is_visible(el):
                    modal_visible = True
                    break
            except Exception:
                pass

        if not modal_visible:
            return False

        logger.info("🔒 [CF] Modal 'Mã xác thực' phát hiện — chờ Turnstile verify...")

        try:
            await maybe_bring_to_front(page)
        except Exception:
            pass

        BTN_SELECTORS = [
            "button:has-text('Xác thực')",
            "button:has-text('Xac thuc')",
        ]
        deadline = time.time() + 20.0
        btn = None
        became_enabled = False

        while time.time() < deadline:
            for sel in BTN_SELECTORS:
                try:
                    el = await page.query_selector(sel)
                    if el and await safe_is_visible(el):
                        btn = el
                        disabled = await el.get_attribute("disabled")
                        if disabled is None:
                            became_enabled = True
                        break
                except Exception:
                    pass
            if became_enabled:
                logger.info("✅ [CF] Turnstile verified — nút 'Xác thực' đã enable")
                break
            await asyncio.sleep(0.3)

        if not btn:
            logger.warning("⚠️ [CF] Không tìm thấy nút Xác thực trong modal")
            return False

        if not became_enabled:
            logger.warning("⚠️ [CF] Nút vẫn disabled sau 20s — thử tải lại captcha")
            for sel in ["button:has-text('Tải lại captcha')", "button:has-text('Tai lai captcha')"]:
                try:
                    reload_btn = await page.query_selector(sel)
                    if reload_btn and await safe_is_visible(reload_btn):
                        await reload_btn.click()
                        await asyncio.sleep(0.5)
                        break
                except Exception:
                    pass
            deadline2 = time.time() + 10.0
            while time.time() < deadline2:
                try:
                    btn = await page.query_selector(BTN_SELECTORS[0])
                    if btn:
                        disabled = await btn.get_attribute("disabled")
                        if disabled is None:
                            became_enabled = True
                            logger.info("✅ [CF] Verified sau khi tải lại captcha")
                            break
                except Exception:
                    pass
                await asyncio.sleep(0.3)

        if not became_enabled or not btn:
            logger.warning("⚠️ [CF] Turnstile không pass — bỏ qua, submit sẽ thất bại")
            return False

        try:
            await btn.hover()
            await asyncio.sleep(random.uniform(0.1, 0.2))
            await btn.click(timeout=3000)
            logger.info("✅ [CF] Đã click 'Xác thực'")
            await asyncio.sleep(0.6)
            return True
        except Exception as e:
            logger.warning(f"⚠️ [CF] Click lỗi: {e} — thử force click")
            try:
                await btn.click(force=True, timeout=2000)
                await asyncio.sleep(0.6)
                return True
            except Exception as e2:
                logger.error(f"❌ [CF] Force click cũng lỗi: {e2}")
                return False

    except Exception as e:
        logger.debug(f"⚠️ CF popup error: {e}")
        return False


# New: wait for manual verification flow
async def wait_for_manual_cf_verification(page, max_wait: float = None) -> bool:
    max_wait = max_wait or float(getattr(Config, "MANUAL_CF_TIMEOUT", 300))
    try:
        await maybe_bring_to_front(page)
    except Exception:
        pass

    # Determine a domain key for this page
    try:
        page_url = page.url
    except Exception:
        page_url = ""
    domain_key = normalize_domain(page_url) or "manual_cf"

    logger.warning("⚠️ SKIP_AUTO_VERIFY=TRUE — please manually complete Cloudflare on Edge.")
    logger.warning(f"⏳ Waiting up to {int(max_wait)}s for manual verification (domain: {domain_key})...")

    BTN_SELECTORS = [
        "button:has-text('Xác thực')",
        "button:has-text('Xac thuc')",
        "button:has-text('Verify')",
        "button:has-text('Tiếp tục')",
    ]
    modal_selectors = [
        "text=Mã xác thực", "text=MÃ XÁC THỰC", "iframe[src*='turnstile']",
        "iframe[src*='challenges.cloudflare.com']"
    ]

    deadline = time.time() + max_wait

    # create an asyncio.Event to allow Telegram /verify signal to wake this wait
    ev = asyncio.Event()
    bot_state._manual_cf_events[domain_key] = ev

    # send Telegram alert if admin configured
    try:
        admin_id = getattr(Config, "TELEGRAM_ADMIN_ID", 0)
        if admin_id and client:
            msg = (
                f"🚨 BOT: Cloudflare/Turnstile detected on {domain_key or page_url} — please verify manually on Edge.\n\n"
                "Sau khi verify, reply here with `/cf_verified {domain}` hoặc `/cf_verified` để tiếp tục."
            )
            asyncio.create_task(send_alert_to_user(client, msg, admin_id))
    except Exception:
        pass

    try:
        while time.time() < deadline:
            # Check if Telegram admin signaled verification
            if ev.is_set():
                logger.info("✅ Manual verification signaled via Telegram")
                return True

            # Check modal presence
            modal_present = False
            for sel in modal_selectors:
                try:
                    el = await page.query_selector(sel)
                    if el and await safe_is_visible(el):
                        modal_present = True
                        break
                except Exception:
                    continue
            if not modal_present:
                logger.info("✅ Modal Turnstile không còn hiển thị → coi như verified")
                return True

            # Check if verify button enabled
            for sel in BTN_SELECTORS:
                try:
                    btn = await page.query_selector(sel)
                    if btn and await safe_is_visible(btn):
                        disabled = await btn.get_attribute("disabled")
                        if disabled is None:
                            logger.info("✅ Nút 'Xác thực' đã enable — người dùng đã verify")
                            return True
                except Exception:
                    continue

            await asyncio.sleep(0.5)

        logger.warning("⏰ Timeout chờ manual verification")
        return False
    finally:
        # cleanup event
        try:
            bot_state._manual_cf_events.pop(domain_key, None)
        except Exception:
            pass


# ============================================================
# RESULT DETECTION
# ============================================================

async def _fetch_element_text(page, selector: str) -> str:
    try:
        elements = await page.query_selector_all(selector)
        texts = []
        for el in elements:
            try:
                text = await el.inner_text(timeout=300)
                if text and text.strip():
                    texts.append(text.strip())
            except Exception:
                pass
        return " ".join(texts)
    except Exception:
        return ""

# ... rest of file unchanged ...

# ============================================================
# TELEGRAM ADMIN COMMANDS (Manual CF verification)
# ============================================================

@client.on(events.NewMessage(pattern=r'^/cf_verified(?:\s+(.+))?$', incoming=True))
async def handle_cf_verified(event):
    try:
        sender = await event.get_sender()
        admin_id = getattr(Config, "TELEGRAM_ADMIN_ID", 0)
        if admin_id and sender and sender.id != admin_id:
            await event.respond("⚠️ Bạn không được phép thực hiện lệnh này.")
            return

        # Extract optional domain
        domain_arg = None
        try:
            domain_arg = event.pattern_match.group(1)
            if domain_arg:
                domain_arg = domain_arg.strip()
        except Exception:
            domain_arg = None

        if domain_arg:
            key = domain_arg.lower()
            ev = bot_state._manual_cf_events.get(key)
            if not ev:
                # try normalized URL
                norm = normalize_domain(domain_arg)
                ev = bot_state._manual_cf_events.get(norm)
                key = norm
            if ev:
                ev.set()
                await event.respond(f"✅ Đã đánh dấu verified cho {key}")
            else:
                await event.respond("⚠️ Không thấy pending CF cho domain đó.")
        else:
            # mark all pending as verified
            for k, ev in list(bot_state._manual_cf_events.items()):
                try:
                    ev.set()
                except Exception:
                    pass
            await event.respond("✅ Đã đánh dấu verified cho tất cả pending CF")
    except Exception as e:
        logger.debug(f"⚠️ handle_cf_verified error: {e}")

