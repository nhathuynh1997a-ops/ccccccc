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
from features import print_version_info, get_shutdown_handler

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
        # ✅ FIX: _site_code_ttl đã bỏ (dead code) — is_site_code_duplicate() đọc Config.SITE_CODE_DEDUP_TTL trực tiếp
        self._page_urls: dict = {}
        self.handler_registered = False
        self._last_cleanup_time = time.time()
        # ✅ UY88 FIX: theo dõi ảnh chưa có caption, chờ MessageEdited
        # key = (chat_id, message_id), value = (event, timestamp_received)
        self._pending_image_msgs: dict = {}
        self._PENDING_IMAGE_TTL: float = getattr(Config, 'PENDING_IMAGE_TTL', 180.0)  # lấy từ config
        # g) Tab fail counter - đếm số lần submit thất bại liên tiếp
        self._tab_fail_count: dict = {}   # key → số lần fail liên tiếp
        self._TAB_FAIL_THRESHOLD: int = getattr(Config, "TAB_FAIL_THRESHOLD", 5)


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
_domain_semaphores: dict = {}  # mỗi domain có semaphore riêng → chạy song song thật
_active_submit_tasks: set[asyncio.Task] = set()

# ============================================================
# HELPERS & UTILITIES
# ============================================================

def normalize_domain(url: str) -> str:
    """Normalize URL to domain."""
    parsed = urlparse(url or "")
    domain = parsed.netloc or parsed.path
    return domain.lower().replace("www.", "").strip("/")


def select_random_code(codes: list) -> str:
    """Select random code from list."""
    if not codes:
        return None
    return random.choice(codes)


def _today_str() -> str:
    """Get today's date string (YYYY-MM-DD)."""
    return datetime.now().strftime("%Y-%m-%d")


# Code history logging
CODE_HISTORY_DIR = Path("logs/code_history")
CODE_HISTORY_DIR.mkdir(parents=True, exist_ok=True)


def _write_history_row(row: dict):
    """Write one row to CSV and JSONL."""
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
    """Background worker for history writing."""
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
    """Start background history writer task."""
    global _history_queue, _history_writer_task
    _history_queue = asyncio.Queue(maxsize=2000)
    _history_writer_task = asyncio.create_task(_history_writer_loop())
    logger.info("✅ Background history writer started")


def get_submit_semaphore() -> asyncio.Semaphore:
    """Get or create GLOBAL submit semaphore (legacy fallback)."""
    global _submit_semaphore
    if _submit_semaphore is None:
        limit = max(1, int(getattr(Config, "MAX_CONCURRENT_SUBMITS", 2)))
        _submit_semaphore = asyncio.Semaphore(limit)
    return _submit_semaphore


def get_domain_semaphore(domain: str) -> asyncio.Semaphore:
    """
    Semaphore RIÊNG cho mỗi domain → các domain khác nhau submit SONG SONG
    hoàn toàn, không phải xếp hàng chờ chung 1 semaphore global.
    Mỗi domain vẫn giới hạn concurrent submit nội bộ (theo MAX_CONCURRENT_SUBMITS)
    để tránh đụng 2 tab cùng lúc trên cùng 1 site.
    """
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
    """Queue code history entry."""
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
    """Clean expired entries from dedup cache."""
    ttl = float(getattr(Config, "SITE_CODE_DEDUP_TTL", 10.0))
    now = time.time()
    expired = [k for k, ts in bot_state._site_code_seen.items() if now - ts > ttl]
    for k in expired:
        del bot_state._site_code_seen[k]


async def _cleanup_scheduler():
    """Periodically clean up memory caches."""
    while bot_state.is_running:
        try:
            await asyncio.sleep(float(getattr(Config, "INPUT_CACHE_CLEANUP_INTERVAL", 300)))
            _prune_site_code_seen()
            # ✅ UY88 FIX: dọn pending image messages quá TTL
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
    """Check if code was recently submitted to this domain."""
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
    """Build end-of-day summary report."""
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
    """Measure Telegram message delay."""
    try:
        if msg_timestamp.tzinfo is None:
            msg_timestamp = msg_timestamp.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - msg_timestamp).total_seconds()
    except Exception:
        return None


def build_unique_account_targets():
    """Build list of unique domain targets with accounts."""
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
        
        # ✅ MM88: tách riêng 1 tab/account để chạy song song
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
    """Get CDP port for user."""
    for port, users_list in getattr(Config, "CDP_CONNECTIONS", {}).items():
        if user in users_list:
            return int(port)
    return 9222


def get_default_account_for_domain(domain_key: str) -> str | None:
    """Get default account for domain (watchdog use)."""
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
    """Verify Telegram session is valid."""
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
    """Verify all configured channels are accessible."""
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
    """Initialize all systems."""
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
    """Safely check if element is visible."""
    try:
        return await element.is_visible()
    except Exception:
        return False


def _invalidate_input_cache(key: str):
    """Invalidate input field cache."""
    bot_state._input_cache.pop(key, None)


async def find_input_fields(page, cache_key: str = None):
    """Find username and code input fields with caching."""
    now = time.time()
    
    # Check cache
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
        # Find username input
        for selector in username_selectors:
            try:
                element = await page.query_selector(selector)
                if element and await safe_is_visible(element):
                    username_input = element
                    break
            except Exception:
                pass
        
        # Find code input
        for selector in code_selectors:
            try:
                element = await page.query_selector(selector)
                if element and await safe_is_visible(element):
                    code_input = element
                    break
            except Exception:
                pass
        
        # Fallback: find visible inputs
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
    
    # Only cache if code_input found
    if cache_key and code_input:
        bot_state._input_cache[cache_key] = (username_input, code_input, now)
    
    return username_input, code_input

# ============================================================
# SCROLL TO INPUT FIELDS
# ============================================================

async def scroll_to_input_fields(page):
    """Cuộn tới input field để hiển thị & chuẩn bị cho Cloudflare"""
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
    """Get current value from input element."""
    try:
        return (await input_element.input_value(timeout=1000)).strip()
    except Exception:
        return ""


# ============================================================
# SUBMIT BUTTON CLICKING
# ============================================================

async def click_submit_fast(page, domain: str = "") -> bool:
    """Click submit button with domain-specific selectors first."""
    # Try domain-specific selector
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
    
    # Try text/aria-label matching
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
                // LOẠI TRỪ: nút CF Turnstile ('xác thực', 'verify', 'check') và nav/menu
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
    
    # Try generic selectors
    generic_selectors = [
        "button[type='submit']",
        "input[type='submit']",
        ".btn-submit",
        ".apply-btn",
        ".submit-btn",
        "[class*='submit' i]",
        "[class*='apply' i]",
        # "[class*='check' i]",  # BỎ: có thể khớp CF checkbox/widget
    ]
    
    for sel in generic_selectors:
        try:
            el = await page.query_selector(sel)
            if el and await safe_is_visible(el):
                await page.evaluate("el => el.click()", el)
                return True
        except Exception:
            pass
    
    # Last resort: Press Enter
    try:
        await page.keyboard.press("Enter")
        return True
    except Exception:
        return False


# ============================================================
# CLOUDFLARE HANDLING
# ============================================================

async def handle_cloudflare_popup(page) -> bool:
    """
    Xử lý popup "Mã xác thực" của QQ88/NEW88 — đây là Cloudflare Turnstile thật,
    nhúng trong modal riêng của site (không phải full-page CF challenge).

    Cấu trúc thật (xem DevTools):
        <h3>Mã xác thực</h3>
        <div>...Turnstile widget... "✅ Thành công!"...</div>
        <button disabled>Xác thực</button>   ← disabled cho tới khi Turnstile pass

    Quy trình đúng:
      1. Phát hiện modal qua tiêu đề "Mã xác thực" (ổn định hơn match nút)
      2. Đợi Turnstile tự verify ngầm — KHÔNG giới hạn cứng 8s, poll tới khi
         nút "Xác thực" hết disabled HOẶC timeout dài hơn (20s)
      3. Click bằng Playwright .click() thật (không phải el.click() qua JS)
         để trigger đúng React onClick + pointer event mà Turnstile/React cần
    """
    try:
        # 1) Phát hiện modal bằng tiêu đề ổn định "Mã xác thực"
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
            await page.bring_to_front()
        except Exception:
            pass

        # 2) Poll tới khi nút "Xác thực" hết disabled, tối đa 20s
        # (Turnstile load + verify có thể mất 3-15s tùy mạng, 8s cũ thường không đủ)
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
            # Hết 20s mà nút vẫn disabled — Turnstile có thể đang chờ tương tác
            # hoặc bị chặn. Thử click "Tải lại captcha" rồi đợi thêm 1 lần.
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
            # Đợi thêm 10s
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

        # 3) Click bằng Playwright thật — không dùng page.evaluate JS click
        # (đảm bảo trigger đúng React onClick + pointer events mà Turnstile cần)
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


# ============================================================
# RESULT DETECTION
# ============================================================

async def _fetch_element_text(page, selector: str) -> str:
    """Fetch text from element(s)."""
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


def _filter_nextjs_noise(text: str) -> str:
    """Filter Next.js hydration noise."""
    if not text:
        return ""
    noise_markers = [
        "__next_f", "__NEXT", "self.__next",
        'push([1,"', '"stylesheet"', '"link"',
        "webpack", "hydrat", '"rel":', '"href":',
        ":[[[\"$\"",
    ]
    t = text.strip()
    for marker in noise_markers:
        if marker in t:
            return ""
    if t.startswith(('{"', '[["', '[[["', 'self.')):
        return ""
    return t


async def detect_result_text(page) -> str:
    """Detect result text on page."""
    PRIORITY_SELECTORS = [
        # SweetAlert2 (dùng phổ biến cho QQ88, NEW88, UY88)
        ".swal2-html-container", ".swal2-title", ".swal2-popup",
        # QQ88 specific
        "div[class*='popup'] p",
        "div[class*='modal'] p",
        "div[class*='dialog'] p",
        "div[class*='alert'] p",
        "div[class*='notice'] p",
        "div[class*='message'] p",
        # Tailwind classes
        ".text-red-600", ".text-green-600", ".text-yellow-600",
        ".text-red-500", ".text-green-500",
        "p.mt-1.text-sm",
        "div[class*='rounded-2xl'] p",
        "div[class*='rounded-xl'] p",
        "div[class*='rounded-lg'] p",
        # ARIA
        "[role='alert']", "[role='status']", "[role='dialog']",
        # Fixed/overlay popups
        "div[style*='position: fixed'] p",
        "div[style*='position:fixed'] p",
    ]
    
    for sel in PRIORITY_SELECTORS:
        try:
            txt = await _fetch_element_text(page, sel)
            if txt and len(txt.strip()) >= 3:
                clean = _filter_nextjs_noise(txt.strip())
                if clean:
                    return clean
        except Exception:
            pass
    
    result_selectors = [
        ".text-red-600", ".text-green-600", "p.mt-1.text-sm",
        "div[class*='rounded-2xl'] p",
        "div[class*='rounded-xl'] p",
        "div[class*='rounded-lg'] p",
        "[role='dialog']", "[role='alert']", "[role='status']",
        ".modal-body", ".modal-content", ".popup-content",
        ".alert", "[class*='success']", "[class*='error']",
        "[class*='toast']", "[class*='result']",
        "[class*='notify']", "[class*='modal']", "[class*='popup']",
        "[class*='notification']", "div[style*='position: fixed']",
    ]
    
    tasks = [_fetch_element_text(page, sel) for sel in result_selectors]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    combined = ""
    
    for r in results:
        if isinstance(r, str) and r.strip():
            filtered = _filter_nextjs_noise(r.strip())
            if filtered:
                combined += filtered + " "
    
    if len(combined.strip()) >= 3:
        return combined.strip()
    
    try:
        page_text = await page.evaluate("""
            () => {
                const keywords = [
                    'thành công', 'thanh cong', 'thất bại', 'that bai',
                    'sai', 'lỗi', 'loi', 'đã sử dụng', 'da su dung',
                    'success', 'failed', 'error', 'invalid', 'used',
                    'không hợp lệ', 'khong hop le', 'hết hạn', 'het han',
                    'không đúng', 'không tồn tại',
                ];
                const noisePatterns = ['__next_f', '__NEXT', 'self.__next', 'push([', 'webpack'];
                const walker = document.createTreeWalker(
                    document.body, NodeFilter.SHOW_TEXT, null, false
                );
                let node;
                while (node = walker.nextNode()) {
                    const parent = node.parentElement;
                    if (!parent) continue;
                    const tag = parent.tagName || '';
                    if (['SCRIPT','STYLE','NOSCRIPT'].includes(tag)) continue;
                    const txt = (node.textContent || '').trim();
                    if (txt.length < 3) continue;
                    if (noisePatterns.some(p => txt.includes(p))) continue;
                    const lower = txt.toLowerCase();
                    if (keywords.some(k => lower.includes(k))) return txt;
                }
                return '';
            }
        """)
        if page_text:
            clean = _filter_nextjs_noise(page_text)
            if clean:
                return clean
    except Exception:
        pass
    
    return ""


async def take_result_screenshot(page, user: str, code: str, target_url: str, status: str) -> str:
    """Take screenshot of result."""
    if not bool(getattr(Config, "SCREENSHOT_ON_UNKNOWN", False)):
        return ""
    try:
        shot_dir = Path("logs/screenshots")
        shot_dir.mkdir(parents=True, exist_ok=True)
        safe_domain = normalize_domain(target_url).replace(".", "_").replace("/", "_")
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = shot_dir / f"{safe_domain}_{user}_{code}_{status}_{ts}.png"
        await page.screenshot(path=str(path), full_page=False)
        return str(path)
    except Exception as e:
        logger.debug(f"⚠️ Cannot take screenshot: {e}")
        return ""


async def connect_to_cdp_port(port: int):
    """Connect to CDP port."""
    if port in bot_state.connected_browsers:
        return bot_state.connected_browsers[port]

    logger.info(f"🖥️ Connecting to CDP port {port}...")
    browser = await bot_state.playwright_instance.chromium.connect_over_cdp(
        f"http://127.0.0.1:{port}"
    )
    bot_state.connected_browsers[port] = browser

    logger.info(f"✅ Connected to CDP port {port}")
    return browser


async def _setup_page_performance(page, label: str = ""):
    """Optimize page performance + ẩn dấu hiệu automation khỏi Cloudflare."""
    _BLOCK_DOMAINS = (
        "google-analytics", "googletagmanager", "doubleclick",
        "facebook.net", "fbcdn.net", "hotjar",
    )
    _BLOCK_TYPES = ("media", "ping")

    async def _handle_route(route):
        req = route.request
        url = req.url.lower()
        rtype = req.resource_type

        if "cloudflare" in url:
            await route.continue_()
            return

        if any(d in url for d in _BLOCK_DOMAINS):
            await route.abort()
            return

        if rtype in _BLOCK_TYPES:
            await route.abort()
            return

        await route.continue_()

    try:
        await page.route("**/*", _handle_route)
    except Exception as e:
        logger.debug(f"⚠️ [{label}] Cannot setup route: {e}")




async def _close_unwanted_popups(page):
    """b) Đóng modal/popup/notification không mong muốn trước khi submit."""
    try:
        closed = await page.evaluate("""
            () => {
                const CLOSE_KEYWORDS = ['đóng', 'close', 'x', 'cancel', 'hủy', 'dismiss', 'got it', 'ok', 'thoát'];
                const SKIP_TEXT = ['xác thực', 'xac thuc', 'submit', 'kiểm tra', 'áp dụng', 'nhận'];
                const OVERLAY_SEL = [
                    '.modal', '[class*="modal" i]', '[class*="popup" i]',
                    '[class*="overlay" i]', '[class*="dialog" i]',
                    '[class*="notification" i]', '[class*="toast" i]',
                    '[class*="alert" i]:not(.alert-success):not(.alert-info)',
                    '[class*="banner" i]', '[class*="announcement" i]',
                ];
                let count = 0;
                for (const sel of OVERLAY_SEL) {
                    const els = [...document.querySelectorAll(sel)];
                    for (const el of els) {
                        const style = window.getComputedStyle(el);
                        if (style.display === 'none' || style.visibility === 'hidden') continue;
                        const rect = el.getBoundingClientRect();
                        if (rect.width === 0 || rect.height === 0) continue;
                        // Tìm nút đóng bên trong
                        const btns = [...el.querySelectorAll('button, [role="button"], a, span')];
                        for (const btn of btns) {
                            const txt = (btn.innerText || btn.textContent || btn.getAttribute('aria-label') || '').trim().toLowerCase();
                            if (SKIP_TEXT.some(s => txt.includes(s))) continue;
                            if (CLOSE_KEYWORDS.some(k => txt === k || txt.startsWith(k))) {
                                btn.click();
                                count++;
                                break;
                            }
                        }
                    }
                }
                return count;
            }
        """)
        if closed and closed > 0:
            logger.debug(f"🧹 Đóng {closed} popup không mong muốn")
            await asyncio.sleep(0.3)
    except Exception:
        pass


async def _wake_tab_for_submit(page):
    """Wake up tab before submitting."""
    try:
        await page.bring_to_front()
        await page.evaluate("""
            Object.defineProperty(document, 'visibilityState', {
                get: () => 'visible', configurable: true
            });
        """)
        await _close_unwanted_popups(page)
    except Exception:
        pass


async def auto_fill_username_on_startup(page, domain: str, username: str):
    """Fill username on page load."""
    try:
        await scroll_to_input_fields(page)
        await asyncio.sleep(0.3)
        
        username_input, _ = await find_input_fields(page, cache_key=None)
        if not username_input:
            return False
        
        current_value = await get_input_value(username_input)
        
        if current_value.lower() == username.lower():
            return True
        
        if current_value == "":
            await username_input.fill(username)
            logger.info(f"✅ [{domain}] Filled username: {username}")
            return True
        
        return False
    
    except Exception as e:
        logger.warning(f"⚠️ [{domain}] Cannot fill username: {e}")
        return False


async def _setup_one_domain_tab(item: dict, assigned_pages: set, assign_lock: asyncio.Lock):
    """Setup one domain tab with timeout."""
    label = item.get("key", item["domain"])
    try:
        return await asyncio.wait_for(
            _setup_one_domain_tab_inner(item, assigned_pages, assign_lock),
            timeout=20.0,
        )
    except asyncio.TimeoutError:
        logger.warning(f"⏰ [{label}] Setup timeout 20s")
        return False
    except Exception as e:
        logger.error(f"❌ [{label}] Setup error: {e}")
        return False


async def _setup_one_domain_tab_inner(item: dict, assigned_pages: set, assign_lock: asyncio.Lock):
    """Inner setup logic."""
    target_url = item["target_url"]
    domain = item["domain"]
    port = item["port"]
    accounts = item["accounts"]
    key = item.get("key", domain)
    
    browser = await connect_to_cdp_port(port)
    if not browser.contexts:
        logger.error(f"❌ [{domain}] Port {port} has no context")
        return False
    
    context = browser.contexts[0]
    page = None
    reason = ""
    
    async with assign_lock:
        for p in context.pages:
            try:
                if domain in p.url.lower() and p not in assigned_pages:
                    page = p
                    reason = "tab_existing"
                    assigned_pages.add(page)
                    break
            except Exception:
                pass
        
        if not page:
            if bool(getattr(Config, "AUTO_OPEN_MISSING_TABS", True)):
                page = await context.new_page()
                assigned_pages.add(page)
                reason = "tab_new"
            else:
                logger.error(f"❌ [{domain}] No tab available")
                return False
    
    if reason == "tab_new":
        await _setup_page_performance(page, label=domain)
        try:
            await page.goto(target_url, wait_until="domcontentloaded", timeout=10000)
            # ✅ DÒNG 925: Thêm 2 dòng
            await scroll_to_input_fields(page)
            await asyncio.sleep(0.5)
        except Exception as e:
            logger.warning(f"⚠️ [{domain}] Page load error (continuing): {e}")
    else:
        await _setup_page_performance(page, label=domain)
        await scroll_to_input_fields(page)
        await asyncio.sleep(0.5)
    
    bot_state.account_pages[key] = page
    bot_state.context_locks[key] = asyncio.Lock()
    bot_state.cf_verified[key] = True
    bot_state.submission_count[key] = 0
    
    try:
        await page.bring_to_front()
    except Exception:
        pass
    
    first_account = accounts[0]["username"] if accounts else ""
    if first_account:
        await auto_fill_username_on_startup(page, key, first_account)
    
    _, code_input = await find_input_fields(page)
    
    if code_input:
        logger.info(f"✅ [{key}] Ready | acc: {[a['username'] for a in accounts]}")
    else:
        logger.warning(f"⚠️ [{key}] Code input not found (Cloudflare?)")
    
    return True


async def preload_browsers_and_accounts():
    """Preload all browser tabs in parallel."""
    # ✅ RESET CACHE RUNTIME + DEDUP
    bot_state._site_code_seen.clear()
    logger.info("🧹 Cleared runtime code cache (_site_code_seen)")
    
    account_targets = build_unique_account_targets()
    if not account_targets:
        logger.error("❌ No channels configured")
        return
    
    total_tabs = len(account_targets)
    logger.info(f"🔄 Opening {total_tabs} tabs...")
    
    assigned_pages = set()
    assign_lock = asyncio.Lock()
    done_count = 0
    done_lock = asyncio.Lock()
    
    async def _setup_with_progress(item):
        nonlocal done_count
        result = await _setup_one_domain_tab(item, assigned_pages, assign_lock)
        async with done_lock:
            done_count += 1
            status = "✅" if result else "❌"
            logger.info(f"  {status} [{done_count}/{total_tabs}] {item.get('key', item['domain'])}")
        return result
    
    results = await asyncio.gather(
        *[_setup_with_progress(item) for item in account_targets],
        return_exceptions=True,
    )
    
    ok = sum(1 for r in results if r is True)
    logger.info(f"✅ Complete: {ok}/{total_tabs} tabs ready")
    if ok < total_tabs:
        logger.warning(f"⚠️ {total_tabs - ok} tabs failed")
    logger.info("🤖 BOT RUNNING — listening to Telegram...")


# ============================================================
# CODE EXTRACTION & VALIDATION
# ============================================================

def validate_candidate(code: str, target_url: str, source: str = "normal"):
    """Validate code candidate."""
    try:
        return CodeValidator.validate_code(code, target_url, source=source)
    except TypeError:
        return CodeValidator.validate_code(code, target_url)


def get_filter_group_name(target_url: str) -> str:
    """Get filter group name for URL."""
    group_name, _ = CodeValidator.get_filter_group(target_url)
    return group_name


def unique_keep_order(items):
    """Remove duplicates while keeping order."""
    seen = set()
    result = []
    for item in items:
        clean = CodeValidator.clean_code(item)
        if not clean:
            continue
        upper = clean.upper()
        if upper not in seen:
            seen.add(upper)
            result.append(clean)
    return result


def remove_noise_from_text(text: str) -> str:
    """Remove URLs and noise from text."""
    cleaned = text or ""
    cleaned = re.sub(r"https?://\S+", " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"www\.\S+", " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(
        r"\b[a-zA-Z0-9.-]+\.(com|net|org|vn|app|info)\b", " ", cleaned, flags=re.IGNORECASE
    )
    cleaned = cleaned.replace("：", ":").replace("|", " ").replace("•", " ")
    return cleaned


def line_has_code_marker(line: str) -> bool:
    """Check if line has code marker."""
    upper = line.upper()
    markers = [
        "NHẬN CODE NGAY", "NHAN CODE NGAY",
        "NHẬN CODE", "NHAN CODE",
        "NHẬP CODE", "NHAP CODE",
        "PHÁT CODE", "PHAT CODE",
        "CODE FREE", "FREE CODE",
        "GIFT CODE", "GIFTCODE",
        "TẶNG CODE", "TANG CODE",
    ]
    return any(m in upper for m in markers)


def line_is_noise(line: str) -> bool:
    """Check if line is noise."""
    upper = line.upper().strip()
    if not upper:
        return True
    noise_keywords = [
        "HTTP", "WWW", ".COM", "FACEBOOK", "TELEGRAM", "TIKTOK", "ZALO",
        "CSKH", "BOT", "CHECK LINK", "LINK",
    ]
    return any(kw in upper for kw in noise_keywords)


def extract_tokens_from_line(line: str):
    """Extract code tokens from line."""
    special_chars = re.escape(getattr(Config, "SPECIAL_CODE_CHARS_30", ""))
    min_len = getattr(Config, "CODE_MIN_LENGTH", 6)
    max_len = getattr(Config, "CODE_MAX_LENGTH", 15)
    max_raw_len = max_len + 30
    
    pattern = rf"[A-Za-z0-9{special_chars}]{{{min_len},{max_raw_len}}}"
    tokens = []
    
    for candidate in re.findall(pattern, line or ""):
        clean = CodeValidator.clean_code(candidate)
        if min_len <= len(clean) <= max_len:
            tokens.append(candidate)
    
    return tokens


def extract_spoiler_codes(event, target_url: str):
    """Extract codes from spoiler text."""
    codes = []
    if not event.message.entities:
        return codes
    
    try:
        for entity, entity_text in event.message.get_entities_text():
            if not isinstance(entity, MessageEntitySpoiler):
                continue
            
            spoiler_text = (entity_text or "").strip()
            if not spoiler_text:
                continue
            
            spoiler_lines = spoiler_text.splitlines() if "\n" in spoiler_text else [spoiler_text]
            
            for spoiler_line in spoiler_lines:
                spoiler_line = spoiler_line.strip()
                if not spoiler_line:
                    continue
                
                tokens = extract_tokens_from_line(spoiler_line) or [spoiler_line]
                for token in tokens:
                    validation = validate_candidate(token, target_url, source="spoiler")
                    if validation["valid"]:
                        codes.append(validation["clean_code"])
                        logger.info(f"🔒 Spoiler code: {validation['clean_code']}")
    
    except Exception as e:
        logger.warning(f"⚠️ Error reading spoiler: {e}")
    
    return unique_keep_order(codes)


def extract_marker_near_codes(text: str, target_url: str):
    """Extract codes near markers."""
    cleaned_text = remove_noise_from_text(text)
    lines = [line.strip() for line in cleaned_text.splitlines()]
    codes = []
    
    for index, line in enumerate(lines):
        if not line_has_code_marker(line):
            continue
        
        scan_lines = [line] if line else []
        for offset in range(1, 9):
            if index + offset < len(lines):
                scan_lines.append(lines[index + offset])
        
        for scan_line in scan_lines:
            if line_is_noise(scan_line):
                continue
            
            for token in extract_tokens_from_line(scan_line):
                clean = CodeValidator.clean_code(token)
                validation = validate_candidate(clean, target_url, source="marker")
                if validation["valid"]:
                    codes.append(validation["clean_code"])
                    logger.info(f"🎯 Marker code: {validation['clean_code']}")
    
    return unique_keep_order(codes)


def extract_codes_by_regex(text: str, site_type: str = "qq88") -> list:
    """Extract codes by regex pattern."""
    if not text:
        return []
    
    codes = []
    
    if site_type == "qq88":
        QQ88_BLACKLIST = {
            "QQ88", "CODE", "DANGNHAP", "GAMEBAI", "NOHU", "CASINO",
            "REVIEWPHIM", "TINTUC", "KHUYENMAI", "GIFTCODE", "FREECODE",
            "CAMERA", "TROLL", "BONGDA", "THETHAO", "MINIGAME",
        }
        for match in re.findall(r'[a-zA-Z0-9]{6,15}', text):
            if any(kw in match.upper() for kw in QQ88_BLACKLIST):
                continue
            has_letter = any(c.isalpha() for c in match)
            has_digit = any(c.isdigit() for c in match)
            has_lower = any(c.islower() for c in match)
            has_upper_c = any(c.isupper() for c in match)
            if has_letter and (has_digit or (has_lower and has_upper_c)):
                codes.append(match)
    
    elif site_type == "llwin":
        LLWIN_SEP = r'[~!@#$%^&*()\-_+{}|:"<>?`=\[\]\\;\',\.\\/]'
        pattern = (
            r'[A-Z0-9]{1,3}' + LLWIN_SEP + r'{1,2}'
            r'[A-Z0-9]{1,3}(?:' + LLWIN_SEP + r'{1,2}[A-Z0-9]{1,3}){2,}'
        )
        codes.extend(re.findall(pattern, text.upper()))
    
    return list(dict.fromkeys(codes))


def extract_codes_from_message(event, raw_text: str, target_url: str):
    """Extract all codes from message."""
    codes = []
    group_name = get_filter_group_name(target_url)
    
    # Try spoiler first
    spoiler_codes = extract_spoiler_codes(event, target_url)
    if spoiler_codes:
        logger.info(f"🎯 Spoiler codes: {spoiler_codes}")
        return spoiler_codes
    
    # ✅ UY88 & QQ88: ưu tiên spoiler, nếu không có spoiler thì vẫn cho phép
    # lấy code đứng cạnh marker rõ ràng (FREE CODE:, GIFT CODE:, CODE:, ...)
    # để tránh bỏ lỡ code thật như "FREE CODE: qB9eD9MUNJ" trong caption ảnh.
    if group_name in ("uy88", "qq88"):
        marker_codes = extract_marker_near_codes(raw_text, target_url)
        if marker_codes:
            logger.info(f"🎯 [{group_name}] Marker code (không spoiler): {marker_codes}")
            return marker_codes
        logger.info(f"⏭️ [{group_name}] Không có spoiler/marker rõ ràng → bỏ qua (tránh lấy code rác)")
        return codes
    
    # Try marker
    marker_codes = extract_marker_near_codes(raw_text, target_url)
    if marker_codes:
        logger.info(f"🎯 Marker codes: {marker_codes}")
        return marker_codes
    
    # Try regex
    if group_name in ("llwin",):
        regex_raw = extract_codes_by_regex(raw_text, site_type=group_name)
        regex_codes = []
        for raw in regex_raw:
            validation = validate_candidate(raw, target_url, source="regex")
            if validation["valid"]:
                regex_codes.append(validation["clean_code"])
        if regex_codes:
            logger.info(f"🎯 Regex codes [{group_name}]: {regex_codes}")
            return regex_codes
    
    return codes



# ============================================================
# SUBMIT CODE
# ============================================================

REACT_FILL_JS = """
    ([el, val]) => {
        const proto = el.tagName === 'TEXTAREA'
            ? window.HTMLTextAreaElement.prototype
            : window.HTMLInputElement.prototype;
        const setter = Object.getOwnPropertyDescriptor(proto, 'value').set;
        el.focus();
        setter.call(el, '');
        setter.call(el, val);
        el.dispatchEvent(new Event('input', {bubbles: true}));
        el.dispatchEvent(new Event('change', {bubbles: true}));
    }
"""


async def _reset_page_after_success(page, key: str, target_url: str, domain: str, user: str):
    """Reset page sau khi submit thành công."""
    try:
        try:
            await page.keyboard.press("Escape")
        except Exception:
            pass

        try:
            await page.evaluate("""
                () => {
                    const closeSelectors = [
                        '.swal2-close', '.swal2-confirm',
                        '[aria-label="Close"]', '[aria-label="close"]',
                        'button[class*="close" i]',
                        '.modal [class*="close" i]',
                    ];
                    for (const sel of closeSelectors) {
                        const btn = document.querySelector(sel);
                        if (btn) { btn.click(); return true; }
                    }
                    return false;
                }
            """)
        except Exception:
            pass

        await asyncio.sleep(0.1)
        await page.goto(target_url, wait_until="domcontentloaded", timeout=10000)
        await asyncio.sleep(0.2)

        _invalidate_input_cache(key)
        bot_state._page_urls[key] = page.url

        logger.info(f"🔄 [{domain}|{user}] Đã F5 reload sau khi nhập thành công")
        return True
    except Exception as e:
        logger.warning(f"⚠️ [{domain}|{user}] Lỗi reset page: {e}")
        return False


async def submit_code_safe(user: str, code: str, target_url: str, systems: dict):
    """Submit code to target URL."""
    start_time = time.time()
    db = systems["db"]
    perf_mon = systems["performance_monitor"]
    domain = normalize_domain(target_url)
    
    if domain == "mm88code.com":
        key = f"{domain}|{user}"
    else:
        key = domain
    
    if key not in bot_state.context_locks:
        logger.warning(f"⏭️ [{user}|{domain}] No tab")
        return {"success": False, "message": "No tab"}
    
    try:
        async with bot_state.context_locks[key]:
            page = bot_state.account_pages.get(key)
            if not page:
                return {"success": False, "message": "No page"}

            # Kiểm tra page đã bị đóng chưa
            if page.is_closed():
                logger.warning(f"🔄 [{domain}] Page đã đóng, thử mở lại...")
                try:
                    context = page.context
                    page = await context.new_page()
                    bot_state.account_pages[key] = page
                    await _setup_page_performance(page, domain)
                    await page.goto(target_url, wait_until="domcontentloaded", timeout=10000)
                    _invalidate_input_cache(key)
                    logger.info(f"✅ [{domain}] Mở lại tab thành công")
                except Exception as e:
                    logger.error(f"❌ [{domain}] Không mở lại được tab: {e}")
                    return {"success": False, "message": "Page closed, reopen failed"}

            # Check page validity
            try:
                page_url = page.url
                bot_state._page_urls[key] = page_url
                page_ok = bool(page_url) and page_url != "about:blank"
            except Exception:
                page_ok = False
            
            if not page_ok:
                logger.warning(f"🔄 [{domain}] Page invalid, reloading...")
                try:
                    await page.goto(target_url, wait_until="domcontentloaded", timeout=8000)
                    await asyncio.sleep(0.5)
                    _invalidate_input_cache(key)
                    bot_state._page_urls[key] = page.url
                    logger.info(f"✅ [{domain}] Reload done")
                except Exception:
                    logger.error(f"❌ [{domain}] Reload failed")
                    return {"success": False, "message": "Reload failed"}
            
            await _wake_tab_for_submit(page)

            # ✅ FIX: Kiểm tra Cloudflare TRƯỚC khi tìm input
            # Nếu trang đang hiện "bạn là con người" → input sẽ không có
            cf_done = await handle_cloudflare_popup(page)
            if cf_done:
                logger.info("✅ [CF] Xác minh xong, tìm input...")
                await asyncio.sleep(1.5)

            username_input, code_input = await find_input_fields(page, cache_key=key)

            if not code_input:
                await asyncio.sleep(0.05)
                _invalidate_input_cache(key)
                username_input, code_input = await find_input_fields(page, cache_key=key)

            if not code_input:
                logger.warning(f"❌ [{user}|{domain}] No code input")
                return {"success": False, "message": "No code input"}
            
            await scroll_to_input_fields(page)
            await asyncio.sleep(0.3)   # chờ JS render sau scroll
            
            try: 
                if username_input:
                    await page.evaluate(REACT_FILL_JS, [username_input, user])
                await page.evaluate(REACT_FILL_JS, [code_input, code])
            except Exception as e:
                _invalidate_input_cache(key)
                username_input, code_input = await find_input_fields(page, cache_key=key)
                if code_input:
                    try:
                        if username_input:
                            await page.evaluate(REACT_FILL_JS, [username_input, user])
                        await page.evaluate(REACT_FILL_JS, [code_input, code])
                    except Exception as e2:
                        logger.warning(f"❌ [{user}|{domain}] Fill error: {e2}")
                        return {"success": False, "message": str(e2)}
                else:
                    return {"success": False, "message": f"Fill error: {e}"}
            
            try:
                await page.bring_to_front()
            except Exception:
                pass

            clicked = await click_submit_fast(page, domain=domain)
            if not clicked:
                logger.warning(f"⚠️ [{user}|{domain}] Submit not clicked")
            
            click_elapsed = time.time() - start_time
            logger.info(f"🚀 [{user}] SUBMIT {code} ({click_elapsed:.2f}s)")

            # Bước 2: Poll result (8s)
            result_text = ""
            poll_deadline = time.time() + 8.0
            while time.time() < poll_deadline:
                try:
                    candidate = await detect_result_text(page)
                    if candidate and len(candidate.strip()) >= 5:
                        result_text = candidate
                        break
                    if candidate and len(candidate.strip()) > len(result_text.strip()):
                        result_text = candidate
                except Exception:
                    pass
                await asyncio.sleep(0.2)
            
            elapsed = time.time() - start_time
            result_upper = result_text.upper()
            
            SUCCESS_KW = ["THÀNH CÔNG", "SUCCESS", "CỘNG", "OK"]
            FAILED_KW = [
                "SAI", "LỖI", "ĐÃ SỬ", "FAILED", "ERROR",
                "KHÔNG ĐÚNG", "KHÔNG TỒN TẠI", "KHÔNG HỢP LỆ",
                "HẾT HẠN", "ĐÃ HẾT", "INVALID", "NOT FOUND",
                "NOT EXIST", "KHÔNG TÌM THẤY",
            ]
            TOO_MANY_KW = ["TOO MANY", "RATE LIMIT", "QUÁ NHIỀU", "429", "THÊM SAU", "THỬ LẠI SAU"]
            POINT_KW = ["ĐIỂM", "XU", "COIN", "POINT"]

            is_success = any(kw in result_upper for kw in SUCCESS_KW)
            is_failed = any(kw in result_upper for kw in FAILED_KW)
            has_points = any(kw in result_upper for kw in POINT_KW)
            # c) Too Many Requests → backoff tự động
            is_rate_limited = any(kw in result_upper for kw in TOO_MANY_KW)
            if is_rate_limited:
                backoff_delay = float(getattr(Config, "MIN_DELAY_BETWEEN_SUBMITS", 0.8)) * 5
                logger.warning(f"🚫 [{user}|{domain}] Too Many Requests — backoff {backoff_delay:.1f}s")
                await asyncio.sleep(backoff_delay)
                return {"success": False, "message": f"RateLimit:{result_text[:60]}"}

            if is_success and not is_failed:
                logger.info(f"✅ [{user}] SUCCESS ({elapsed:.2f}s) — {result_text[:60]}")
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(
                    None, db.record_submission, code, user, target_url, "SUCCESS", result_text[:100]
                )
                bot_state.submission_count[key] = bot_state.submission_count.get(key, 0) + 1
                perf_mon.record_task("submit_code", elapsed, True)
                append_code_history(
                    event_type="RESULT", code=code, target_url=target_url,
                    account=user, status="SUCCESS", submit_elapsed=elapsed,
                    message=result_text[:100],
                )
                bot_state._tab_fail_count[key] = 0  # g) reset fail counter khi thành công
                await _reset_page_after_success(page, key, target_url, domain, user)
                return {"success": True, "has_points": has_points, "message": result_text[:100]}
            
            if len(result_text.strip()) < 5:
                screenshot = await take_result_screenshot(page, user, code, target_url, "UNKNOWN")
                logger.warning(f"⚠️ [{user}] NO RESULT after {elapsed:.2f}s")
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(
                    None, db.record_submission, code, user, target_url, "UNKNOWN", "No popup"
                )
                perf_mon.record_task("submit_code", elapsed, False)
                append_code_history(
                    event_type="RESULT", code=code, target_url=target_url,
                    account=user, status="UNKNOWN", submit_elapsed=elapsed,
                    message="No popup", screenshot=screenshot,
                )
                # g) Tăng fail counter, reload tab nếu vượt ngưỡng
                bot_state._tab_fail_count[key] = bot_state._tab_fail_count.get(key, 0) + 1
                fail_count = bot_state._tab_fail_count[key]
                threshold = bot_state._TAB_FAIL_THRESHOLD
                logger.debug(f"⚠️ [{domain}] Fail count: {fail_count}/{threshold}")
                if fail_count >= threshold:
                    logger.warning(f"🔄 [{domain}] {fail_count} lần thất bại liên tiếp → reload tab")
                    try:
                        await page.goto(target_url, wait_until="domcontentloaded", timeout=10000)
                        _invalidate_input_cache(key)
                        bot_state._tab_fail_count[key] = 0
                        logger.info(f"✅ [{domain}] Tab reloaded sau {fail_count} lần thất bại")
                    except Exception as reload_err:
                        logger.error(f"❌ [{domain}] Reload thất bại: {reload_err}")
                return {"success": False, "message": "No popup"}
            
            screenshot = await take_result_screenshot(page, user, code, target_url, "FAILED")
            logger.warning(f"❌ [{user}] FAILED ({elapsed:.2f}s) — {result_text[:60]}")
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None, db.record_submission, code, user, target_url, "FAILED", result_text[:100]
            )
            perf_mon.record_task("submit_code", elapsed, False)
            append_code_history(
                event_type="RESULT", code=code, target_url=target_url,
                account=user, status="FAILED", submit_elapsed=elapsed,
                message=result_text[:100], screenshot=screenshot,
            )
            return {"success": False, "message": result_text[:100]}
    
    except Exception as e:
        elapsed = time.time() - start_time
        err_str = str(e)
        # a) Xử lý mất kết nối CDP / Playwright TargetClosedError
        if "Target page, context or browser has been closed" in err_str or "TargetClosedError" in type(e).__name__:
            logger.warning(f"🔌 [{domain}] CDP mất kết nối, thử reconnect page...")
            try:
                port = None
                for p, users_list in getattr(Config, "CDP_CONNECTIONS", {}).items():
                    if any(u == user or key.startswith(domain) for u in users_list):
                        port = int(p)
                        break
                if port and port in bot_state.connected_browsers:
                    browser = bot_state.connected_browsers[port]
                    context = browser.contexts[0] if browser.contexts else None
                    if context:
                        new_page = await context.new_page()
                        bot_state.account_pages[key] = new_page
                        await _setup_page_performance(new_page, domain)
                        await new_page.goto(target_url, wait_until="domcontentloaded", timeout=10000)
                        _invalidate_input_cache(key)
                        logger.info(f"✅ [{domain}] Reconnect CDP thành công")
            except Exception as re_err:
                logger.error(f"❌ [{domain}] Reconnect CDP thất bại: {re_err}")
        logger.error(f"❌ [{user}] Submit error: {e}\n{traceback.format_exc()}")
        perf_mon.record_task("submit_code", elapsed, False)
        append_code_history(
            event_type="ERROR", code=code, target_url=target_url,
            account=user, status="ERROR", submit_elapsed=elapsed, message=str(e),
        )
        return {"success": False, "message": str(e)}


async def submit_code_with_delay(user: str, code: str, target_url: str, systems: dict):
    """Submit with min delay after semaphore released.
    
    ✅ Dùng semaphore RIÊNG theo domain → các site khác nhau (QQ88, NEW88, UY88...)
    chạy SONG SONG hoàn toàn, không phải xếp hàng chờ chung 1 semaphore global.
    Tránh tình trạng delay 600s khi nhiều tin đến cùng lúc từ nhiều kênh khác nhau.
    """
    domain = normalize_domain(target_url)
    sem = get_domain_semaphore(domain)
    result = {"success": False, "message": "Not started"}  # init mặc định tránh UnboundLocalError
    async with sem:
        try:
            result = await asyncio.wait_for(
                submit_code_safe(user, code, target_url, systems),
                timeout=25.0,
            )
        except asyncio.TimeoutError:
            logger.warning(f"⏰ [{user}] Submit timeout 25s")
            result = {"success": False, "message": "Timeout 25s"}
        except Exception as e:
            logger.error(f"❌ [{user}] submit_code_with_delay error: {e}")
            result = {"success": False, "message": str(e)}

    delay = float(getattr(Config, "MIN_DELAY_BETWEEN_SUBMITS", 0.5))
    if delay > 0:
        await asyncio.sleep(delay)

    return result


def track_submit_task(task: asyncio.Task, label: str = ""):
    """Track active submit task."""
    _active_submit_tasks.add(task)
    
    def _done(t: asyncio.Task):
        _active_submit_tasks.discard(t)
        try:
            result = t.result()
            if isinstance(result, dict):
                ok = "✅" if result.get("success") else "⚠️"
                logger.info(f"{ok} [TASK] {label} | {result.get('message', '')[:60]}")
        except asyncio.CancelledError:
            logger.debug(f"🛑 [TASK CANCELLED] {label}")
        except Exception as e:
            logger.error(f"❌ [TASK ERROR] {label}: {e}")
    
    task.add_done_callback(_done)
    return task


async def _submit_sequential_for_channel(
    codes: list,
    available_accounts: list,
    target_url: str,
    channel_name: str,
    domain: str,
):
    """Submit codes sequentially through accounts."""
    if not codes:
        return
    
    selected_code = select_random_code(codes)
    logger.info(f"🎲 [{domain}] Code: {selected_code} (from {len(codes)})")
    
    total = len(available_accounts)
    max_retries = getattr(Config, "MAX_RETRIES_PER_ACCOUNT", 2)
    retry_on_timeout = getattr(Config, "RETRY_ON_TIMEOUT", True)

    for idx, account in enumerate(available_accounts):
        user = account["username"]
        is_last = idx == total - 1

        logger.info(f"🔄 [{domain}] [{idx+1}/{total}] Submit for: {user}")

        for attempt in range(1, max_retries + 1):
            result = await submit_code_with_delay(user, selected_code, target_url, _systems)

            success = result.get("success", False) if result else False
            has_points = result.get("has_points", False) if result else False
            msg = (result.get("message", "") if result else "No result")[:80]

            if success and has_points:
                logger.info(f"✅ [{domain}|{user}] SUCCESS+POINTS ✨ Done!")
                append_code_history(
                    event_type="FINAL_RESULT",
                    code=selected_code,
                    target_url=target_url,
                    account=user,
                    status="SUCCESS_POINTS",
                    message="Code thành công với điểm/xu"
                )
                return

            if success and not has_points:
                logger.warning(f"⚠️ [{domain}|{user}] Code OK nhưng KHÔNG có điểm → DỪNG")
                append_code_history(
                    event_type="FINAL_RESULT",
                    code=selected_code,
                    target_url=target_url,
                    account=user,
                    status="SUCCESS_NO_POINTS",
                    message="Code đúng nhưng không điểm"
                )
                return

            if success is False and ("SAI" in msg.upper() or "LỖI" in msg.upper() or "KHÔNG" in msg.upper()):
                logger.warning(f"❌ [{domain}|{user}] Code sai/lỗi → Thử acc tiếp")
                append_code_history(
                    event_type="SUBMIT_ATTEMPT",
                    code=selected_code,
                    target_url=target_url,
                    account=user,
                    status="FAILED",
                    message=msg
                )
                break  # code sai → chuyển acc ngay, không retry

            # Timeout / no popup
            if retry_on_timeout and attempt < max_retries:
                logger.info(
                    f"🔄 [{domain}|{user}] Retry {attempt}/{max_retries} | "
                    f"Lý do: timeout/no popup | msg={msg[:40]} | chờ 2s..."
                )
                await asyncio.sleep(2)
            else:
                logger.warning(
                    f"⏰ [{domain}|{user}] Hết {attempt} lần retry | "
                    f"Lý do: timeout/no popup | msg={msg[:40]} → chuyển acc tiếp"
                )
                append_code_history(
                    event_type="FINAL_RESULT" if is_last else "SUBMIT_ATTEMPT",
                    code=selected_code,
                    target_url=target_url,
                    account=user,
                    status="NO_RESULT",
                    message=f"Timeout sau {attempt} lần thử"
                )
                break

        if is_last:
            logger.error(f"❌ [{domain}] Đã thử tất cả acc, không có kết quả")


async def _submit_one_account(account: dict, code: str, target_url: str, domain: str):
    """Submit 1 code cho 1 account (MM88 parallel)."""
    user = account["username"]
    logger.info(f"🔄 [{domain}|{user}] Submit code: {code}")

    result = await submit_code_with_delay(user, code, target_url, _systems)

    success = result.get("success", False) if result else False
    has_points = result.get("has_points", False) if result else False
    msg = (result.get("message", "") if result else "No result")[:80]

    if success and has_points:
        status = "SUCCESS_POINTS"
        logger.info(f"✅ [{domain}|{user}] SUCCESS+POINTS ✨")
    elif success and not has_points:
        status = "SUCCESS_NO_POINTS"
        logger.warning(f"⚠️ [{domain}|{user}] Code OK nhưng KHÔNG có điểm")
    elif success is False and ("SAI" in msg.upper() or "LỖI" in msg.upper() or "KHÔNG" in msg.upper()):
        status = "FAILED"
        logger.warning(f"❌ [{domain}|{user}] Code sai/lỗi: {msg}")
    else:
        status = "NO_RESULT"
        logger.warning(f"⏰ [{domain}|{user}] TIMEOUT hoặc NO POPUP")

    append_code_history(
        event_type="FINAL_RESULT",
        code=code,
        target_url=target_url,
        account=user,
        status=status,
        message=msg,
    )
    return {"user": user, "code": code, "status": status, "result": result}


async def _submit_parallel_for_mm88(
    codes: list,
    available_accounts: list,
    target_url: str,
    channel_name: str,
    domain: str,
):
    """MM88 submit SONG SONG."""
    if not codes or not available_accounts:
        return

    unique_codes = list(dict.fromkeys(codes))
    n_acc = len(available_accounts)
    assignments = []

    if len(unique_codes) >= n_acc:
        for i, acc in enumerate(available_accounts):
            assignments.append((acc, unique_codes[i]))
        logger.info(
            f"🎯 [{domain}] {n_acc} acc ↔ {n_acc} code riêng: "
            + ", ".join(f"{a['username']}→{c}" for a, c in assignments)
        )
    else:
        selected_code = select_random_code(unique_codes)
        for acc in available_accounts:
            assignments.append((acc, selected_code))
        logger.info(f"🎯 [{domain}] Chỉ có {len(unique_codes)} code → {n_acc} acc dùng chung: {selected_code}")

    tasks = [
        _submit_one_account(acc, code, target_url, domain)
        for acc, code in assignments
    ]
    await asyncio.gather(*tasks, return_exceptions=True)


# ============================================================
# OCR PROCESSING
# ============================================================

async def process_image_from_telegram(event, channel_config: dict, systems: dict):
    """Process image with OCR."""
    target_url = channel_config.get("url", "")
    
    try:
        logger.info("📸 [OCR] Image detected - Processing...")
        temp_dir = tempfile.mkdtemp(prefix="ocr_")
        
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            image_path = await event.download_media(file=temp_dir)
            
            if not image_path:
                logger.warning("⚠️ [OCR] Download failed")
                return {"success": False, "codes": [], "message": "Download failed", "text": ""}
            
            unique_name = f"ocr_{timestamp}.png"
            unique_path = Path(temp_dir) / unique_name
            Path(image_path).rename(unique_path)
            image_path = str(unique_path)
            
            logger.info(f"✅ [OCR] Downloaded: {unique_name}")
            
            extractor = get_image_extractor()
            if extractor is None:
                logger.error("❌ [OCR] Tesseract not installed")
                return {
                    "success": False, 
                    "codes": [], 
                    "message": "Tesseract not installed",
                    "text": ""
                }
            
            loop = asyncio.get_event_loop()
            
            def _run_ocr():
                text = extractor.extract_code_from_image(image_path, lang="eng")
                if not text or len(text.strip()) < 3:
                    logger.debug(f"⚠️ [OCR] English failed, trying Vietnamese...")
                    text = extractor.extract_code_from_image(image_path, lang="vie+eng")
                return text or ""
            
            extracted_text = await loop.run_in_executor(None, _run_ocr)
            
            if not extracted_text:
                logger.warning("⚠️ [OCR] No text detected in image")
                return {"success": False, "codes": [], "message": "No text in image", "text": ""}
            
            logger.info(f"✅ [OCR] Extracted {len(extracted_text)} chars: {extracted_text[:80]}")
            
            extracted_codes = []
            for line in extracted_text.split("\n"):
                line = line.strip()
                if len(line) < 4:
                    continue
                
                clean_code = CodeValidator.clean_code(line)
                if not clean_code or len(clean_code) < 4:
                    continue
                
                try:
                    validation = CodeValidator.validate_code(clean_code, target_url=target_url, source="image_ocr")
                except TypeError:
                    validation = CodeValidator.validate_code(clean_code, target_url)
                
                if validation["valid"]:
                    extracted_codes.append({
                        "code": clean_code,
                        "raw": line,
                        "confidence": 0.9
                    })
                    logger.info(f"✅ [OCR] CODE: {clean_code}")
                else:
                    logger.debug(f"⚠️ [OCR] Invalid: {clean_code} - {validation['reason']}")
            
            if extracted_codes:
                logger.info(f"✅ [OCR] Found {len(extracted_codes)} valid code(s)")
                return {
                    "success": True,
                    "codes": extracted_codes,
                    "message": f"{len(extracted_codes)} code(s)",
                    "text": extracted_text
                }
            
            logger.warning("⚠️ [OCR] Extracted text but no valid codes found")
            return {
                "success": False,
                "codes": [],
                "message": "No valid codes extracted",
                "text": extracted_text
            }
        
        finally:
            try:
                shutil.rmtree(temp_dir)
            except Exception:
                pass
    
    except Exception as e:
        logger.error(f"❌ [OCR] Error: {e}\n{traceback.format_exc()}")
        return {"success": False, "codes": [], "message": f"OCR error: {e}", "text": ""}


async def submit_codes_from_image(user: str, codes_data: list, target_url: str, channel_config: dict, systems: dict):
    """Submit codes extracted from image."""
    if not codes_data:
        return
    
    domain = normalize_domain(target_url)
    db_ref = systems.get("db") if systems else None
    logger.info(f"📤 [IMG] Submitting {len(codes_data)} code(s) for {user}")
    
    for idx, code_item in enumerate(codes_data, 1):
        code = code_item.get("code", "").strip()
        if not code:
            continue

        if db_ref is not None:
            try:
                loop = asyncio.get_event_loop()
                marked_ok = await loop.run_in_executor(None, db_ref.mark_code_used, domain, code)
            except Exception as e:
                logger.debug(f"⚠️ mark_code_used error: {e}")
                marked_ok = True
            if not marked_ok:
                logger.warning(f"⏭️ [OCR#{idx}] [DEDUP-VĨNH VIỄN] {code} đã từng xử lý → bỏ qua")
                continue
        
        try:
            result = await submit_code_with_delay(user, code, target_url, systems)
            status = "✅" if (result and result.get("success")) else "❌"
            logger.info(f"  {status} [OCR#{idx}] {code}")
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"  ❌ [OCR#{idx}] Error: {e}")


# ============================================================
# MESSAGE PROCESSING
# ============================================================

async def process_telegram_message(event):
    """Process Telegram message."""
    if not _systems:
        return
    
    # ✅ KIỂM TRA NGAY - không để delay
    if event.chat_id not in Config.CHANNEL_CONFIG:
        logger.debug(f"⏭️ Chat {event.chat_id} not in config")
        return
    
    channel_config = Config.CHANNEL_CONFIG.get(event.chat_id)
    if not channel_config:
        return
    
    # ✅ KIỂM TRA NGAY - bỏ qua tin cũ
    msg_date = event.message.date
    if msg_date.tzinfo is None:
        msg_date = msg_date.replace(tzinfo=timezone.utc)
    
    if msg_date < BOT_START_TIME:
        logger.debug(f"⏭️ [OLD MSG] Skip ({msg_date} < {BOT_START_TIME})")
        return
    
    target_url = channel_config["url"]
    accounts = channel_config["accounts"]
    raw_text = event.message.text or ""
    group_name = get_filter_group_name(target_url)
    
    logger.info(f"\n👀 [{channel_config['name']}] NEW MESSAGE")
    
    # ── QQ88: lọc tin ────────────────────────────────────────────────────────
    # Thực tế: ảnh có code luôn kèm "tangquaqq88.com" hoặc "link nhập code" trong caption
    QQ88_OCR_TRIGGER = ["tangquaqq88.com", "link nhập code", "link nhap code",
                        "tangquaqq88", "nhập code", "nhap code"]
    QQ88_OCR_SKIP    = ["khuyến mãi", "khuyen mai", "giảm giá", "sale",
                        "promo", "quảng cáo", "nạp tiền"]

    if group_name == "qq88":
        # Telethon: caption ảnh nằm ở event.message.message (không phải .caption)
        caption_text = (event.message.message or raw_text or "").strip().lower()
        has_media    = bool(event.media)
        has_raw_text = bool(raw_text.strip())

        if has_raw_text:
            logger.info("✅ [QQ88] Text/spoiler → xử lý ngay")
        elif has_media:
            if any(kw in caption_text for kw in QQ88_OCR_SKIP):
                logger.info(f"⏭️ [QQ88] Ảnh quảng cáo → bỏ qua (caption: {caption_text[:80]})")
                return
            if any(kw in caption_text for kw in QQ88_OCR_TRIGGER):
                logger.info(f"✅ [QQ88] Ảnh có link nhập code → OCR (caption: {caption_text[:80]})")
            else:
                logger.info(f"⏭️ [QQ88] Không có link nhập code → bỏ qua (caption: {caption_text[:80]})")
                return
        else:
            logger.info("⏭️ [QQ88] Không có text lẫn ảnh → bỏ qua")
            return
    
    # ── XỬ LÝ ẢNH ────────────────────────────────────────────────────────────
    if event.media and not raw_text:
        # Caption trong Telethon nằm ở .message (không phải .caption)
        caption = (getattr(event.message, "message", None) or "").strip()
        caption_lower = caption.lower()

        AD_KEYWORDS = ["khuyến mãi", "khuyen mai", "km ", "sale", "giảm giá", "giam gia",
                         "promo", "quảng cáo", "quang cao", "qc ", "tấu", "khmerads",
                         "nạp tiền", "nap tien", "ưu đãi", "uu dai", "hoàn tiền",
                         "cashback", "event", "sự kiện", "su kien", "thông báo"]
        if any(kw in caption_lower for kw in AD_KEYWORDS):
            logger.info(f"⏭️ [IMG] Ad image detected → skip")
            return

        default_account = accounts[0]["username"] if accounts else None
        if not default_account:
            return

        # ✅ UY88 FIX: Kênh UY88 hay gửi ảnh trước, caption/spoiler đến sau qua MessageEdited.
        # Nếu caption hiện tại TRỐNG → lưu vào pending và chờ edit thay vì OCR ngay.
        # Nếu caption đã có → xử lý text ngay (có thể chứa spoiler code).
        if group_name == "uy88" and not caption:
            pending_key = (event.chat_id, event.message.id)
            bot_state._pending_image_msgs[pending_key] = (event, time.time())
            logger.info(
                f"⏳ [UY88] Ảnh chưa có caption — lưu pending (msg_id={event.message.id}), "                f"chờ MessageEdited tối đa {bot_state._PENDING_IMAGE_TTL:.0f}s"
            )
            return

        # Caption đã có (hoặc kênh khác) → xử lý text từ caption trước
        if caption:
            logger.info(f"🖼️+📝 Image với caption ({len(caption)} chars) → extract code từ caption")
            # Xử lý caption như text thường (spoiler, marker, regex)
            extracted = extract_codes_from_message(event, caption, target_url)
            if extracted:
                logger.info(f"✅ [IMG-CAPTION] Codes từ caption: {extracted}")
                raw_text = caption  # chuyển sang nhánh text bên dưới
                # fall-through: tiếp tục xử lý như tin nhắn text
            else:
                # Caption có nhưng không có code → thử OCR ảnh
                logger.info("🖼️ Caption không có code → OCR ảnh")
                async def _handle_image_task_with_caption():
                    try:
                        ocr_result = await process_image_from_telegram(event, channel_config=channel_config, systems=_systems)
                        if ocr_result["success"]:
                            await submit_codes_from_image(
                                user=default_account,
                                codes_data=ocr_result["codes"],
                                target_url=target_url,
                                channel_config=channel_config,
                                systems=_systems,
                            )
                        else:
                            logger.warning(f"⚠️ OCR failed: {ocr_result['message']}")
                    except Exception as e:
                        logger.error(f"❌ Image task error: {e}")
                img_task = asyncio.create_task(_handle_image_task_with_caption())
                track_submit_task(img_task, label=f"img|{channel_config.get('name','')}")
                # Xóa pending_key sau khi đã dispatch xử lý
                pending_key = (event.chat_id, event.message.id)
                bot_state._pending_image_msgs.pop(pending_key, None)
                return
        else:
            # Không caption, không phải UY88 → OCR
            logger.info("🖼️ Image không caption → OCR")
            async def _handle_image_task_no_caption():
                try:
                    ocr_result = await process_image_from_telegram(event, channel_config=channel_config, systems=_systems)
                    if ocr_result["success"]:
                        await submit_codes_from_image(
                            user=default_account,
                            codes_data=ocr_result["codes"],
                            target_url=target_url,
                            channel_config=channel_config,
                            systems=_systems,
                        )
                    else:
                        logger.warning(f"⚠️ OCR failed: {ocr_result['message']}")
                except Exception as e:
                    logger.error(f"❌ Image task error: {e}")
            img_task = asyncio.create_task(_handle_image_task_no_caption())
            track_submit_task(img_task, label=f"img|{channel_config.get('name','')}")
            # Xóa pending_key sau khi đã dispatch xử lý
            pending_key = (event.chat_id, event.message.id)
            bot_state._pending_image_msgs.pop(pending_key, None)
            return
    
    # Measure delay
    msg_timestamp = event.message.date
    telegram_delay = measure_telegram_delay_fast(msg_timestamp)
    if telegram_delay is not None:
        logger.warning(f"⏱️ Delay: {telegram_delay:.2f}s")
    
    # Extract codes
    final_codes = extract_codes_from_message(event, raw_text, target_url)
    if not final_codes:
        logger.info("⏭️ No codes")
        return
    
    logger.info(f"📋 Codes: {final_codes}")
    
    for code in final_codes:
        append_code_history(
            event_type="DETECTED", code=code, target_url=target_url,
            channel=channel_config.get("name", ""), source="telegram",
            status="PENDING", telegram_delay=telegram_delay,
        )
    
    # Dedup
    domain = normalize_domain(target_url)
    final_codes_dedup = []
    db_ref = _systems["db"] if _systems else None
    for code in final_codes:
        if db_ref is not None:
            try:
                loop = asyncio.get_event_loop()
                marked_ok = await loop.run_in_executor(None, db_ref.mark_code_used, domain, code)
            except Exception as e:
                logger.debug(f"⚠️ mark_code_used error: {e}")
                marked_ok = True
            if not marked_ok:
                logger.warning(f"⏭️ [DEDUP-VĨNH VIỄN] {code} đã từng xử lý → bỏ qua")
                continue
        if is_site_code_duplicate(domain, code):
            logger.warning(f"⏭️ [DEDUP] {code} recently submitted")
        else:
            final_codes_dedup.append(code)
    
    if not final_codes_dedup:
        logger.info("⏭️ All codes deduped")
        return
    
    # Submit
    available_accounts = sorted(accounts, key=lambda a: a.get("priority", 999))
    if not available_accounts:
        return

    # MM88 parallel mode
    if domain == "mm88code.com" and len(available_accounts) >= 2:
        missing = [a["username"] for a in available_accounts if f"{domain}|{a['username']}" not in bot_state.account_pages]
        if len(missing) == len(available_accounts):
            logger.warning(f"⚠️ [{domain}] No tab for any account")
            return

        task = asyncio.create_task(
            _submit_parallel_for_mm88(
                codes=final_codes_dedup,
                available_accounts=available_accounts,
                target_url=target_url,
                channel_name=channel_config.get("name", ""),
                domain=domain,
            )
        )
        track_submit_task(task, label=f"parallel|{domain}|{len(final_codes_dedup)}")
        logger.warning(f"⚡ [PARALLEL] {len(final_codes_dedup)} code(s) → '{channel_config['name']}'")
        return

    # Sequential mode
    # ✅ FIX: key thực tế trong account_pages là domain (non-MM88) hoặc domain|user (MM88)
    # Kiểm tra xem có ít nhất 1 tài khoản có tab không
    has_any_tab = any(
        (f"{domain}|{a['username']}" in bot_state.account_pages or domain in bot_state.account_pages)
        for a in available_accounts
    )
    if not has_any_tab:
        logger.warning(f"⚠️ [{domain}] No tab for any account")
        return

    task = asyncio.create_task(
        _submit_sequential_for_channel(
            codes=final_codes_dedup,
            available_accounts=available_accounts,
            target_url=target_url,
            channel_name=channel_config.get("name", ""),
            domain=domain,
        )
    )
    track_submit_task(task, label=f"seq|{domain}|{len(final_codes_dedup)}")
    logger.warning(f"⚡ Task: {len(final_codes_dedup)} code(s) → '{channel_config['name']}'")


# ============================================================
# MESSAGE WORKERS
# ============================================================

async def message_worker(worker_id: int):
    """Message processing worker - OPTIMIZED (không busy-wait)"""
    global message_queue
    logger.info(f"👷 Worker #{worker_id} started")

    while bot_state.is_running:
        try:
            # ✅ FIX: dùng await get() thay vì get_nowait() + sleep(0)
            # get_nowait + sleep(0) tạo busy-wait loop tiêu tốn CPU liên tục
            # await get() sẽ suspend coroutine cho đến khi có tin → CPU free
            event = await asyncio.wait_for(message_queue.get(), timeout=1.0)
        except asyncio.TimeoutError:
            # Không có tin trong 1s → kiểm tra is_running rồi chờ tiếp
            continue
        except asyncio.CancelledError:
            break
        except Exception:
            await asyncio.sleep(0.01)
            continue

        try:
            await process_telegram_message(event)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"❌ Worker #{worker_id}: {e}")
        finally:
            try:
                message_queue.task_done()
            except Exception:
                pass


def start_message_workers():
    """Start message processing workers - OPTIMIZED"""
    global message_queue, message_workers
    
    queue_maxsize = int(getattr(Config, "MESSAGE_QUEUE_MAXSIZE", 500))
    worker_count = int(getattr(Config, "MESSAGE_WORKERS", 3))  # ✅ FIX: default 3 khớp với Config
    
    if message_queue is None:
        message_queue = asyncio.Queue(maxsize=queue_maxsize)  
    
    if message_workers:
        return
    
    for wid in range(1, worker_count + 1):
        message_workers.append(asyncio.create_task(message_worker(wid)))
    
    logger.info(f"🚀 Message queue: maxsize={queue_maxsize}, workers={worker_count}")


# ============================================================
# TELEGRAM HANDLER
# ============================================================

# ============================================================
# UY88 EDITED MESSAGE HANDLER
# ============================================================

async def _cleanup_pending_images():
    """d) Xóa pending image hết TTL — thử OCR trước khi xóa."""
    now = time.time()
    ttl = bot_state._PENDING_IMAGE_TTL
    expired = [k for k, (_, ts) in bot_state._pending_image_msgs.items() if now - ts > ttl]
    for k in expired:
        event, ts = bot_state._pending_image_msgs.pop(k, (None, None))
        if event is None:
            continue
        logger.info(f"⏰ [PENDING] Hết TTL {ttl:.0f}s — thử OCR ảnh (msg_id={k[1]})")
        try:
            chat_id = k[0]
            channel_config = Config.CHANNEL_CONFIG.get(chat_id)
            if channel_config and _systems:
                target_url = channel_config["url"]
                accounts = channel_config.get("accounts", [])
                default_account = accounts[0]["username"] if accounts else ""
                async def _ocr_expired(ev=event, url=target_url, cfg=channel_config, acc=default_account):
                    try:
                        ocr_result = await process_image_from_telegram(ev, channel_config=cfg, systems=_systems)
                        if ocr_result.get("success"):
                            logger.info(f"✅ [PENDING-OCR] OCR tìm được code sau TTL")
                            await submit_codes_from_image(
                                user=acc, codes_data=ocr_result["codes"],
                                target_url=url, channel_config=cfg, systems=_systems,
                            )
                        else:
                            logger.warning(f"⚠️ [PENDING-OCR] OCR không tìm được code: {ocr_result.get('message','')}")
                    except Exception as ocr_err:
                        logger.error(f"❌ [PENDING-OCR] Lỗi: {ocr_err}")
                ocr_task = asyncio.create_task(_ocr_expired())
                track_submit_task(ocr_task, label=f"pending-ocr|{k[1]}")
        except Exception as e:
            logger.debug(f"⚠️ [PENDING] OCR fallback error: {e}")
    return len(expired)


async def process_edited_message(event):
    """
    Xử lý MessageEdited — dành cho trường hợp caption xuất hiện SAU khi ảnh đã gửi.
    Luồng: ảnh gửi trước (NewMessage, không caption) → bot lưu pending
           caption/spoiler đến sau (MessageEdited) → bot bắt và xử lý code.
    """
    if not _systems:
        return

    chat_id = event.chat_id
    if chat_id not in Config.CHANNEL_CONFIG:
        return

    # Chỉ quan tâm tin có trong pending (đã thấy ảnh trước)
    msg_id = event.message.id
    pending_key = (chat_id, msg_id)

    channel_config = Config.CHANNEL_CONFIG[chat_id]
    target_url = channel_config["url"]
    group_name = get_filter_group_name(target_url)
    accounts = channel_config["accounts"]

    # Lấy text mới nhất từ edited message
    new_text = (event.message.text or event.message.message or "").strip()

    if pending_key in bot_state._pending_image_msgs:
        # ── CASE 1: Đây là edit cho ảnh đang pending ──────────────────────
        orig_event, pending_ts = bot_state._pending_image_msgs.pop(pending_key)
        wait_secs = time.time() - pending_ts
        logger.info(
            f"✏️ [UY88-EDIT] Caption đến sau {wait_secs:.1f}s cho msg_id={msg_id} "            f"| chat={channel_config['name']} | text={new_text[:80]}"
        )

        if not new_text:
            logger.info("⏭️ [UY88-EDIT] Edit nhưng vẫn không có text → bỏ qua")
            return

        # Extract code từ caption mới (ưu tiên spoiler)
        extracted = extract_codes_from_message(event, new_text, target_url)
        if not extracted:
            logger.info(f"⏭️ [UY88-EDIT] Không tìm thấy code trong caption: {new_text[:60]}")
            return

        logger.info(f"🎯 [UY88-EDIT] Codes từ caption muộn: {extracted}")

        # Dedup + submit (giống luồng text thường)
        domain = normalize_domain(target_url)
        db_ref = _systems["db"] if _systems else None
        final_codes = []
        for code in extracted:
            if db_ref is not None:
                try:
                    loop = asyncio.get_event_loop()
                    ok = await loop.run_in_executor(None, db_ref.mark_code_used, domain, code)
                except Exception:
                    ok = True
                if not ok:
                    logger.warning(f"⏭️ [UY88-EDIT DEDUP] {code} đã xử lý rồi")
                    continue
            if is_site_code_duplicate(domain, code):
                logger.warning(f"⏭️ [UY88-EDIT DEDUP-TTL] {code} vừa submit")
            else:
                final_codes.append(code)

        if not final_codes:
            return

        available_accounts = sorted(accounts, key=lambda a: a.get("priority", 999))
        task = asyncio.create_task(
            _submit_sequential_for_channel(
                codes=final_codes,
                available_accounts=available_accounts,
                target_url=target_url,
                channel_name=channel_config.get("name", ""),
                domain=domain,
            )
        )
        track_submit_task(task, label=f"uy88_edit|{domain}|{msg_id}")
        logger.info(f"⚡ [UY88-EDIT] Submit task created: {final_codes}")

    else:
        # ── CASE 2: Edit thông thường (không phải ảnh pending) ────────────
        # Chỉ xử lý nếu kênh là UY88 và có text mới
        if group_name != "uy88" or not new_text:
            return

        # Kiểm tra tin có đủ mới không (trong vòng 5 phút kể từ gửi)
        msg_date = event.message.date
        if msg_date.tzinfo is None:
            msg_date = msg_date.replace(tzinfo=timezone.utc)
        age_secs = (datetime.now(timezone.utc) - msg_date).total_seconds()
        if age_secs > 300:
            logger.debug(f"⏭️ [UY88-EDIT] Tin quá cũ ({age_secs:.0f}s) → bỏ qua")
            return
        if msg_date < BOT_START_TIME:
            return

        logger.info(f"✏️ [UY88-EDIT] Edit trên tin text: {new_text[:60]}")
        extracted = extract_codes_from_message(event, new_text, target_url)
        if not extracted:
            return

        domain = normalize_domain(target_url)
        db_ref = _systems["db"] if _systems else None
        final_codes = []
        for code in extracted:
            if db_ref is not None:
                try:
                    loop = asyncio.get_event_loop()
                    ok = await loop.run_in_executor(None, db_ref.mark_code_used, domain, code)
                except Exception:
                    ok = True
                if not ok:
                    continue
            if not is_site_code_duplicate(domain, code):
                final_codes.append(code)

        if not final_codes:
            return

        available_accounts = sorted(accounts, key=lambda a: a.get("priority", 999))
        task = asyncio.create_task(
            _submit_sequential_for_channel(
                codes=final_codes,
                available_accounts=available_accounts,
                target_url=target_url,
                channel_name=channel_config.get("name", ""),
                domain=domain,
            )
        )
        track_submit_task(task, label=f"uy88_edit_text|{domain}|{msg_id}")
        logger.info(f"⚡ [UY88-EDIT-TEXT] Submit task: {final_codes}")


async def setup_telegram_handler():
    """Setup Telegram message handler - PRIORITY MODE"""
    if bot_state.handler_registered:
        return
    
    channel_ids = list(Config.CHANNEL_CONFIG.keys())
    if not channel_ids:
        logger.error("❌ No channels in CONFIG")
        return
    
    start_message_workers()
    
    @client.on(events.NewMessage(chats=channel_ids))
    async def handler(event):
        try:
            # Lọc tức thì tại handler — không await, không blocking, <0.01ms
            if not event.message:
                return
            msg_date = event.message.date
            if msg_date is None:
                return
            if msg_date.tzinfo is None:
                msg_date = msg_date.replace(tzinfo=timezone.utc)
            if msg_date < BOT_START_TIME:
                return
            if event.chat_id not in Config.CHANNEL_CONFIG:
                return
            # ✅ FIX CRITICAL: put vào message_queue để workers xử lý
            # (trước đây dùng create_task trực tiếp → queue không bao giờ được dùng)
            if message_queue is not None:
                try:
                    message_queue.put_nowait(event)
                except asyncio.QueueFull:
                    logger.warning(f"⚠️ message_queue đầy! Bỏ qua msg chat={event.chat_id}")
            else:
                # fallback nếu queue chưa khởi tạo (không nên xảy ra)
                asyncio.create_task(
                    _safe_process(event),
                    name=f"msg|{event.chat_id}|{event.message.id}"
                )
        except Exception as e:
            logger.debug(f"⚠️ handler error (bỏ qua): {e}")

    async def _safe_process(event):
        try:
            await process_telegram_message(event)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"❌ process error chat={event.chat_id}: {e}", exc_info=True)
    
    # ✅ UY88 FIX: Handler cho MessageEdited — bắt caption xuất hiện sau ảnh
    @client.on(events.MessageEdited(chats=channel_ids))
    async def edit_handler(event):
        # Chỉ quan tâm kênh trong config
        if event.chat_id not in Config.CHANNEL_CONFIG:
            return
        # Bỏ qua edit từ tin cũ trước khi bot khởi động
        msg_date = event.message.date
        if msg_date.tzinfo is None:
            msg_date = msg_date.replace(tzinfo=timezone.utc)
        if msg_date < BOT_START_TIME:
            return
        # Xử lý bất đồng bộ, không block handler
        asyncio.create_task(process_edited_message(event), name=f"edit|{event.chat_id}|{event.message.id}")

    bot_state.handler_registered = True
    logger.info(f"✅ Handler ready (priority=10, channels={len(channel_ids)}, +MessageEdited)")


# ============================================================
# WATCHDOGS
# ============================================================

async def auto_fill_usernames_watchdog():
    """Auto-fill usernames when empty."""
    last_filled_time: dict = {}
    
    while bot_state.is_running:
        try:
            await asyncio.sleep(10)
            
            if not bot_state.account_pages:
                continue
            
            for domain_key, page in list(bot_state.account_pages.items()):
                try:
                    if page.is_closed():
                        last_filled_time.pop(domain_key, None)
                        continue
                    
                    username_input, _ = await find_input_fields(page, cache_key=None)
                    if not username_input:
                        continue
                    
                    current_value = await get_input_value(username_input)
                    if current_value.strip():
                        last_filled_time.pop(domain_key, None)
                        continue
                    
                    now = time.time()
                    last_filled = last_filled_time.get(domain_key)
                    if last_filled and (now - last_filled) < 300:
                        continue
                    
                    default_user = get_default_account_for_domain(domain_key)
                    if not default_user:
                        continue
                    
                    await page.evaluate(
                        "([el, val]) => { el.value = val; "
                        "el.dispatchEvent(new Event('input', {bubbles:true})); "
                        "el.dispatchEvent(new Event('change', {bubbles:true})); }",
                        [username_input, default_user],
                    )
                    logger.info(f"🔄 [{domain_key}] Auto-filled username")
                    last_filled_time[domain_key] = now
                
                except Exception as e:
                    logger.debug(f"⚠️ Username watchdog error: {e}")
        
        except asyncio.CancelledError:
            break
        except Exception:
            pass


async def cdp_health_watchdog():
    """j) Ping CDP định kỳ — tự động mở lại Edge nếu cần."""
    import urllib.request
    CDP_PING_INTERVAL = float(getattr(Config, "CDP_PING_INTERVAL", 60.0))

    while bot_state.is_running:
        try:
            await asyncio.sleep(CDP_PING_INTERVAL)
            ports = [int(p) for p in getattr(Config, "CDP_CONNECTIONS", {}).keys()]
            for port in ports:
                alive = False
                try:
                    url = f"http://127.0.0.1:{port}/json/version"
                    with urllib.request.urlopen(url, timeout=3) as resp:
                        alive = resp.status == 200
                except Exception:
                    alive = False

                if not alive:
                    logger.warning(f"🔌 [CDP] Port {port} không phản hồi → thử mở lại Edge...")
                    try:
                        from browser_manager import _launch_edge, _wait_for_cdp
                        _launch_edge(port)
                        import asyncio as _aio
                        await _aio.get_event_loop().run_in_executor(
                            None, _wait_for_cdp, port, 20.0
                        )
                        # Reconnect playwright browser
                        if port in bot_state.connected_browsers:
                            try:
                                await bot_state.connected_browsers[port].close()
                            except Exception:
                                pass
                            del bot_state.connected_browsers[port]
                        browser = await bot_state.playwright_instance.chromium.connect_over_cdp(
                            f"http://127.0.0.1:{port}"
                        )
                        bot_state.connected_browsers[port] = browser
                        logger.info(f"✅ [CDP] Port {port} đã kết nối lại")
                    except Exception as e:
                        logger.error(f"❌ [CDP] Không mở lại được port {port}: {e}")
                else:
                    logger.debug(f"✅ [CDP] Port {port} OK")

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.debug(f"⚠️ cdp_health_watchdog error: {e}")


async def cloudflare_watchdog():
    """Detect Cloudflare challenges."""
    CF_DETECT_SELECTORS = [
        "iframe[src*='challenges.cloudflare.com']",
        "iframe[src*='turnstile']",
        ".cf-turnstile",
        "[data-sitekey]",
    ]
    
    while bot_state.is_running:
        try:
            await asyncio.sleep(random.uniform(300.0, 600.0))
            
            for key, page in list(bot_state.account_pages.items()):
                try:
                    current_url = page.url
                except Exception:
                    continue
                
                cf_found = (
                    "challenges.cloudflare.com" in current_url
                    or "/cdn-cgi/challenge-platform" in current_url
                )
                
                if not cf_found:
                    for sel in CF_DETECT_SELECTORS:
                        try:
                            el = await page.query_selector(sel)
                            if el and await el.is_visible():
                                cf_found = True
                                break
                        except Exception:
                            continue
                
                if cf_found:
                    logger.warning(f"⚠️ Cloudflare [{key}] — please verify!")
                    try:
                        await page.bring_to_front()
                    except Exception:
                        pass
        
        except asyncio.CancelledError:
            break
        except Exception:
            pass


async def cleanup_browsers():
    """Close all browser connections."""
    for port, browser in bot_state.connected_browsers.items():
        try:
            await browser.close()
        except Exception:
            pass


# ============================================================
# MAIN
# ============================================================

async def main():
    """Main entry point."""
    global _systems, BOT_START_TIME
    
    try:
        logger.info("🚀 BOT v7.4 (PRODUCTION READY)")
        
        # ✅ SET BOT_START_TIME NGAY TẬP ĐẦU
        BOT_START_TIME = datetime.now(timezone.utc)
        logger.info(f"⏰ BOT_START_TIME set to: {BOT_START_TIME.strftime('%H:%M:%S %d/%m/%Y')}")
        logger.info(f"⏰ Chỉ xử lý tin nhắn MỚI từ thời điểm này trở đi\n")
        
        _systems = await init_systems()
        # ✅ FIX: KHÔNG close database ở đây!
        # db.close() sẽ làm hỏng kết nối SQLite cho toàn bộ phiên chạy.
        # SQLite WAL mode + check_same_thread=False đã xử lý concurrent access an toàn.
        # Comment cũ "tránh SQLite lock" là không cần thiết và gây lỗi nghiêm trọng.

        await asyncio.sleep(0.5)
        
        # ✅ Telethon v1.35.0 không support catch_up parameter
        await client.start()
        
        # Verify session
        if not await verify_telegram_session():
            return
        
        # Verify channels
        valid_channels = await verify_channels_and_get_ids()
        if not valid_channels:
            logger.error("❌ No valid channels")
            return
        
        # ✅ SETUP HANDLER NGAY (để ready nhận tin mới)
        await setup_telegram_handler()
        
        # Preload tabs
        await preload_browsers_and_accounts()
        
        # ✅ SET LẠI BOT_START_TIME SAU PRELOAD
        BOT_START_TIME = datetime.now(timezone.utc)
        logger.info(f"✅ BOT READY! Listening from: {BOT_START_TIME.strftime('%H:%M:%S')}\n")
        
        # Start background tasks
        async def heartbeat_loop():
            while bot_state.is_running:
                try:
                    await asyncio.sleep(300.0)
                    pages = len(bot_state.account_pages)
                    tasks = len(_active_submit_tasks)
                    q_size = message_queue.qsize() if message_queue else 0
                    tg_ok = client.is_connected()
                    logger.info(
                        f"💓 Heartbeat | tabs={pages} | tasks={tasks} | queue={q_size} | tg={tg_ok}"
                    )
                    # h) Kiểm tra session Telegram: connected nhưng get_me() thất bại → đăng nhập lại
                    if tg_ok:
                        try:
                            me = await asyncio.wait_for(client.get_me(), timeout=10.0)
                            if me is None:
                                raise Exception("get_me() returned None")
                            logger.debug(f"✅ Telegram session OK: {me.username or me.id}")
                        except Exception as sess_err:
                            logger.warning(f"⚠️ Telegram session lỗi ({sess_err}) → reconnect...")
                            try:
                                await client.disconnect()
                                await asyncio.sleep(2)
                                await client.start()
                                logger.info("✅ Telegram session reconnect thành công")
                            except Exception as rc_err:
                                logger.error(f"❌ Telegram reconnect thất bại: {rc_err}")
                except asyncio.CancelledError:
                    break
                except Exception:
                    pass
        
        # ✅ FIX: Lưu reference để tránh bị garbage collected
        _bg_tasks = set()
        _bg_tasks.add(asyncio.create_task(heartbeat_loop(), name="heartbeat"))
        _bg_tasks.add(asyncio.create_task(auto_fill_usernames_watchdog(), name="autofill_watchdog"))
        _bg_tasks.add(asyncio.create_task(cloudflare_watchdog(), name="cloudflare_watchdog"))
        _bg_tasks.add(asyncio.create_task(_cleanup_scheduler(), name="cleanup_scheduler"))
        _bg_tasks.add(asyncio.create_task(cdp_health_watchdog(), name="cdp_watchdog"))
        
        # Main loop
        _reconnect_delay = 5.0
        _reconnect_backoff = 1.0
        
        while bot_state.is_running:
            try:
                if not client.is_connected():
                    logger.warning("🔄 Reconnecting Telegram...")
                    await client.connect()
                
                await client.run_until_disconnected()
                break

            except (ConnectionError, OSError):
                wait = min(_reconnect_delay * _reconnect_backoff, 60.0)
                logger.warning(f"⚠️ Reconnecting in {wait:.0f}s...")
                await asyncio.sleep(wait)
                _reconnect_backoff = min(_reconnect_backoff * 2, 12)
            
            except Exception as e:
                logger.error(f"❌ Error: {e}")
                await asyncio.sleep(_reconnect_delay)
    
    except Exception as e:
        logger.critical(f"❌ Critical error: {e}\n{traceback.format_exc()}")
    
    finally:
        logger.info("\n🛑 Shutting down...")
        bot_state.is_running = False
        
        if _history_queue is not None:
            try:
                await asyncio.wait_for(_history_queue.join(), timeout=5.0)
            except Exception:
                pass
            if _history_writer_task:
                _history_writer_task.cancel()
        
        if _active_submit_tasks:
            logger.info(f"⏳ Waiting for {len(_active_submit_tasks)} tasks...")
            try:
                await asyncio.wait_for(
                    asyncio.gather(*list(_active_submit_tasks), return_exceptions=True),
                    timeout=8.0,
                )
            except Exception:
                for t in list(_active_submit_tasks):
                    t.cancel()
        
        for worker in message_workers:
            worker.cancel()
        
        await cleanup_browsers()
        build_daily_summary()
        
        if bot_state.playwright_instance:
            await bot_state.playwright_instance.stop()
        
        logger.info("✅ Shutdown complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Bot stopped")