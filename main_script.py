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

    logger.warning("⚠️ SKIP_AUTO_VERIFY=TRUE — please manually complete Cloudflare on Edge.")
    logger.warning(f"⏳ Waiting up to {int(max_wait)}s for manual verification...")

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

    # send Telegram alert if admin configured
    try:
        admin_id = getattr(Config, "TELEGRAM_ADMIN_ID", 0)
        if admin_id and client:
            msg = "🚨 BOT: Cloudflare/Turnstile detected — please verify manually on Edge."
            asyncio.create_task(send_alert_to_user(client, msg, admin_id))
    except Exception:
        pass

    while time.time() < deadline:
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


def _filter_nextjs_noise(text: str) -> str:
    if not text:
        return ""
    noise_markers = [
        "__next_f", "__NEXT", "self.__next",
        'push([1,"', '"stylesheet"', '"link"',
        "webpack", "hydrat", '"rel":', '"href":',
        ":[[[\"$\"',
    ]
    t = text.strip()
    for marker in noise_markers:
        if marker in t:
            return ""
    if t.startswith(('{"', '[[', '[[["', 'self.')):
        return ""
    return t


async def detect_result_text(page) -> str:
    PRIORITY_SELECTORS = [
        ".swal2-html-container", ".swal2-title", ".swal2-popup",
        "div[class*='popup'] p",
        "div[class*='modal'] p",
        "div[class*='dialog'] p",
        "div[class*='alert'] p",
        "div[class*='notice'] p",
        ".text-red-600", ".text-green-600", ".text-yellow-600",
        ".text-red-500", ".text-green-500",
        "p.mt-1.text-sm",
        "div[class*='rounded-2xl'] p",
        "div[class*='rounded-xl'] p",
        "div[class*='rounded-lg'] p",
        "[role='alert']", "[role='status']", "[role='dialog']",
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
    try:
        await maybe_bring_to_front(page)
        await page.evaluate("""
            Object.defineProperty(document, 'visibilityState', {
                get: () => 'visible', configurable: true
            });
        """)
        await _close_unwanted_popups(page)
    except Exception:
        pass


async def auto_fill_username_on_startup(page, domain: str, username: str):
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
        await maybe_bring_to_front(page)
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
    try:
        return CodeValidator.validate_code(code, target_url, source=source)
    except TypeError:
        return CodeValidator.validate_code(code, target_url)


def get_filter_group_name(target_url: str) -> str:
    group_name, _ = CodeValidator.get_filter_group(target_url)
    return group_name


def unique_keep_order(items):
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
    cleaned = text or ""
    cleaned = re.sub(r"https?://\S+", " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"www\.\S+", " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(
        r"\b[a-zA-Z0-9.-]+\.(com|net|org|vn|app|info)\b", " ", cleaned, flags=re.IGNORECASE
    )
    cleaned = cleaned.replace("：", ":").replace("|", " ").replace("•", " ")
    return cleaned


def line_has_code_marker(line: str) -> bool:
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
    upper = line.upper().strip()
    if not upper:
        return True
    noise_keywords = [
        "HTTP", "WWW", ".COM", "FACEBOOK", "TELEGRAM", "TIKTOK", "ZALO",
        "CSKH", "BOT", "CHECK LINK", "LINK",
    ]
    return any(kw in upper for kw in noise_keywords)


def extract_tokens_from_line(line: str):
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

# (file continues unchanged for brevity...)
