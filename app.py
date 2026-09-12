"""
JADI GR — Solana memecoin intelligence bot (single-file, GitHub Actions edition)

This is the whole bot in one file, on purpose: it's meant to be pasted into
a GitHub repo from a phone, then run on a schedule by GitHub Actions
(free, no credit card). Each run does ONE pass:
  1. Check for any button presses (Refresh / Trade Taken / Trade Ignored /
     Watchlist) since the last run, and respond to them.
  2. Scan for new opportunities and send alerts for anything good.
  3. Save its database and exit.

Because it's not running continuously, button presses get handled on the
NEXT scheduled run (up to ~15 min later), not instantly. Everything else
works the same as the always-on version.
"""
import asyncio
import json
import logging
import os
import sqlite3
import time
from contextlib import contextmanager

import httpx
from dotenv import load_dotenv
from telegram import Bot, Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("jadi_gr")

# ============================== CONFIG ======================================

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

HELIUS_API_KEY = os.getenv("HELIUS_API_KEY", "")
BIRDEYE_API_KEY = os.getenv("BIRDEYE_API_KEY", "")
TWITTER_BEARER_TOKEN = os.getenv("TWITTER_BEARER_TOKEN", "")
RUGCHECK_API_KEY = os.getenv("RUGCHECK_API_KEY", "")

DEXSCREENER_BASE = "https://api.dexscreener.com"
PUMPFUN_FRONTEND_API = "https://frontend-api-v3.pump.fun"
RUGCHECK_BASE = "https://api.rugcheck.xyz/v1"

DB_PATH = os.getenv("JADI_DB_PATH", "jadi_gr.sqlite3")
MIN_LIQUIDITY_USD = float(os.getenv("MIN_LIQUIDITY_USD", "2000"))
FAST_ALERT_SCORE = int(os.getenv("FAST_ALERT_SCORE", "80"))
REGULAR_ALERT_SCORE = int(os.getenv("REGULAR_ALERT_SCORE", "60"))
BATCH_DISPLAY_LIMIT = int(os.getenv("BATCH_DISPLAY_LIMIT", "15"))  # max shown per batch message

HAS_TWITTER_API = bool(TWITTER_BEARER_TOKEN)
HTTP_TIMEOUT = 15.0

# ============================== DATABASE ====================================

SCHEMA = """
CREATE TABLE IF NOT EXISTS tokens (
    address TEXT PRIMARY KEY, symbol TEXT, name TEXT, first_seen_at REAL,
    category TEXT, creator_wallet TEXT, last_updated_at REAL
);
CREATE TABLE IF NOT EXISTS snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT, token_address TEXT, taken_at REAL,
    mc REAL, liquidity REAL, volume_24h REAL, price REAL,
    buys_5m INTEGER, sells_5m INTEGER, pair_age_minutes REAL, raw_json TEXT
);
CREATE TABLE IF NOT EXISTS scores (
    id INTEGER PRIMARY KEY AUTOINCREMENT, token_address TEXT, scored_at REAL,
    market_score REAL, safety_score REAL, social_score REAL, smart_money_score REAL,
    entry_score REAL, overall_score REAL, opportunity_level TEXT,
    entry_mc_low REAL, entry_mc_high REAL, warnings_json TEXT
);
CREATE TABLE IF NOT EXISTS alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT, token_address TEXT, sent_at REAL,
    alert_type TEXT, telegram_message_id INTEGER, status TEXT DEFAULT 'pending'
);
CREATE TABLE IF NOT EXISTS wallets (
    address TEXT PRIMARY KEY, reputation TEXT DEFAULT 'watch',
    reputation_score REAL DEFAULT 0, observations INTEGER DEFAULT 0,
    last_seen_at REAL, notes TEXT
);
CREATE TABLE IF NOT EXISTS wallet_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT, wallet_address TEXT, token_address TEXT,
    event_type TEXT, happened_at REAL, detail_json TEXT
);
CREATE TABLE IF NOT EXISTS watchlist (
    token_address TEXT PRIMARY KEY, added_at REAL, note TEXT
);
CREATE TABLE IF NOT EXISTS trade_outcomes (
    id INTEGER PRIMARY KEY AUTOINCREMENT, token_address TEXT, alert_id INTEGER,
    entry_mc REAL, checked_at REAL, mc_at_check REAL, multiple REAL
);
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY, value TEXT
);
"""


@contextmanager
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db():
    with get_conn() as conn:
        conn.executescript(SCHEMA)


def get_meta(key, default=None):
    with get_conn() as conn:
        row = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return row["value"] if row else default


def set_meta(key, value):
    with get_conn() as conn:
        conn.execute("INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)", (key, str(value)))


def upsert_token(address, symbol, name, category):
    now = time.time()
    with get_conn() as conn:
        if conn.execute("SELECT 1 FROM tokens WHERE address=?", (address,)).fetchone():
            conn.execute("UPDATE tokens SET symbol=?, name=?, category=?, last_updated_at=? WHERE address=?",
                         (symbol, name, category, now, address))
        else:
            conn.execute("INSERT INTO tokens (address, symbol, name, first_seen_at, category, last_updated_at) "
                         "VALUES (?,?,?,?,?,?)", (address, symbol, name, now, category, now))


def add_snapshot(token_address, data: dict):
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO snapshots (token_address, taken_at, mc, liquidity, volume_24h, price, buys_5m, "
            "sells_5m, pair_age_minutes, raw_json) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (token_address, time.time(), data.get("mc"), data.get("liquidity"), data.get("volume_24h"),
             data.get("price"), data.get("buys_5m"), data.get("sells_5m"), data.get("pair_age_minutes"),
             json.dumps(data.get("raw", {}))),
        )


def add_score(token_address, score: dict):
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO scores (token_address, scored_at, market_score, safety_score, social_score, "
            "smart_money_score, entry_score, overall_score, opportunity_level, entry_mc_low, entry_mc_high, "
            "warnings_json) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (token_address, time.time(), score["market_score"], score["safety_score"], score["social_score"],
             score["smart_money_score"], score["entry_score"], score["overall_score"], score["opportunity_level"],
             score.get("entry_mc_low"), score.get("entry_mc_high"), json.dumps(score.get("warnings", []))),
        )


def has_alert(token_address):
    with get_conn() as conn:
        return bool(conn.execute("SELECT 1 FROM alerts WHERE token_address=?", (token_address,)).fetchone())


def record_alert(token_address, alert_type, telegram_message_id=None):
    with get_conn() as conn:
        cur = conn.execute("INSERT INTO alerts (token_address, sent_at, alert_type, telegram_message_id) "
                           "VALUES (?,?,?,?)", (token_address, time.time(), alert_type, telegram_message_id))
        return cur.lastrowid


def set_alert_status(alert_id, status):
    with get_conn() as conn:
        conn.execute("UPDATE alerts SET status=? WHERE id=?", (status, alert_id))


def upsert_wallet(address):
    with get_conn() as conn:
        if not conn.execute("SELECT 1 FROM wallets WHERE address=?", (address,)).fetchone():
            conn.execute("INSERT INTO wallets (address, last_seen_at) VALUES (?,?)", (address, time.time()))
        else:
            conn.execute("UPDATE wallets SET last_seen_at=? WHERE address=?", (time.time(), address))


def log_wallet_event(wallet_address, token_address, event_type, detail=None):
    upsert_wallet(wallet_address)
    with get_conn() as conn:
        conn.execute("INSERT INTO wallet_events (wallet_address, token_address, event_type, happened_at, "
                     "detail_json) VALUES (?,?,?,?,?)",
                     (wallet_address, token_address, event_type, time.time(), json.dumps(detail or {})))
        conn.execute("UPDATE wallets SET observations = observations + 1 WHERE address=?", (wallet_address,))


def get_wallet(address):
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM wallets WHERE address=?", (address,)).fetchone()
        return dict(row) if row else None


def update_wallet_reputation(address, reputation, reputation_score, notes=None):
    with get_conn() as conn:
        conn.execute("UPDATE wallets SET reputation=?, reputation_score=?, notes=? WHERE address=?",
                     (reputation, reputation_score, notes, address))


def add_to_watchlist(token_address, note=""):
    with get_conn() as conn:
        conn.execute("INSERT OR REPLACE INTO watchlist (token_address, added_at, note) VALUES (?,?,?)",
                     (token_address, time.time(), note))


def get_watchlist():
    with get_conn() as conn:
        return [dict(r) for r in conn.execute("SELECT * FROM watchlist ORDER BY added_at DESC").fetchall()]


def top_opportunities(limit=10):
    with get_conn() as conn:
        rows = conn.execute(
            """SELECT t.address, t.symbol, t.category, s.overall_score, s.opportunity_level,
                      s.entry_mc_low, s.entry_mc_high
               FROM scores s JOIN tokens t ON t.address = s.token_address
               WHERE s.id IN (SELECT MAX(id) FROM scores GROUP BY token_address)
               ORDER BY s.overall_score DESC LIMIT ?""", (limit,),
        ).fetchall()
        return [dict(r) for r in rows]


def performance_summary():
    with get_conn() as conn:
        rows = conn.execute(
            """SELECT alert_type, COUNT(*) as total,
                      SUM(CASE WHEN status='taken' THEN 1 ELSE 0 END) as taken,
                      SUM(CASE WHEN status='ignored' THEN 1 ELSE 0 END) as ignored
               FROM alerts GROUP BY alert_type""",
        ).fetchall()
        return [dict(r) for r in rows]


# ============================== DATA SOURCES ================================
# All free, no API keys required.

async def dex_get_token_pairs(token_address: str) -> list:
    url = f"{DEXSCREENER_BASE}/token-pairs/v1/solana/{token_address}"
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        return resp.json() or []


def dex_normalize_pair(pair: dict) -> dict:
    created_at_ms = pair.get("pairCreatedAt", 0)
    age_minutes = (time.time() * 1000 - created_at_ms) / 60000 if created_at_ms else None
    txns_5m = pair.get("txns", {}).get("m5", {})
    socials = {s.get("type"): s.get("url") for s in pair.get("info", {}).get("socials", [])}
    websites = [w.get("url") for w in pair.get("info", {}).get("websites", [])]
    return {
        "token_address": pair.get("baseToken", {}).get("address"),
        "symbol": pair.get("baseToken", {}).get("symbol"),
        "name": pair.get("baseToken", {}).get("name"),
        "price": float(pair.get("priceUsd") or 0),
        "mc": pair.get("marketCap") or pair.get("fdv") or 0,
        "liquidity": (pair.get("liquidity") or {}).get("usd", 0),
        "volume_24h": (pair.get("volume") or {}).get("h24", 0),
        "buys_5m": txns_5m.get("buys", 0),
        "sells_5m": txns_5m.get("sells", 0),
        "pair_age_minutes": age_minutes,
        "price_change_5m": (pair.get("priceChange") or {}).get("m5"),
        "has_socials": bool(socials or websites),
        "socials": socials,
        "raw": pair,
    }


PUMPFUN_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Referer": "https://pump.fun/",
    "Origin": "https://pump.fun",
}


async def pumpfun_get_new_mints(limit=100) -> list:
    url = f"{PUMPFUN_FRONTEND_API}/coins"
    params = {"offset": 0, "limit": limit, "sort": "created_timestamp", "order": "DESC"}
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT, headers=PUMPFUN_HEADERS) as client:
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        return resp.json() or []


def pumpfun_bonding_progress(coin: dict) -> float:
    if coin.get("complete"):
        return 1.0
    real_sol = coin.get("real_sol_reserves", 0) / 1e9
    return min(real_sol / 85.0, 1.0)


def pumpfun_classify_stage(coin: dict) -> str:
    if coin.get("complete") or coin.get("raydium_pool"):
        return "migrated"
    if pumpfun_bonding_progress(coin) >= 0.85:
        return "nearly_graduated"
    return "new_pair"


async def pumpfun_candidates_by_stage() -> dict:
    mints = await pumpfun_get_new_mints(limit=100)
    buckets = {"new_pair": [], "nearly_graduated": [], "migrated": []}
    for coin in mints:
        buckets[pumpfun_classify_stage(coin)].append(coin)
    return buckets


async def pumpfun_get_trades(mint_address: str, limit=200) -> list:
    url = f"{PUMPFUN_FRONTEND_API}/trades/all/{mint_address}"
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT, headers=PUMPFUN_HEADERS) as client:
        resp = await client.get(url, params={"limit": limit, "offset": 0})
        if resp.status_code == 404:
            return []
        resp.raise_for_status()
        return resp.json() or []


async def rugcheck_get_report(token_address: str):
    """
    Brand-new tokens (seconds old) often aren't indexed by RugCheck yet, and
    return 400/404 rather than a report. That must not kill the whole
    analysis for that token - treat it as "unknown risk" and continue
    scoring with the data we do have, same as an early-buyer-data gap.
    """
    url = f"{RUGCHECK_BASE}/tokens/{token_address}/report"
    headers = {"Authorization": f"Bearer {RUGCHECK_API_KEY}"} if RUGCHECK_API_KEY else {}
    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT, headers=headers) as client:
            resp = await client.get(url)
            if resp.status_code != 200:
                return None
            return resp.json()
    except Exception:
        logger.info(f"RugCheck report unavailable for {token_address}")
        return None


def rugcheck_summarize(report):
    if not report:
        return {"risks": [], "top_holder_pct": None, "mint_authority_revoked": None,
                "freeze_authority_revoked": None, "lp_locked_pct": None}
    risks = [r.get("name") for r in report.get("risks", [])]
    holders = report.get("topHolders", [])
    top_holder_pct = sum(h.get("pct", 0) for h in holders[:1]) if holders else None
    lp_locked = None
    if report.get("markets"):
        lp_locked = (report["markets"][0].get("lp") or {}).get("lpLockedPct")
    return {
        "risks": risks, "top_holder_pct": top_holder_pct,
        "mint_authority_revoked": report.get("mintAuthority") is None,
        "freeze_authority_revoked": report.get("freezeAuthority") is None,
        "lp_locked_pct": lp_locked,
    }


# ============================== ANALYSIS ====================================

def score_market(pair: dict) -> dict:
    warnings, positives, points, max_points = [], [], 0, 0
    mc, liquidity = pair.get("mc") or 0, pair.get("liquidity") or 0
    volume, buys, sells, age = pair.get("volume_24h") or 0, pair.get("buys_5m") or 0, pair.get("sells_5m") or 0, pair.get("pair_age_minutes")

    max_points += 25
    if liquidity <= 0:
        warnings.append("❌ No liquidity data")
    elif liquidity < 2000:
        warnings.append("❌ Very low liquidity"); points += 3
    elif liquidity < 8000:
        points += 12
    else:
        points += 25; positives.append("✅ Solid liquidity")

    max_points += 25
    if mc > 0 and volume > 0:
        ratio = volume / mc
        if ratio > 3: points += 25; positives.append("🔥 Volume >> market cap (very active)")
        elif ratio > 1: points += 18
        elif ratio > 0.3: points += 10
        else: points += 3; warnings.append("❌ Volume low relative to MC")
    else:
        warnings.append("❌ Insufficient volume/MC data")

    max_points += 25
    total_txns = buys + sells
    if total_txns == 0:
        warnings.append("❌ No recent transaction activity")
    else:
        buy_ratio = buys / total_txns
        if buy_ratio > 0.65: points += 25; positives.append(f"✅ Strong buy pressure ({buy_ratio:.0%} buys)")
        elif buy_ratio > 0.5: points += 15
        else: points += 5; warnings.append(f"⚠️ More sells than buys ({buy_ratio:.0%} buys)")

    max_points += 25
    if age is None: points += 10
    elif age < 2: points += 8
    elif age < 30: points += 22
    elif age < 240: points += 18
    else: points += 10

    return {"market_score": round((points / max_points) * 100, 1) if max_points else 0,
            "warnings": warnings, "positives": positives}


def score_safety(risk: dict, liquidity: float) -> dict:
    warnings, positives, points, max_points = [], [], 0, 0

    max_points += 30
    if risk.get("mint_authority_revoked") is True:
        points += 15; positives.append("✅ Mint authority revoked")
    elif risk.get("mint_authority_revoked") is False:
        warnings.append("⛔️ Mint authority NOT revoked - dev can print more supply")
    else:
        points += 7
    if risk.get("freeze_authority_revoked") is True:
        points += 15; positives.append("✅ Freeze authority revoked")
    elif risk.get("freeze_authority_revoked") is False:
        warnings.append("⛔️ Freeze authority NOT revoked - dev can freeze your wallet")
    else:
        points += 7

    max_points += 25
    top_pct = risk.get("top_holder_pct")
    if top_pct is None: points += 10
    elif top_pct > 30: warnings.append(f"❌ Top holder owns {top_pct:.0f}% of supply"); points += 2
    elif top_pct > 15: warnings.append(f"⚠️ Top holder owns {top_pct:.0f}% of supply"); points += 12
    else: points += 25; positives.append("✅ Healthy holder distribution")

    max_points += 20
    lp_locked = risk.get("lp_locked_pct")
    if lp_locked is None: points += 8
    elif lp_locked > 80: points += 20; positives.append("✅ Liquidity locked")
    elif lp_locked > 30: points += 10
    else: warnings.append("❌ Liquidity mostly unlocked - can be pulled"); points += 2

    max_points += 15
    if liquidity and liquidity >= 5000: points += 15
    elif liquidity and liquidity >= 1500: points += 8
    else: warnings.append("❌ Liquidity thin enough to be a rug risk on its own"); points += 2

    max_points += 10
    flagged = risk.get("risks") or []
    if flagged:
        warnings.append(f"⛔️ RugCheck flags: {', '.join(str(f) for f in flagged[:3])}")
        points += max(0, 10 - 3 * len(flagged))
    else:
        points += 10

    return {"safety_score": round((points / max_points) * 100, 1) if max_points else 0,
            "warnings": warnings, "positives": positives}


def proxy_social_score(pair: dict) -> dict:
    warnings, positives, points, max_points = [], [], 0, 40
    socials = pair.get("socials") or {}
    if socials.get("twitter"): points += 20; positives.append("✅ Has linked Twitter/X")
    else: warnings.append("⚠️ No Twitter/X link found")
    if socials.get("telegram"): points += 12; positives.append("✅ Has linked Telegram")
    if pair.get("has_socials"): points += 8
    return {"social_score": round((points / max_points) * 100, 1), "warnings": warnings, "positives": positives,
            "posts": {"initial_post": None, "ca_post": None, "other_posts": []}, "is_real_social_analysis": False}


async def fetch_real_social_proof(symbol: str, contract_address: str) -> dict:
    headers = {"Authorization": f"Bearer {TWITTER_BEARER_TOKEN}"}
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT, headers=headers) as client:
        no_ca = await client.get("https://api.twitter.com/2/tweets/search/recent",
                                  params={"query": f'"{symbol}" -is:retweet lang:en', "max_results": 10,
                                          "tweet.fields": "created_at,public_metrics"})
        ca = await client.get("https://api.twitter.com/2/tweets/search/recent",
                               params={"query": f'"{contract_address}" -is:retweet', "max_results": 10,
                                       "tweet.fields": "created_at,public_metrics"})
    no_ca_tweets = (no_ca.json() or {}).get("data", []) if no_ca.status_code == 200 else []
    ca_tweets = (ca.json() or {}).get("data", []) if ca.status_code == 200 else []
    no_ca_tweets.sort(key=lambda t: t.get("created_at", ""))
    ca_tweets.sort(key=lambda t: t.get("public_metrics", {}).get("like_count", 0), reverse=True)
    activity = len(no_ca_tweets) + len(ca_tweets)
    return {
        "social_score": min(100, activity * 8),
        "warnings": [] if activity else ["⚠️ No social mentions found"],
        "positives": ["🔥 Active social chatter"] if activity >= 8 else [],
        "posts": {"initial_post": no_ca_tweets[0] if no_ca_tweets else None,
                  "ca_post": ca_tweets[0] if ca_tweets else None, "other_posts": no_ca_tweets[1:6]},
        "is_real_social_analysis": True,
    }


async def analyze_social(pair: dict, token_address: str) -> dict:
    if HAS_TWITTER_API:
        return await fetch_real_social_proof(pair.get("symbol", ""), token_address)
    return proxy_social_score(pair)


async def get_early_buyers(mint_address: str) -> list:
    trades = await pumpfun_get_trades(mint_address)
    if not trades:
        return []
    trades.sort(key=lambda t: t.get("timestamp", 0))
    first_ts = trades[0].get("timestamp", 0)
    return [t for t in trades if t.get("is_buy") and (t.get("timestamp", 0) - first_ts) <= 120]


def _reputation_label(score):
    if score >= 50: return "trusted"
    if score >= 10: return "watch"
    if score >= -20: return "suspicious"
    return "high_risk"


def nudge_reputation(wallet_address, delta, reason):
    wallet = get_wallet(wallet_address) or {"reputation_score": 0}
    new_score = max(-50, min(100, wallet.get("reputation_score", 0) + delta))
    update_wallet_reputation(wallet_address, _reputation_label(new_score), new_score, notes=reason)


async def score_smart_money(token_address: str) -> dict:
    warnings, positives = [], []
    early_buyers = await get_early_buyers(token_address)
    if not early_buyers:
        return {"smart_money_score": 40, "warnings": ["⚠️ No early-buyer data available yet"],
                "positives": [], "early_buyer_count": 0, "trusted_buyer_count": 0}

    trusted_count = suspicious_count = 0
    for trade in early_buyers:
        wallet_address = trade.get("user")
        if not wallet_address:
            continue
        log_wallet_event(wallet_address, token_address, "early_buy", {"sol_amount": trade.get("sol_amount")})
        wallet = get_wallet(wallet_address)
        rep = wallet.get("reputation") if wallet else "watch"
        if rep == "trusted": trusted_count += 1
        elif rep in ("suspicious", "high_risk"): suspicious_count += 1

    total = len(early_buyers)
    points = 40
    if trusted_count:
        points += min(40, trusted_count * 10)
        positives.append(f"🐋 {trusted_count} previously-trusted wallet(s) bought early")
    if suspicious_count:
        points -= min(40, suspicious_count * 12)
        warnings.append(f"⛔️ {suspicious_count} flagged wallet(s) among early buyers")
    if total >= 5:
        top_buy = max(t.get("sol_amount", 0) for t in early_buyers)
        total_sol = sum(t.get("sol_amount", 0) for t in early_buyers) or 1
        if top_buy / total_sol > 0.4:
            warnings.append("⚠️ Early buys concentrated in one wallet"); points -= 10

    return {"smart_money_score": max(0, min(100, points)), "warnings": warnings, "positives": positives,
            "early_buyer_count": total, "trusted_buyer_count": trusted_count}


def suggest_entry_zone(pair: dict, safety_score: float, smart_money_score: float) -> dict:
    mc = pair.get("mc") or 0
    change_5m = pair.get("price_change_5m") or 0
    warnings = []
    if mc <= 0:
        return {"entry_mc_low": None, "entry_mc_high": None, "note": "No reliable MC to base an entry on",
                "warnings": ["❌ No MC data"]}
    if change_5m > 60:
        low, high = mc * 0.55, mc * 0.80
        note = "Already up sharply in the last 5m - zone assumes a pullback, don't chase the top"
        warnings.append("⚠️ Chasing a fast pump - high risk of buying the top")
    elif change_5m > 20:
        low, high = mc * 0.85, mc * 1.05
        note = "Modest momentum - current MC is roughly in range"
    elif change_5m < -20:
        low, high = mc * 0.90, mc * 1.10
        note = "Pulling back - fine if safety/smart-money support it, confirm it's not dying"
        warnings.append("⚠️ Negative momentum - confirm this isn't a fade, not just a dip")
    else:
        low, high = mc * 0.90, mc * 1.15
        note = "Stable-ish - current MC is a reasonable reference zone"
    if safety_score < 50 or smart_money_score < 40:
        low *= 0.85
        warnings.append("⚠️ Weak safety/smart-money support - only consider deep in the zone, small size")
    return {"entry_mc_low": round(low, -2) if low >= 1000 else round(low, 0),
            "entry_mc_high": round(high, -2) if high >= 1000 else round(high, 0),
            "note": note, "warnings": warnings}


def estimate_round_trip_cost_pct(trade_size_usd, liquidity_usd, sol_price_usd=150):
    def slippage(size, liq):
        if not liq or liq <= 0: return 1.0
        ratio = size / liq
        return min(1.0, ratio + ratio ** 2)
    s_in = slippage(trade_size_usd, liquidity_usd)
    s_out = slippage(trade_size_usd, liquidity_usd)
    tx_fee_usd = (0.000005 + 0.0005) * sol_price_usd * 2
    total_pct = s_in + s_out + 0.02 + 0.006  # + bot fee(1%x2) + dex fee(0.3%x2)
    warnings = []
    if total_pct > 0.25: warnings.append("❌ Round-trip cost likely exceeds 25% at this size/liquidity")
    elif total_pct > 0.12: warnings.append("⚠️ Round-trip cost is significant at this size/liquidity")
    return {"estimated_slippage_pct": round((s_in + s_out) * 100, 2), "tx_fee_usd": round(tx_fee_usd, 4),
            "total_cost_pct": round(total_pct * 100, 2), "warnings": warnings}


WEIGHTS = {"market": 0.25, "safety": 0.35, "social": 0.10, "smart_money": 0.20, "entry_quality": 0.10}


def _opportunity_level(overall, safety_score):
    if safety_score < 35: return "LOW"
    if overall >= 80 and safety_score >= 60: return "VERY_HIGH"
    if overall >= 65: return "HIGH"
    if overall >= 45: return "MEDIUM"
    return "LOW"


async def score_token(pair: dict, risk_summary: dict, token_address: str) -> dict:
    m = score_market(pair)
    s = score_safety(risk_summary, pair.get("liquidity") or 0)
    soc = await analyze_social(pair, token_address)
    sm = await score_smart_money(token_address)
    ent = suggest_entry_zone(pair, s["safety_score"], sm["smart_money_score"])
    fee_info = estimate_round_trip_cost_pct(200, pair.get("liquidity") or 0)

    entry_quality = 100 - min(100, len(ent.get("warnings", [])) * 30)
    overall = round(
        m["market_score"] * WEIGHTS["market"] + s["safety_score"] * WEIGHTS["safety"] +
        soc["social_score"] * WEIGHTS["social"] + sm["smart_money_score"] * WEIGHTS["smart_money"] +
        entry_quality * WEIGHTS["entry_quality"], 1)

    all_warnings = m["warnings"] + s["warnings"] + soc["warnings"] + sm["warnings"] + ent.get("warnings", []) + fee_info["warnings"]
    all_positives = m["positives"] + s["positives"] + soc["positives"] + sm["positives"]

    return {
        "market_score": m["market_score"], "safety_score": s["safety_score"], "social_score": soc["social_score"],
        "smart_money_score": sm["smart_money_score"], "entry_score": entry_quality, "overall_score": overall,
        "opportunity_level": _opportunity_level(overall, s["safety_score"]),
        "entry_mc_low": ent.get("entry_mc_low"), "entry_mc_high": ent.get("entry_mc_high"),
        "entry_note": ent.get("note"), "warnings": all_warnings, "positives": all_positives,
        "fee_info": fee_info, "is_real_social_analysis": soc.get("is_real_social_analysis", False),
        "early_buyer_count": sm.get("early_buyer_count", 0), "trusted_buyer_count": sm.get("trusted_buyer_count", 0),
    }


async def analyze_token(token_address: str, category: str):
    pairs = await dex_get_token_pairs(token_address)
    if not pairs:
        return None, None
    pairs_sorted = sorted(pairs, key=lambda p: (p.get("liquidity") or {}).get("usd", 0), reverse=True)
    pair = dex_normalize_pair(pairs_sorted[0])
    report = await rugcheck_get_report(token_address)
    risk_summary = rugcheck_summarize(report)
    score = await score_token(pair, risk_summary, token_address)
    upsert_token(token_address, pair.get("symbol"), pair.get("name"), category)
    add_snapshot(token_address, pair)
    add_score(token_address, score)
    return pair, score


# ============================== TELEGRAM UI =================================

def _fmt_usd(n):
    if n is None: return "N/A"
    if n >= 1_000_000: return f"${n/1_000_000:.2f}M"
    if n >= 1_000: return f"${n/1_000:.1f}K"
    return f"${n:.0f}"


def alert_type_for_score(overall_score):
    return "fast" if overall_score >= FAST_ALERT_SCORE else "regular"


def alert_keyboard(token_address, alert_id):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔄 REFRESH", callback_data=f"refresh:{token_address}:{alert_id}")],
        [InlineKeyboardButton("🟢 Trade Taken", callback_data=f"taken:{alert_id}"),
         InlineKeyboardButton("⚪ Trade Ignored", callback_data=f"ignored:{alert_id}")],
        [InlineKeyboardButton("⭐ Watchlist", callback_data=f"watch:{token_address}"),
         InlineKeyboardButton("📊 Dexscreener", url=f"https://dexscreener.com/solana/{token_address}")],
    ])


def main_menu_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🆕 New Pairs", callback_data="menu:new_pair"),
         InlineKeyboardButton("🚀 Nearly Graduated", callback_data="menu:nearly_graduated")],
        [InlineKeyboardButton("🔄 Migrated", callback_data="menu:migrated"),
         InlineKeyboardButton("🔥 Top Opportunities", callback_data="menu:top")],
        [InlineKeyboardButton("⭐ Watchlist", callback_data="menu:watchlist"),
         InlineKeyboardButton("📈 Performance", callback_data="menu:performance")],
    ])


def generate_thesis(pair: dict, score: dict, category: str) -> str:
    """
    A short, human-readable case for the token, built only from signals
    already computed above - never invented. This is a read of current
    on-chain data, not a prediction; it's flagged as such at the end.
    """
    parts = []
    symbol = pair.get("symbol", "This token")
    volume, mc = pair.get("volume_24h") or 0, pair.get("mc") or 0

    if score["market_score"] >= 70:
        ratio_note = f" (volume is {volume/mc:.1f}x its market cap)" if mc else ""
        parts.append(f"{symbol} is showing strong buy pressure and real trading volume{ratio_note}, "
                      f"not just a stagnant listing.")
    elif score["market_score"] >= 50:
        parts.append(f"{symbol}'s market activity is moderate — buys outweigh sells, but volume isn't heavy yet.")
    else:
        parts.append(f"{symbol}'s market activity is thin right now, which limits the case for it.")

    if score.get("trusted_buyer_count"):
        parts.append(f"{score['trusted_buyer_count']} wallet(s) with a track record JADI GR has seen before "
                      f"bought in early — a real signal, not just volume.")
    elif score.get("early_buyer_count"):
        parts.append("There's early-buyer data, but none of those wallets are flagged as previously "
                      "trusted yet, so treat that part as unproven.")

    if score["safety_score"] >= 70:
        parts.append("Safety checks are clean — key authorities revoked, liquidity locked, no alarming "
                      "holder concentration — which lowers (not eliminates) rug risk.")
    elif score["safety_score"] < 40:
        parts.append("Safety is the weak point here and is the main thing holding back a higher rating.")

    if category == "nearly_graduated":
        parts.append("It's close to graduating off the bonding curve, which often brings a fresh wave of "
                      "attention and liquidity once it migrates.")
    elif category == "migrated":
        parts.append("It's already migrated, so this case rests on sustained momentum, not a graduation pump.")

    parts.append("This is a read of current signals, not a guarantee — momentum can reverse in minutes.")
    return " ".join(parts)


def format_alert(pair, score, category):
    alert_type = alert_type_for_score(score["overall_score"])
    header = "⚡ FAST ALERT" if alert_type == "fast" else "📊 REGULAR ALERT"
    lines = [
        f"{header}  {category.replace('_', ' ').title()}",
        f"*{pair.get('symbol', '?')}* — {pair.get('name', '')}",
        f"`{pair.get('token_address', '')}`", "",
        f"Opportunity: *{score['opportunity_level']}*  ({score['overall_score']}/100)",
        f"Market {score['market_score']} | Safety {score['safety_score']} | Social {score['social_score']} | Smart-Money {score['smart_money_score']}",
        "",
        f"🧠 *Thesis*: {generate_thesis(pair, score, category)}",
        "",
        f"💰 MC: {_fmt_usd(pair.get('mc'))}   💧 Liq: {_fmt_usd(pair.get('liquidity'))}   📈 Vol24h: {_fmt_usd(pair.get('volume_24h'))}",
        f"⏱️ Age: {(pair.get('pair_age_minutes') or 0):.0f}m   🟢 Buys(5m): {pair.get('buys_5m', 0)}  🔴 Sells(5m): {pair.get('sells_5m', 0)}",
        "",
        f"🎯 Suggested Entry MC: {_fmt_usd(score.get('entry_mc_low'))} – {_fmt_usd(score.get('entry_mc_high'))}",
        f"   _{score.get('entry_note', '')}_", "",
        f"💸 Est. round-trip cost: {score['fee_info']['total_cost_pct']}%",
    ]
    if score.get("early_buyer_count"):
        lines.append(f"🐋 Early buyers: {score['early_buyer_count']} ({score.get('trusted_buyer_count', 0)} trusted)")
    if not score.get("is_real_social_analysis"):
        lines.append("📵 Social proof is proxy-only (no X/Twitter API key configured)")
    if score.get("positives"):
        lines.append(""); lines.extend(score["positives"][:6])
    if score.get("warnings"):
        lines.append(""); lines.extend(score["warnings"][:8])
    return "\n".join(lines)


def format_top_opportunities(rows):
    if not rows:
        return "No scored opportunities yet - give the scanner a bit more time."
    lines = ["🔥 *Top Opportunities*", ""]
    for r in rows:
        lines.append(f"*{r['symbol']}* ({r['category']}) — {r['overall_score']}/100 — "
                     f"entry {_fmt_usd(r['entry_mc_low'])}-{_fmt_usd(r['entry_mc_high'])}")
    return "\n".join(lines)


def format_performance(rows):
    if not rows:
        return "No alerts sent yet."
    lines = ["📈 *Performance*", ""]
    for r in rows:
        lines.append(f"{r['alert_type'].upper()}: {r['total']} sent | {r['taken'] or 0} taken | {r['ignored'] or 0} ignored")
    return "\n".join(lines)


def format_watchlist(rows):
    if not rows:
        return "Watchlist is empty. Add tokens from any alert with ⭐."
    lines = ["⭐ *Watchlist*", ""]
    for r in rows:
        lines.append(f"`{r['token_address']}` — {r.get('note') or 'no note'}")
    return "\n".join(lines)


# ============================== BOT LOGIC ====================================

async def safe_answer(query, *args, **kwargs):
    """
    Button taps get processed minutes after the user tapped them (since this
    runs on a schedule, not continuously). Telegram often rejects the
    acknowledgement by then ("query is too old"). That must never stop the
    actual action (refresh, mark taken, etc.) from running.
    """
    try:
        await query.answer(*args, **kwargs)
    except Exception:
        logger.info("Could not answer callback query (likely expired) - continuing anyway")


async def safe_edit(query, text, **kwargs):
    """Telegram errors if the new text is byte-identical to the old message - harmless, ignore it."""
    try:
        await query.edit_message_text(text, **kwargs)
    except Exception as e:
        if "not modified" not in str(e).lower():
            logger.exception("Failed to edit message")


async def handle_update(bot: Bot, update: Update):
    if update.message and update.message.text in ("/start", "/menu"):
        await bot.send_message(chat_id=update.message.chat_id,
                                text="🧠 *JADI GR* — Solana memecoin intelligence assistant\n\n"
                                     "Scanning New Pairs, Nearly Graduated, and Migrated tokens every run. "
                                     "This bot checks in periodically (free hosting), so button presses "
                                     "aren't instant, but everything else works the same.",
                                reply_markup=main_menu_keyboard(), parse_mode=ParseMode.MARKDOWN)
        return

    query = update.callback_query
    if not query:
        return
    data = query.data

    if data.startswith("menu:"):
        section = data.split(":", 1)[1]
        if section == "top":
            text = format_top_opportunities(top_opportunities())
        elif section == "watchlist":
            text = format_watchlist(get_watchlist())
        elif section == "performance":
            text = format_performance(performance_summary())
        else:
            with get_conn() as conn:
                rows = conn.execute(
                    """SELECT t.address, t.symbol, s.overall_score, s.opportunity_level FROM tokens t
                       LEFT JOIN scores s ON s.token_address = t.address
                       WHERE t.category=? AND s.id IN (SELECT MAX(id) FROM scores GROUP BY token_address)
                       ORDER BY s.overall_score DESC LIMIT 15""", (section,)).fetchall()
            text = (f"No {section.replace('_',' ')} tokens scored yet." if not rows else
                    f"*{section.replace('_',' ').title()}*\n\n" + "\n".join(
                        f"{r['symbol']} — {r['overall_score']}/100 `{r['address']}`" for r in rows))
        await safe_answer(query)
        await safe_edit(query, text, reply_markup=main_menu_keyboard(), parse_mode=ParseMode.MARKDOWN)

    elif data.startswith("refresh:"):
        _, token_address, alert_id = data.split(":")
        await safe_answer(query, "Refreshing...")
        with get_conn() as conn:
            row = conn.execute("SELECT category FROM tokens WHERE address=?", (token_address,)).fetchone()
        category = row["category"] if row else "new_pair"
        pair, score = await analyze_token(token_address, category)
        if not pair:
            await safe_answer(query, "Couldn't refresh - no live pair data.", show_alert=True)
            return
        await safe_edit(query, format_alert(pair, score, category),
                         reply_markup=alert_keyboard(token_address, int(alert_id)),
                         parse_mode=ParseMode.MARKDOWN)

    elif data.startswith(("taken:", "ignored:")):
        action, alert_id = data.split(":")
        set_alert_status(int(alert_id), "taken" if action == "taken" else "ignored")
        await safe_answer(query, f"Marked as {action}")

    elif data.startswith("watch:"):
        token_address = data.split(":", 1)[1]
        add_to_watchlist(token_address)
        await safe_answer(query, "Added to watchlist ⭐")


async def send_alert(bot: Bot, token_address, category, pair, score):
    text = format_alert(pair, score, category)
    alert_type = alert_type_for_score(score["overall_score"])
    msg = await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=text, parse_mode=ParseMode.MARKDOWN,
                                 reply_markup=alert_keyboard(token_address, 0))
    alert_id = record_alert(token_address, alert_type, msg.message_id)
    await bot.edit_message_reply_markup(chat_id=TELEGRAM_CHAT_ID, message_id=msg.message_id,
                                        reply_markup=alert_keyboard(token_address, alert_id))


def _thesis_snippet(pair, score, category) -> str:
    full = generate_thesis(pair, score, category)
    return full.split(". ")[0].rstrip(".") + "."


async def send_batch_alert(bot: Bot, candidates: list):
    """
    Sends everything that cleared the bar in this scan pass, ranked
    best-first, as full detail messages back-to-back - not a summary that
    needs a tap to expand. Since button taps here take up to the scan
    interval to process, requiring a tap just to see details would defeat
    the whole point of batching.
    """
    candidates = sorted(candidates, key=lambda c: c["score"]["overall_score"], reverse=True)
    shown, overflow = candidates[:BATCH_DISPLAY_LIMIT], candidates[BATCH_DISPLAY_LIMIT:]

    await bot.send_message(
        chat_id=TELEGRAM_CHAT_ID,
        text=f"🔥 *{len(candidates)} opportunit{'y' if len(candidates)==1 else 'ies'} found this pass* "
             f"— ranked best first:",
        parse_mode=ParseMode.MARKDOWN,
    )

    for c in shown:
        pair, score, category, mint = c["pair"], c["score"], c["category"], c["mint"]
        alert_type = alert_type_for_score(score["overall_score"])
        msg = await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=format_alert(pair, score, category),
                                     parse_mode=ParseMode.MARKDOWN,
                                     reply_markup=alert_keyboard(mint, 0))
        alert_id = record_alert(mint, alert_type, msg.message_id)
        await bot.edit_message_reply_markup(chat_id=TELEGRAM_CHAT_ID, message_id=msg.message_id,
                                            reply_markup=alert_keyboard(mint, alert_id))
        await asyncio.sleep(0.4)  # stay well under Telegram's rate limit

    if overflow:
        for c in overflow:
            record_alert(c["mint"], alert_type_for_score(c["score"]["overall_score"]))
        await bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text=f"+{len(overflow)} more cleared the bar this pass — check 🔥 Top Opportunities in the menu.",
        )


# ============================== SCANNER ======================================

async def scan_once(bot: Bot):
    try:
        buckets = await pumpfun_candidates_by_stage()
    except Exception:
        logger.exception("Failed to fetch pump.fun candidates")
        return

    qualifying = []
    for category, coins in buckets.items():
        for coin in coins:
            mint = coin.get("mint")
            if not mint or has_alert(mint):
                continue
            try:
                pair, score = await analyze_token(mint, category)
            except Exception:
                logger.exception(f"Failed to analyze {mint}")
                continue
            if not pair or (pair.get("liquidity") or 0) < MIN_LIQUIDITY_USD:
                continue
            if score["overall_score"] >= REGULAR_ALERT_SCORE:
                qualifying.append({"mint": mint, "category": category, "pair": pair, "score": score})

    if qualifying:
        try:
            await send_batch_alert(bot, qualifying)
        except Exception:
            logger.exception("Failed to send batch alert")


# ============================== ENTRY POINT ==================================

async def run_once():
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        raise RuntimeError("TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID must be set as environment variables.")

    init_db()
    bot = Bot(token=TELEGRAM_BOT_TOKEN)

    last_offset = int(get_meta("telegram_offset", 0))
    updates = await bot.get_updates(offset=last_offset, timeout=0)
    for update in updates:
        try:
            await handle_update(bot, update)
        except Exception:
            logger.exception("Failed handling an update")
        last_offset = update.update_id + 1
    set_meta("telegram_offset", last_offset)

    await scan_once(bot)
    logger.info("Run complete.")


if __name__ == "__main__":
    asyncio.run(run_once())
