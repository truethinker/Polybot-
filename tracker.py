import os
import sys
import json
import time
import csv
import signal
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple
import requests
from websocket import WebSocketApp

# -------------------------
# Config
# -------------------------
GAMMA_BASE = os.getenv("GAMMA_BASE", "https://gamma-api.polymarket.com")
WS_BASE = os.getenv("WS_BASE", "wss://ws-subscriptions-clob.polymarket.com")  # per docs  [oai_citation:2‡Polymarket](https://docs.polymarket.com/quickstart/websocket/WSS-Quickstart)

MARKET_SLUG = os.getenv("MARKET_SLUG", "").strip()
SERIES_SLUG = os.getenv("SERIES_SLUG", "").strip()  # e.g. btc-up-or-down-5m
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "3"))

WRITE_CSV = os.getenv("WRITE_CSV", "0").strip() == "1"
CSV_PATH = os.getenv("CSV_PATH", "quotes.csv").strip()

# Websocket auth is NOT required to subscribe to market channel in many setups,
# but docs show auth object. We'll keep it optional.
API_KEY = os.getenv("CLOB_API_KEY", "").strip()
API_SECRET = os.getenv("CLOB_API_SECRET", "").strip()
API_PASSPHRASE = os.getenv("CLOB_API_PASSPHRASE", "").strip()

# -------------------------
# Helpers
# -------------------------
def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)

def parse_iso_z(s: str) -> dt.datetime:
    # Accept "2026-02-18T09:50:00Z"
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return dt.datetime.fromisoformat(s)

def http_get_json(url: str, params: Optional[dict] = None, timeout: int = 15) -> Any:
    r = requests.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()

def gamma_market_by_slug(slug: str) -> Dict[str, Any]:
    # Docs recommend /markets/slug/<slug>  [oai_citation:3‡Polymarket](https://docs.polymarket.com/developers/gamma-markets-api/fetch-markets-guide)
    url = f"{GAMMA_BASE}/markets/slug/{slug}"
    return http_get_json(url)

def gamma_find_active_market_in_series(series_slug: str, max_pages: int = 8, page_size: int = 100) -> Optional[Dict[str, Any]]:
    """
    Robust approach: scan newest active events and find one with matching seriesSlug.
    The Gamma docs recommend using /events with closed=false, descending order.  [oai_citation:4‡Polymarket](https://docs.polymarket.com/developers/gamma-markets-api/fetch-markets-guide)
    """
    for page in range(max_pages):
        offset = page * page_size
        params = {
            "order": "id",
            "ascending": "false",
            "closed": "false",
            "limit": str(page_size),
            "offset": str(offset),
        }
        events = http_get_json(f"{GAMMA_BASE}/events", params=params)
        if not isinstance(events, list) or len(events) == 0:
            return None

        for ev in events:
            if ev.get("seriesSlug") != series_slug:
                continue

            # events typically include "markets": [...]
            markets = ev.get("markets") or []
            for m in markets:
                # prefer live, accepting orders, orderbook enabled
                if m.get("closed") is True:
                    continue
                if m.get("acceptingOrders") is False:
                    continue
                if m.get("enableOrderBook") is False:
                    continue

                # also ensure endDate is in the future
                end_date = m.get("endDate")
                if end_date:
                    try:
                        if parse_iso_z(end_date) <= utc_now():
                            continue
                    except Exception:
                        pass

                return m

    return None

def extract_market_tokens_and_outcomes(market: Dict[str, Any]) -> Tuple[str, List[str], List[str]]:
    """
    Returns (conditionId, clobTokenIds[], outcomes[])
    """
    condition_id = market.get("conditionId", "")

    outcomes_raw = market.get("outcomes", "[]")
    if isinstance(outcomes_raw, str):
        outcomes = json.loads(outcomes_raw)
    else:
        outcomes = outcomes_raw

    token_ids_raw = market.get("clobTokenIds", "[]")
    if isinstance(token_ids_raw, str):
        token_ids = json.loads(token_ids_raw)
    else:
        token_ids = token_ids_raw

    if not isinstance(outcomes, list) or not isinstance(token_ids, list) or len(outcomes) != len(token_ids):
        raise RuntimeError("No pude extraer outcomes y clobTokenIds correctamente del payload del market.")

    return condition_id, token_ids, outcomes

def safe_print_market_header(market: Dict[str, Any]) -> None:
    print("\n================ MARKET ================")
    print("question:", market.get("question"))
    print("slug:", market.get("slug"))
    print("closed:", market.get("closed"))
    print("acceptingOrders:", market.get("acceptingOrders"))
    print("restricted:", market.get("restricted"))
    print("startTime:", (market.get("events") or [{}])[0].get("startTime") if isinstance(market.get("events"), list) else None)
    print("endDate:", market.get("endDate"))
    print("updatedAt:", market.get("updatedAt"))
    print("========================================\n")

# -------------------------
# CSV
# -------------------------
class CSVWriter:
    def __init__(self, path: str):
        self.path = path
        self._fp = None
        self._writer = None

    def open(self):
        exists = os.path.exists(self.path)
        self._fp = open(self.path, "a", newline="", encoding="utf-8")
        self._writer = csv.writer(self._fp)
        if not exists:
            self._writer.writerow(["ts_utc", "market_slug", "outcome", "token_id", "best_bid", "best_ask", "last_trade_price"])

    def write_row(self, row: List[Any]):
        if self._writer is None:
            return
        self._writer.writerow(row)
        self._fp.flush()

    def close(self):
        try:
            if self._fp:
                self._fp.close()
        except Exception:
            pass

# -------------------------
# Websocket market tracker
# -------------------------
class MarketWS:
    """
    Subscribes to MARKET_CHANNEL and tracks bestBid/bestAsk per tokenId.
    Websocket structure shown in docs.  [oai_citation:5‡Polymarket](https://docs.polymarket.com/quickstart/websocket/WSS-Quickstart)
    """
    MARKET_CHANNEL = "market"

    def __init__(self, ws_base: str, token_ids: List[str], token_to_outcome: Dict[str, str], market_slug: str, csvw: Optional[CSVWriter] = None):
        self.ws_base = ws_base.rstrip("/")
        self.token_ids = token_ids
        self.token_to_outcome = token_to_outcome
        self.market_slug = market_slug
        self.csvw = csvw

        self.state: Dict[str, Dict[str, Any]] = {tid: {} for tid in token_ids}
        self._stop = False

        # Optional auth object
        self.auth = None
        if API_KEY and API_SECRET and API_PASSPHRASE:
            self.auth = {"apiKey": API_KEY, "secret": API_SECRET, "passphrase": API_PASSPHRASE}

        # Per docs: connect to "{WS_BASE}/ws/market"  [oai_citation:6‡Polymarket](https://docs.polymarket.com/quickstart/websocket/WSS-Quickstart)
        self.ws = WebSocketApp(
            f"{self.ws_base}/ws/{self.MARKET_CHANNEL}",
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
            on_open=self.on_open,
        )

    def stop(self):
        self._stop = True
        try:
            self.ws.close()
        except Exception:
            pass

    def on_open(self, ws):
        # Subscribe message per docs: {"assets_ids":[...], "type":"market"}  [oai_citation:7‡Polymarket](https://docs.polymarket.com/quickstart/websocket/WSS-Quickstart)
        msg = {"assets_ids": self.token_ids, "type": self.MARKET_CHANNEL}
        if self.auth:
            msg["auth"] = self.auth
        ws.send(json.dumps(msg))
        # Start ping loop
        self._start_ping(ws)

    def _start_ping(self, ws):
        # Keepalive, per docs example uses "PING" every 10s  [oai_citation:8‡Polymarket](https://docs.polymarket.com/quickstart/websocket/WSS-Quickstart)
        def loop():
            while not self._stop:
                try:
                    ws.send("PING")
                except Exception:
                    return
                time.sleep(10)
        import threading
        threading.Thread(target=loop, daemon=True).start()

    def on_close(self, ws, close_status_code, close_msg):
        if self._stop:
            return
        print(f"[WS] closed: code={close_status_code}, msg={close_msg}")

    def on_error(self, ws, error):
        if self._stop:
            return
        print("[WS] error:", error)

    def _pretty(self, v: Any) -> str:
        if v is None:
            return "-"
        try:
            # normalize numeric-ish strings
            return str(v)
        except Exception:
            return "-"

    def _emit_line(self, token_id: str):
        s = self.state.get(token_id, {})
        outcome = self.token_to_outcome.get(token_id, token_id)

        best_bid = s.get("bestBid") or s.get("best_bid")
        best_ask = s.get("bestAsk") or s.get("best_ask")
        last_trade = s.get("lastTradePrice") or s.get("last_trade_price")

        ts = utc_now().isoformat()
        print(f"{ts} | {self.market_slug} | {outcome:<5} | bid={self._pretty(best_bid)} ask={self._pretty(best_ask)} last={self._pretty(last_trade)}")

        if self.csvw:
            self.csvw.write_row([ts, self.market_slug, outcome, token_id, best_bid, best_ask, last_trade])

    def on_message(self, ws, message: str):
    if self._stop:
        return

    # Siempre loguea algo (aunque sea ping/pong)
    if message in ("PONG", "PING"):
        print(f"[WS] {message}")
        return

    # Imprime el raw si no es JSON
    try:
        payload = json.loads(message)
    except Exception:
        print("[WS] raw(non-json):", message[:500])
        return

    # DEBUG: imprime el JSON entero (capado a 2000 chars)
    raw = json.dumps(payload, ensure_ascii=False)
    print("[WS] msg:", raw[:2000])

    # Intento normal de extraer token id
    def try_update_from_obj(obj: dict):
        token_id = obj.get("asset_id") or obj.get("assetId") or obj.get("token_id") or obj.get("tokenId") or obj.get("id")
        if token_id and str(token_id) in self.state:
            tid = str(token_id)
            self.state[tid].update(obj)
            self._emit_line(tid)
            return True
        return False

    if isinstance(payload, dict):
        # Caso 1: payload directo
        if try_update_from_obj(payload):
            return

        # Caso 2: payload tiene "data"
        d = payload.get("data")
        if isinstance(d, dict) and try_update_from_obj(d):
            return

        # Caso 3: payload tiene una lista en alguna key común
        for k in ("assets", "data", "updates", "items", "market", "markets"):
            v = payload.get(k)
            if isinstance(v, list):
                for item in v:
                    if isinstance(item, dict):
                        try_update_from_obj(item)
                return

    # Caso 4: payload es lista
    if isinstance(payload, list):
        for item in payload:
            if isinstance(item, dict):
                try_update_from_obj(item)

    def run_forever(self):
        self.ws.run_forever()

# -------------------------
# Main loop
# -------------------------
def pick_market() -> Dict[str, Any]:
    if MARKET_SLUG:
        m = gamma_market_by_slug(MARKET_SLUG)
        # If expired/closed, we try series fallback if SERIES_SLUG given or derivable
        closed = bool(m.get("closed"))
        end_date = m.get("endDate")
        expired = False
        if end_date:
            try:
                expired = parse_iso_z(end_date) <= utc_now()
            except Exception:
                expired = False

        if (closed or expired or m.get("acceptingOrders") is False) and not SERIES_SLUG:
            # Try deriving series slug from embedded event object if present
            evs = m.get("events")
            if isinstance(evs, list) and len(evs) > 0 and isinstance(evs[0], dict):
                derived = evs[0].get("seriesSlug")
                if isinstance(derived, str) and derived.strip():
                    os.environ["SERIES_SLUG"] = derived.strip()

        if (closed or expired or m.get("acceptingOrders") is False):
            series = os.getenv("SERIES_SLUG", "").strip()
            if series:
                print(f"[INFO] El market '{MARKET_SLUG}' está cerrado/expirado/no acepta órdenes. Buscando market activo en series '{series}'...")
                m2 = gamma_find_active_market_in_series(series)
                if m2:
                    return m2
                print("[WARN] No encontré un market activo en esa series. Salgo sin crashear.")
                sys.exit(0)
            else:
                print("[WARN] Market cerrado/expirado y no tengo SERIES_SLUG para buscar el siguiente. Salgo sin crashear.")
                sys.exit(0)

        return m

    # If no MARKET_SLUG provided, we must have SERIES_SLUG
    if not SERIES_SLUG:
        raise RuntimeError("Define MARKET_SLUG o SERIES_SLUG en variables de entorno.")
    m = gamma_find_active_market_in_series(SERIES_SLUG)
    if not m:
        raise RuntimeError(f"No encontré markets activos para SERIES_SLUG={SERIES_SLUG}")
    return m

def main():
    print("[BOOT] Polymarket 5m tracker (Up/Down) - tracking bid/ask via CLOB websocket")
    print("[BOOT] GAMMA_BASE:", GAMMA_BASE)
    print("[BOOT] WS_BASE:", WS_BASE)
    print("[BOOT] MARKET_SLUG:", MARKET_SLUG or "(none)")
    print("[BOOT] SERIES_SLUG:", SERIES_SLUG or "(none)")
    print("[BOOT] WRITE_CSV:", WRITE_CSV, "CSV_PATH:", CSV_PATH)

    csvw = CSVWriter(CSV_PATH) if WRITE_CSV else None
    if csvw:
        csvw.open()

    ws_runner: Optional[MarketWS] = None

    def shutdown(*_):
        print("\n[INFO] Shutting down...")
        try:
            if ws_runner:
                ws_runner.stop()
        except Exception:
            pass
        try:
            if csvw:
                csvw.close()
        except Exception:
            pass
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # Pick market (slug or series fallback)
    market = pick_market()
    safe_print_market_header(market)

    # Extract token IDs + outcomes
    condition_id, token_ids, outcomes = extract_market_tokens_and_outcomes(market)

    # Map token->outcome (e.g., Up/Down)
    token_to_outcome = {token_ids[i]: outcomes[i] for i in range(len(token_ids))}

    print("[INFO] conditionId:", condition_id)
    print("[INFO] outcomes:", outcomes)
    print("[INFO] token_ids:", token_ids)

    # Start websocket
    ws_runner = MarketWS(
        ws_base=WS_BASE,
        token_ids=token_ids,
        token_to_outcome=token_to_outcome,
        market_slug=market.get("slug", MARKET_SLUG),
        csvw=csvw,
    )

    while True:
        try:
            ws_runner.run_forever()
        except Exception as e:
            print("[WARN] WS loop error:", repr(e))
        # If WS drops, wait and reconnect
        time.sleep(POLL_SECONDS)

if __name__ == "__main__":
    main()
