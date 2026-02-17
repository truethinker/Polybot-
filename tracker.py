import os
import json
import time
import csv
import threading
from dataclasses import dataclass
from typing import Dict, Optional, Tuple, List, Any

import requests
from websocket import WebSocketApp

# ---- Endpoints base ----
GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE = "https://clob.polymarket.com"
WS_MARKET_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

# ---- Config por ENV (Railway-friendly) ----
MARKET_QUERY = os.getenv("MARKET_QUERY", "bitcoin").strip()
MARKET_SLUG = os.getenv("MARKET_SLUG", "").strip()  # si lo pones, se usa preferentemente
CSV_PATH = os.getenv("CSV_PATH", "quotes_5m.csv")

SLOT_SECONDS = int(os.getenv("SLOT_SECONDS", "300"))  # 5 minutos
VERBOSE = os.getenv("VERBOSE", "1") == "1"

# Fallback polling si no llega best_bid_ask por WS
ENABLE_POLLING_FALLBACK = os.getenv("ENABLE_POLLING_FALLBACK", "1") == "1"
POLL_SECONDS = float(os.getenv("POLL_SECONDS", "2.0"))
NO_WS_DATA_TIMEOUT_SEC = int(os.getenv("NO_WS_DATA_TIMEOUT_SEC", "20"))

# Ping nativo del cliente WS
WS_PING_INTERVAL = int(os.getenv("WS_PING_INTERVAL", "20"))
WS_PING_TIMEOUT = int(os.getenv("WS_PING_TIMEOUT", "10"))

REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "20"))


# -------------------- Helpers de tiempo --------------------
def now_ms() -> int:
    return int(time.time() * 1000)


def floor_slot_start_ms(ts_ms: int, slot_seconds: int) -> int:
    return (ts_ms // (slot_seconds * 1000)) * (slot_seconds * 1000)


def slot_iso(slot_start_ms: int) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(slot_start_ms / 1000.0))


# -------------------- Modelos --------------------
@dataclass
class Candle:
    slot_start_ms: int
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    last_bid: Optional[float] = None
    last_ask: Optional[float] = None
    last_spread: Optional[float] = None
    n_updates: int = 0

    def update(self, mid: float, bid: float, ask: float, spread: float):
        if self.open is None:
            self.open = mid
            self.high = mid
            self.low = mid
        else:
            self.high = max(self.high, mid) if self.high is not None else mid
            self.low = min(self.low, mid) if self.low is not None else mid

        self.close = mid
        self.last_bid = bid
        self.last_ask = ask
        self.last_spread = spread
        self.n_updates += 1


class FiveMinTracker:
    def __init__(self, asset_map: Dict[str, str], csv_path: str):
        self.asset_map = asset_map  # asset_id -> label (Up/Down)
        self.csv_path = csv_path

        self.current_candles: Dict[str, Candle] = {}
        self.last_flushed_slot_ms: Dict[str, int] = {}

        self.last_any_update_ms: int = 0

        self._ensure_csv_header()

    def _ensure_csv_header(self):
        header = [
            "slot_start_iso",
            "slot_start_ms",
            "side_label",
            "asset_id",
            "open_mid",
            "high_mid",
            "low_mid",
            "close_mid",
            "last_bid",
            "last_ask",
            "last_spread",
            "n_updates",
        ]
        try:
            with open(self.csv_path, "x", newline="") as f:
                csv.writer(f).writerow(header)
        except FileExistsError:
            pass

    def on_quote(self, asset_id: str, best_bid: float, best_ask: float, ts_ms: int):
        if asset_id not in self.asset_map:
            return

        if best_bid <= 0 or best_ask <= 0:
            return
        if best_ask < best_bid:
            return

        spread = best_ask - best_bid
        mid = (best_bid + best_ask) / 2.0

        slot_ms = floor_slot_start_ms(ts_ms, SLOT_SECONDS)
        candle = self.current_candles.get(asset_id)

        if candle is None or candle.slot_start_ms != slot_ms:
            if candle is not None:
                self.flush_candle(asset_id, candle)
            candle = Candle(slot_start_ms=slot_ms)
            self.current_candles[asset_id] = candle

        candle.update(mid=mid, bid=best_bid, ask=best_ask, spread=spread)
        self.last_any_update_ms = now_ms()

        if VERBOSE:
            side = self.asset_map.get(asset_id, "?")
            print(f"[{side}] {asset_id[-6:]} bid={best_bid:.4f} ask={best_ask:.4f} mid={mid:.4f} spr={spread:.4f}")

    def flush_candle(self, asset_id: str, candle: Candle):
        last = self.last_flushed_slot_ms.get(asset_id)
        if last is not None and candle.slot_start_ms <= last:
            return

        label = self.asset_map.get(asset_id, "UNKNOWN")

        row = [
            slot_iso(candle.slot_start_ms),
            candle.slot_start_ms,
            label,
            asset_id,
            candle.open,
            candle.high,
            candle.low,
            candle.close,
            candle.last_bid,
            candle.last_ask,
            candle.last_spread,
            candle.n_updates,
        ]
        with open(self.csv_path, "a", newline="") as f:
            csv.writer(f).writerow(row)

        self.last_flushed_slot_ms[asset_id] = candle.slot_start_ms
        if VERBOSE:
            print(f"FLUSH {label} slot={slot_iso(candle.slot_start_ms)} updates={candle.n_updates}")


# -------------------- Gamma discovery robusto --------------------
def _as_list(payload: Any) -> List[Any]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        # algunos endpoints devuelven {"markets": [...]}
        for k in ("markets", "data", "results"):
            if k in payload and isinstance(payload[k], list):
                return payload[k]
    return []


def _safe_lower(x: Any) -> str:
    return str(x or "").lower()


def gamma_fetch_markets(limit: int = 200) -> List[dict]:
    url = f"{GAMMA_BASE}/markets"
    params = {"limit": limit, "active": "true"}
    r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    data = r.json()
    markets = _as_list(data)
    return [m for m in markets if isinstance(m, dict)]


def _extract_asset_ids_and_outcomes(m: dict) -> Tuple[List[str], List[str]]:
    """
    Intenta sacar 2 asset_ids y sus labels de múltiples formatos posibles.
    """
    # 1) campos directos habituales
    for key in ("assets", "assetIds", "assets_ids", "tokenIds", "clobTokenIds", "clob_token_ids"):
        v = m.get(key)
        if isinstance(v, list) and len(v) >= 2:
            asset_ids = [str(v[0]), str(v[1])]
            break
    else:
        asset_ids = []

    # 2) tokens/answers/outcomes objects
    if len(asset_ids) < 2:
        tokens = m.get("tokens") or m.get("outcomeTokens") or m.get("outcome_tokens")
        if isinstance(tokens, list) and len(tokens) >= 2:
            # token id puede venir con varios nombres
            def tok_id(t: dict) -> Optional[str]:
                for kk in ("token_id", "tokenId", "asset_id", "assetId", "id", "clobTokenId", "clob_token_id"):
                    if kk in t and t[kk] is not None:
                        return str(t[kk])
                return None

            ids = [tok_id(t) for t in tokens[:2]]
            if all(ids):
                asset_ids = [ids[0], ids[1]]

    # Outcomes/labels
    outcomes = m.get("outcomes") or m.get("outcomeLabels") or m.get("outcome_labels")
    if not (isinstance(outcomes, list) and len(outcomes) >= 2):
        # intentar desde tokens
        tokens = m.get("tokens") or m.get("outcomeTokens") or m.get("outcome_tokens")
        if isinstance(tokens, list) and len(tokens) >= 2:
            labels = []
            for t in tokens[:2]:
                if isinstance(t, dict):
                    labels.append(str(t.get("outcome") or t.get("label") or t.get("name") or "Outcome"))
                else:
                    labels.append("Outcome")
            outcomes = labels
        else:
            outcomes = ["Up/Yes", "Down/No"]

    outcomes = [str(outcomes[0]), str(outcomes[1])]
    return asset_ids, outcomes


def gamma_select_market(markets: List[dict], query: str, slug: str) -> dict:
    if slug:
        slug_l = slug.lower()
        for m in markets:
            if _safe_lower(m.get("slug")) == slug_l:
                return m
        raise RuntimeError(f"No encontré ningún market con slug EXACTO '{slug}' en Gamma.")

    q = query.lower()
    def score(m: dict) -> int:
        hay = " ".join([
            str(m.get("question", "")),
            str(m.get("title", "")),
            str(m.get("slug", "")),
            str(m.get("description", "")),
        ]).lower()
        # score simple: más matches => mejor
        return (q in hay) * 10 + hay.count(q)

    ranked = sorted(markets, key=score, reverse=True)
    if not ranked or score(ranked[0]) == 0:
        raise RuntimeError(
            f"No encontré markets en Gamma que contengan '{query}'. "
            "Ajusta MARKET_QUERY o usa MARKET_SLUG."
        )
    return ranked[0]


def gamma_find_market_assets(query: str, slug: str) -> Tuple[str, List[str], List[str]]:
    markets = gamma_fetch_markets(limit=200)
    if not markets:
        raise RuntimeError("Gamma devolvió 0 markets (o formato inesperado).")

    m = gamma_select_market(markets, query=query, slug=slug)

    condition_id = m.get("conditionId") or m.get("condition_id") or m.get("condition") or m.get("id")
    if condition_id is None:
        # no es crítico para trackeo, pero lo mostramos si existe
        condition_id = "UNKNOWN"

    asset_ids, outcomes = _extract_asset_ids_and_outcomes(m)
    if len(asset_ids) < 2:
        # Debug útil si vuelve a petar
        print("DEBUG selected market payload:", json.dumps(m, indent=2)[:2500])
        raise RuntimeError("No pude extraer 2 asset_ids del market seleccionado. Usa MARKET_SLUG.")

    # Si outcomes vienen raros, asigna Up/Down por texto
    o0, o1 = outcomes[0], outcomes[1]
    o0l, o1l = o0.lower(), o1.lower()
    if ("down" in o0l and "up" in o1l) or ("no" == o0l and "yes" == o1l):
        # swap para que 0 sea Up, 1 sea Down
        asset_ids = [asset_ids[1], asset_ids[0]]
        outcomes = [outcomes[1], outcomes[0]]

    return str(condition_id), [str(asset_ids[0]), str(asset_ids[1])], [str(outcomes[0]), str(outcomes[1])]


# -------------------- CLOB REST fallback (book) --------------------
def clob_get_best_bid_ask(asset_id: str) -> Optional[Tuple[float, float]]:
    """
    Fallback: consulta el book del token y saca top bid/ask.
    """
    url = f"{CLOB_BASE}/book"
    params = {"token_id": asset_id}
    try:
        r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()
    except Exception:
        return None

    bids = data.get("bids")
    asks = data.get("asks")

    def top_price(levels: Any, side: str) -> Optional[float]:
        if not isinstance(levels, list) or len(levels) == 0:
            return None
        first = levels[0]
        # formatos posibles: ["0.53","100"] o {"price":"0.53",...}
        if isinstance(first, list) and len(first) >= 1:
            try:
                return float(first[0])
            except Exception:
                return None
        if isinstance(first, dict):
            for k in ("price", "p"):
                if k in first:
                    try:
                        return float(first[k])
                    except Exception:
                        pass
        return None

    best_bid = top_price(bids, "bids")
    best_ask = top_price(asks, "asks")

    if best_bid is None or best_ask is None:
        return None
    return best_bid, best_ask


# -------------------- WebSocket Market Channel --------------------
class MarketWS:
    def __init__(self, asset_ids: List[str], on_event_fn):
        self.asset_ids = asset_ids
        self.on_event_fn = on_event_fn
        self.ws: Optional[WebSocketApp] = None

    def _on_open(self, ws):
        # mensaje de subscripción (quickstart style)
        ws.send(json.dumps({"assets_ids": self.asset_ids, "type": "market"}))
        print("WS open: subscribed to assets_ids:", self.asset_ids)

    def _on_message(self, ws, message: str):
        # pueden venir dict o list
        try:
            data = json.loads(message)
        except Exception:
            return

        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    self.on_event_fn(item)
        elif isinstance(data, dict):
            self.on_event_fn(data)

    def _on_error(self, ws, error):
        print("WS error:", error)

    def _on_close(self, ws, code, msg):
        print("WS closed:", code, msg)

    def run_forever(self):
        self.ws = WebSocketApp(
            WS_MARKET_URL,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
        )
        self.ws.run_forever(ping_interval=WS_PING_INTERVAL, ping_timeout=WS_PING_TIMEOUT)


# -------------------- Main --------------------
def main():
    condition_id, asset_ids, outcomes = gamma_find_market_assets(MARKET_QUERY, MARKET_SLUG)

    asset_map = {
        asset_ids[0]: outcomes[0],
        asset_ids[1]: outcomes[1],
    }

    print("Selected market condition/id:", condition_id)
    print("Assets:", asset_map)
    print("CSV:", CSV_PATH)
    print("WS:", WS_MARKET_URL)
    if MARKET_SLUG:
        print("Mode: MARKET_SLUG =", MARKET_SLUG)
    else:
        print("Mode: MARKET_QUERY =", MARKET_QUERY)

    tracker = FiveMinTracker(asset_map=asset_map, csv_path=CSV_PATH)

    # ---- handler de eventos WS ----
    def handle_event(evt: dict):
        # Caso ideal: event_type == best_bid_ask
        et = evt.get("event_type")

        if et == "best_bid_ask":
            try:
                asset_id = str(evt["asset_id"])
                best_bid = float(evt["best_bid"])
                best_ask = float(evt["best_ask"])
                ts_ms = int(evt.get("timestamp") or now_ms())
            except Exception:
                return
            tracker.on_quote(asset_id, best_bid, best_ask, ts_ms)
            return

        # Caso alternativo: llegan fields sin event_type (o con otro nombre)
        if "asset_id" in evt and "best_bid" in evt and "best_ask" in evt:
            try:
                asset_id = str(evt["asset_id"])
                best_bid = float(evt["best_bid"])
                best_ask = float(evt["best_ask"])
                ts_ms = int(evt.get("timestamp") or evt.get("ts") or now_ms())
            except Exception:
                return
            tracker.on_quote(asset_id, best_bid, best_ask, ts_ms)
            return

        # Ignora otros eventos (book, trades, etc.)

    ws = MarketWS(asset_ids=asset_ids, on_event_fn=handle_event)

    # ---- Thread fallback polling ----
    def polling_loop():
        if not ENABLE_POLLING_FALLBACK:
            return

        print(f"Polling fallback enabled: every {POLL_SECONDS}s if no WS data for {NO_WS_DATA_TIMEOUT_SEC}s")
        while True:
            try:
                # si WS está vivo y recibimos quotes recientes, no hacemos polling
                last = tracker.last_any_update_ms
                if last and (now_ms() - last) < (NO_WS_DATA_TIMEOUT_SEC * 1000):
                    time.sleep(POLL_SECONDS)
                    continue

                # polling a ambos assets
                for aid in asset_ids:
                    res = clob_get_best_bid_ask(aid)
                    if res is None:
                        continue
                    bid, ask = res
                    tracker.on_quote(aid, bid, ask, now_ms())

                time.sleep(POLL_SECONDS)
            except Exception as e:
                print("Polling loop error:", e)
                time.sleep(POLL_SECONDS)

    t = threading.Thread(target=polling_loop, daemon=True)
    t.start()

    # ---- Auto-reconnect WS ----
    while True:
        try:
            ws.run_forever()
        except Exception as e:
            print("WS crashed, reconnecting:", e)
            time.sleep(3)


if __name__ == "__main__":
    main()
