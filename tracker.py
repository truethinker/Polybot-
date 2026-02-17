import os
import json
import time
import csv
import math
import threading
from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple, List

import requests
from websocket import WebSocketApp

GAMMA_BASE = "https://gamma-api.polymarket.com"  #  [oai_citation:4‡docs.polymarket.com](https://docs.polymarket.com/quickstart/reference/endpoints)
WS_BASE = "wss://ws-subscriptions-clob.polymarket.com"  #  [oai_citation:5‡docs.polymarket.com](https://docs.polymarket.com/quickstart/reference/endpoints)

# --- Config por ENV (Railway-friendly) ---
MARKET_QUERY = os.getenv("MARKET_QUERY", "bitcoin").lower()
CSV_PATH = os.getenv("CSV_PATH", "quotes_5m.csv")
SLOT_SECONDS = int(os.getenv("SLOT_SECONDS", "300"))  # 5 minutos
PING_SECONDS = int(os.getenv("PING_SECONDS", "10"))
VERBOSE = os.getenv("VERBOSE", "1") == "1"


@dataclass
class BestBidAsk:
    best_bid: Optional[float] = None
    best_ask: Optional[float] = None
    spread: Optional[float] = None
    ts_ms: Optional[int] = None

    def mid(self) -> Optional[float]:
        if self.best_bid is None or self.best_ask is None:
            return None
        return (self.best_bid + self.best_ask) / 2.0


@dataclass
class Candle:
    slot_start_ms: int
    # OHLC del mid
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    # Snapshot final bid/ask/spread
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


def floor_slot_start_ms(ts_ms: int, slot_seconds: int) -> int:
    return (ts_ms // (slot_seconds * 1000)) * (slot_seconds * 1000)


def gamma_find_market_assets(query: str) -> Tuple[str, List[str], List[str]]:
    """
    Devuelve (market_condition_id, asset_ids, outcomes) del primer market que matchee.
    Usa Gamma /markets para discovery. Base URL documentada.  [oai_citation:6‡docs.polymarket.com](https://docs.polymarket.com/quickstart/reference/endpoints)
    """
    # Gamma "GET /markets" (parámetros pueden variar; hacemos búsqueda simple por texto en client-side)
    url = f"{GAMMA_BASE}/markets"
    params = {"limit": 50, "active": "true"}
    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    markets = r.json()

    # Intento 1: match por question/title/slug conteniendo query
    def matches(m: dict) -> bool:
        hay = " ".join([
            str(m.get("question", "")),
            str(m.get("title", "")),
            str(m.get("slug", "")),
            str(m.get("description", "")),
        ]).lower()
        return query in hay

    candidates = [m for m in markets if matches(m)]
    if not candidates:
        raise RuntimeError(
            f"No encontré markets en Gamma con query='{query}'. "
            "Prueba un MARKET_QUERY más específico (slug o parte del título)."
        )

    m = candidates[0]

    # Campos típicos: "conditionId"/"condition_id" y outcomes/asset ids
    # Si tu payload difiere, imprime m y ajustamos en 1 minuto.
    condition_id = m.get("conditionId") or m.get("condition_id") or m.get("market")
    if not condition_id:
        raise RuntimeError("No pude extraer condition_id/market del payload de Gamma.")

    # asset ids / outcomes
    asset_ids = m.get("assets") or m.get("assets_ids") or m.get("assetIds") or m.get("tokenIds")
    outcomes = m.get("outcomes") or m.get("outcomeLabels") or m.get("outcome_labels")

    if not isinstance(asset_ids, list) or len(asset_ids) < 2:
        raise RuntimeError(f"No pude extraer 2 asset_ids del market. asset_ids={asset_ids}")

    if not isinstance(outcomes, list) or len(outcomes) < 2:
        # fallback: si no hay outcomes, ponemos placeholders
        outcomes = ["Up/Yes", "Down/No"]

    # Nos quedamos con los 2 primeros (Up/Down)
    return condition_id, [str(asset_ids[0]), str(asset_ids[1])], [str(outcomes[0]), str(outcomes[1])]


class FiveMinTracker:
    def __init__(self, asset_map: Dict[str, str], csv_path: str):
        self.asset_map = asset_map  # asset_id -> "UP"/"DOWN" (o "Yes"/"No")
        self.csv_path = csv_path

        self.latest: Dict[str, BestBidAsk] = {aid: BestBidAsk() for aid in asset_map.keys()}
        self.current_candles: Dict[str, Candle] = {}  # asset_id -> Candle
        self.last_flushed_slot_ms: Dict[str, int] = {}

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
                writer = csv.writer(f)
                writer.writerow(header)
        except FileExistsError:
            pass

    def on_best_bid_ask(self, asset_id: str, best_bid: float, best_ask: float, spread: float, ts_ms: int):
        bba = self.latest.get(asset_id)
        if bba is None:
            return

        bba.best_bid = best_bid
        bba.best_ask = best_ask
        bba.spread = spread
        bba.ts_ms = ts_ms

        mid = bba.mid()
        if mid is None:
            return

        slot_ms = floor_slot_start_ms(ts_ms, SLOT_SECONDS)

        candle = self.current_candles.get(asset_id)
        if candle is None or candle.slot_start_ms != slot_ms:
            # flush anterior si existe
            if candle is not None:
                self.flush_candle(asset_id, candle)
            candle = Candle(slot_start_ms=slot_ms)
            self.current_candles[asset_id] = candle

        candle.update(mid=mid, bid=best_bid, ask=best_ask, spread=spread)

        if VERBOSE:
            side = self.asset_map.get(asset_id, "?")
            print(f"[{side}] {asset_id[-6:]} bid={best_bid:.4f} ask={best_ask:.4f} mid={mid:.4f} spread={spread:.4f}")

    def flush_candle(self, asset_id: str, candle: Candle):
        # evita duplicados si reinicios
        last = self.last_flushed_slot_ms.get(asset_id)
        if last is not None and candle.slot_start_ms <= last:
            return

        side_label = self.asset_map.get(asset_id, "UNKNOWN")
        slot_iso = time.strftime(
            "%Y-%m-%dT%H:%M:%SZ",
            time.gmtime(candle.slot_start_ms / 1000.0)
        )

        row = [
            slot_iso,
            candle.slot_start_ms,
            side_label,
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
            print(f"FLUSH {side_label} slot={slot_iso} updates={candle.n_updates}")


class MarketWS:
    """
    Implementación mínima del Market Channel.
    En el quickstart, al abrir se envía:
      {"assets_ids": [...], "type": "market"}
    y luego puedes subscribe/unsubscribe con operation.  [oai_citation:7‡docs.polymarket.com](https://docs.polymarket.com/quickstart/websocket/WSS-Quickstart)
    """
    def __init__(self, asset_ids: List[str], on_message_fn):
        self.asset_ids = asset_ids
        self.on_message_fn = on_message_fn
        self.ws: Optional[WebSocketApp] = None

    def _ping_loop(self, ws):
        while True:
            try:
                ws.send("PING")
            except Exception:
                return
            time.sleep(PING_SECONDS)

    def _on_open(self, ws):
        ws.send(json.dumps({"assets_ids": self.asset_ids, "type": "market"}))  #  [oai_citation:8‡docs.polymarket.com](https://docs.polymarket.com/quickstart/websocket/WSS-Quickstart)
        threading.Thread(target=self._ping_loop, args=(ws,), daemon=True).start()
        if VERBOSE:
            print("WS open: subscribed to assets_ids")

    def _on_message(self, ws, message: str):
        # Los mensajes vienen como JSON con event_type, asset_id, etc.  [oai_citation:9‡docs.polymarket.com](https://docs.polymarket.com/developers/CLOB/websocket/market-channel)
        try:
            data = json.loads(message)
        except Exception:
            return

        # A veces pueden venir batches/listas según implementación; soportamos ambos.
        if isinstance(data, list):
            for item in data:
                self.on_message_fn(item)
        elif isinstance(data, dict):
            self.on_message_fn(data)

    def _on_error(self, ws, error):
        print("WS error:", error)

    def _on_close(self, ws, code, msg):
        print("WS closed:", code, msg)

    def run_forever(self):
        url = f"{WS_BASE}/ws/market"  # channel market  [oai_citation:10‡docs.polymarket.com](https://docs.polymarket.com/quickstart/reference/endpoints)
        self.ws = WebSocketApp(
            url,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
        )
        self.ws.run_forever(ping_interval=None)  # ping manual


def main():
    condition_id, asset_ids, outcomes = gamma_find_market_assets(MARKET_QUERY)
    # Mapeo “Up/Down” por outcomes. Si outcomes no vienen, serán placeholders.
    asset_map = {
        asset_ids[0]: outcomes[0],
        asset_ids[1]: outcomes[1],
    }

    print("Market (condition id):", condition_id)  # condition id sale en best_bid_ask como "market"  [oai_citation:11‡docs.polymarket.com](https://docs.polymarket.com/developers/CLOB/websocket/market-channel)
    print("Assets:", asset_map)
    print("CSV:", CSV_PATH)

    tracker = FiveMinTracker(asset_map=asset_map, csv_path=CSV_PATH)

    def handle_event(evt: dict):
        if evt.get("event_type") != "best_bid_ask":
            return

        # Estructura documentada: asset_id, best_bid, best_ask, spread, timestamp  [oai_citation:12‡docs.polymarket.com](https://docs.polymarket.com/developers/CLOB/websocket/market-channel)
        try:
            asset_id = str(evt["asset_id"])
            best_bid = float(evt["best_bid"])
            best_ask = float(evt["best_ask"])
            spread = float(evt["spread"])
            ts_ms = int(evt["timestamp"])
        except Exception:
            return

        if asset_id in asset_map:
            tracker.on_best_bid_ask(asset_id, best_bid, best_ask, spread, ts_ms)

    ws = MarketWS(asset_ids=asset_ids, on_message_fn=handle_event)

    # Loop con auto-reconnect simple
    while True:
        try:
            ws.run_forever()
        except Exception as e:
            print("WS crashed, reconnecting:", e)
            time.sleep(3)


if __name__ == "__main__":
    main()
