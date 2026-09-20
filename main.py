"""
CryExc Example Backend - FastAPI + DuckDB
Simple reference implementation for Binance Futures.
"""
import asyncio
import json
import logging
import math
import time
from contextlib import asynccontextmanager
from typing import Optional
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

from binance import BinanceClient, fetch_tickers_24hr, fetch_klines, fetch_exchange_info, fetch_exchange_metadata
from models import validate_subscription
from database import db
from news import news_client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SYMBOLS = ["BTCUSDT"]
metadata = {}

binance_client: Optional[BinanceClient] = None

TIME_RANGE_MS = {
    "15m": 15 * 60 * 1000,
    "30m": 30 * 60 * 1000,
    "1h": 60 * 60 * 1000,
    "4h": 4 * 60 * 60 * 1000,
    "12h": 12 * 60 * 60 * 1000,
    "24h": 24 * 60 * 60 * 1000,
}

INTERVAL_MS = {
    "raw": 1000,
    "30s": 30 * 1000,
    "1m": 60 * 1000,
    "5m": 5 * 60 * 1000,
    "15m": 15 * 60 * 1000,
    "30m": 30 * 60 * 1000,
    "1h": 60 * 60 * 1000,
    "4h": 4 * 60 * 60 * 1000,
}


class ConnectionManager:
    def __init__(self):
        self.active_connections: dict[WebSocket, dict] = {}

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections[websocket] = {"subscriptions": {}}

    def disconnect(self, websocket: WebSocket):
        self.active_connections.pop(websocket, None)

    def demanded_symbols(self):
        return {'BTCUSDT'} | {sub['config']['symbol'] for conn in self.active_connections.values()
                              for sub in conn['subscriptions'].values() if 'symbol' in sub['config']}

    def get_subscriptions(self, websocket: WebSocket) -> dict:
        conn = self.active_connections.get(websocket)
        return conn["subscriptions"] if conn else {}

    def subscribe(self, websocket: WebSocket, stream: str, config: dict,
                  instance_id: Optional[str] = None):
        subs = self.get_subscriptions(websocket)
        key = f"{stream}:{instance_id}" if instance_id else stream
        subs[key] = {"stream": stream, "config": config, "instanceId": instance_id}

    def unsubscribe(self, websocket: WebSocket, stream: str,
                    instance_id: Optional[str] = None):
        subs = self.get_subscriptions(websocket)
        key = f"{stream}:{instance_id}" if instance_id else stream
        subs.pop(key, None)

    def get_subscribers_for_stream(self, stream: str, symbol: str,
                                   exchange: str = "binancef") -> list[tuple[WebSocket, dict]]:
        result = []
        for ws, conn in self.active_connections.items():
            for key, sub in conn["subscriptions"].items():
                if sub["stream"] != stream:
                    continue
                config = sub["config"]
                cfg_symbol = config.get("symbol", "").upper()
                if cfg_symbol != symbol.upper():
                    continue
                cfg_exchanges = config.get("exchanges", [])
                cfg_exchange = config.get("exchange", "")
                if cfg_exchanges and exchange not in cfg_exchanges:
                    continue
                if cfg_exchange and cfg_exchange != exchange:
                    continue
                result.append((ws, sub))
        return result

    def get_all_subscribers_for_stream(self, stream: str) -> list[tuple[WebSocket, dict]]:
        result = []
        for ws, conn in self.active_connections.items():
            for key, sub in conn["subscriptions"].items():
                if sub["stream"] == stream:
                    result.append((ws, sub))
        return result


manager = ConnectionManager()


def compute_orderbook_stats(orderbook: dict) -> dict:
    """Compute orderbook statistics from raw orderbook."""
    bids = orderbook.get("bids", [])
    asks = orderbook.get("asks", [])

    if not bids or not asks:
        return None

    best_bid = bids[0]["price"] if bids else 0
    best_ask = asks[0]["price"] if asks else 0
    mid_price = (best_bid + best_ask) / 2 if best_bid and best_ask else 0
    spread = best_ask - best_bid if best_bid and best_ask else 0

    def sum_qty_within_pct(levels: list, ref_price: float, pct: float, is_bid: bool) -> float:
        total = 0
        for lvl in levels:
            price = lvl["price"]
            if is_bid:
                if price >= ref_price * (1 - pct / 100):
                    total += lvl["quantity"]
            else:
                if price <= ref_price * (1 + pct / 100):
                    total += lvl["quantity"]
        return total

    return {
        "exchange": orderbook["exchange"],
        "symbol": orderbook["symbol"],
        "timestamp": orderbook["timestamp"],
        "bestBid": best_bid,
        "bestAsk": best_ask,
        "midPrice": mid_price,
        "spread": spread,
        "bidQuantity_0_5pct": sum_qty_within_pct(bids, mid_price, 0.5, True),
        "askQuantity_0_5pct": sum_qty_within_pct(asks, mid_price, 0.5, False),
        "bidQuantity_2pct": sum_qty_within_pct(bids, mid_price, 2, True),
        "askQuantity_2pct": sum_qty_within_pct(asks, mid_price, 2, False),
        "bidQuantity_10pct": sum_qty_within_pct(bids, mid_price, 10, True),
        "askQuantity_10pct": sum_qty_within_pct(asks, mid_price, 10, False),
        "totalBidQuantity": sum(lvl["quantity"] for lvl in bids),
        "totalAskQuantity": sum(lvl["quantity"] for lvl in asks),
    }


def group_orderbook_by_tick(orderbook: dict, tick_size: float) -> dict:
    """Group orderbook levels by tick size."""
    if tick_size <= 0:
        return orderbook

    bids_grouped: dict[float, float] = {}
    asks_grouped: dict[float, float] = {}

    for lvl in orderbook.get("bids", []):
        price_bucket = math.floor(round(lvl["price"] / tick_size, 8)) * tick_size
        bids_grouped[price_bucket] = bids_grouped.get(price_bucket, 0) + lvl["quantity"]

    for lvl in orderbook.get("asks", []):
        price_bucket = math.ceil(round(lvl["price"] / tick_size, 8)) * tick_size
        asks_grouped[price_bucket] = asks_grouped.get(price_bucket, 0) + lvl["quantity"]

    return {
        "exchange": orderbook["exchange"],
        "symbol": orderbook["symbol"],
        "timestamp": orderbook["timestamp"],
        "bids": [{"price": p, "quantity": q} for p, q in sorted(bids_grouped.items(), reverse=True)],
        "asks": [{"price": p, "quantity": q} for p, q in sorted(asks_grouped.items())],
    }


async def broadcast_orderbook_stats():
    """Periodically broadcast orderbook stats to subscribers."""
    while True:
        await asyncio.sleep(0.5)
        if not binance_client:
            continue

        for symbol in list(binance_client.symbols):
            subscribers = manager.get_subscribers_for_stream("orderbook_stats", symbol)
            if not subscribers:
                continue

            ob = binance_client.get_orderbook(symbol)
            if not ob:
                continue

            stats = compute_orderbook_stats(ob)
            if not stats:
                continue

            for ws, sub in subscribers:
                try:
                    await ws.send_json({"type": "orderbook_stats", "data": stats})
                except Exception:
                    pass


async def broadcast_dom():
    """Periodically broadcast DOM data to subscribers."""
    while True:
        await asyncio.sleep(0.5)
        if not binance_client:
            continue

        for ws, sub in manager.get_all_subscribers_for_stream("dom"):
            config = sub["config"]
            symbol = config.get("symbol", "").upper()
            exchange = config.get("exchange", "binancef")
            tick_size = config.get("tickSize", 10)
            time_range = config.get("timeRange", "1h")
            time_range_ms = TIME_RANGE_MS.get(time_range, 60 * 60 * 1000)

            ob = binance_client.get_orderbook(symbol)
            if not ob:
                continue

            trade_levels, session_high, session_low, data_start = db.get_dom_levels(
                exchange, symbol, tick_size, time_range_ms
            )

            ob_grouped = group_orderbook_by_tick(ob, tick_size)
            ob_bids = {lvl["price"]: lvl["quantity"] for lvl in ob_grouped["bids"]}
            ob_asks = {lvl["price"]: lvl["quantity"] for lvl in ob_grouped["asks"]}

            # Build a merged levels dict - start with trade levels
            levels_map: dict[float, dict] = {}
            for lvl in trade_levels:
                levels_map[lvl["price"]] = lvl

            # Add orderbook bid levels (creating new levels if needed)
            for price, qty in ob_bids.items():
                if price in levels_map:
                    levels_map[price]["bid"] = qty
                else:
                    levels_map[price] = {
                        "price": price,
                        "bid": qty,
                        "ask": 0,
                        "sold": 0,
                        "bought": 0,
                        "delta": 0,
                        "volume": 0,
                    }

            # Add orderbook ask levels (creating new levels if needed)
            for price, qty in ob_asks.items():
                if price in levels_map:
                    levels_map[price]["ask"] = qty
                else:
                    levels_map[price] = {
                        "price": price,
                        "bid": 0,
                        "ask": qty,
                        "sold": 0,
                        "bought": 0,
                        "delta": 0,
                        "volume": 0,
                    }

            # Sort by price descending
            levels = sorted(levels_map.values(), key=lambda x: -x["price"])

            mid_price = (ob["bids"][0]["price"] + ob["asks"][0]["price"]) / 2 if ob["bids"] and ob["asks"] else 0

            dom_data = {
                "exchange": exchange,
                "symbol": symbol,
                "midPrice": mid_price,
                "timestamp": int(time.time() * 1000),
                "dataStartTime": data_start,
                "sessionHigh": session_high,
                "sessionLow": session_low,
                "levels": levels,
            }

            try:
                await ws.send_json({"type": "dom", "data": dom_data, "instanceId": sub.get('instanceId')})
            except Exception:
                pass


async def broadcast_footprint():
    """Periodically broadcast live footprint candle updates."""
    while True:
        await asyncio.sleep(0.5)  # 500ms for reasonable update rate

        for ws, sub in manager.get_all_subscribers_for_stream("footprint"):
            config = sub["config"]
            instance_id = sub.get("instanceId")
            symbol = config.get("symbol", "").upper()
            exchange = config.get("exchange", "binancef")
            interval = config.get("interval", "1m")
            tick_size = config.get("tickSize", 10)

            interval_ms = INTERVAL_MS.get(interval, 60 * 1000)
            now = int(time.time() * 1000)
            current_candle_start = (now // interval_ms) * interval_ms

            # Only query for the current candle (last 2 intervals to ensure we catch it)
            candles = db.get_footprint_candles(
                exchange, symbol, interval_ms, tick_size, interval_ms * 2
            )

            if not candles:
                continue

            # Find the current candle by timestamp
            current_candle = None
            for c in candles:
                if c["timestamp"] == current_candle_start:
                    current_candle = c
                    break

            if not current_candle:
                continue

            # Frontend expects: { instanceId: "...", candle: {...} } inside data
            data = {"candle": current_candle, "symbol": symbol}
            if instance_id:
                data["instanceId"] = instance_id

            msg = {"type": "footprint", "data": data}
            if instance_id:
                msg["instanceId"] = instance_id

            try:
                await ws.send_json(msg)
            except Exception:
                pass


async def store_orderbook_snapshots():
    """Periodically store orderbook snapshots for heatmap at 1-minute boundaries."""
    last_minute_boundary: dict[str, int] = {}
    tick_size = 10  # Default tick size for storage

    while True:
        await asyncio.sleep(0.5)  # Check frequently but only store at minute boundaries
        if not binance_client:
            continue

        now = int(time.time() * 1000)
        one_minute_ms = 60 * 1000
        current_minute = (now // one_minute_ms) * one_minute_ms

        # Store snapshots for all tracked symbols at minute boundaries
        for symbol in list(binance_client.symbols):
            tick_size = float(metadata.get(symbol, {}).get('tickSize', '0.1'))
            exchange = "binancef"
            cache_key = f"{symbol}:{exchange}:{tick_size}"
            last_boundary = last_minute_boundary.get(cache_key, 0)

            # Only store once per minute boundary
            if current_minute <= last_boundary:
                continue

            ob = binance_client.get_orderbook(symbol)
            if not ob:
                continue

            ob_grouped = group_orderbook_by_tick(ob, tick_size)

            levels = []
            for lvl in ob_grouped["bids"]:
                levels.append({"price": lvl["price"], "bid_qty": lvl["quantity"], "ask_qty": 0})
            for lvl in ob_grouped["asks"]:
                existing = next((l for l in levels if l["price"] == lvl["price"]), None)
                if existing:
                    existing["ask_qty"] = lvl["quantity"]
                else:
                    levels.append({"price": lvl["price"], "bid_qty": 0, "ask_qty": lvl["quantity"]})

            # Use the minute boundary as timestamp (aligned with candles)
            db.insert_orderbook_snapshot(exchange, symbol, current_minute, tick_size, levels)
            last_minute_boundary[cache_key] = current_minute

            # Broadcast live update to subscribers
            heatmap_subs = manager.get_all_subscribers_for_stream("orderbook_heatmap")
            for ws, sub in heatmap_subs:
                sub_config = sub["config"]
                if sub_config.get("symbol", "").upper() != symbol:
                    continue
                if sub_config.get("exchange", "binancef") != exchange:
                    continue

                # Sort bids descending, asks ascending (best price first)
                bids = sorted(
                    [{"price": l["price"], "quantity": l["bid_qty"]} for l in levels if l["bid_qty"] > 0],
                    key=lambda x: -x["price"]
                )
                asks = sorted(
                    [{"price": l["price"], "quantity": l["ask_qty"]} for l in levels if l["ask_qty"] > 0],
                    key=lambda x: x["price"]
                )
                live_snapshot = {
                    "timestamp": current_minute,
                    "bids": bids,
                    "asks": asks,
                }

                requested_tick = sub_config.get('tickSize', tick_size)
                grouped = group_orderbook_by_tick(ob, requested_tick)
                interval_ms = INTERVAL_MS.get(sub_config.get('interval', '1m'), 60000)
                live_snapshot = {'timestamp': current_minute // interval_ms * interval_ms, 'bids': grouped['bids'], 'asks': grouped['asks']}
                msg = {
                    "type": "orderbook_heatmap",
                    "data": {
                        "exchange": exchange,
                        "symbol": symbol,
                        "live": live_snapshot
                    }
                }
                if sub.get("instanceId"):
                    msg["instanceId"] = sub["instanceId"]

                try:
                    await ws.send_json(msg)
                except Exception:
                    pass


@asynccontextmanager
async def lifespan(app: FastAPI):
    global binance_client, metadata
    try:
        metadata = await fetch_exchange_metadata()
    except Exception as exc:
        logger.warning('Exchange metadata unavailable: %s', exc)
    binance_client = BinanceClient(SYMBOLS)

    async def on_trade(trade: dict):
        db.insert_trade(
            trade["exchange"], trade["symbol"], trade["price"],
            trade["quantity"], trade["quoteQty"], trade["isBuyerMaker"],
            trade["timestamp"]
        )

        for ws, sub in manager.get_subscribers_for_stream("trade", trade["symbol"]):
            config = sub["config"]
            min_notional = config.get("minNotional", 0)
            if trade["quoteQty"] < min_notional:
                continue
            side_filter = config.get("side")
            if side_filter:
                is_buy = not trade["isBuyerMaker"]
                if side_filter == "buy" and not is_buy:
                    continue
                if side_filter == "sell" and is_buy:
                    continue
            try:
                await ws.send_json({"type": "trade", "data": trade, "instanceId": sub.get('instanceId')})
            except Exception:
                pass

    async def on_depth(orderbook: dict):
        for ws, sub in manager.get_subscribers_for_stream("orderbook", orderbook["symbol"]):
            config = sub["config"]
            tick_size = config.get("tickSize", 0)
            depth = config.get("depth", 0)

            if tick_size > 0:
                ob = group_orderbook_by_tick(orderbook, tick_size)
            else:
                ob = orderbook.copy()
                ob["bids"] = list(orderbook["bids"])
                ob["asks"] = list(orderbook["asks"])

            if depth > 0:
                ob["bids"] = ob["bids"][:depth]
                ob["asks"] = ob["asks"][:depth]

            try:
                await ws.send_json({"type": "orderbook", "data": ob, "instanceId": sub.get('instanceId')})
            except Exception:
                pass

    async def on_liquidation(liq: dict):
        for ws, sub in manager.get_subscribers_for_stream("liquidation", liq["symbol"]):
            config = sub["config"]
            min_notional = config.get("minNotional", 0)
            if liq["quoteQty"] < min_notional:
                continue
            try:
                await ws.send_json({"type": "liquidation", "data": liq, "instanceId": sub.get('instanceId')})
            except Exception:
                pass

    binance_client.on_trade = on_trade
    binance_client.on_depth = on_depth
    binance_client.on_liquidation = on_liquidation

    async def on_kline(event):
        for ws, sub in manager.get_subscribers_for_stream('kline', event['symbol']):
            if sub['config'].get('interval') != event['interval']: continue
            try:
                await ws.send_json({'type':'kline', 'instanceId':sub.get('instanceId'), 'data':event})
            except Exception: pass

    binance_client.on_kline = on_kline

    async def on_news(news: dict):
        for ws, sub in manager.get_all_subscribers_for_stream("news"):
            try:
                await ws.send_json({"type": "news", "data": news})
            except Exception:
                pass

    news_client.on_news = on_news

    await binance_client.start()
    logger.info("Binance client started")

    await news_client.start()
    logger.info("News client started")

    tasks = [asyncio.create_task(coro()) for coro in (cleanup_task, broadcast_orderbook_stats,
        broadcast_dom, broadcast_footprint, store_orderbook_snapshots, reconcile_symbols, broadcast_cvd)]
    try:
        yield
    finally:
        for task in tasks: task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await binance_client.stop()
        await news_client.stop()
        db.close()


async def reconcile_symbols():
    unused = {}
    while True:
        try:
            wanted = manager.demanded_symbols()
            now = time.monotonic()
            for symbol in set(binance_client.symbols) - wanted:
                unused.setdefault(symbol, now)
            for symbol in wanted: unused.pop(symbol, None)
            keep = wanted | {s for s, since in unused.items() if now - since < 10}
            await binance_client.set_symbols(keep)
            unused = {s:t for s,t in unused.items() if s in keep}
        except Exception:
            logger.exception('Subscription reconciliation failed')
        await asyncio.sleep(1)


async def broadcast_cvd():
    while True:
        await asyncio.sleep(2)
        # ponytail: full rolling windows; incrementally aggregate if measured query load warrants it.
        cache = {}
        for ws, sub in manager.get_all_subscribers_for_stream('cvd'):
            config = sub['config']
            key = (config['symbol'], config.get('interval','1m'), config.get('timeRange','1h'))
            if key not in cache:
                cache[key] = db.get_cvd_historical('binancef', key[0], INTERVAL_MS[key[1]], TIME_RANGE_MS[key[2]])
            try:
                await ws.send_json({'type':'cvd_historical','symbol':key[0], 'instanceId':sub.get('instanceId'),
                                   'data':[{'exchange':'binancef', **p} for p in cache[key]]})
            except Exception: pass


async def cleanup_task():
    while True:
        await asyncio.sleep(3600)
        db.cleanup_old_trades()
        logger.info("Cleaned up old data")


app = FastAPI(title="Cryexc Terminal", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            try:
                if len(data) > 65536: raise ValueError('Message too large')
                msg = json.loads(data)
                if not isinstance(msg, dict): raise ValueError('Message must be an object')
                msg_type = msg.get('type')
                if msg_type == 'ping':
                    await websocket.send_json({'type':'pong'})
                elif msg_type in ('stream_subscribe', 'stream_update'):
                    await handle_subscribe(websocket, msg)
                elif msg_type == 'stream_subscribe_batch':
                    subs = msg.get('subscriptions')
                    if not isinstance(subs, list) or len(subs) > 32: raise ValueError('Invalid subscription batch')
                    for sub in subs:
                        if not isinstance(sub, dict): raise ValueError('Invalid subscription')
                        await handle_subscribe(websocket, sub)
                elif msg_type == 'stream_unsubscribe':
                    await handle_unsubscribe(websocket, msg)
                else: raise ValueError('Unknown message type')
            except (ValueError, TypeError, KeyError) as exc:
                await websocket.send_json({'type':'error', 'message': str(exc)[:300]})

    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
    finally:
        manager.disconnect(websocket)


async def handle_subscribe(websocket: WebSocket, msg: dict):
    stream = msg.get("stream")
    config = validate_subscription(stream, msg.get('config', {}), metadata)
    instance_id = msg.get("instanceId")
    if instance_id is not None and (not isinstance(instance_id, str) or len(instance_id) > 128):
        raise ValueError('Invalid instance ID')
    key = f'{stream}:{instance_id}' if instance_id else stream
    subs = manager.get_subscriptions(websocket)
    if key not in subs and len(subs) >= 32: raise ValueError('Too many subscriptions')

    manager.subscribe(websocket, stream, config, instance_id)

    response = {"type": "subscribed", "stream": stream, "config": config}
    if instance_id:
        response["instanceId"] = instance_id
    await websocket.send_json(response)

    if stream == "cvd":
        await send_cvd_historical(websocket, config, instance_id)
    elif stream == "footprint":
        await send_footprint_historical(websocket, config, instance_id)
    elif stream == "orderbook_heatmap":
        await send_orderbook_heatmap_historical(websocket, config, instance_id)
    elif stream == "news":
        await send_news_historical(websocket)

    logger.info(f"Subscribed to {stream}: {config}")


async def handle_update(websocket: WebSocket, msg: dict):
    await handle_subscribe(websocket, msg)


async def handle_unsubscribe(websocket: WebSocket, msg: dict):
    stream = msg.get("stream")
    instance_id = msg.get("instanceId")

    manager.unsubscribe(websocket, stream, instance_id)

    response = {"type": "unsubscribed", "stream": stream}
    if instance_id:
        response["instanceId"] = instance_id
    await websocket.send_json(response)


async def send_cvd_historical(websocket: WebSocket, config: dict, instance_id: Optional[str]):
    symbol = config.get("symbol", "BTCUSDT").upper()
    exchanges = config.get("exchanges", [])
    exchange = exchanges[0] if exchanges else "binancef"
    time_range = config.get("timeRange", "1h")
    interval = config.get("interval", "1m")

    time_range_ms = TIME_RANGE_MS.get(time_range, 60 * 60 * 1000)
    interval_ms = INTERVAL_MS.get(interval, 60 * 1000)

    points = db.get_cvd_historical(exchange, symbol, interval_ms, time_range_ms)

    # Frontend expects flat array with exchange in each point
    data = [{"exchange": exchange, **pt} for pt in points]

    msg = {"type": "cvd_historical", "data": data, "symbol": symbol}
    if instance_id:
        msg["instanceId"] = instance_id

    await websocket.send_json(msg)


async def send_footprint_historical(websocket: WebSocket, config: dict, instance_id: Optional[str]):
    symbol = config.get("symbol", "BTCUSDT").upper()
    exchange = config.get("exchange", "binancef")
    interval = config.get("interval", "1m")
    tick_size = config.get("tickSize", 10)
    time_range = config.get("timeRange", "1h")

    time_range_ms = TIME_RANGE_MS.get(time_range, 60 * 60 * 1000)
    interval_ms = INTERVAL_MS.get(interval, 60 * 1000)

    candles = db.get_footprint_candles(exchange, symbol, interval_ms, tick_size, time_range_ms)

    # Frontend expects: { instanceId: "...", candles: [...] } inside data
    data = {"candles": candles, "symbol": symbol}
    if instance_id:
        data["instanceId"] = instance_id

    msg = {"type": "footprint_historical", "data": data}
    if instance_id:
        msg["instanceId"] = instance_id

    await websocket.send_json(msg)


async def send_orderbook_heatmap_historical(websocket: WebSocket, config: dict, instance_id: Optional[str]):
    symbol = config.get("symbol", "BTCUSDT").upper()
    exchange = config.get("exchange", "binancef")
    interval = config.get("interval", "1m")
    tick_size = config.get("tickSize", 10)
    time_range = config.get("timeRange", "1h")

    time_range_ms = TIME_RANGE_MS.get(time_range, 60 * 60 * 1000)
    interval_ms = INTERVAL_MS.get(interval, 60 * 1000)

    snapshots = db.get_orderbook_heatmap_historical(exchange, symbol, tick_size, time_range_ms, interval_ms)

    logger.info(f"Sending {len(snapshots)} historical heatmap snapshots for {symbol}")
    if snapshots:
        first = snapshots[0]
        logger.info(f"First snapshot: ts={first['timestamp']}, bids={len(first['bids'])}, asks={len(first['asks'])}")
        if first['bids']:
            logger.info(f"First bid: {first['bids'][0]}")
        if first['asks']:
            logger.info(f"First ask: {first['asks'][0]}")

    # Frontend expects: { exchange, symbol, historical: [...] } format
    msg = {
        "type": "orderbook_heatmap",
        "data": {
            "exchange": exchange,
            "symbol": symbol,
            "historical": snapshots
        }
    }
    if instance_id:
        msg["instanceId"] = instance_id

    await websocket.send_json(msg)


async def send_news_historical(websocket: WebSocket):
    history = news_client.get_history()

    logger.info(f"Sending {len(history)} historical news items")

    msg = {"type": "news_historical", "data": history}
    await websocket.send_json(msg)


@app.get("/api/exchanges")
async def get_exchanges():
    global metadata
    try:
        metadata = await fetch_exchange_metadata()
        symbols = sorted(metadata)
    except Exception as exc:
        raise HTTPException(503, 'Exchange metadata unavailable') from exc

    return [{
        "name": "binancef",
        "symbols": symbols,
        "symbolDetails": metadata,
        "capabilities": {
            "trades": True,
            "orderbook": True,
            "liquidations": True,
            "marketStats": True,
            "orderbookHeatmap": True,
            "screener": True,
        }
    }]


@app.get("/api/features")
async def get_features():
    return {
        "pythMarkets": False,
        "walletCohorts": False,
        "options": False,
        "news": True,
    }


_market_stats_cache: dict = {"data": [], "timestamp": 0}
_market_stats_cache_ttl = 10 * 60 * 1000  # 10 minutes in ms

@app.get("/api/market-stats")
async def get_market_stats(symbol: Optional[str] = None):
    if symbol and symbol.upper() not in metadata: raise HTTPException(422, 'Unknown symbol')
    if not binance_client:
        return []
    return [stats for s in ([symbol.upper()] if symbol else list(binance_client.symbols))
            if (stats := binance_client.get_market_stats(s))]


_screener_cache = {'at': 0, 'data': []}

@app.get("/screener")
async def get_screener():
    try:
        if time.time() - _screener_cache['at'] > 10:
            data = await fetch_tickers_24hr()
            _screener_cache.update(at=time.time(), data=data)
        return _screener_cache['data']
    except Exception as e:
        logger.error(f"Error fetching tickers: {e}")
        raise HTTPException(502, 'Binance screener unavailable') from e


@app.get("/klines")
async def get_klines(
    symbol: str = Query(...),
    interval: str = Query(default="1m"),
    limit: int = Query(default=500, ge=1, le=1000),
):
    if symbol.upper() not in metadata: raise HTTPException(422, 'Unknown symbol')
    if interval not in ('1m','5m','15m','30m','1h','4h'): raise HTTPException(422, 'Invalid interval')
    try:
        return await fetch_klines(symbol, interval, limit)
    except Exception as e:
        logger.error(f"Error fetching klines: {e}")
        raise HTTPException(502, 'Binance candles unavailable') from e


@app.get('/api/status')
async def get_status():
    c = binance_client
    return {'serverTime': int(time.time()*1000), 'marketConnected': bool(c and 'market' in c.sockets),
            'depthConnected': bool(c and 'public' in c.sockets),
            'symbols': {s: {'lastTradeAt': c.last_trade.get(s, 0), 'lastDepthAt': c.last_depth.get(s, 0),
                            'lastMarkAt': c.last_mark.get(s, 0), 'bookReady': c.initialized.get(s, False)}
                        for s in c.symbols} if c else {},
            'news': {'connected': news_client.connected, 'lastReceivedAt': news_client.last_received}}


@app.get('/api/candles')
async def candle_seed(symbol: str, interval: str = '1m', limit: int = Query(300, ge=1, le=1000)):
    candles = await get_klines(symbol, interval, limit)
    return {'candles': candles, 'asOf': int(time.time()*1000)}


static_dir = Path(__file__).resolve().parent / 'static'
# check_dir=False allows API-only startup before the UI assets are installed.
app.mount('/static', StaticFiles(directory=static_dir, check_dir=False), name='static')

@app.get('/', include_in_schema=False)
async def root(): return RedirectResponse('/app')

app.mount('/app', StaticFiles(directory=static_dir, html=True), name='dashboard')


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8086)
