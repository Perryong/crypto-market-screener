from pydantic import BaseModel
from typing import Optional
import math
from decimal import Decimal


class TradeConfig(BaseModel):
    symbol: str
    exchanges: list[str] = []
    minNotional: float = 0
    side: Optional[str] = None


class OrderbookConfig(BaseModel):
    symbol: str
    exchanges: list[str] = []
    tickSize: float = 0
    depth: int = 20


class LiquidationConfig(BaseModel):
    symbol: str
    exchanges: list[str] = []
    minNotional: float = 0


class CVDConfig(BaseModel):
    symbol: str
    exchanges: list[str] = []
    interval: str = "raw"
    timeRange: str = "1h"


class DOMConfig(BaseModel):
    symbol: str
    exchange: str
    tickSize: float
    timeRange: str = "1h"


class FootprintConfig(BaseModel):
    symbol: str
    exchange: str
    interval: str
    tickSize: float
    timeRange: str


class OrderbookHeatmapConfig(BaseModel):
    symbol: str
    exchange: str
    interval: str
    tickSize: float
    timeRange: str


class OrderbookStatsConfig(BaseModel):
    symbol: str
    exchanges: list[str] = []


class StreamSubscribe(BaseModel):
    type: str = "stream_subscribe"
    stream: str
    instanceId: Optional[str] = None
    config: dict


class StreamUpdate(BaseModel):
    type: str = "stream_update"
    stream: str
    instanceId: Optional[str] = None
    config: dict


class StreamUnsubscribe(BaseModel):
    type: str = "stream_unsubscribe"
    stream: str
    instanceId: Optional[str] = None


class StreamSubscribeBatch(BaseModel):
    type: str = "stream_subscribe_batch"
    subscriptions: list[dict]


class Trade(BaseModel):
    exchange: str
    symbol: str
    price: float
    qty: float
    quoteQty: float
    isBuyerMaker: bool
    timestamp: int


class PriceLevel(BaseModel):
    price: float
    qty: float


class Orderbook(BaseModel):
    exchange: str
    symbol: str
    timestamp: int
    bids: list[PriceLevel]
    asks: list[PriceLevel]


class Liquidation(BaseModel):
    exchange: str
    symbol: str
    side: str
    price: float
    qty: float
    quoteQty: float
    timestamp: int


def validate_subscription(stream, config, metadata):
    classes = {'kline': CVDConfig, 'trade': TradeConfig, 'orderbook': OrderbookConfig, 'liquidation': LiquidationConfig,
               'cvd': CVDConfig, 'dom': DOMConfig, 'footprint': FootprintConfig,
               'orderbook_heatmap': OrderbookHeatmapConfig, 'orderbook_stats': OrderbookStatsConfig}
    if not isinstance(config, dict): raise ValueError('Config must be an object')
    if stream == 'news': return {}
    if stream not in classes: raise ValueError('Unknown stream')
    config = {**config, 'symbol': str(config.get('symbol', '')).upper()}
    config.setdefault('exchange', 'binancef')
    config.setdefault('timeRange', '1h')
    config.setdefault('interval', '1m')
    result = classes[stream].model_validate(config).model_dump()
    if result['symbol'] not in metadata: raise ValueError('Unknown Binance symbol')
    if result.get('exchange', 'binancef') != 'binancef' or any(x != 'binancef' for x in result.get('exchanges', [])):
        raise ValueError('Only Binance Futures is supported')
    if result.get('interval', '1m') not in ('raw','30s','1m','5m','15m','30m','1h','4h'):
        raise ValueError('Invalid interval')
    if result.get('timeRange', '1h') not in ('15m','30m','1h','4h','12h','24h'):
        raise ValueError('Invalid time range')
    if result.get('side') not in (None, 'buy', 'sell'): raise ValueError('Invalid side')
    for key in ('tickSize', 'minNotional'):
        value = result.get(key, 0)
        if not math.isfinite(value) or value < 0: raise ValueError(f'Invalid {key}')
    if stream in ('dom','footprint','orderbook_heatmap') and result['tickSize'] <= 0:
        raise ValueError('Tick size must be positive')
    if stream in ('dom','footprint','orderbook_heatmap','orderbook') and result.get('tickSize', 0) > 0:
        tick = Decimal(str(result['tickSize']))
        base = Decimal(metadata[result['symbol']]['tickSize'])
        if tick < base or tick % base: raise ValueError('Tick must be a multiple of exchange tick size')
    if not 0 <= result.get('depth', 0) <= 1000: raise ValueError('Depth must be 0–1000')
    return result
