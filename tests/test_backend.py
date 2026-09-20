import asyncio
import os
import unittest
from unittest.mock import AsyncMock, patch

os.environ['CRYEXC_DB_PATH'] = ':memory:'
from binance import BinanceClient


class IngestionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        timer=patch('binance.time.time', return_value=1)
        timer.start()
        self.addCleanup(timer.stop)

    async def test_stale_depth_is_unavailable_to_all_consumers(self):
        c=BinanceClient(['BTCUSDT'])
        c._apply_snapshot('BTCUSDT', {'lastUpdateId':100,'bids':[['99','1']],'asks':[['101','1']]})
        await c._handle_depth({'s':'BTCUSDT','U':99,'u':101,'pu':98,'b':[],'a':[],'E':1000})
        with patch('binance.time.time', return_value=20):
            self.assertIsNone(c.get_orderbook('BTCUSDT'))

    async def test_exchange_kline_preserves_authoritative_volume(self):
        c = BinanceClient(['BTCUSDT'])
        events=[]
        async def capture(event): events.append(event)
        c.on_kline=capture
        await c._handle_message('{"stream":"btcusdt@kline_1m","data":{"e":"kline","E":120100,"s":"BTCUSDT","k":{"t":120000,"i":"1m","o":"10","h":"12","l":"9","c":"11","v":"7"}}}')
        self.assertEqual(events[0]['candle']['volume'],7)
        self.assertEqual(events[0]['interval'],'1m')

    async def test_snapshot_bridge_replays_contiguous_updates_and_suppresses_gaps(self):
        c = BinanceClient(['BTCUSDT'])
        c.on_depth = AsyncMock()
        c._apply_snapshot('BTCUSDT', {'lastUpdateId': 100, 'bids': [['99', '1']], 'asks': [['101', '1']]})
        self.assertIsNone(c.get_orderbook('BTCUSDT'))
        for first, last, prev, qty in [(99, 102, 98, '2'), (103, 105, 102, '3')]:
            await c._handle_depth({'s': 'BTCUSDT', 'U': first, 'u': last, 'pu': prev,
                                   'b': [['99', qty]], 'a': [], 'E': 1000})
        self.assertEqual(c.get_orderbook('BTCUSDT')['bids'][0]['quantity'], 3)
        await c._handle_depth({'s': 'BTCUSDT', 'U': 99, 'u': 102, 'pu': 98, 'b': [['99', '8']], 'a': [], 'E': 999})
        self.assertEqual(c.get_orderbook('BTCUSDT')['bids'][0]['quantity'], 3)
        await c._handle_depth({'s': 'BTCUSDT', 'U': 108, 'u': 109, 'pu': 107, 'b': [], 'a': [], 'E': 1001})
        self.assertIsNone(c.get_orderbook('BTCUSDT'))

    async def test_buffer_before_snapshot_keeps_every_continuation(self):
        c = BinanceClient(['BTCUSDT'])
        for first, last, prev, price in [(99, 102, 98, '99'), (103, 105, 102, '98')]:
            await c._handle_depth({'s': 'BTCUSDT', 'U': first, 'u': last, 'pu': prev, 'b': [[price, '2']], 'a': [], 'E': 1000})
        c._apply_snapshot('BTCUSDT', {'lastUpdateId': 100, 'bids': [['97', '1']], 'asks': [['101', '1']]})
        self.assertEqual(len(c.get_orderbook('BTCUSDT')['bids']), 3)

    async def test_liquidation_notional_and_invalid_trade(self):
        c = BinanceClient(['BTCUSDT'])
        events = []
        async def capture(event): events.append(event)
        c.on_liquidation = capture
        c.on_trade = capture
        await c._handle_liquidation({'e': 'forceOrder', 'E': 1000, 'o': {'s': 'BTCUSDT', 'p': '10', 'q': '2', 'S': 'SELL'}})
        self.assertEqual(events[0]['quoteQty'], 20)
        await c._handle_trade({'e': 'trade', 's': 'BTCUSDT', 'p': 'NaN', 'q': '2', 'm': False, 'T': 1000})
        self.assertEqual(len(events), 1)


class DashboardBackendTests(unittest.IsolatedAsyncioTestCase):
    async def test_live_heatmap_respects_subscription_interval(self):
        import main
        c=BinanceClient(['BTCUSDT'])
        c.orderbooks['BTCUSDT']={'bids':{99:1},'asks':{101:1}}
        c.initialized['BTCUSDT']=True
        c.last_depth['BTCUSDT']=125000
        frames=[]
        class Socket:
            async def send_json(self, msg): frames.append(msg)
        ws=Socket()
        manager=main.ConnectionManager()
        manager.active_connections[ws]={'subscriptions':{}}
        manager.subscribe(ws,'orderbook_heatmap',{'symbol':'BTCUSDT','exchange':'binancef','tickSize':1,'interval':'5m'},'x')
        with patch.object(main,'binance_client',c),patch.object(main,'manager',manager),patch.object(main,'metadata',{'BTCUSDT':{'tickSize':'0.1'}}),patch('main.time.time',return_value=125),patch('main.asyncio.sleep',side_effect=[None,asyncio.CancelledError()]):
            with self.assertRaises(asyncio.CancelledError): await main.store_orderbook_snapshots()
        self.assertEqual(frames[0]['data']['live']['timestamp'],0)

    async def test_invalid_websocket_message_does_not_disconnect(self):
        import main
        from fastapi.testclient import TestClient
        with TestClient(main.app).websocket_connect('/ws') as ws:
            for payload in ('[]','{','{"type":"stream_subscribe","stream":"wat","config":{}}'):
                ws.send_text(payload)
                self.assertEqual(ws.receive_json()['type'],'error')
            ws.send_json({'type':'ping'})
            self.assertEqual(ws.receive_json()['type'],'pong')

    async def test_candle_seed_endpoint_and_http_errors(self):
        import main
        import httpx
        main.metadata = {'BTCUSDT': {'tickSize': '0.1'}}
        transport = httpx.ASGITransport(app=main.app)
        async with httpx.AsyncClient(transport=transport, base_url='http://test') as client:
            with patch('main.fetch_klines', AsyncMock(return_value=[{'timestamp':60000,'open':1,'high':2,'low':1,'close':2,'volume':3}])):
                response = await client.get('/api/candles?symbol=BTCUSDT&interval=1m')
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json()['candles'][0]['volume'], 3)
                self.assertGreater(response.json()['asOf'], 0)
            self.assertEqual((await client.get('/klines?symbol=NOPE')).status_code,422)
            self.assertEqual((await client.get('/klines?symbol=BTCUSDT&limit=0')).status_code,422)
            with patch('main.fetch_klines', AsyncMock(side_effect=RuntimeError('offline'))):
                self.assertEqual((await client.get('/klines?symbol=BTCUSDT')).status_code,502)

    async def test_validation_rejects_invalid_aggregates(self):
        from models import validate_subscription
        metadata = {'BTCUSDT': {'tickSize': '0.1'}}
        for tick in (0, -1, float('nan')):
            with self.assertRaises(ValueError):
                validate_subscription('dom', {'symbol': 'BTCUSDT', 'exchange': 'binancef', 'tickSize': tick}, metadata)
        for stream, config in [('unknown', {}), ('trade', {'symbol': 'NOPE'}), ('trade', {'symbol': 'BTCUSDT', 'minNotional': -1})]:
            with self.assertRaises(ValueError): validate_subscription(stream, config, metadata)
        self.assertEqual(validate_subscription('trade', {'symbol': 'btcusdt'}, metadata)['symbol'], 'BTCUSDT')

    async def test_low_price_keys_and_heatmap_resolution(self):
        from database import Database
        db = Database(':memory:')
        self.addCleanup(db.close)
        with patch('database.time.time', return_value=120):
            for price in (0.012, 0.013): db.insert_trade('binancef', 'X', price, 2, price*2, False, 110000)
            candles = db.get_footprint_candles('binancef', 'X', 60000, .001, 60000)
            self.assertEqual(len(candles[0]['footprint']), 2)
            self.assertEqual(candles[0]['volume'], 4)
            db.insert_orderbook_snapshot('binancef', 'X', 60000, .001, [{'price': .012, 'bid_qty': 2}])
            db.insert_orderbook_snapshot('binancef', 'X', 60000, .01, [{'price': .01, 'bid_qty': 9}])
            snaps = db.get_orderbook_heatmap_historical('binancef', 'X', .002, 120000, 60000)
            self.assertEqual(sum(x['quantity'] for x in snaps[0]['bids']), 2)

    async def test_cvd_replacement_rolling_window(self):
        from database import Database
        db = Database(':memory:')
        self.addCleanup(db.close)
        with patch('database.time.time', return_value=120):
            db.insert_trade('binancef', 'BTCUSDT', 10, 2, 20, False, 110000)
            db.insert_trade('binancef', 'BTCUSDT', 5, 1, 5, True, 111000)
            self.assertEqual(db.get_cvd_historical('binancef', 'BTCUSDT', 60000, 60000)[-1]['value'], 15)
            db.insert_trade('binancef', 'BTCUSDT', 3, 1, 3, True, 112000)
            self.assertEqual(db.get_cvd_historical('binancef', 'BTCUSDT', 60000, 60000)[-1]['value'], 12)
        with patch('database.time.time', return_value=180):
            self.assertEqual(db.get_cvd_historical('binancef', 'BTCUSDT', 60000, 60000), [])

    async def test_two_clients_keep_shared_symbol(self):
        from main import ConnectionManager
        m = ConnectionManager()
        a, b = object(), object()
        m.active_connections = {a: {'subscriptions': {}}, b: {'subscriptions': {}}}
        m.subscribe(a, 'trade', {'symbol': 'ETHUSDT'})
        m.subscribe(b, 'trade', {'symbol': 'ETHUSDT'})
        m.subscribe(a, 'trade', {'symbol': 'BTCUSDT'})
        self.assertEqual(m.demanded_symbols(), {'BTCUSDT', 'ETHUSDT'})
        m.disconnect(b)
        self.assertEqual(m.demanded_symbols(), {'BTCUSDT'})

if __name__ == '__main__': unittest.main()
