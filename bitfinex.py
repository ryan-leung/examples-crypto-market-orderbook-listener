'''
Bitfinex orderbook monitor
- With websocket API wss://api.bitfinex.com/ws/2
- The program subscribe to the websocket and monitor the updates
- Corresponding updates will update to nsq broker.
'''

from asyncio import Queue
from typing import List

import aiohttp
import orderbook
import ujson
from structlog import get_logger

import msg
from msg import Packer

'''Time now'''
time_now = orderbook.time_now

'''Logging Function'''
logger = get_logger(__name__)

class OrderBook(object):
    WSS_URL = "wss://api.bitfinex.com/ws/2"
    BROKER = "BITFINEX"

    def __init__(self, market_pairs: List, update_queue=None):
        self.pairs = market_pairs
        self.last_heartbeat = {k: time_now() for k in market_pairs}
        self.books = {k: orderbook.OrderBook() for k in market_pairs}
        self.chanId_map = dict()
        self.update_queue = update_queue if update_queue is not None else Queue()
        self.ws_session = None

    def get_sub_msg(self, pair):
        msg = {
            "event": "subscribe",
            "channel": "book",
            #"freq": "R0", #Adjust for real time "R0"
            "len": "25",
            "prec": "R0",
            'symbol': pair
        }
        msg_str = ujson.dumps(msg)
        logger.info("subscription", msg=msg_str)
        return msg_str


    def prase_msg(self, msg):
        if msg is None:
            return None
        elif msg[0] == "{":
            msg = ujson.loads(msg)
            if msg.get('event') == 'subscribed':
                pair = msg.get('pair')
                chanId = msg.get('chanId')
                self.chanId_map[chanId] = pair
                logger.info("subscription", pair=pair, chanid=chanId)
            return None
        elif msg[0] == "[":
            data = ujson.loads(msg)
            chanId = data[0]
            pair = self.chanId_map.get(chanId)
            if data[1] == 'hb':
                self.last_heartbeat[pair] = time_now()
                logger.debug("heartbeat", pair=pair, last_hb=self.last_heartbeat[pair])
                return None
            elif len(data[1]) != 3:
                for row in data[1]:
                    payload = self.update_book(pair, row)
                return payload
            else:
                row = data[1]
                payload = self.update_book(pair, row)
                return payload


    def update_book(self, pair, row):
        book = self.books.get(pair)
        book.update_timestamp()
        Id, price, amount = row
        if amount < 0:
            book_side = book.asks
            side = 'asks'
        elif amount > 0:
            book_side = book.bids
            side = 'bids'
        if price > 0:
            book_side[Id] = (price, abs(amount))
            action = 'append'
        else:
            book_side.pop(Id, None)
            action = 'delete'
        payload = {
            "timestamp": book.timestamp,
            "broker": self.BROKER,
            "symbol": pair,
            "action": action,
            "side": side,
            "Id": Id,
            "size": abs(amount) if price > 0 else amount,
            "price": price if price > 0 else 0,
        }
        book.update()
        return payload


    def publish_update(self, payload):
        packed = Packer.dumps(payload)
        self.update_queue.put_nowait(packed)
        return


    async def subscribe(self, websocket):
        for pair in self.pairs:
            await websocket.send_str(self.get_sub_msg(pair))


    async def start(self):
        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    self.ws_session = session
                    async with session.ws_connect(self.WSS_URL) as websocket:
                        '''Send subscription strings'''
                        await self.subscribe(websocket)
                        '''Get updates'''
                        while True:
                            msg = await websocket.receive()
                            try:
                                start = time_now()
                                payload = self.prase_msg(msg.data)
                                if payload:
                                    self.publish_update(payload)
                                    logger.debug("orderbook", runtime=(time_now() - start), **payload)
                            except Exception as e:
                                logger.debug("updates", error=e)
                                break
            except Exception as e:
                logger.debug("connection", error=e)


def main():
    import asyncio
    import uvloop
    import logging
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

    async def clear_queue(q):
        while True:
            q._queue.clear()
            q._finished.set()
            q._unfinished_tasks = 0
            await asyncio.sleep(10)

    market_pairs = [
        "BTCUSDT", "ETHUSDT", "LTCUSDT", "XRPUSDT", "EOSUSDT", "BCHUSDT", "IOTAUSDT", "NEOUSDT", "DASHUSDT",
        "ETHBTC", "XRPBTC", "LTCBTC", "EOSBTC", "BCHBTC", "IOTABTC",
        "EOSETH", "BCHETH", "IOTAETH", "NEOETH",
    ]

    bitfinex = OrderBook(market_pairs)

    coros = [
        bitfinex.start(),
        clear_queue(bitfinex.update_queue),
    ]

    '''asyncio loop'''
    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(asyncio.gather(*coros))

if __name__ == '__main__':
    main()
