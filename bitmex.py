'''
Bitmex orderbook monitor
- With websocket API wss://www.bitmex.com/realtime
- The program subscribe to the websocket and monitor the updates
- Corresponding updates will update to a queue.
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
    WSS_URL = "wss://www.bitmex.com/realtime"
    BROKER = "BITMEX" # Broker is always block letter

    def __init__(self, market_pairs: List, update_queue=None):
        self.pairs = market_pairs
        self.last_heartbeat = {k: time_now() for k in market_pairs}
        self.books = {k: orderbook.OrderBook() for k in market_pairs}
        self.chanId_map = dict()
        self.update_queue = update_queue if update_queue is not None else Queue()
        self.ws_session = None

    def get_sub_msg(self, pairs):
        msg = {
            "op": "subscribe",
            "args": ["orderBookL2:{}".format(v) for v in pairs]
        }
        msg_str = ujson.dumps(msg)
        logger.info("subscription", msg=msg_str)
        return msg_str

    def prase_msg(self, msg):
        msg = ujson.loads(msg)
        if msg.get('success'):
            logger.info("subscription", **msg)
        if msg.get('action'):
            action = msg.get('action')
            data = msg.get('data')
            logger.info("action", **msg)
            if len(data) > 0:
                if action == "partial":
                    return self.update_book('insert', data)
                elif action == "insert":
                    return self.update_book('insert', data)
                elif action == "delete":
                    return self.update_book('delete', data)
                elif action == "update":
                    return self.update_book('update', data)
        return

    def update_book(self, action, data):
        payloads = []
        for row in data:
            symbol = row.get('symbol')
            payload = {'symbol': symbol}
            book = self.books.get(symbol)
            Id  = row['id']
            side = row.get('side').lower()
            price = row.get('price', 0)
            amount = row.get('size', 0)
            if action == "insert":
                if side == "sell":
                    book.asks[Id] = (price, amount)
                elif side == "buy":
                    book.bids[Id] = (price, amount)
                logger.info("insert", **row)
            elif action == "delete":
                if side == "sell":
                    book.asks.pop(Id, None)
                elif side == "buy":
                    book.bids.pop(Id, None)
                logger.info("delete", **row)
            elif action == "update":
                amount = row["size"]
                if side == "sell":
                    v = book.asks.get(Id)
                    if v:
                        book.asks[Id] = (v[0], amount)
                if side == "buy":
                    v = book.bids.get(Id)
                    if v:
                        book.bids[Id] = (v[0], amount)
                logger.info("update", **row)
                payload = {
                    'topics': msg.get_topics(self.BROKER, symbol)
                }
                self.update_queue.put_nowait(payload)
            book.update_timestamp()
            book.update()
        return

    async def consume(self):
        while True:
            payload = await self.update_queue.get()
            await asyncio.sleep(0.1)
            self.update_queue.task_done()

    async def publish(self, n):
        for symbol, book in self.books.items():
            payload = {
                'topics': msg.get_topics(self.BROKER, symbol),
                'data': Packer.dumps(book.level)
            }
            self.update_queue.put_nowait(payload)
            logger.debug("publish", payload=payload)

    async def subscribe(self, websocket):
        await websocket.send_str(self.get_sub_msg(self.pairs))

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
                                self.prase_msg(msg.data)
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

    market_pairs = ["ETHZ18", "XRPZ18", "EOSZ18", "BCHZ18"]

    bitmex = OrderBook(market_pairs)

    coros = [
        bitmex.start(),
        bitmex.publish(1)
    ]

    '''asyncio loop'''
    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(asyncio.gather(*coros))

if __name__ == '__main__':
    main()
