import datetime
import pickle
from pydantic import BaseModel

import numpy as np

import ujson

time_now = lambda: int(round(datetime.datetime.utcnow().timestamp() * 1e6)) #microsecond

class OrderBook(object):
    '''Basic orderbook datatype for storing the orderbook in price level'''
    def __init__(self):
        self.timestamp = time_now()
        self.bids = dict() # with order ID
        self.asks = dict() # with order ID
        self._bids = np.array([[0,0]]) # in price level
        self._asks = np.array([[0,0]]) # in price level
        self.level = None
        self.empty = True

    def __str__(self):
        return ujson.dumps({
            'ts':self.timestamp,
            'bids':self._bids,
            'asks':self._asks,
            'stats':self.stats
        }, double_precision=9)

    def __repr__(self):
            return self.__str__()

    def update_timestamp(self):
        self.timestamp = time_now()

    def to_dict(self):
        return {'timestamp':self.timestamp,
            'bids':self._bids,
            'asks':self._asks
        }

    def sort(self):
        '''Sort the orderbook'''
        self._asks = self._asks[self._asks[:,0].argsort()]        # Asks from small to large
        self._bids = self._bids[self._bids[:,0].argsort()][::-1]  # Bids from large to small
        return self

    def update(self):
        if len(self.asks) > 0:
            self._asks = np.array([r for _, r in self.asks.items()])
        if len(self.bids) > 0:
            self._bids = np.array([r for _, r in self.bids.items()])
        if len(self.asks) > 0 and len(self.bids) > 0:
            self.sort()
            self.level = self.get_level(5)
            self.empty = False

    def get_level(self, n):
        if self.empty:
            return dict()
        asks = self._asks[:n]
        bids = self._bids[:n]
        asks_total = asks[:,0] * asks[:,1] # price, amount
        asks_total_amount = asks[:,1].sum()
        bids_total = bids[:,0] * bids[:,1] # price, amount
        bids_total_amount = bids[:,1].sum()
        bids_average_price = bids_total.sum() / bids_total_amount if bids_total_amount > 0 else 0
        asks_average_price = asks_total.sum() / asks_total_amount if asks_total_amount > 0 else 0

        return {
            'bids':{'total': bids_total.sum(), 'total_amount':bids_total_amount, 'average_price':bids_average_price, 'level':n},
            'asks':{'total': asks_total.sum(), 'total_amount':asks_total_amount, 'average_price':asks_average_price, 'level':n},
        }