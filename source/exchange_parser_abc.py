import datetime
from abc import ABC, abstractmethod


class ExchangeParserABC(ABC):
    def __init__(self, depth_limit):
        self._depth_limit = depth_limit

    @abstractmethod
    def parse_row(self, row):
        pass

    @staticmethod
    def _timestamp_to_utc(timestamp):
        return datetime.datetime.fromtimestamp(int(timestamp / 1e3)).isoformat(sep=' ', timespec='seconds')
