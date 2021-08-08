from saver import SaverDB
from loader import LoaderDB
from binance_parser import BinanceParser
from huobi_parser import HuobiParser


import datetime


class DepthParser:
    def __init__(self):
        self._saver = SaverDB()
        self._loader = LoaderDB()
        self._huobi_parser = HuobiParser()
        self._binance_parser = BinanceParser()

    def parse_table(self):
        data = self._loader.load_data()

        while data:
            self._parse_batch(data)
            del data

            data = self._loader.load_data()

        self._loader.end_connection()
        self._saver.end_connection()

    def _parse_batch(self, batch):

        for row in batch:
            exchange_name = row[1]
            exchange_data = row[2]

            if exchange_name == 'binance':
                parsed_row = self._binance_parser.parse_row(exchange_data)

            elif exchange_name == 'huobi':
                parsed_row = self._huobi_parser.parse_row(exchange_data)

            else:
                raise ValueError(f"WRONG EXCHANGE NAME:{row[0]}")

            if parsed_row:
                self._saver.save_data(parsed_row)
