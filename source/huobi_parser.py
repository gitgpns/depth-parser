from exchange_parser_abc import ExchangeParserABC


class HuobiParser(ExchangeParserABC):
    def __init__(self, depth_limit=10):
        super().__init__(depth_limit)

        self.exchange = 'huobi'

    def parse_row(self, row):
        try:
            row["ch"]

        except KeyError:
            return

        parsed_row = list()

        timestamp = self._timestamp_to_utc(row["ts"])
        pair = row["ch"].split('.')[1].split('-')
        if pair[0] != "UNI":
            return

        bids = self._parse_bids(row["tick"])
        asks = self._parse_asks(row["tick"])

        for bid in bids:
            parsed_row.append([self.exchange, timestamp, pair, 'bid', bid[0], bid[1]])

        for ask in asks:
            parsed_row.append([self.exchange, timestamp, pair, 'ask', ask[0], ask[1]])

        return parsed_row

    def _parse_bids(self, row):
        highest_bids = row['bids'][:self._depth_limit]
        parsed_bids = self._get_parsed_orders(highest_bids)

        return parsed_bids

    def _parse_asks(self, row):
        lowest_asks = row['asks'][:self._depth_limit]
        parsed_asks = self._get_parsed_orders(lowest_asks)

        return parsed_asks

    @staticmethod
    def _get_parsed_orders(orders):
        parsed_orders = list()

        for order in orders:
            price = float(order[0])
            volume = float(order[1])
            parsed_orders.append([price, volume])

        return parsed_orders
