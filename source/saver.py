import psycopg2


class SaverDB:
    table_name = 'parsed_depth_binance'
    dbname = 'oleg'
    user = 'oleg'
    fields = 'timestamp, pair, side, price, volume'

    def __init__(self):
        self._conn = psycopg2.connect(f"dbname={self.dbname} user={self.user}")
        self._cur = self._conn.cursor()

    def save_data(self, data):
        for row in data:
            self._insert_single_row(row)

        self._conn.commit()

    def _insert_single_row(self, row):
        values = f"'{row[0]}', '{row[1]}', '{row[2]}', {row[3]}, {row[4]}"
        query = f"INSERT INTO {self.table_name} ({self.fields}) VALUES ({values})"
        self._cur.execute(query)

    def end_connection(self):
        self._conn.close()
