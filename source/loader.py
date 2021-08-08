import psycopg2


class LoaderDB:
    table_name = 'depth_binance'
    dbname = 'oleg'
    user = 'oleg'
    fields = 'id, exchange, extra'

    def __init__(self):
        self._limit = 10000
        self._conn = psycopg2.connect(f"dbname={self.dbname} user={self.user}")
        self._cur = self._conn.cursor()

        self._offset = 13550000

    def load_data(self):
        query = f"SELECT {self.fields} FROM {self.table_name} ORDER BY id ASC offset {self._offset} FETCH FIRST {self._limit} ROWS ONLY;"

        self._cur.execute(query)
        result = self._cur.fetchall()

        print(f'Successful data pull offset: {self._offset}')
        self._offset += self._limit

        return result

    def end_connection(self):
        self._conn.close()
