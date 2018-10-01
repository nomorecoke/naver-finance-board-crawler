import os
import sqlite3

DB_DIR = 'data'
DB_FILENAME = 'naver_board.db'
DB_PATH = os.path.join(DB_DIR, DB_FILENAME)

if not os.path.exists(DB_DIR):
    os.mkdir(DB_DIR)


class DB_manager:
    def __init__(self):
        self.latest_date = self.get_latest_date_df()  # k: code, v: DB에 있는 최근 날짜
        print(self.latest_date)

    def write(self, code, df):
        latest_date = self.latest_date.get(code, 0)
        if latest_date != 0:
            # 이미 DB에 있는 데이터 제외
            df = df.loc[latest_date:]
            df = df.iloc[1:]

        with sqlite3.connect(DB_PATH) as con:
            df.to_sql(code, con, if_exists='append')

    @staticmethod
    def get_latest_date_df():
        with sqlite3.connect(DB_PATH) as con:
            cursor = con.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            db_code_list = cursor.fetchall()
            db_code_list = [code[0] for code in db_code_list]

            latest_date = {}
            for code in db_code_list:
                cursor.execute("SELECT date FROM '{}' ORDER BY date DESC LIMIT 1".format(code))
                latest_date[code] = cursor.fetchall()[0][0]

            return latest_date
