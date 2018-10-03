import os
import sqlite3
import pandas as pd

DB_DIR = 'data'
DB_FILENAME = 'naver_board.db'
DB_PATH = os.path.join(DB_DIR, DB_FILENAME)

if not os.path.exists(DB_DIR):
    os.mkdir(DB_DIR)


class DB_manager:
    def __init__(self):
        self.latest_nid = self.get_latest_nid()    # k: code, v: DB에 있는 최근 nid(게시글 고유id)
        self.latest_date = self.get_latest_date()  # k: code, v: DB에 있는 최근 date

    def write(self, code, df):
        """
        :param code: 종목코드
        :param df: index가 nid인 df
        """
        latest_nid = self.latest_nid.get(code, 0)
        if latest_nid != 0:
            # 이미 DB에 있는 데이터 제외
            df = df.loc[latest_nid:]
            from_1 = int(latest_nid in df.index)  # DB의 latest_nid가 받아온 데이터에는 있는지 여부.
            df = df.iloc[from_1:]

        with sqlite3.connect(DB_PATH) as con:
            df.to_sql(code, con, if_exists='append')

    @staticmethod
    def get_latest_nid():
        """
        DB에 저장된 종목들의 가장 최신 글의 nid를 가져오는 메소드.
        :return: dict {k: code, v: latest posted date}
        """
        with sqlite3.connect(DB_PATH) as con:
            cursor = con.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            db_code_list = cursor.fetchall()
            db_code_list = [code[0] for code in db_code_list]

            latest_nid = {}
            for code in db_code_list:
                cursor.execute("SELECT nid FROM '{}' ORDER BY nid DESC LIMIT 1".format(code))
                latest_nid[code] = cursor.fetchall()[0][0]

            return latest_nid

    @staticmethod
    def get_latest_date():
        """
        DB에 저장된 종목들의 가장 최신 글의 날짜를 가져오는 메소드.
        :return: dict {k: code, v: latest posted date}
        """
        with sqlite3.connect(DB_PATH) as con:
            cursor = con.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            db_code_list = cursor.fetchall()
            db_code_list = [code[0] for code in db_code_list]

            latest_date = {}
            for code in db_code_list:
                cursor.execute("SELECT date FROM '{}' ORDER BY date DESC LIMIT 1".format(code))
                latest_date[code] = pd.to_datetime(cursor.fetchall()[0][0])

            return latest_date