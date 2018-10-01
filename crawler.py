import io
import re
import time

import requests
from bs4 import BeautifulSoup
import pandas as pd

import multiprocessing
from db_manager import DB_manager

BASE_URL = 'https://finance.naver.com'

class Crawler:
    
    def __init__(self, n_process):
        self.n_process = n_process
        self.stock_df = self.get_stock_df()
        self.db = DB_manager()

    @staticmethod
    def get_stock_df():
        """
        현재 상장되어있는 종목 리스트를 df로 반환
        (https://woosa7.github.io/krx_stock_master/)
        """
        url = 'http://kind.krx.co.kr/corpgeneral/corpList.do'
        data = {
            'method':'download',
            'orderMode':'1',           # 정렬컬럼
            'orderStat':'D',           # 정렬 내림차순
            'searchType':'13',         # 검색유형: 상장법인
            'fiscalYearEnd':'all',     # 결산월: 전체
            'location':'all',          # 지역: 전체
        }

        r = requests.post(url, data=data)
        f = io.BytesIO(r.content)
        dfs = pd.read_html(f, header=0, parse_dates=['상장일'])
        df = dfs[0].copy()

        # 숫자를 앞자리가 0인 6자리 문자열로 변환
        df['종목코드'] = df['종목코드'].astype(str)
        df['종목코드'] = df['종목코드'].str.zfill(6)
        return df

    def fetch_by_page(self, code, page, event):
        if not event.is_set():
            print(BASE_URL+'/item/board.nhn?code='+code+'&page=%d'%page, flush=True)
            req = requests.get(BASE_URL+'/item/board.nhn?code='+code+'&page=%d'%page)
            page_soup = BeautifulSoup(req.text, 'html.parser')
            title_atags = page_soup.select('td.title > a')

            def fetch_by_post(title_atag):
                req = requests.get(BASE_URL+title_atag.get('href'))
                content_soup = BeautifulSoup(req.text, 'html.parser')

                date = content_soup.select_one('tr > th.gray03.p9.tah').text

                post_info = content_soup.select_one('tr > th:nth-of-type(2)')
                post_info = post_info.getText(',', strip=True).split(',')

                content = content_soup.select_one('#body')
                content = content.getText().replace(u'\xa0\r', '\n')
                content = content.replace('\r', '\n')

                href = title_atag.get('href')

                posts = {}
                posts['title'] = title_atag.get('title')
                posts['nid'] = int(re.search('(?<=nid=)[0-9]+', href)[0])
                posts['date'] = date
                posts['view'] = post_info[1]
                posts['agree'] = post_info[3]
                posts['disagree'] = post_info[5]
                posts['opinion'] = post_info[7]
                posts['content'] = content
                return posts

            pool = multiprocessing.pool.ThreadPool(20)
            posts = [pool.apply_async(fetch_by_post, args={title_atag: title_atag}) for title_atag in title_atags]
            pool.close()
            pool.join()
            posts = [post.get() for post in posts]

            # list of dict -> dict of list
            posts = {k: [dic[k] for dic in posts] for k in posts[0]}

            latest_date = self.db.latest_date.get(code, 0)
            if latest_date != 0:
                if min(posts['date']) > latest_date:
                    event.set()

            return posts


    def fetch_by_code(self, code):

        req = requests.get(BASE_URL+'/item/board.nhn?code='+code)
        page_soup = BeautifulSoup(req.text, 'html.parser')
        total_page_num = page_soup.select_one('tr > td.pgRR > a').get('href').split('=')[-1]
        total_page_num = int(total_page_num)

        pool = multiprocessing.Pool(self.n_process)
        m = multiprocessing.Manager()
        event = m.Event()

        posts_list = [pool.apply_async(self.fetch_by_page, args=(code, i, event)) for i in range(1, total_page_num+1)]
        pool.close()
        pool.join()
        posts_list = [res.get() for res in posts_list]

        df = pd.concat(list(map(pd.DataFrame, posts_list)))
        df.date = pd.to_datetime(df.date)
        df['index'] = range(len(df) - 1, -1, -1)
        df.sort_values(by=['date', 'index'], inplace=True)
        del df['index']
        df.set_index('date', inplace=True)
        df['opinion'].replace('의견없음', 0, inplace=True)

        return df

    def fetch_all(self):

        for i, code in enumerate(self.stock_df['종목코드']):
            print(code)
            t = time.time()
            df = self.fetch_by_code(code)
            print(time.time() - t)
            self.db.write(code, df)
            del df