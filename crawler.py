import requests
from bs4 import BeautifulSoup
import os
import io
import pandas as pd
from multiprocessing import Pool

BASE_URL = 'https://finance.naver.com'
DB_PATH = 'data'

if not os.path.exists(DB_PATH):
    os.mkdir(DB_PATH)

class Crawler:
    
    def __init__(self, n_process=1):
        self.n_process = n_process
        self.stock_df = self.get_stock_df()

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

    @staticmethod
    def fetch_by_page(code, page):
        posts = {'title':[], 'href':[], 'date':[], 'view':[], 'agree':[],
                 'disagree':[], 'opinion':[], 'content':[]}
        req = requests.get(BASE_URL+'/item/board.nhn?code='+code+'&page=%d'%page)
        page_soup = BeautifulSoup(req.text, 'html.parser')
        titles = page_soup.select('td.title > a')
        for title in titles:
            req = requests.get(BASE_URL+title.get('href'))
            content_soup = BeautifulSoup(req.text, 'html.parser')

            date = content_soup.select_one('tr > th.gray03.p9.tah').text

            post_info = content_soup.select_one('tr > th:nth-of-type(2)')
            post_info = post_info.getText(',', strip=True).split(',')

            content = content_soup.select_one('#body')
            content = content.getText().replace(u'\xa0\r', '\n')
            content = content.replace('\r', '\n')

            posts['title'].append(title.get('title'))
            posts['href'].append(title.get('href'))
            posts['date'].append(date)
            posts['view'].append(post_info[1])
            posts['agree'].append(post_info[3])
            posts['disagree'].append(post_info[5])
            posts['opinion'].append(post_info[7])
            posts['content'].append(content)

        return posts

    def fetch_by_code(self, code):
        req = requests.get(BASE_URL+'/item/board.nhn?code='+code)
        page_soup = BeautifulSoup(req.text, 'html.parser')
        total_page_num = page_soup.select_one('tr > td.pgRR > a').get('href').split('=')[-1]
        total_page_num = int(total_page_num)

        with Pool(processes=self.n_process) as p:
            posts_list = p.starmap(self.fetch_by_page, [(code, i) for i in range(1, total_page_num+1)])

        df = pd.concat(list(map(pd.DataFrame, posts_list)))
        df.date = pd.to_datetime(df.date)
        df['index'] = range(len(df) - 1, -1, -1)
        df.sort_values(by=['date', 'index'], inplace=True)
        df.set_index('index', inplace=True)
        df['opinion'].replace('의견없음', 0, inplace=True)

        return df

    def fetch_all(self):

        for code in self.stock_df['종목코드']:
            print(code)
            df = self.fetch_by_code(code)
            df.to_csv(os.path.join(DB_PATH, code+'.csv'))
            del df