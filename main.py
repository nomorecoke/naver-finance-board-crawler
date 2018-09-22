from crawler import Crawler

def main():
    naver_crawler = Crawler(8)
    naver_crawler.fetch_all()

if __name__ == '__main__':
    main()
