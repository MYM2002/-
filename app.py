import requests
from lxml import etree
from queue import Empty
import re
import csv
import time
import random
import os
from urllib.parse import urljoin
import concurrent.futures
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import logging
import threading
from queue import Queue
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import psutil
import mysql.connector
from mysql.connector import Error
from flask import Flask, render_template, jsonify, request
import json
from datetime import datetime

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('douban_spider.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 数据目录
DATA_DIR = 'douban_data'
os.makedirs(DATA_DIR, exist_ok=True)

app = Flask(__name__)
spider = None
app_status = {
    'is_running': False,
    'processed': 0,
    'total': 0,
    'queue_size': 0,
    'crawled_count': 0,
    'status_text': '准备就绪',
    'last_update': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    'error_count': 0,
    'active_threads': 0,
    'start_time': None,
    'end_time': None,
    'current_movie': '无'
}

class DoubanStealthSpider:
    def __init__(self):
        self.thread_local_db = threading.local()
        self.movies_file = os.path.join(DATA_DIR, 'movies.csv')
        self.comments_file = os.path.join(DATA_DIR, 'short_comments.csv')
        self.reviews_file = os.path.join(DATA_DIR, 'reviews.csv')
        self._init_csv_files()
        self.crawled_movies = set()
        self.movie_queue = Queue(maxsize=200)
        
        # 添加初始种子
        initial_seeds = [
            '1292052', '1291546', '1292720', '1291561', '1292722',
            '3541415', '25662329', '26387939', '3011091', '3793023',
            '26752088', '27622447', '1291560', '1291841', '1309046',
            '1292001', '1295644', '1307914', '1292063', '1295124'
        ]
        
        for movie_id in initial_seeds:
            self.movie_queue.put(movie_id)
        
        self.proxies = None
        self.logged_in = False
        self.lock = threading.Lock()
        self.db_lock = threading.Lock()
        self.driver_pool = {}
        self.local = threading.local()
        self.request_count = 0
        self.last_cleanup = time.time()
        
        # 数据库连接配置
        self.db_config = {
            'host': 'localhost',
            'user': 'root',
            'password': 'mym2003222',
            'database': 'douban_movie_db',
            'charset': 'utf8mb4'
        }
        
        self.conn = None
        self.cursor = None
        self.create_db_connection()
        self.init_db_tables()
        
        # 状态变量
        self.processed = 0
        self.total = 0
        self.stop_requested = False

    def get_thread_db(self):
        if not hasattr(self.thread_local_db, "conn") or not self.thread_local_db.conn.is_connected():
            self.thread_local_db.conn = mysql.connector.connect(**self.db_config)
            self.thread_local_db.cursor = self.thread_local_db.conn.cursor()
        return self.thread_local_db.conn, self.thread_local_db.cursor
    
    def create_db_connection(self):
        try:
            self.conn = mysql.connector.connect(**self.db_config)
            self.cursor = self.conn.cursor()
            logger.info("数据库连接成功")
        except Error as e:
            logger.error(f"数据库连接失败: {e}")
            self.conn = None
            self.cursor = None
    
    def reconnect_db(self):
        if self.conn and self.conn.is_connected():
            return True
        
        logger.warning("尝试重新连接数据库...")
        try:
            self.conn = mysql.connector.connect(**self.db_config)
            self.cursor = self.conn.cursor()
            logger.info("数据库重新连接成功")
            return True
        except Error as e:
            logger.error(f"数据库重新连接失败: {e}")
            return False
    
    def init_db_tables(self):
        if not self.conn:
            return
        
        try:
            self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS movies (
                id INT AUTO_INCREMENT PRIMARY KEY,
                movie_id VARCHAR(20) NOT NULL,
                chinese_name VARCHAR(255) NOT NULL,
                english_name VARCHAR(255),
                poster_url VARCHAR(500),
                summary TEXT,
                directors TEXT,
                screenwriters TEXT,
                actors TEXT,
                genres TEXT,
                countries VARCHAR(255),
                languages VARCHAR(255),
                release_date VARCHAR(255),
                runtime INT,
                imdb_url VARCHAR(255),
                alias TEXT,
                douban_rating FLOAT,
                rating_count INT,
                crawl_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY unique_movie_id (movie_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)
            
            self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS short_comments (
                id INT AUTO_INCREMENT PRIMARY KEY,
                movie_id VARCHAR(20) NOT NULL,
                user_nickname VARCHAR(100) NOT NULL,
                comment_time VARCHAR(50),
                content TEXT NOT NULL,
                useful_count INT DEFAULT 0,
                crawl_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_movie_id (movie_id),
                INDEX idx_comment_time (comment_time),
                FOREIGN KEY (movie_id) REFERENCES movies(movie_id) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)
            
            self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS reviews (
                id INT AUTO_INCREMENT PRIMARY KEY,
                movie_id VARCHAR(20) NOT NULL,
                user_nickname VARCHAR(100) NOT NULL,
                review_time VARCHAR(50),
                title VARCHAR(500) NOT NULL,
                content TEXT NOT NULL,
                useful_count INT DEFAULT 0,
                useless_count INT DEFAULT 0,
                share_count INT DEFAULT 0,
                response_count INT DEFAULT 0,
                crawl_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_movie_id (movie_id),
                INDEX idx_review_time (review_time),
                FOREIGN KEY (movie_id) REFERENCES movies(movie_id) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)
            
            self.conn.commit()
            logger.info("数据库表初始化完成")
        except Error as e:
            logger.error(f"数据库表初始化失败: {e}")
    
    def get_driver(self, thread_id):
        if thread_id not in self.driver_pool:
            driver = self._init_selenium_driver()
            if driver:
                self.driver_pool[thread_id] = driver
                logger.info(f"线程 {thread_id} 创建了新的浏览器实例")
            return driver
        return self.driver_pool[thread_id]
    
    def _init_selenium_driver(self):
        options = webdriver.ChromeOptions()
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        options.add_argument('--disable-infobars')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-gpu')
        options.add_argument('--start-maximized')
        options.add_argument('--disable-software-rasterizer')
        options.add_argument('--disable-extensions')
        options.add_argument('--disable-logging')
        options.add_argument('--log-level=3')
        
        desktop_user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
        options.add_argument(f'user-agent={desktop_user_agent}')
        options.add_experimental_option("mobileEmulation", {"enabled": False})
        
        try:
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=options)
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            logger.info("Selenium驱动初始化成功")
            return driver
        except Exception as e:
            logger.error(f"初始化Selenium驱动失败: {str(e)}")
            return None
    
    def manual_login(self, driver):
        if not driver:
            logger.error("Selenium驱动未初始化，无法登录")
            return False
        
        try:
            driver.delete_all_cookies()
            driver.get('https://accounts.douban.com/passport/login?source=main')
            logger.info("请手动完成登录...")
            logger.info("您有5分钟时间完成登录操作...")
            
            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.ID, 'account'))
                )
                logger.info("当前为桌面版登录页面")
            except:
                logger.warning("未检测到桌面版登录元素，尝试切换到桌面版")
                driver.get('https://www.douban.com/')
                time.sleep(2)
                driver.get('https://accounts.douban.com/passport/login?source=main')
            
            start_time = time.time()
            timeout = 300
            
            while time.time() - start_time < timeout:
                try:
                    driver.find_element(By.CLASS_NAME, 'nav-user-avatar')
                    self.logged_in = True
                    logger.info("登录成功 (桌面版)")
                    return True
                except:
                    pass
                
                try:
                    driver.find_element(By.CSS_SELECTOR, 'div.user-info')
                    self.logged_in = True
                    logger.info("登录成功 (移动版)")
                    return True
                except:
                    pass
                
                current_url = driver.current_url
                if 'accounts.douban.com' not in current_url and 'login' not in current_url:
                    logger.info(f"已离开登录页面，当前URL: {current_url}")
                    try:
                        username_element = driver.find_element(By.CSS_SELECTOR, '.nav-user-account, .user-info')
                        username = username_element.text.strip()
                        if username:
                            self.logged_in = True
                            logger.info(f"登录成功! 用户名: {username}")
                            return True
                    except:
                        logger.warning("已离开登录页面但未检测到用户名")
                
                logger.info("等待登录完成...")
                time.sleep(5)
            
            logger.error("登录超时")
            return False
        except Exception as e:
            logger.error(f"登录过程中出错: {str(e)}")
            return False
    
    def _init_csv_files(self):
        if not os.path.exists(self.movies_file):
            with open(self.movies_file, 'w', encoding='utf-8-sig', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    '电影ID', '中文名', '英文名', '海报URL', '剧情简介', '导演', '编剧', 
                    '主演', '类型', '制片国家', '语言', '上映日期', '片长(分钟)', 
                    'IMDB链接', '又名', '豆瓣评分', '评分人数'
                ])
        
        if not os.path.exists(self.comments_file):
            with open(self.comments_file, 'w', encoding='utf-8-sig', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    '电影ID', '用户昵称', '评论时间', '评论内容', '有用数'
                ])
        
        if not os.path.exists(self.reviews_file):
            with open(self.reviews_file, 'w', encoding='utf-8-sig', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    '电影ID', '用户昵称', '评论时间', '评论标题', '评论内容', 
                    '有用数', '无用数', '分享数', '回应数'
                ])
    
    def get_session(self):
        if not hasattr(self.local, "session"):
            session = requests.Session()
            retry_strategy = Retry(
                total=3,
                backoff_factor=0.5,
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=["GET"]
            )
            adapter = HTTPAdapter(
                max_retries=retry_strategy,
                pool_connections=20,
                pool_maxsize=100,
                pool_block=False
            )
            session.mount("http://", adapter)
            session.mount("https://", adapter)
            self.local.session = session
            logger.info(f"线程 {threading.get_ident()} 创建新的Session")
        return self.local.session
    
    def safe_request(self, url, max_retries=3, timeout=15):
        session = self.get_session()
        
        headers = {
            'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Connection': 'keep-alive',
            'Referer': 'https://movie.douban.com/',
        }
        
        for attempt in range(max_retries):
            try:
                delay = random.uniform(1, 3)
                logger.debug(f"等待 {delay:.1f} 秒后请求: {url}")
                time.sleep(delay)
                
                response = session.get(
                    url, 
                    headers=headers,
                    timeout=timeout,
                    proxies=self.proxies,
                    allow_redirects=False
                )
                
                if response.status_code in [302, 403]:
                    location = response.headers.get('Location', '')
                    if 'sec.douban.com' in location or 'verify.douban.com' in location:
                        logger.warning(f"触发反爬验证: {url}, 尝试 {attempt+1}/{max_retries}")
                        time.sleep(random.uniform(15, 25))
                        continue
                
                if response.status_code == 200:
                    if '请点击这里进行验证' in response.text:
                        logger.warning(f"页面包含安全验证: {url}")
                        time.sleep(random.uniform(15, 25))
                        continue
                    
                    if 'captcha' in response.url:
                        logger.warning(f"遇到验证码: {url}")
                        time.sleep(random.uniform(15, 25))
                        continue
                    
                    return response
                
                logger.warning(f"请求失败: {url} - 状态码: {response.status_code} (尝试 {attempt+1}/{max_retries})")
                time.sleep(random.uniform(3, 5))
                
            except Exception as e:
                wait_time = random.uniform(3, 5)
                logger.warning(f"请求出错: {url} - {str(e)}，等待 {wait_time:.1f} 秒后重试... (尝试 {attempt+1}/{max_retries})")
                time.sleep(wait_time)
        
        return None
    
    def save_to_csv(self, data, file_path):
        with self.lock:
            with open(file_path, 'a', encoding='utf-8-sig', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(data)
    
    def extract_related_movies(self, movie_id):
        logger.info(f"提取相关电影: {movie_id}")
        url = f'https://movie.douban.com/subject/{movie_id}/'
        
        try:
            response = self.safe_request(url, timeout=15)
            if not response:
                logger.error(f"无法获取电影详情: {movie_id}")
                return []
            
            html = response.text
            tree = etree.HTML(html)
            
            if '请点击这里进行验证' in html:
                logger.warning(f"需要手动验证: {movie_id}")
                return []
            
            related_movies = []
            related_items = tree.xpath('//div[@id="recommendations"]/div[@class="recommendations-bd"]/dl')
            
            for item in related_items:
                movie_link = item.xpath('.//a/@href')
                if movie_link:
                    match = re.search(r'subject/(\d+)/', movie_link[0])
                    if match:
                        related_id = match.group(1)
                        with self.lock:
                            if related_id not in self.crawled_movies:
                                related_movies.append(related_id)
            
            logger.info(f"找到 {len(related_movies)} 部相关电影")
            return related_movies
            
        except Exception as e:
            logger.error(f"提取相关电影出错: {movie_id} - {str(e)}")
            return []
    
    def cleanup_resources(self):
        current_time = time.time()
        if current_time - self.last_cleanup > 300:
            self.last_cleanup = current_time
            logger.info("执行定期资源清理...")
            
            for driver in self.driver_pool.values():
                try:
                    driver.execute_script("window.open('', '_blank').close()")
                    driver.execute_script("window.open('', '_blank').close()")
                    logger.debug("清理浏览器标签页")
                except Exception as e:
                    logger.warning(f"清理浏览器失败: {str(e)}")
            
            process = psutil.Process(os.getpid())
            mem_info = process.memory_info()
            logger.info(f"当前内存使用: {mem_info.rss / (1024 * 1024):.2f} MB")
    
    def save_movie_to_db(self, movie_data):
        try:
            conn, cursor = self.get_thread_db()
            with self.db_lock:
                insert_query = """
                INSERT INTO movies (
                    movie_id, chinese_name, english_name, poster_url, summary, 
                    directors, screenwriters, actors, genres, countries, languages, 
                    release_date, runtime, imdb_url, alias, douban_rating, rating_count
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    chinese_name = VALUES(chinese_name),
                    english_name = VALUES(english_name),
                    poster_url = VALUES(poster_url),
                    summary = VALUES(summary),
                    directors = VALUES(directors),
                    screenwriters = VALUES(screenwriters),
                    actors = VALUES(actors),
                    genres = VALUES(genres),
                    countries = VALUES(countries),
                    languages = VALUES(languages),
                    release_date = VALUES(release_date),
                    runtime = VALUES(runtime),
                    imdb_url = VALUES(imdb_url),
                    alias = VALUES(alias),
                    douban_rating = VALUES(douban_rating),
                    rating_count = VALUES(rating_count)
                """
                
                data = (
                    movie_data['id'],
                    movie_data['title'],
                    movie_data['original_title'],
                    movie_data['poster_url'],
                    movie_data['summary'],
                    movie_data['directors'],
                    movie_data['screenwriters'],
                    movie_data['actors'],
                    movie_data['genres'],
                    movie_data['countries'],
                    movie_data['languages'],
                    movie_data['release_date'],
                    movie_data['runtime'],
                    movie_data['imdb_url'],
                    movie_data['alias'],
                    movie_data['rating'],
                    movie_data['rating_count']
                )
                
                self.cursor.execute(insert_query, data)
                self.conn.commit()
                logger.info(f"已保存电影到数据库: {movie_data['title']}")
        except Error as e:
            logger.error(f"保存电影到数据库失败: {e}")
    
    def save_comment_to_db(self, comment_data):
        try:
            conn, cursor = self.get_thread_db()
            with self.db_lock:
                insert_query = """
                INSERT INTO short_comments (
                    movie_id, user_nickname, comment_time, content, useful_count
                ) VALUES (%s, %s, %s, %s, %s)
                """
                
                self.cursor.execute(insert_query, comment_data)
                self.conn.commit()
        except Error as e:
            logger.error(f"保存短评到数据库失败: {e}")
    
    def save_review_to_db(self, review_data):
        try:
            conn, cursor = self.get_thread_db()
            with self.db_lock:
                insert_query = """
                INSERT INTO reviews (
                    movie_id, user_nickname, review_time, title, content, 
                    useful_count, useless_count, share_count, response_count
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                self.cursor.execute(insert_query, review_data)
                self.conn.commit()
        except Error as e:
            logger.error(f"保存影评到数据库失败: {e}")
    
    def crawl_movie_detail(self, movie_id, thread_id):
        if movie_id in self.crawled_movies:
            return False
            
        logger.info(f"线程 {thread_id} 开始爬取电影详情: {movie_id}")
        
        # 更新当前正在爬取的电影
        with self.lock:
            app_status['current_movie'] = movie_id
        
        driver = self.get_driver(thread_id)
        
        try:
            self.cleanup_resources()
            
            if driver and self.logged_in:
                try:
                    driver.get(f'https://movie.douban.com/subject/{movie_id}/')
                    
                    if 'sec.douban.com' in driver.current_url:
                        logger.warning(f"遇到验证码: {movie_id}")
                    
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.XPATH, '//h1/span'))
                    )
                    
                    html = driver.page_source
                    tree = etree.HTML(html)
                except Exception as e:
                    logger.warning(f"使用Selenium获取电影详情失败: {str(e)}，回退到requests")
                    response = self.safe_request(f'https://movie.douban.com/subject/{movie_id}/', timeout=20)
                    if not response:
                        logger.error(f"无法获取电影详情: {movie_id}")
                        return False
                    html = response.text
                    tree = etree.HTML(html)
            else:
                response = self.safe_request(f'https://movie.douban.com/subject/{movie_id}/', timeout=20)
                if not response:
                    logger.error(f"无法获取电影详情: {movie_id}")
                    return False
                html = response.text
                tree = etree.HTML(html)
            
            if '请点击这里进行验证' in html:
                logger.warning(f"需要手动验证: {movie_id}")
                return False
            
            title = self._extract_text(tree, '//h1/span/text()') or self._extract_text(tree, '//span[@property="v:itemreviewed"]/text()')
            original_title = self._extract_text(tree, '//span[@class="original"]/text()')
            poster_url = self._extract_attr(tree, '//img[@rel="v:image"]/@src') or self._extract_attr(tree, '//img[@rel="nofollow"]/@src')
            
            summary = ""
            summary_element = tree.xpath('//span[@property="v:summary"]')
            if summary_element:
                summary = "\n".join([text.strip() for text in summary_element[0].xpath('.//text()') if text.strip()])
            else:
                summary_element = tree.xpath('//div[@id="link-report-intra"]/span/text()')
                if summary_element:
                    summary = "\n".join([text.strip() for text in summary_element if text.strip()])
            
            directors = self._extract_list(tree, '//a[@rel="v:directedBy"]/text()')
            screenwriters = self._extract_list(tree, '//span[contains(text(), "编剧")]/following-sibling::span/a/text()') or \
                           self._extract_list(tree, '//span[contains(text(), "编剧")]/following::span[1]/a/text()')
            actors = self._extract_list(tree, '//a[@rel="v:starring"]/text()')
            genres = self._extract_list(tree, '//span[@property="v:genre"]/text()')
            countries = self._extract_text(tree, '//span[contains(text(), "制片国家")]/following::text()[1]') or \
                       self._extract_text(tree, '//span[contains(text(), "制片国家/地区")]/following::text()[1]')
            countries = countries.strip()
            languages = self._extract_text(tree, '//span[contains(text(), "语言")]/following::text()[1]').strip()
            release_date = self._extract_release_date(tree)
            runtime = self._extract_runtime(tree)
            imdb_url = self._extract_imdb_url(tree)
            alias = self._extract_alias(tree)
            rating = self._extract_rating(tree)
            rating_count = self._extract_rating_count(tree)
            
            movie_data = {
                'id': movie_id,
                'title': title,
                'original_title': original_title,
                'poster_url': poster_url,
                'summary': summary,
                'directors': directors,
                'screenwriters': screenwriters,
                'actors': actors,
                'genres': genres,
                'countries': countries,
                'languages': languages,
                'release_date': release_date,
                'runtime': runtime,
                'imdb_url': imdb_url,
                'alias': alias,
                'rating': rating,
                'rating_count': rating_count
            }
            
            self.save_movie_to_csv(movie_data)
            self.save_movie_to_db(movie_data)
            
            self.crawl_comments(movie_id, thread_id)
            self.crawl_reviews(movie_id, thread_id)
            
            with self.lock:
                self.crawled_movies.add(movie_id)
            
            related_movies = self.extract_related_movies(movie_id)
            for related_id in related_movies:
                with self.lock:
                    if related_id not in self.crawled_movies and not self.movie_queue.full():
                        self.movie_queue.put(related_id)
            
            return True
            
        except Exception as e:
            logger.error(f"爬取电影详情出错: {movie_id} - {str(e)}")
            with self.lock:
                app_status['error_count'] += 1
            return False

    def save_movie_to_csv(self, movie_data):
        data_row = [
            movie_data['id'],
            movie_data['title'],
            movie_data['original_title'],
            movie_data['poster_url'],
            movie_data['summary'],
            movie_data['directors'],
            movie_data['screenwriters'],
            movie_data['actors'],
            movie_data['genres'],
            movie_data['countries'],
            movie_data['languages'],
            movie_data['release_date'],
            movie_data['runtime'],
            movie_data['imdb_url'],
            movie_data['alias'],
            movie_data['rating'],
            movie_data['rating_count']
        ]
        self.save_to_csv(data_row, self.movies_file)
        logger.info(f"已保存电影信息: {movie_data['title']}")

    def crawl_comments(self, movie_id, thread_id, max_comments=10):
        logger.info(f"线程 {thread_id} 开始爬取短评: {movie_id} (最多{max_comments}条)")
        
        driver = self.get_driver(thread_id)
        if not driver:
            logger.error(f"线程 {thread_id} 无法获取浏览器实例")
            return
        
        try:
            driver.get(f'https://movie.douban.com/subject/{movie_id}/comments?sort=new_score&status=P')
            
            try:
                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.XPATH, '//div[@id="comments"]'))
                )
            except Exception:
                logger.warning("短评区域加载超时，尝试继续")
            
            html = driver.page_source
            tree = etree.HTML(html)
            
            comment_items = tree.xpath('//div[@class="comment-item"]') or \
                           tree.xpath('//div[contains(@class, "CommentItem")]') or \
                           tree.xpath('//div[contains(@id, "comment-")]')
            
            logger.info(f"找到 {len(comment_items)} 个短评元素")
            
            comments = []
            for i, item in enumerate(comment_items[:max_comments]):
                try:
                    nickname = self._extract_text(item, './/span[@class="comment-info"]//a/text()') or \
                              self._extract_text(item, './/a[@class=""]/text()')
                    
                    comment_time = self._extract_attr(item, './/span[@class="comment-time"]/@title')
                    if not comment_time:
                        comment_time = self._extract_text(item, './/span[@class="comment-time"]/text()')
                    comment_time = comment_time.strip().replace('\n', '').replace('\t', '')
                    
                    content = self._extract_text(item, './/span[@class="short"]/text()') or \
                             self._extract_text(item, './/div[@class="comment-content"]/span/text()')
                    
                    useful_count = self._extract_text(item, './/span[contains(@class, "votes")]/text()') or \
                                  self._extract_text(item, './/span[contains(@class, "vote-count")]/text()')
                    
                    comment_data = [
                        movie_id,
                        nickname.strip() if nickname else "匿名用户",
                        comment_time,
                        content.strip() if content else "",
                        int(useful_count) if useful_count and useful_count.strip() else 0
                    ]
                    self.save_to_csv(comment_data, self.comments_file)
                    self.save_comment_to_db(comment_data)
                    comments.append(comment_data)
                except Exception as e:
                    logger.error(f"处理第 {i+1} 条短评出错: {str(e)}")
            
            logger.info(f"已爬取 {len(comments)} 条短评")
            
        except Exception as e:
            logger.error(f"爬取短评出错: {str(e)}")
            with self.lock:
                app_status['error_count'] += 1

    def crawl_reviews(self, movie_id, thread_id, max_reviews=5):
        logger.info(f"线程 {thread_id} 开始爬取影评: {movie_id} (最多{max_reviews}条)")
        
        driver = self.get_driver(thread_id)
        if not driver:
            logger.error(f"线程 {thread_id} 无法获取浏览器实例")
            return
        
        try:
            driver.get(f'https://movie.douban.com/subject/{movie_id}/reviews')
            
            try:
                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.XPATH, '//div[@class="review-list"]'))
                )
            except Exception:
                logger.warning("影评区域加载超时，尝试继续")
            
            html = driver.page_source
            tree = etree.HTML(html)
            
            review_items = tree.xpath('//div[contains(@class, "review-item")]') or \
                          tree.xpath('//div[@class="main review-item"]') or \
                          tree.xpath('//div[contains(@id, "review-")]')
            
            logger.info(f"找到 {len(review_items)} 个影评元素")
            
            review_data_list = []
            for i, item in enumerate(review_items[:max_reviews]):
                try:
                    nickname = self._extract_text(item, './/a[contains(@class, "name")]/text()') or \
                              self._extract_text(item, './/header//a/text()')
                    
                    review_time = self._extract_text(item, './/span[contains(@class, "main-meta")]/text()') or \
                                self._extract_text(item, './/span[@class="main-meta"]/text()')
                    review_time = review_time.strip().replace('\n', '').replace('\t', '')
                    
                    title = self._extract_text(item, './/a[contains(@class, "title")]/text()') or \
                           self._extract_text(item, './/h2/a/text()')
                    
                    content = self._extract_text(item, './/div[contains(@class, "short-content")]//text()') or \
                             self._extract_text(item, './/div[@class="short-content"]//text()')
                    
                    useful_count = self._extract_text(item, './/a[contains(@class, "up")]/span/text()')
                    
                    useless_count = self._extract_text(item, './/a[contains(@class, "down")]/span/text()')
                    
                    response_text = self._extract_text(item, './/a[contains(@class, "reply")]/text()')
                    response_count = 0
                    if response_text:
                        match = re.search(r'\d+', response_text)
                        if match:
                            response_count = match.group()
                    
                    review_data = [
                        movie_id,
                        nickname.strip() if nickname else "匿名用户",
                        review_time,
                        title.strip() if title else "",
                        content.strip() if content else "",
                        int(useful_count) if useful_count else 0,
                        int(useless_count) if useless_count else 0,
                        0,
                        int(response_count) if response_count else 0
                    ]
                    
                    self.save_to_csv(review_data, self.reviews_file)
                    self.save_review_to_db(review_data)
                    review_data_list.append(review_data)
                except Exception as e:
                    logger.error(f"处理第 {i+1} 条影评出错: {str(e)}")
            
            logger.info(f"已爬取 {len(review_data_list)} 条影评")
            
        except Exception as e:
            logger.error(f"爬取影评列表出错: {str(e)}")
            with self.lock:
                app_status['error_count'] += 1

    def _extract_text(self, element, xpath):
        result = element.xpath(xpath)
        if result:
            try:
                return ''.join([text.strip() for text in result if text.strip()])
            except:
                return ""
        return ""

    def _extract_list(self, element, xpath):
        results = element.xpath(xpath)
        return ", ".join([item.strip() for item in results if item.strip()])

    def _extract_attr(self, element, xpath):
        result = element.xpath(xpath)
        return result[0] if result else ""

    def _extract_rating(self, element):
        result = element.xpath('//strong[contains(@class, "rating")]/text()') or \
                element.xpath('//strong[@property="v:average"]/text()')
        try:
            return float(result[0]) if result else 0.0
        except:
            return 0.0

    def _extract_rating_count(self, element):
        result = element.xpath('//span[@property="v:votes"]/text()')
        try:
            return int(result[0]) if result else 0
        except:
            return 0

    def _extract_runtime(self, element):
        result = element.xpath('//span[@property="v:runtime"]/text()')
        if result:
            match = re.search(r'\d+', result[0])
            return int(match.group()) if match else 0
        return 0

    def _extract_alias(self, element):
        result = element.xpath('//span[contains(text(), "又名")]/following::text()[1]') or \
                element.xpath('//span[contains(text(), "又名")]/following-sibling::text()[1]')
        return result[0].strip() if result else ""

    def _extract_imdb_url(self, element):
        result = element.xpath('//a[contains(@href, "imdb.com/title")]/@href')
        return result[0] if result else ""

    def _extract_release_date(self, element):
        result = element.xpath('//span[@property="v:initialReleaseDate"]/text()')
        return ", ".join(result) if result else ""

    def worker_task(self, thread_key, movie_limit, lock):
        """工作线程任务，从队列中获取电影ID并爬取"""
        logger.info(f"线程 {thread_key} 开始工作")
        while not self.stop_requested:
            with lock:
                if self.processed >= movie_limit:
                    logger.info(f"线程 {thread_key} 达到处理上限，退出")
                    break
            try:
                movie_id = self.movie_queue.get(block=False)
            except Empty:
                logger.info(f"线程 {thread_key} 队列空，退出")
                break

            with lock:
                app_status['queue_size'] = self.movie_queue.qsize()
                logger.info(f"线程 {thread_key} 获取电影: {movie_id}")

            try:
                if self.crawl_movie_detail(movie_id, thread_key):
                    with lock:
                        self.processed += 1
                        app_status['processed'] = self.processed
                        app_status['crawled_count'] = len(self.crawled_movies)
                        logger.info(f"线程 {thread_key} 完成电影: {movie_id} (进度: {self.processed}/{movie_limit})")
            except Exception as e:
                logger.error(f"线程 {thread_key} 处理电影 {movie_id} 出错: {str(e)}")
                with lock:
                    app_status['error_count'] += 1

            time.sleep(0.1)

    def run(self, movie_limit=100):
        global app_status
        movie_limit = int(movie_limit)
        app_status['is_running'] = True
        app_status['processed'] = 0
        app_status['total'] = movie_limit
        app_status['queue_size'] = self.movie_queue.qsize()
        app_status['crawled_count'] = len(self.crawled_movies)
        app_status['status_text'] = '爬虫运行中'
        app_status['error_count'] = 0
        app_status['start_time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        app_status['end_time'] = None
        app_status['current_movie'] = '无'
        self.stop_requested = False
        self.processed = 0
        self.total = movie_limit
        
        main_driver = self._init_selenium_driver()
        if main_driver:
            logger.warning("强制重新登录豆瓣账号...")
            if self.manual_login(main_driver):
                logger.info("登录成功，开始爬取...")
                self.logged_in = True
                self.driver_pool["main"] = main_driver
            else:
                logger.error("登录失败，程序退出")
                self.close_all_drivers()
                self.close_db_connection()
                app_status['status_text'] = '登录失败'
                app_status['is_running'] = False
                return
        else:
            logger.error("主浏览器实例初始化失败，程序退出")
            self.close_db_connection()
            app_status['status_text'] = '浏览器初始化失败'
            app_status['is_running'] = False
            return

        # 创建工作线程的浏览器实例
        for i in range(2):
            worker_id = f"worker_{i+1}"
            driver = self._init_selenium_driver()
            if driver:
                self.driver_pool[worker_id] = driver
                logger.info(f"{worker_id} 浏览器实例初始化成功")
            else:
                logger.error(f"{worker_id} 浏览器实例初始化失败")

        logger.info("=" * 60)
        logger.info(f"开始爬取电影详情和评论 (目标: {movie_limit} 部电影)")
        logger.info("=" * 60)

        lock = threading.Lock()
        threads = []
        thread_keys = list(self.driver_pool.keys())
        
        # 创建并启动工作线程
        for thread_key in thread_keys:
            thread = threading.Thread(
                target=self.worker_task,
                args=(thread_key, movie_limit, lock),
                daemon=True
            )
            thread.start()
            threads.append(thread)
            logger.info(f"启动工作线程: {thread_key}")
        
        # 等待所有线程完成
        for thread in threads:
            thread.join()
            logger.info(f"线程 {thread.name} 已完成")

        logger.info("=" * 60)
        logger.info(f"爬虫完成! 共处理 {self.processed} 部电影")
        logger.info("=" * 60)

        self.close_all_drivers()
        self.close_db_connection()
        app_status['is_running'] = False
        app_status['status_text'] = '爬虫已完成'
        app_status['end_time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    def stop(self):
        self.stop_requested = True
        app_status['status_text'] = '正在停止爬虫...'
        logger.info("收到停止请求，爬虫将在当前任务完成后停止")
    
    def close_db_connection(self):
        if self.conn and self.conn.is_connected():
            try:
                if self.cursor:
                    self.cursor.close()
                self.conn.close()
                logger.info("数据库连接已关闭")
            except Error as e:
                logger.error(f"关闭数据库连接失败: {e}")
    
    def close_all_drivers(self):
        for thread_id, driver in self.driver_pool.items():
            try:
                driver.quit()
                logger.info(f"已关闭线程 {thread_id} 的浏览器")
            except Exception as e:
                logger.error(f"关闭浏览器失败: {str(e)}")
        self.driver_pool = {}
        self.logged_in = False

# Flask 路由
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/status')
def status():
    app_status['last_update'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    app_status['active_threads'] = threading.active_count()
    return jsonify(app_status)

@app.route('/start', methods=['POST'])
def start_spider():
    global spider
    if spider and app_status['is_running']:
        return jsonify({'status': 'already_running', 'message': '爬虫已经在运行中'})
    
    data = request.get_json()
    movie_limit = data.get('movie_limit', 100)
    
    spider = DoubanStealthSpider()
    
    # 在单独的线程中运行爬虫
    def run_spider():
        spider.run(movie_limit=movie_limit)
    
    thread = threading.Thread(target=run_spider)
    thread.daemon = True
    thread.start()
    
    return jsonify({'status': 'started', 'message': f'爬虫已启动，目标爬取 {movie_limit} 部电影'})

@app.route('/stop', methods=['POST'])
def stop_spider():
    global spider
    if spider and app_status['is_running']:
        spider.stop()
        return jsonify({'status': 'stopping', 'message': '正在停止爬虫...'})
    return jsonify({'status': 'not_running', 'message': '爬虫未在运行'})

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000, debug=True)