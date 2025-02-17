from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, urlunparse
import urllib.request
import urllib.error
import urllib.robotparser
import re
from queue import PriorityQueue
from collections import deque, defaultdict
from pybloom_live import ScalableBloomFilter
import time as time_module
import concurrent.futures
from threading import Lock


# Robots.txt cache
class robots_cache:
    def __init__(self, expired_time=3600):
        self.cache = defaultdict(dict)
        self.expired_time = expired_time

    def fetch_robots(self, url):
        domain = urlparse(url).netloc

        if domain in self.cache:
            rp, last_accessed = self.cache[domain]
            if time_module.time() - last_accessed < self.expired_time:
                return rp

        rp = urllib.robotparser.RobotFileParser()
        robots_url = 'http://' + domain + '/robots.txt'
        rp.set_url(robots_url)

        try:
            response = urllib.request.urlopen(robots_url, timeout=60)
            if response.getcode() == 200:
                rp.read()
            else:
                rp.allow_all = True

        except Exception as e:
            rp.allow_all = True

        self.cache[domain] = (rp, time_module.time())
        return rp

    def can_fetch(self, url):
        rp = self.fetch_robots(url)
        return rp.can_fetch('*', url)

    def crawl_delay(self, url):
        rp = self.fetch_robots(url)
        return rp.crawl_delay('*')


# Redirect handler
class Redirect_Handler(urllib.request.HTTPRedirectHandler):
    def __init__(self, max_redirections=10):
        self.max_redirections = max_redirections

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        if not hasattr(self, 'count'):
            self.count = 0

        if self.count > self.max_redirections:
            raise urllib.error.HTTPError(
                req.get_full_url(), code, "Too many redirects", headers, fp)
        self.count += 1

        return super().redirect_request(req, fp, code, msg, headers, newurl)


# Webcrawler using BFS
class webcrawler_BFS:
    def __init__(self, urls):
        # urls to crawl
        self.urls = urls
        for i in range(len(urls)):
            if self.urls[i][-1] != '/':
                self.urls[i] += '/'
        print("Crawling the following urls:", self.urls)

        # data structures
        self.pq = PriorityQueue()
        for url in self.urls:
            self.pq.put((0, 0, url))

        self.visited = set()
        self.links = ScalableBloomFilter()
        self.crawled = []
        self.log = deque()

        # crawler settings
        self.max_depth = 100
        self.max_crawl = 200
        self.completed = False

        # priority settings
        self.level2_domain_freq = defaultdict(int)
        self.all_domain_freq = defaultdict(int)
        self.total_domain_num = 0
        for url in urls:
            all_domain = self.normalize_www(urlparse(url).netloc)
            get_level2_domain = self.get_level2_domain(all_domain)
            self.all_domain_freq[all_domain] += 1
            self.level2_domain_freq[get_level2_domain] += 1
            self.total_domain_num += 1

        # robots.txt cache
        self.robots_cache = robots_cache()
        for url in urls:
            self.robots_cache.fetch_robots(url)

        # multithreading locks
        self.visited_lock = Lock()
        self.links_lock = Lock()
        self.pq_lock = Lock()
        self.crawled_lock = Lock()
        self.log_lock = Lock()
        self.all_domain_freq_lock = Lock()
        self.level2_domain_freq_lock = Lock()
        self.total_domain_num_lock = Lock()

    # Fetch the url
    def fetch_url(self, url):
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }

        # Redirect handler
        redirect_handler = Redirect_Handler()
        opener = urllib.request.build_opener(redirect_handler)
        req = urllib.request.Request(url, headers=headers)

        try:
            with opener.open(req, timeout=60) as response:
                headers = response.info()
                content = response.read()
                final_url = response.geturl()
                status = response.getcode()
                return headers, content, final_url, status

        except urllib.error.HTTPError as e:
            print('HTTPError:', e)
            return None, None, url, e.code

        except urllib.error.URLError as e:
            print('URLError:', e)
            return None, None, url, e.code if hasattr(e, 'code') else 400

        except Exception as e:
            print('Error:', e)
            return None, None, url, 500

    # Url handling -- Normalization, Filtering, Parsing
    def normalize_url(self, url, link):
        url = urljoin(url, link)
        parsed = urlparse(url)

        normalized = parsed._replace(
            scheme=parsed.scheme.lower(),
            netloc=parsed.netloc.lower(),
            fragment=''
        )

        if not normalized.path.endswith('/') and not normalized.path.split('/')[-1].count('.'):
            normalized = normalized._replace(path=normalized.path + '/')

        del_words = re.compile(r'utm_.*|rss|xml')
        normalized = normalized._replace(
            query='&'.join(q for q in normalized.query.split('&') if not del_words.search(q)))

        if re.search(r'\.(rss|xml)$', normalized.path):
            normalized = normalized._replace(path='/', fragment='', query='')

        return urlunparse(normalized)

    # Check if the url is crawlable
    def isHtml(self, url):
        return not url.endswith(('.rss', '.ico', '.png', '.jpg', '.jpeg', '.gif', '.bmp', '.svg', 'pdf', 'css', 'js', '.mp4', '.avi', '.mov', '.wmv', '.flv', '.webm', '.aspx')) and not url.startswith(('mailto:', 'tel:', 'javascript:', 'data:', 'ftp:', 'file:'))

    def in_nz_domain(self, url):
        url = urlparse(url).netloc
        return url.endswith('.nz')

    def isCrawlable(self, url):
        with self.visited_lock:
            if url in self.visited:
                return False

        with self.links_lock:
            if url in self.links:
                return False

        return url and self.isHtml(url) and self.in_nz_domain(url) and self.robots_cache.can_fetch(url)

    # Computer priority of the url
    def normalize_www(self, domain):
        return domain.lower().lstrip('www.')

    def get_level2_domain(self, domain):
        domain_parts = domain.split('.')
        if len(domain_parts) > 1:
            return "".join(domain_parts[-2:])
        return domain

    def get_priority(self, priority, depth, url, link):
        dynamic_adjustment = 0
        url = urlparse(url)
        link = urlparse(link)

        url_all_domain = self.normalize_www(url.netloc)
        link_all_domain = self.normalize_www(link.netloc)
        link_level2_domain = self.get_level2_domain(link_all_domain)

        # depth penalty
        if depth < 2:
            dynamic_adjustment += 0.3
        else:
            dynamic_adjustment += 0.2

        # short path bonus
        if len(link.path) < 30:
            dynamic_adjustment -= 0.1

        # same domain penalty
        if url_all_domain == link_all_domain:
            dynamic_adjustment += 0.35

        # define the impact of the domain frequency
        level2_weight = 0.15
        all_weight = 0.05

        with self.all_domain_freq_lock:
            self.all_domain_freq[link_all_domain] += 1
            all_domain_freq = self.all_domain_freq[link_all_domain]

        with self.level2_domain_freq_lock:
            self.level2_domain_freq[link_level2_domain] += 1
            level2_domain_freq = self.level2_domain_freq[link_level2_domain]

        with self.total_domain_num_lock:
            self.total_domain_num += 1
            total_domain_num = self.total_domain_num

        # Ratio of the domain
        level2_domain_ratio = 1 - \
            ((level2_domain_freq-1) / total_domain_num)
        all_domain_ratio = 1 - ((all_domain_freq - 1) / total_domain_num)

        # Dynamic adjustment based on the occurrence of the domain
        dynamic_adjustment -= level2_weight * level2_domain_ratio
        dynamic_adjustment -= all_weight * all_domain_ratio

        return priority + dynamic_adjustment

    # Log handling -- Writing, Reading, Printing
    def write_log(self, time, size, depth, url, status):
        self.log.append((time, size, depth, url, status))
        return True

    def get_log(self):
        return self.log

    def print_log(self):
        for log in self.log:
            time, size, depth, url, status = log
            print('Time:', time, 'Size', size, 'Depth:', depth,
                  'URL:', url, 'Status:', status)
        return True

    # Process the url and hyperlinks
    def process_url(self, priority, depth, url):
        try:
            headers, content, final_url, status = self.fetch_url(url)

            with self.visited_lock:
                self.visited.add(final_url)

            if not content:
                with self.log_lock:
                    self.write_log(time_module.strftime('%Y-%m-%d %H:%M:%S',
                                                        time_module.localtime()), '0', str(depth), final_url, str(status))
                    return

            if headers.get_content_type() == 'text/html':
                page_size = len(content)
                with self.crawled_lock:
                    self.crawled.append(final_url)
                with self.log_lock:
                    self.write_log(time_module.strftime(
                        '%Y-%m-%d %H:%M:%S', time_module.localtime()), str(page_size), str(depth), final_url, str(status))

                # Extract hyperlinks
                soup = BeautifulSoup(
                    content, 'html.parser')
                if depth < self.max_depth:
                    atags = soup.find_all('a')

                    for atag in atags:
                        link = atag.get('href')
                        link = self.normalize_url(final_url, link)

                        if self.isCrawlable(link):
                            with self.links_lock:
                                self.links.add(link)

                            new_priority = self.get_priority(
                                priority, depth, final_url, link)

                            with self.pq_lock:
                                self.pq.put((new_priority, depth + 1, link))
                            print('Adding:', link)

            else:
                with self.log_lock:
                    self.write_log(time_module.strftime('%Y-%m-%d %H:%M:%S',
                                                        time_module.localtime()), '0', str(depth), final_url, str(status))

        except Exception as e:
            print('Error:', e)
            pass

    # Execute the crawling process
    def crawl(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            futures = []
            while True:
                with self.crawled_lock:
                    if len(self.crawled) > self.max_crawl:
                        break

                with self.pq_lock:
                    if self.pq.empty():
                        break
                    priority, depth, url = self.pq.get()

                with self.visited_lock:
                    if url in self.visited:
                        continue

                print('Crawling:', url)
                futures.append(executor.submit(
                    self.process_url, priority, depth, url))

                crawl_delay = self.robots_cache.crawl_delay(url) or 1
                time_module.sleep(crawl_delay)

            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"An error occurred: {e}")

        self.completed = True
        return self.crawled

    # Get the number of crawled urls
    def get_count(self):
        return len(self.crawled)


class webcrawler_task:
    def __init__(self, crawl_list_path):
        self.crawl_list_path = crawl_list_path
        with open(crawl_list_path, 'r') as f:
            urls = f.readlines()
        self.urls = [url.strip() for url in urls]
        f.close()

        self.crawl_complete = False
        self.start_time = None
        self.end_time = None

        # Initialize the webcrawler
        self.crawler = webcrawler_BFS(self.urls)

    def start_task(self):
        self.start_time = time_module.time()
        self.crawler.crawl()
        if self.crawler.completed:
            self.end_time = time_module.time()
        self.crawl_complete = True

    def get_log(self):
        with open('log_{}.txt'.format(self.crawl_list_path.split('.')[0]), 'w') as f:
            while not self.crawl_complete or self.crawler.log:
                if self.crawler.log:
                    time, size, depth, url, status = self.crawler.log.popleft()
                    f.write('Time: {} Depth: {} Status: {} Size: {} URL: {}\n'.format(
                        time, depth, status, size, url))
                else:
                    time_module.sleep(1)

            total_crawled = self.crawler.get_count()
            duration = self.end_time - self.start_time
            f.write('Total Crawled: {} Duration: {}\n'.format(
                total_crawled, duration))

        f.close()


if __name__ == '__main__':

    task1 = webcrawler_task('crawl_list1.txt')
    task2 = webcrawler_task('crawl_list2.txt')

    task1.start_task()
    task1.get_log()

    task2.start_task()
    task2.get_log()

    print("All tasks completed")
