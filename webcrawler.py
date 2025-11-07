from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, urlunparse, parse_qsl, urlencode
import urllib.request
import urllib.error
import urllib.robotparser
import re
from queue import PriorityQueue, Empty
from collections import defaultdict
from pybloom_live import ScalableBloomFilter
import time as time_module
import concurrent.futures
from threading import Lock
import threading
import tldextract
import logging
from logging.handlers import RotatingFileHandler
import json
from typing import List, Tuple, Optional, Dict, Any, Union
import os
# Robots.txt cache


class robots_cache:
    def __init__(self, expired_time: int = 3600, opener: Optional[urllib.request.OpenerDirector] = None,
                 headers: Optional[Dict[str, str]] = None, timeout: int = 60):
        self.cache: Dict[str,
                         Tuple[urllib.robotparser.RobotFileParser, float]] = {}
        self.expired_time = expired_time
        self.default_opener = opener
        self.default_headers = headers or {}
        self.default_timeout = timeout

    def fetch_robots(self, url: str, opener: Optional[urllib.request.OpenerDirector] = None,
                     headers: Optional[Dict[str, str]] = None, timeout: Optional[int] = None) -> urllib.robotparser.RobotFileParser:
        domain = urlparse(url).netloc

        if domain in self.cache:
            rp, last_accessed = self.cache[domain]
            if time_module.time() - last_accessed < self.expired_time:
                return rp

        rp = urllib.robotparser.RobotFileParser()
        # Try HTTPS first, then fall back to HTTP, and reuse opener/headers.
        for scheme in ("https://", "http://"):
            robots_url = scheme + domain + "/robots.txt"
            try:
                opn = opener or self.default_opener or urllib.request.build_opener()
                req = urllib.request.Request(
                    robots_url, headers=(headers or self.default_headers))
                with opn.open(req, timeout=(timeout or self.default_timeout)) as resp:
                    content = resp.read()
                rp.parse(content.decode(errors='ignore').splitlines())
                self.cache[domain] = (rp, time_module.time())
                return rp
            except Exception:
                continue

        # If both attempts fail, allow all to avoid blocking the crawl entirely.
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
    def __init__(self, max_redirections: int = 10):
        self.max_redirections = max_redirections

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        current = getattr(req, 'redirect_count', 0)
        if current >= self.max_redirections:
            raise urllib.error.HTTPError(
                req.get_full_url(), code, "Too many redirects", headers, fp)
        setattr(req, 'redirect_count', current + 1)
        return super().redirect_request(req, fp, code, msg, headers, newurl)


# Webcrawler using BFS
class webcrawler_BFS:
    def __init__(self, urls: List[str], *,
                 max_depth=100,
                 max_crawl=200,
                 max_workers=8,
                 request_headers=None,
                 request_timeout=60,
                 robots_expired_time=3600,
                 bloom_initial_capacity=100000,
                 bloom_error_rate=0.001,
                 rate_limit_min_interval=0.5,
                 per_host_burst_capacity=1,
                 query_param_blocklist=None,
                 # Priority weights
                 level2_weight=0.15,
                 all_weight=0.05,
                 same_domain_penalty=0.35,
                 depth_penalty_shallow=0.3,
                 depth_penalty_deep=0.2,
                 short_path_bonus=-0.1,
                 logger: Optional[logging.Logger] = None):
        # urls to crawl
        self.urls = urls
        for i in range(len(urls)):
            if self.urls[i][-1] != '/':
                self.urls[i] += '/'
        self.logger = logger or logging.getLogger(__name__)
        self.logger.info("Crawling the following urls: %s", self.urls)

        # data structures
        self.pq = PriorityQueue()
        for url in self.urls:
            self.pq.put((0, 0, url))

        self.visited = set()
        self.links = ScalableBloomFilter(initial_capacity=bloom_initial_capacity,
                                         error_rate=bloom_error_rate)
        self.crawled = []

        # crawler settings
        self.max_depth = max_depth
        self.max_crawl = max_crawl
        self.completed = False
        self.max_workers = max_workers
        self.request_headers = request_headers or {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        self.request_timeout = request_timeout
        patterns: List[Union[str, re.Pattern[str]]] = query_param_blocklist or [
            r'^utm_', r'^fbclid$', r'^gclid$', r'^mc_cid$', r'^mc_eid$'
        ]
        compiled_blocklist: List[re.Pattern[str]] = []
        for p in patterns:
            if isinstance(p, str):
                try:
                    compiled_blocklist.append(re.compile(p))
                except re.error as err:
                    self.logger.warning(
                        'Invalid regex in query_param_blocklist: %s (%s)', p, err)
            elif isinstance(p, re.Pattern):
                compiled_blocklist.append(p)
            else:
                self.logger.warning(
                    'Ignoring non-regex pattern in query_param_blocklist: %r', p)
        self.query_param_blocklist = compiled_blocklist
        # Priority weights (configurable)
        self.level2_weight = float(level2_weight)
        self.all_weight = float(all_weight)
        self.same_domain_penalty = float(same_domain_penalty)
        self.depth_penalty_shallow = float(depth_penalty_shallow)
        self.depth_penalty_deep = float(depth_penalty_deep)
        self.short_path_bonus = float(short_path_bonus)
        # Thread-local opener storage
        self._local = threading.local()
        self._validate_params()

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
        self.robots_cache = robots_cache(expired_time=robots_expired_time,
                                         opener=None,
                                         headers=self.request_headers,
                                         timeout=self.request_timeout)
        for url in urls:
            self.robots_cache.fetch_robots(url)

        # multithreading locks
        self.visited_lock = Lock()
        self.links_lock = Lock()
        self.pq_lock = Lock()
        self.crawled_lock = Lock()
        self.all_domain_freq_lock = Lock()
        self.level2_domain_freq_lock = Lock()
        self.total_domain_num_lock = Lock()
        self.bucket_lock = Lock()

        # per-host token-bucket rate limiting
        self.rate_limit_min_interval = float(rate_limit_min_interval)
        self.per_host_burst_capacity = int(per_host_burst_capacity)
        # host -> {tokens, capacity, last_ts, rate_per_sec}
        self._host_buckets: Dict[str, Dict[str, float]] = {}

    # Fetch the url
    def _get_opener(self) -> urllib.request.OpenerDirector:
        if not hasattr(self._local, 'opener'):
            self._local.opener = urllib.request.build_opener(
                Redirect_Handler())
        return self._local.opener

    def fetch_url(self, url):
        req = urllib.request.Request(url, headers=self.request_headers)

        try:
            opener = self._get_opener()
            with opener.open(req, timeout=self.request_timeout) as response:
                headers = response.info()
                content = response.read()
                final_url = response.geturl()
                status = response.getcode()
                return headers, content, final_url, status

        except urllib.error.HTTPError as e:
            self.logger.warning('http error while fetching %s: %s', url, e)
            return None, None, url, e.code

        except urllib.error.URLError as e:
            status = e.code if hasattr(e, 'code') else 400
            self.logger.warning('url error while fetching %s: %s', url, e)
            return None, None, url, status

        except Exception as e:
            self.logger.error(
                'unexpected error while fetching %s: %s', url, e, exc_info=True)
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

        # Drop known tracking parameters using configured blocklist
        if normalized.query:
            filtered = []
            for key, value in parse_qsl(normalized.query, keep_blank_values=True):
                if any(p.match(key) for p in self.query_param_blocklist):
                    continue
                filtered.append((key, value))
            normalized = normalized._replace(query=urlencode(filtered))

        # Collapse explicit RSS/XML endpoints to root
        if re.search(r'\.(rss|xml)$', normalized.path):
            normalized = normalized._replace(path='/', fragment='', query='')

        return urlunparse(normalized)

    # Check if the url is crawlable
    def isHtml(self, url):
        # Skip non-HTML resources and non-http(s) schemes
        return (
            not url.endswith((
                '.rss', '.ico', '.png', '.jpg', '.jpeg', '.gif', '.bmp', '.svg',
                '.pdf', '.css', '.js', '.mp4', '.avi', '.mov', '.wmv', '.flv', '.webm', '.aspx'
            ))
            and not url.startswith(('mailto:', 'tel:', 'javascript:', 'data:', 'ftp:', 'file:'))
        )

    def in_nz_domain(self, url):
        url = urlparse(url).netloc
        return url.endswith('.nz')

    def isCrawlable(self, url):
        if not url:
            return False
        with self.visited_lock:
            if url in self.visited:
                return False
        return self.isHtml(url) and self.in_nz_domain(url) and self.robots_cache.can_fetch(url)

    # Computer priority of the url
    def normalize_www(self, domain):
        return domain.lower().lstrip('www.')

    def get_level2_domain(self, domain):
        # Use tldextract for robust public suffix parsing
        extracted = tldextract.extract(domain)
        if extracted.suffix and '.' in extracted.suffix:
            # e.g., co.nz
            return extracted.suffix
        # otherwise use registrable domain like example.nz
        registrable = '.'.join(
            part for part in [extracted.domain, extracted.suffix] if part)
        return registrable or domain

    def get_priority(self, priority, depth, url, link):
        dynamic_adjustment = 0
        url = urlparse(url)
        link = urlparse(link)

        url_all_domain = self.normalize_www(url.netloc)
        link_all_domain = self.normalize_www(link.netloc)
        link_level2_domain = self.get_level2_domain(link_all_domain)

        # depth penalty
        if depth < 2:
            dynamic_adjustment += self.depth_penalty_shallow
        else:
            dynamic_adjustment += self.depth_penalty_deep

        # short path bonus
        if len(link.path) < 30:
            dynamic_adjustment += self.short_path_bonus

        # same domain penalty
        if url_all_domain == link_all_domain:
            dynamic_adjustment += self.same_domain_penalty

        # define the impact of the domain frequency
        level2_weight = self.level2_weight
        all_weight = self.all_weight

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

    # Process the url and hyperlinks
    def process_url(self, priority, depth, url):
        try:
            headers, content, final_url, status = self.fetch_url(url)

            with self.visited_lock:
                self.visited.add(final_url)

            if not content:
                self.logger.info(
                    "status=%s depth=%s size=%s url=%s",
                    status,
                    depth,
                    0,
                    final_url,
                )
                return

            if headers and headers.get_content_type() == 'text/html':
                page_size = len(content)
                with self.crawled_lock:
                    self.crawled.append(final_url)
                self.logger.info(
                    "status=%s depth=%s size=%s url=%s",
                    status,
                    depth,
                    page_size,
                    final_url,
                )

                # Extract hyperlinks
                soup = BeautifulSoup(
                    content, 'html.parser')
                if depth < self.max_depth:
                    atags = soup.find_all('a')

                    for atag in atags:
                        link = atag.get('href')
                        # Skip empty and non-navigational links early
                        if not link or link.startswith(('#', 'javascript:', 'mailto:', 'tel:', 'data:')):
                            continue
                        link = self.normalize_url(final_url, link)

                        if not self.isCrawlable(link):
                            continue

                        with self.links_lock:
                            if link in self.links:
                                continue
                            self.links.add(link)

                        new_priority = self.get_priority(
                            priority, depth, final_url, link)

                        with self.pq_lock:
                            self.pq.put((new_priority, depth + 1, link))
                        self.logger.debug('Adding: %s', link)

            else:
                payload_size = len(content) if content else 0
                self.logger.info(
                    "status=%s depth=%s size=%s url=%s",
                    status,
                    depth,
                    payload_size,
                    final_url,
                )

        except Exception:
            self.logger.exception('error processing url %s', url)

    # Execute the crawling process
    def _get_or_init_bucket(self, host: str, url_for_robots: str) -> Dict[str, float]:
        if host not in self._host_buckets:
            robots_delay = self.robots_cache.crawl_delay(url_for_robots) or 0
            interval = max(self.rate_limit_min_interval, float(
                robots_delay) if robots_delay else self.rate_limit_min_interval)
            rate = 1.0 / interval if interval > 0 else 1.0
            self._host_buckets[host] = {
                'tokens': float(self.per_host_burst_capacity),
                'capacity': float(self.per_host_burst_capacity),
                'last_ts': time_module.time(),
                'rate': rate,
            }
        return self._host_buckets[host]

    def _try_consume_token(self, host: str, url_for_robots: str) -> bool:
        # protect bucket mutations with a lock
        with self.bucket_lock:
            bucket = self._get_or_init_bucket(host, url_for_robots)
            now = time_module.time()
            elapsed = now - bucket['last_ts']
            if elapsed > 0:
                bucket['tokens'] = min(
                    bucket['capacity'], bucket['tokens'] + elapsed * bucket['rate'])
                bucket['last_ts'] = now
            if bucket['tokens'] >= 1.0:
                bucket['tokens'] -= 1.0
                return True
            return False

    def crawl(self):
        executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self.max_workers)
        active_futures: Dict[concurrent.futures.Future,
                             Tuple[float, int, str]] = {}
        try:
            while True:
                with self.crawled_lock:
                    if len(self.crawled) >= self.max_crawl:
                        self.logger.info("reached max crawl limit")
                        break

                if active_futures:
                    done, _ = concurrent.futures.wait(
                        set(active_futures),
                        timeout=0.1,
                        return_when=concurrent.futures.FIRST_COMPLETED
                    )
                    for future in done:
                        priority, depth, url = active_futures.pop(future, None)
                        try:
                            future.result()
                        except Exception as e:
                            self.logger.error(
                                "error processing %s: %s", url, e, exc_info=True)

                with self.pq_lock:
                    pq_empty = self.pq.empty()

                if pq_empty and not active_futures:
                    self.logger.info(
                        "queue is empty and no active tasks. crawler finished")
                    break

                while len(active_futures) < self.max_workers:
                    try:
                        with self.pq_lock:
                            priority, depth, url = self.pq.get_nowait()
                    except Empty:
                        break

                    with self.visited_lock:
                        if url in self.visited:
                            continue

                    host = urlparse(url).netloc.lower()
                    if not self._try_consume_token(host, url):
                        with self.pq_lock:
                            self.pq.put((priority, depth, url))
                        time_module.sleep(0.05)
                        continue

                    with self.visited_lock:
                        if url in self.visited:
                            continue
                        self.visited.add(url)

                    self.logger.info(
                        'submitting task: %s (Depth: %d)', url, depth)
                    future = executor.submit(
                        self.process_url, priority, depth, url)
                    active_futures[future] = (priority, depth, url)

        finally:
            self.logger.info("closing workers...")
            for future in active_futures:
                future.cancel()
            executor.shutdown(wait=True)

        self.completed = True
        return self.crawled

    def _validate_params(self) -> None:
        if not (isinstance(self.max_crawl, (int, float)) and self.max_crawl > 0):
            raise ValueError('max_crawl must be > 0')
        if not (isinstance(self.max_depth, (int, float)) and self.max_depth >= 0):
            raise ValueError('max_depth must be >= 0')
        if not (isinstance(self.max_workers, int) and self.max_workers > 0):
            raise ValueError('max_workers must be a positive integer')
        if not (isinstance(self.request_timeout, (int, float)) and self.request_timeout > 0):
            raise ValueError('request_timeout must be > 0')
        if not (isinstance(self.rate_limit_min_interval, (int, float)) and self.rate_limit_min_interval >= 0):
            raise ValueError('rate_limit_min_interval must be >= 0')
        if not (isinstance(self.per_host_burst_capacity, int) and self.per_host_burst_capacity >= 1):
            raise ValueError('per_host_burst_capacity must be an integer >= 1')
        for name, val in (
            ('level2_weight', self.level2_weight),
            ('all_weight', self.all_weight),
            ('same_domain_penalty', self.same_domain_penalty),
            ('depth_penalty_shallow', self.depth_penalty_shallow),
            ('depth_penalty_deep', self.depth_penalty_deep),
        ):
            if not isinstance(val, (int, float)):
                raise ValueError(f'{name} must be numeric')
        if not (self.level2_weight >= 0 and self.all_weight >= 0):
            raise ValueError('level2_weight and all_weight must be >= 0')
        if self.short_path_bonus > 0:
            raise ValueError(
                'short_path_bonus should be <= 0 (negative for bonus)')

    # Get the number of crawled urls
    def get_count(self):
        return len(self.crawled)


class webcrawler_task:
    def __init__(self, crawl_list_path, logging_config: Optional[Dict[str, Any]] = None):
        self.crawl_list_path = crawl_list_path
        with open(crawl_list_path, 'r') as f:
            urls = f.readlines()
        self.urls = [url.strip() for url in urls if url.strip()]

        self.crawl_complete = False
        self.start_time = None
        self.end_time = None
        self.logger: Optional[logging.Logger] = None

        # Logging/output settings (externalizable)
        logging_config = logging_config or {}
        self.log_output_dir: str = logging_config.get('output_dir', '.')
        self.log_filename_pattern: str = logging_config.get(
            'filename_pattern', 'log_{seed}.txt')
        rotation_cfg = logging_config.get('rotation', {})
        self.log_rotation_enabled: bool = bool(
            rotation_cfg.get('enabled', False))
        self.log_rotation_max_bytes: int = int(
            rotation_cfg.get('max_bytes', 10 * 1024 * 1024))
        self.log_rotation_backup_count: int = int(
            rotation_cfg.get('backup_count', 5))

    def start_task(self):
        if self.logger:
            self.logger.info("task started with %d seed urls", len(self.urls))
        self.start_time = time_module.time()
        self.crawler.crawl()
        if self.crawler.completed:
            self.end_time = time_module.time()
            duration = self.end_time - self.start_time
            total_crawled = self.crawler.get_count()
            if self.logger:
                self.logger.info(
                    "task finished total_crawled=%s duration=%.2fs",
                    total_crawled,
                    duration,
                )
        self.crawl_complete = True

    def _resolve_log_path(self) -> str:
        seed = os.path.splitext(os.path.basename(self.crawl_list_path))[0]
        filename = self.log_filename_pattern.format(seed=seed)
        os.makedirs(self.log_output_dir, exist_ok=True)
        return os.path.join(self.log_output_dir, filename)

    def setup_logging(self) -> RotatingFileHandler:
        file_path = self._resolve_log_path()
        logger_name = f'crawler.{os.path.splitext(os.path.basename(self.crawl_list_path))[0]}'
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.INFO)
        for handler in list(logger.handlers):
            logger.removeHandler(handler)
            handler.close()

        max_bytes = self.log_rotation_max_bytes if self.log_rotation_enabled else 0
        handler = RotatingFileHandler(
            file_path,
            maxBytes=max_bytes,
            backupCount=self.log_rotation_backup_count,
            encoding='utf-8',
        )
        formatter = logging.Formatter(
            '%(asctime)s %(levelname)s %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.propagate = False

        self.logger = logger
        return handler

    def teardown_logging(self, handler: RotatingFileHandler) -> None:
        if self.logger:
            self.logger.removeHandler(handler)
        handler.close()
        self.logger = None


def run_task(task: webcrawler_task, crawler_kwargs: Dict[str, Any]) -> None:
    handler = task.setup_logging()
    try:
        crawler_options = dict(crawler_kwargs)
        crawler_options['logger'] = task.logger
        task.crawler = webcrawler_BFS(task.urls, **crawler_options)
        task.start_task()
    except Exception as exc:
        if task.logger:
            task.logger.exception(
                "crawler task %s failed", task.crawl_list_path)
        else:
            logging.error("crawler task %s failed: %s",
                          task.crawl_list_path, exc, exc_info=True)
        task.crawl_complete = True
    finally:
        task.teardown_logging(handler)


def load_config(path: str = 'crawler_config.json') -> Dict[str, Any]:
    try:
        with open(path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


if __name__ == '__main__':

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(message)s')

    cfg = load_config()
    seed_files: List[str] = cfg.get(
        'seed_files', ['crawl_list1.txt', 'crawl_list2.txt'])
    crawler_kwargs = cfg.get('crawler', {})

    log_cfg = cfg.get('logging', {})
    tasks = [webcrawler_task(seed, logging_config=log_cfg)
             for seed in seed_files]

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(tasks)) as executor:
        futures = [executor.submit(run_task, task, crawler_kwargs)
                   for task in tasks]
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as exc:
                logging.error("task execution raised an error: %s",
                              exc, exc_info=True)

    logging.info("All tasks completed")
