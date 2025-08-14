## Webcrawler (BFS-based, NZ domain focused)

A multi-threaded web crawler centered on a BFS strategy with a priority queue, targeting only the `.nz` TLD. It respects robots.txt (with caching and crawl-delay), follows redirects with a safety cap, normalizes hyperlinks aggressively, and uses a Bloom filter to reduce memory footprint.

### Key Features
- Multi-threaded fetching with `ThreadPoolExecutor`
- robots.txt cache with per-domain `crawl-delay` politeness
- Priority queue that promotes domain diversity and penalizes depth
- Scalable Bloom Filter to reduce duplicate enqueues
- Custom redirect handler with a maximum redirection limit

### Repository Layout
- `webcrawler.py`: main program and core classes (`robots_cache`, `Redirect_Handler`, `webcrawler_BFS`, `webcrawler_task`).
- `crawl_list1.txt`, `crawl_list2.txt`: example seed lists (one URL per line).
- `log_crawl_list1.txt`, `log_crawl_list2.txt`: example outputs (ignored by VCS).
- `explain.txt`: high-level explanation of flow and design.
- `readme.txt`: original text readme (kept for reference).

### Environment
- Python 3.12+
- Dependencies: see `requirements.txt`

### Installation
```bash
python3 -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### Usage
1) Prepare seed lists: put start URLs in `crawl_list1.txt` and/or `crawl_list2.txt` (one per line, include scheme, e.g., `https://example.nz/`).

2) Run:
```bash
python webcrawler.py
```

3) Outputs:
- The crawler writes `log_crawl_list1.txt` and `log_crawl_list2.txt` into the repo root. Each line contains time, depth, HTTP status, size, and URL.

### Core Techniques and Concepts
This project applies standard IR/crawling techniques adapted for breadth-first exploration with fairness and politeness constraints:

- URL and HTML handling
  - Fetching with a desktop User-Agent and reasonable timeouts
  - Content-type filter: only process `text/html`
  - HTML parsed by BeautifulSoup to extract `<a href>` links
  - URL normalization rules:
    - Absolute resolution via `urljoin`
    - Lowercase scheme/host, remove fragments
    - Heuristic trailing slash for directory-like paths
    - Drop tracking query params (e.g., `utm_*`), collapse RSS/XML endpoints
  - Non-HTML and non-HTTP(S) schemes are skipped (images, media, `mailto:`, `javascript:`, etc.)

- Scope restriction
  - Only `.nz` domains are crawlable (`in_nz_domain`) to bound the crawl space and comply with assignment requirements

- Politeness and robots.txt
  - Per-domain robots.txt is cached with an expiry (`robots_cache`)
  - `can_fetch` is consulted before enqueuing/visiting
  - `crawl_delay` is respected between fetches to avoid overloading hosts

- Frontier data structures
  - A min-heap priority queue holds tuples `(priority, depth, url)`
  - A visited set guards already-fetched final URLs (post-redirect)
  - A Scalable Bloom Filter tracks seen links to reduce duplicate enqueues at scale
  - Thread-safe operations are protected by fine-grained locks (visited, links, PQ, logs, and domain stats)

- Priority model (domain diversity and depth-aware)
  - Penalize deeper pages; small bonus for short paths
  - Penalize links staying in the same domain as the parent
  - Maintain occurrence counts for:
    - all-domain: full host without leading `www.`
    - level-2 domain: last two labels (e.g., `co.nz`, `ac.nz`, or `example.nz`)
  - Convert counts to ratios against a running total; subtract weighted ratios from priority to prefer less-seen domains
  - All counters are updated with locks to remain thread-safe

- Redirect handling
  - A custom `HTTPRedirectHandler` enforces a maximum number of redirects to prevent cycles and long chains

- Concurrency model
  - Fetching is I/O-bound; threads hide latency while maintaining politeness
  - `ThreadPoolExecutor(max_workers=8)` is configurable based on machine capability
  - Exceptions from worker futures are surfaced and logged without crashing the crawl

- Logging
  - Each processed URL logs: timestamp, content size (if HTML), depth, final URL, and HTTP status
  - A background loop drains an internal deque and writes to `log_<seed>.txt`

### Configuration Knobs (inside `webcrawler.py`)
- `max_depth`: maximum exploration depth (default 100)
- `max_crawl`: maximum number of pages to crawl (default 200)
- Worker count: `ThreadPoolExecutor(max_workers=8)`
- robots cache expiry: `robots_cache(expired_time=3600)` seconds
- Priority weights: tune in `get_priority` (depth penalty, same-domain penalty, short-path bonus, level-2/all-domain weights)

### Ethics and Legal
- Only crawl `.nz` domains, and always honor robots.txt and crawl delays
- Use this code only for lawful, authorized data collection

### Troubleshooting
- If installation fails, upgrade pip: `python -m pip install -U pip`
- If timeouts or redirect loops occur, adjust request timeouts and `max_redirections`
- Log files can be large; rotate/clean them as needed (they are ignored by VCS)

### Development Tips
- Start with a small seed set and low `max_crawl` to verify behavior, then scale up
- To change tasks or seeds, edit the `__main__` section and the target files

### License
Not specified. Consider adding a `LICENSE` file if you intend to open-source.


