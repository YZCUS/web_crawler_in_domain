## Webcrawler（NZ 網域 BFS 網頁爬蟲）

一個以 BFS 與優先順序佇列為核心的多執行緒網頁爬蟲，聚焦於 `.nz` 網域，支援 robots.txt、導向處理、超連結正規化，以及以 Bloom Filter 降低記憶體占用。

### 特色
- **多執行緒**：使用 ThreadPoolExecutor 以提升下載效率。
- **遵守 robots.txt**：快取 robots 規則並尊重 `crawl-delay`。
- **優先佇列策略**：依據網域出現頻率與深度動態調整，優先探索「不同且較少被訪問」的網域。
- **Bloom Filter**：避免重複入列的連結，兼顧記憶體效率。
- **導向處理**：自訂 redirect handler，限制最大轉址次數。

### 專案結構
- `webcrawler.py`：主程式與核心類別（`webcrawler_BFS`、`webcrawler_task`）。
- `crawl_list1.txt`, `crawl_list2.txt`：範例種子清單（每行一個 URL）。
- `log_crawl_list1.txt`, `log_crawl_list2.txt`：範例執行輸出（已在 `.gitignore` 忽略）。
- `explain.txt`：爬蟲流程與設計說明。
- `readme.txt`：原始說明文件（保留）。

### 環境需求
- Python 3.12+
- 相依套件見 `requirements.txt`

### 安裝
```bash
python3 -m venv venv
source venv/bin/activate  # Windows 使用: venv\\Scripts\\activate
pip install -r requirements.txt
```

### 使用方式
1) 準備種子清單：在 `crawl_list1.txt`、`crawl_list2.txt` 中放入要起始抓取的 URL（每行一個，需含協定，例如 `https://example.nz/`）。

2) 執行：
```bash
python webcrawler.py
```

3) 產出：
- 於專案根目錄輸出 `log_crawl_list1.txt` 與 `log_crawl_list2.txt`，內容包含時間、深度、狀態碼、大小與 URL。

### 重要設定（於 `webcrawler.py`）
- **`max_depth`**：最大抓取深度（預設 100）。
- **`max_crawl`**：最大抓取頁數（預設 200）。
- **執行緒數量**：`ThreadPoolExecutor(max_workers=8)` 中的 8 可依機器調整。
- **robots 快取時間**：`robots_cache(expired_time=3600)`，秒為單位。
- **優先權策略**：在 `get_priority` 中可調整各權重（深度懲罰、同網域懲罰、短路徑加分、網域頻率比重等）。

### 合規與注意事項
- 僅抓取 `.nz` 網域（`in_nz_domain`）。
- 尊重網站的 `robots.txt` 規範與 `crawl-delay`，避免過度請求。
- 僅用於合法、合規且取得授權的資料蒐集情境。

### 疑難排解
- 若安裝失敗，請先升級 pip：`python -m pip install -U pip`。
- 若連線逾時或頻繁轉址，可調整 `timeout` 與 `max_redirections`。
- 若日誌檔案過大，請定期清理（已透過 `.gitignore` 避免加入版本控管）。

### 開發建議
- 先以少量種子與較小 `max_crawl` 測試，確認策略與效能後再擴大規模。
- 若要更改任務或清單，可修改 `__main__` 中的 `webcrawler_task('your_list.txt')`。

### 授權
未指定（如需開源授權，建議新增 `LICENSE`）。


