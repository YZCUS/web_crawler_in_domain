Files in this submission:
    1. webcrawler.py (Source code)
    2. crawl_list1.txt (Seed list1 to crawl)
    3. crawl_list2.txt (Seed list2 to crawl)
    4. log_crawl_list1.txt (Result of list1)
    5. log_crawl_list2.txt (Result of list2)
    6. readme.txt (This file contain infomation of this directory)
    7. explain.txt (This file explain how source code work and disclose resourse used)

Libraries required:
    1. Python         3.12.6
    2. beautifulsoup4 4.12.3
    3. pybloom_live   4.0.0

Instructions:
    1. Install required libraries via pip tool
    2. Execute Python webcrawler.py or Python3 webcrawler.py on your terminal
    3. Progress will print out on terminal
    4. You may need I/O permission for writing log into your local device.

Limitations and Parameters:
    For this code, there are some parameters you can adjust to have best performance in your end:
        1. max_depth:
           With this setting, links deeper than this value will not be explored
        2. max_crawl:
           This parameter determine how many sites your desired to crawl
        3. worker_nums:
           This number can help you to control the level of concurrence
        4. parameters inside get_priority method:
           These parameters can control the behavior of this crawler
        5. timeout and max_redirection:
           Setting reasonable values can avoid infinite redirection or deadlock and have better performance
        6. Above parameters should be considered by your local configurations and network connections