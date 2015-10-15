import multiprocessing
bind = "127.0.0.1:18000"
workers = multiprocessing.cpu_count() * 2 + 1
