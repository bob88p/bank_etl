"""utils/logger.py – Centralised logging setup."""

import logging
import os
import sys
from datetime import datetime

def get_logger(name: str, log_dir: str = "logs") -> logging.Logger:
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"etl_{datetime.today().strftime('%Y%m%d')}.log")

    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)-25s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    handler_file = logging.FileHandler(log_file, encoding='utf-8')
    
    # Force console to use UTF-8
    if sys.stdout.encoding != 'utf-8':
        # For Windows: wrap stdout with UTF-8 writer
        import codecs
        sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer)
        sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer)
    handler_console = logging.StreamHandler()
    
    for h in (handler_file, handler_console):
        h.setFormatter(fmt)

    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.addHandler(handler_file)
        logger.addHandler(handler_console)
    logger.setLevel(logging.DEBUG)
    return logger