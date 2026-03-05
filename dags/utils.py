import logging
import os

def setup_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    log_path = os.path.join(os.path.dirname(__file__), "..", "logs", "Ejecucion.log")

    file_handler = logging.FileHandler(log_path)
    file_handler.setLevel(logging.INFO)
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

def load_credentials(filename):
    with open(filename, 'r',encoding='utf-8') as f:
        return f.read().strip()