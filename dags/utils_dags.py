import logging

def setup_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Crear un manejador de archivo
    file_handler = logging.FileHandler('logs/Ejecucion.log')
    file_handler.setLevel(logging.INFO)
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    # Crear un manejador de consola
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

def load_credentials(filename):
    with open(filename, 'r') as f:
        return f.read().strip()