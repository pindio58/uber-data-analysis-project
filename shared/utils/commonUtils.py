import logging
import requests
from pathlib import Path
from datetime import datetime 
from zoneinfo import ZoneInfo

curr_time = datetime.now(ZoneInfo('Asia/Kolkata')).strftime("%Y%m%d%H%M%S%f")[:-5]


BASE_DIR = Path(__file__).resolve().parent.parent.parent
# print(BASE_DIR)
LOG_DIR = BASE_DIR / "shared" / "logs"
LOG_FILE = LOG_DIR / f"logs-{curr_time}.log"

def get_logger(name:str):
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.DEBUG)

    # file handler
    file_handler = logging.FileHandler(LOG_FILE, delay=True)
    file_handler.setLevel(logging.DEBUG)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    #format
    formattter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(funcName)s | %(message)s"
    )
    file_handler.setFormatter(formattter)
    console_handler.setFormatter(formattter)

    logger.addHandler(file_handler)
    # logger.addHandler(console_handler)

    return logger



def download_file(url, path="BigMartSales.csv"):
    """Download the CSV dataset."""
    logger = get_logger(__name__)
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"Downloading dataset from {url}")
    response = requests.get(url)
    response.raise_for_status()
    try:
        with open(path, "w") as f:
            f.write(response.text)
            logger.info(f"File downloaded successfully: {path}")
    except ValueError as e:
        logging.debug("Error while loading the file - {e}")
    return str(path)


def delete_file(file_name:str) -> bool:
    logger = get_logger(__name__)
    file_name = Path(file_name)
    logger.info(f"deleting file {file_name}")
    if file_name.exists() and file_name.is_file():
        file_name.unlink()
        logger.info(f"{file_name} deleted successfully")
    else:
        logger.info(f"{file_name} does not exist")