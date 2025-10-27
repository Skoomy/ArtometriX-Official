import logging
import sys

__version__ = "0.0.1"

__author__ = "ArtometriX"

# format = "[%(asctime)s][%(levelname)s][%(filename)s][%(module)s.%(funcName)s:%(lineno)d][PID%(process)d] %(message)s"
_format = (
    "[%(asctime)s][%(levelname)s][%(filename)s][%(module)s.%(funcName)s]" " %(message)s"
)

datefmt = "%Y-%m-%d %H:%M:%S"


logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format=_format,
    datefmt=datefmt,
)
logger = logging.getLogger(__name__)
