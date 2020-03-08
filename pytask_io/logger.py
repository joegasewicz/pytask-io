import logging
import os


def set_log_level():
    """
    Example::
        export PYTASKIO_DEBUG=1
    :return:
    """
    PYTASKIO_DEBUG: str = os.getenv("PYTASKIO_DEBUG") or "0"
    if int(PYTASKIO_DEBUG) == 1:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)
    return logging.getLogger(__name__)


logger = set_log_level()
