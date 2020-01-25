import asyncio
import logging
from toolz.functoolz import compose, curry, pipe
from typing import List, Any, Dict, Tuple, Callable


from pytask_io.worker_queue import create_worker_queue
from pytask_io.event_loop import event_loop


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def client(work_units: List[Callable]):
    """
    - Client
        - Issues tasks to be enqueued.
    :return:
    """
    # Create the worker queue
    queue = create_worker_queue()
    logger.info("PytaskIO: Worker queue started")

    # Create the event loop

    # Serialize units of work (UOW)

    # Push UOW into task queue

    # Get units of work from Redis Store

    # Deserialize UOW
