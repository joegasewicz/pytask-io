"""
Pytask IO Class
===============
"""
import asyncio
from typing import List, Callable
import redis
import time
from threading import Thread, Event, currentThread#
from typing import Dict, Any, Union

from pytask_io.task_queue import (
    pole_for_store_results,
)
from pytask_io.logger import logger
from pytask_io.client import client
from pytask_io.store import init_unit_of_work, get_uow_from_store


class PyTaskIO:
    """
    :kwargs:
    :key store_host: The store host name. Default is `localhost`.
    :key store_port: The store port. Default is 0
    :key store db: The store db number. Default is 6379
    :key workers: The amount of workers in the asyncio task queue. Default is 1.
    """

    #: PytaskIO is a python task queue that leverages CPython's asyncio library
    #: to make long running task trivial. The library aims to make the public
    #: API as simple and intuitive as possible.
    #: Basic usage. Example::
    #:
    #:    Starts the task runner
    #:          pytask = PytaskIO(
    #:          store_port=8080,
    #:          store_host="localhost",
    #:          broker="redis",  # rabbitmq coming soon...
    #:          db=0,
    #:     )
    #:
    #:     # Start the PytaskIO task queue on a separate thread.
    #:     pytask.run()
    #:
    #:     # Handle a long running process, in this case a send email function
    #:     metadata = pytask.add_task(send_email, title, body)
    #:
    #:     # Try once to get the results of your email sometime in the future
    #:     result = get_task(metadata)
    #:
    #:     # Stop PytaskIO completly (This will not effect any units of work that havent yet executed)
    #:     pytask.stop()
    #:
    #: The connected queue client object. Use this object exactly as you would if you were referencing
    #: the queue's client directly. Example::
    #:
    #:    # Example for default Redis queue pushing a task into the queue
    #:    pytaskio = PytaskIO()
    #:    results = pytaskio.queue_client.lpush("my_queue", my_task)
    queue_client: redis.Redis

    queue_store: redis.Redis
    loop_thread: Thread
    main_loop: asyncio.AbstractEventLoop
    pole_loop: asyncio.AbstractEventLoop
    task_results: str = "task_results"
    polled_result: Dict = None
    current_thread = None
    store_host: str = "localhost"
    store_port: int = 6379
    store_db: int = 0

    def __init__(self, *args, **kwargs):
        self.store_host = kwargs.get("store_host") or self.store_host
        self.store_port = kwargs.get("store_port") or self.store_port
        self.store_db = kwargs.get("store_db") or self.store_db

    def run(self):
        """
        Starts an event loop on a new thread with a name of `event_loop`
        :return:
        """
        self.queue_client = self._connect_to_store()
        self.queue_store = self._connect_to_store()
        self.loop_thread = Thread(
            name="event_loop",
            target=self._run_event_loop,
        )
        self.loop_thread.daemon = True
        self.loop_thread.start()

    def _connect_to_store(self):
        return redis.Redis(
            host=self.store_host,
            port=self.store_port,
            db=self.store_db
        )

    def stop(self):
        """
        Stops the event loop
        :return:
        """
        # stop event loop
        current_loop = asyncio.get_event_loop()
        current_loop.call_soon_threadsafe(current_loop.stop)
        self.loop_thread.join()

    def _run_event_loop(self):
        self.main_loop = asyncio.new_event_loop()
        self.main_loop.create_task(client(self.queue_client))
        self.main_loop.run_forever()
        logger.info("asyncIO event loop running")

    def add_task(self, unit_of_work, *args) -> Dict[str, Any]:
        """
        Adds units of work to the queue client
        :param unit_of_work: A callable / executable Python function
        :param args: The list of arguments required by `unit_of_work`
        :return:
        """
        return init_unit_of_work(self.queue_client, unit_of_work, *args)

    def get_task(self, unit_of_work_metadata: Dict[str, Any]) -> Union[Dict[str, Any], bool]:
        """
        :param unit_of_work_metadata: Dict[str, Any] -
        :return Union[Dict, bool]: The result is non blocking
        """
        result = get_uow_from_store(unit_of_work_metadata["store_name"])
        return result

    def poll_for_task(self, task_meta: Dict, *args, **kwargs) -> Union[Dict[str, Any], bool]:
        """
        Blocking function to be used either with an async library or from a separate thread
        from the client application's main thread. Example::

        :param task_meta:
        :param args:
        :param kwargs:
        :return:
        """
        tries = kwargs.get("tries")
        interval = kwargs.get("interval")
        if tries:
            # Create event loop in the main thread
            self.pole_loop = asyncio.get_event_loop()
            # Coroutine to pole store on event loop
            get_store_results = pole_for_store_results(self.queue_store, task_meta, interval, tries)
            asyncio.set_event_loop(self.pole_loop)
            self.polled_result = self.pole_loop.run_until_complete(get_store_results)
        if self.polled_result:
            task_result_data = {
                "data": self.polled_result,
                **task_meta,
            }
            return task_result_data
