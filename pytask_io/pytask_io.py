"""
Pytask IO Class
===============
"""
import asyncio
from typing import List, Callable
import redis
from threading import Thread
from typing import Dict, Any, Union

from pytask_io.task_queue import (
    poll_for_store_results,
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

    #: The `queue_store` is available to work with & can be accessed on the PyTaskIO instance
    #: & all the available methods from the store framework used will be available on this object.
    #: Foe example::
    #:
    #:    # Example fpr default Redis store setting a new key
    #:    pytaskio = PytaskIO()
    #:    pytaskio.queue_store.set('myfield', 'my_value')
    queue_store: redis.Redis

    #: The thread that the asyncio event loop runs in. This thread has been tagged with the name of
    #: `event_loop`. The thread will die gracefully when PytaskIO calls `event_loop.join()` for you. If
    #: you require custom handling of the `event_loop` thread, then you can access it directly using
    #: the :class:`pytask_io.loop_thread` object. Example::
    #:
    #:    pytaskio = PytaskIO()
    #:    pytaskio.loop_thread.is_alive() # check if the thread is still alive
    loop_thread: Thread

    #: The main loop that is used by PytaskIO. If you wish to handle some of the asyncio behavior of the
    #: main loop, then you can access the asyncio object directly with :class:`pytask_io.main_loop`.
    main_loop: asyncio.AbstractEventLoop

    #: The pole loop that is available for PytaskIO public methods such as :class:`pytask_io.poll_for_task`
    #: If you wish to handle some of the asyncio behavior of the pole loop, then you can access the asyncio
    #: object directly with :class:`pytask_io.pole_loop`.
    pole_loop: asyncio.AbstractEventLoop

    #: The store host name. Default is `localhost`.
    store_host: str = "localhost"

    #: The store port. Default is 0
    store_port: int = 6379

    #: The store db number. Default is 6379
    store_db: int = 0

    #: The amount of workers in the asyncio task queue. Default is 1.
    workers: int = 1

    _polled_result: Dict = None

    def __init__(self, *args, **kwargs):
        self.store_host = kwargs.get("store_host") or self.store_host
        self.store_port = kwargs.get("store_port") or self.store_port
        self.store_db = kwargs.get("store_db") or self.store_db
        self.workers = kwargs.get("workers") or self.workers

    def run(self):
        """
        Starts an event loop on a new thread with a name of `event_loop`
        Example::

            # Create a `pytaskio` object & run the event loop in new thread:
            pytaskio = PyTaskIO()
            pytaskio.run()

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

    def _connect_to_store(self) -> redis.Redis:
        """
        Private method that is used to connect to the client queue & store.
        This will change when we introduce more store & queue options.
        :return: None
        """
        return redis.Redis(
            host=self.store_host,
            port=self.store_port,
            db=self.store_db
        )

    def stop(self) -> None:
        """
        Method to elegantly stop the asyncio event loop & join the `event_loop` thread.
        This method will only be executed when all active task have finished executing.
        If there are any pending tasks left in the clients queue then these can be executed
        once PytaskIO is run again.
        Example::

            pytaskio = PyTaskIO()
            pytaskio.run()
            try:
                metadata = pytask.add_task(send_email, title, body)
            except RunTimeError:
                pytaskio.stop()

        :return: None
        """
        # stop event loop
        current_loop = asyncio.get_event_loop()
        current_loop.call_soon_threadsafe(current_loop.stop)
        self.loop_thread.join()

    def _run_event_loop(self) -> None:
        """
        :return: None
        """
        self.main_loop = asyncio.new_event_loop()
        self.main_loop.create_task(client(self.queue_client, self.workers))
        self.main_loop.run_forever()
        logger.info("asyncIO event loop running")

    def add_task(self, unit_of_work: Callable, *args) -> Dict[str, Any]:
        """
        :class:`pytask_io.add_task` method take a function as a first argument
        & the function arguments as the next arguments. When :class:`pytask_io.add_task`
        is called, it will return a dictionary with some useful data. Although the data
        returned can be used in many ways, the easiest & most straight forward use is to
        pass this dict directly to :class:`pytask_io.get_task`.
        Example::

            # Create a `pytaskio` object & run the event loop in new thread:
            pytaskio = PyTaskIO()
            pytaskio.run()

            # the `add_task` method task in a function as a first argument
            metadata = pytask.add_task(send_email, title, body)


            # Later we can use the `metadata` result to pass to `add_task`
            result = get_task(metadata)

        :param unit_of_work: A callable / executable Python function
        :param args: The list of arguments required by `unit_of_work`
        :return: metadata
        """
        return init_unit_of_work(self.queue_client, self.queue_store, unit_of_work, *args)

    def get_task(self, unit_of_work_metadata: Dict[str, Any]) -> Union[Dict[str, Any], None]:
        """
        Method to get the task results from the store. This function can be called
        directly after executing the :class:`pytask_io.add_task`. If the results
        referenced from the `metadata` dict *see below* are available when this method
        is called, then the result is return otherwise `None` is returned. If the
        result is `None`, then you have to retry to call this method again, sometime
        in the future.
        Example::

            # Create a `pytaskio` object & run the event loop in new thread:
            pytaskio = PyTaskIO()
            pytaskio.run()

            # the `add_task` method task in a function as a first argument
            metadata = pytask.add_task(send_email, title, body)


            # Later we can use the `metadata` result to pass to `add_task`
            result = get_task(metadata)

        :param unit_of_work_metadata: Dict[str, Any] -
        :return Union[Dict, bool]: The result is non blocking
        """
        result = get_uow_from_store(self.queue_store, unit_of_work_metadata["store_name"])
        if result == "":
            return None
        return result

    def poll_for_task(self, task_meta: Dict, **kwargs) -> Union[Dict[str, Any], bool]:
        """
        Warning: This method is still in a beta state, use with care!
        Blocking function to be used either with an async library or from a separate thread
        from the client application's main thread.
        This method will create an asyncio event loop & make periodic requests to the store.
        Once the results are returned, the loop with be stopped and the main thread will
        be available once again.
        Example::

            # Create a `pytaskio` object & run the event loop in new thread:
            pytaskio = PyTaskIO()
            pytaskio.run()

            # the `add_task` method task in a function as a first argument
            metadata = pytask.add_task(send_email, title, body)


            # Later we can use the `metadata` result to pass to `add_task`
            result = poll_for_task(metadata, tries=100, interval=60)

        :param task_meta:
        :param kwargs:
        :key tries: The amount of times the method polls the store for the task execution results.
        :key interval: The time in seconds for each try
        :return:
        """
        tries = kwargs.get("tries")
        interval = kwargs.get("interval")
        if tries:
            # Create event loop in the main thread
            self.pole_loop = asyncio.get_event_loop()
            # Coroutine to pole store on event loop
            get_store_results = poll_for_store_results(self.queue_store, task_meta, interval, tries)
            asyncio.set_event_loop(self.pole_loop)
            self._polled_result = self.pole_loop.run_until_complete(get_store_results)
        if self._polled_result:
            task_result_data = {
                "data": self._polled_result,
                **task_meta,
            }
            return task_result_data
