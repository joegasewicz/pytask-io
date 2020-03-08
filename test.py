from pytask_io import PyTaskIO
from time import sleep
import concurrent.futures
import threading
pytask = PyTaskIO(
            store_host="localhost", store_port=6379, store_db=0, workers=1
        )
pytask.run()
print(concurrent.futures.as_completed())
sleep(2)
pytask.stop()
