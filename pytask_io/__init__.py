"""
    - Concepts
        - PYTASKIO / CLIENT: Is the client.
        - WORKERS: Processes to handle unit of work
        - QUEUE: where the tasks are stored.
        - EVENT LOOP: the process which tasks are run on.
        - STORE: Redis is the store used to store data returned from tasks.

    - Abstract
        - A client issues a task to be enqueued on the QUEUE.
        - A worker pulls tasks off the queue, deserializes & runs them on an asyncIO EVENT LOOP.

    - Setup
        - Redis is the message broker.

"""
from .pytask_io import PyTaskIO

if __name__ == "__main__":
    pass
