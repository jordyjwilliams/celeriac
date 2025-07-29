import logging
import threading
import time
from queue import Empty, Queue
from typing import Callable

from const import BATCH_MAX_TASK_NUMBER, BATCH_MAX_WAIT_TIME_MS
from executor import MockTaskExecutor
from tasks import CeleriacTask

logger = logging.getLogger(__name__)


class Celeriac:
    def __init__(self, name):
        self.name = name
        self.tasks = {}
        self.client = MockTaskExecutor()
        self.max_batch_size = BATCH_MAX_TASK_NUMBER
        self.max_wait_seconds = BATCH_MAX_WAIT_TIME_MS / 1000.0

        # Threading components
        # Originally explored using `asyncio` and `culsans` here.
        # Could not get this to nicely work without:
        # * Modifying existing tests.
        # * The methods getting overly complex to ensure thread safety.
        # Set max queue size to 3x the batch size to allow for overflow.
        # But without being infinitely large.
        self.task_queue = Queue(maxsize=self.max_batch_size * 3)
        # Tasks as collected from queue. Sent when max_batch_size is reached.
        # OR when timeout is hit.
        # However buffer will only be cleared once max_batch_size is reached.
        self.buffer = []
        # Avoid race-conditions.
        self.buffer_lock = threading.Lock()
        # Event to enable the dispatcher to stop.
        self.stop_event = threading.Event()
        self.dispatcher_thread = None

    def _name_from_func(self, func) -> str:
        return f"{func.__module__}${func.__name__}"

    def register(self, func=None) -> Callable:
        def _decorate(function):
            name = self._name_from_func(function)
            self.tasks[name] = function
            return CeleriacTask(
                name=name,
                func=function,
                parent=self,
            )

        if func:
            return _decorate(func)

        return _decorate

    def send_task(self, payload):
        # TODO: Implement buffering and batch sending to MockTaskExecutor
        logger.debug(f"Sending task {payload}")
        self.client.receive_batch([payload])
