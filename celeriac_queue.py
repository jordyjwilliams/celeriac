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
    def _get_first_task(self) -> list[dict] | None:
        """Pull the first task from the queue.

        Returns:
            list[dict] | None: The first task from the queue.
            None if the queue is empty.

        """
        try:
            task = self.task_queue.get_nowait()
            logger.debug("Got first task %s", task)
        except Empty:
            return None
        return task

    def _collect_tasks_into_buffer(self, first_task: list[dict]) -> None:
        """Collect tasks into the buffer.

        Seperation here was to allow for single tasks to be processed immediately.
        EG if many requests are not being sent at once.
        """
        # Add first task to buffer
        with self.buffer_lock:
            self.buffer.append(first_task)

        # immediately collect all available tasks (non-blocking)
        while len(self.buffer) < self.max_batch_size:
            try:
                next_task = self.task_queue.get_nowait()
                with self.buffer_lock:
                    self.buffer.append(next_task)
                    logger.debug("Buffer: Added Task. Buffer Size %d", len(self.buffer))
            except Empty:
                break

    def _send_and_clear_buffer(self, logging_message: str) -> None:
        """Send buffer to client and clear.

        logging_message is used to identify the reason for the send.
        EG full, partial, single.
        """
        batch = self.buffer.copy()  # potentially could be improved memory wise here.
        self.buffer.clear()
        logger.debug("Sending %s batch of %d tasks", logging_message, len(batch))
        self.client.receive_batch(batch)

    def _wait_and_process_batch(self) -> None:
        """Wait for timeout or more tasks, then process the batch."""
        start_time = time.time()
        deadline = start_time + self.max_wait_seconds
        logger.debug(
            "Batching: buffer size: %d timeout deadline: %d",
            len(self.buffer),
            deadline,
        )

        while len(self.buffer) < self.max_batch_size:
            timeout = deadline - time.time()
            if timeout <= 0:
                self._send_and_clear_buffer("Batching: Timeout reached")
                break

            try:
                next_task = self.task_queue.get_nowait()
                with self.buffer_lock:
                    self.buffer.append(next_task)
                    logger.debug(
                        "Batching: Added Task to batch. Buffer size: %d",
                        len(self.buffer),
                    )

                    # During batching, if reach max buffer size. Send.
                    if len(self.buffer) >= self.max_batch_size:
                        self._send_full_batch()
                        break

            except Empty:
                logger.debug("Batching: Queue Empty. buffer size: %d", len(self.buffer))
                with self.buffer_lock:
                    if self.buffer:
                        self._send_and_clear_buffer("Batching: Queue Empty: Remaining Tasks")
                break

        logger.debug("Batching: Complete. Buffer Size: %d", len(self.buffer))

    def _process_buffer(self) -> None:
        """Process the buffer based on its current state.

        * Buffer empty: do nothing
        * Sends buffer (_send_and_clear_buffer) if:
            * Buffer full
            * Buffer contains one task
            * Task queue empty
        * Otherwise: wraps _wait_and_process_batch
        """
        with self.buffer_lock:
            buffer_size = len(self.buffer)
            if buffer_size == 0:
                return
            if buffer_size == self.max_batch_size:
                self._send_and_clear_buffer("Buffer Full")
                return
            if buffer_size == 1:
                self._send_and_clear_buffer("Single Task")
                return
            if self.task_queue.empty():
                self._send_and_clear_buffer("Queue Empty after many messages")
                return
        # Otherwise, wait for timeout or more tasks
        self._wait_and_process_batch()

    def _dispatcher(self) -> None:
        """Dispatcher class logic."""
        logger.debug("Dispatcher started")

        while not self.stop_event.is_set():
            # Get the first task (non-blocking)
            first_task = self._get_first_task()
            if first_task is None:
                time.sleep(0.001)  # Brief sleep to avoid busy waiting
                continue

            # Collect tasks into buffer
            self._collect_tasks_into_buffer(first_task)
            # Process the buffer based on its state
            self._process_buffer()
