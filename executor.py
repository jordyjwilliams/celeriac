import logging

logger = logging.getLogger(__name__)


class MockTaskExecutor:
    def receive_batch(self, payload: list[dict]):
        # Let's pretend if you see this log that the task was executed
        logger.debug(f"Executed tasks {payload}")
