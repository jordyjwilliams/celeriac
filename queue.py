
from executor import MockTaskExecutor
from tasks import CeleriacTask
import logging

logger = logging.getLogger(__name__)


class Celeriac:
    def __init__(self, name):
        self.name = name
        self.tasks = {}
        self.client = MockTaskExecutor()

    def _name_from_func(self, func):
        return f"{func.__module__}${func.__name__}"

    def register(self, func=None):
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
