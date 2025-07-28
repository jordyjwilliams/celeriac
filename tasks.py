import json
from typing import Callable, TYPE_CHECKING

if TYPE_CHECKING:
    from queue import Celeriac


class CeleriacTask:
    def __init__(
        self,
        name: str,
        func: Callable,
        parent: "Celeriac",
    ):
        self.name = name
        self.func = func
        self.parent = parent

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)

    def serialize_call(self, *args, **kwargs) -> dict:
        base_result = {
            "task": self.name,
            "args": args,
            "kwargs": kwargs,
        }

        return base_result

    def to_json(self, *args, **kwargs) -> str:
        return json.dumps(self.serialize_call(*args, **kwargs))

    def delay(self, *args, **kwargs):
        payload = self.serialize_call(*args, **kwargs)
        return self.parent.send_task(payload)

    def __repr__(self) -> str:
        module, name = self.name.split("$")
        return f"<@celeriac_task {name} in {module}>"

    def __str__(self) -> str:
        return self.__repr__()
