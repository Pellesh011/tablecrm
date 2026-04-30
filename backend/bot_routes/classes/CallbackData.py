import json
from typing import Any, Callable, Dict

from aiogram.types import CallbackQuery


class CallbackData:
    def __init__(self, *args: str):
        self.fields = args

    def new(self, **kwargs: Any) -> str:
        data = {}
        for key in self.fields:
            if key not in kwargs:
                raise ValueError(f"Missing required field: {key}")
            data[key] = kwargs[key]
        return json.dumps(
            data, ensure_ascii=False
        )  # ensure_ascii=False for Unicode support

    def parse(self, callback_data: str) -> Dict[str, Any]:
        try:
            data = json.loads(callback_data)
            for key in self.fields:
                if key not in data:
                    raise ValueError(f"Missing required field: {key}")
            return data
        except (json.JSONDecodeError, ValueError) as e:
            raise ValueError(f"Invalid callback data: {e}") from e

    def filter(self, **config: Any) -> Callable[[CallbackQuery], bool]:
        """
        Creates a filter function for callback query handlers based on the provided config.

        This is a custom filter solution for Aiogram 2.x, as it doesn't have BoundFilter.

        Example:
        ```python
        @dp.callback_query_handler(lambda c: my_callback_data.filter(action='my_action')(c))
        async def handler(query: types.CallbackQuery):
            data = my_callback_data.parse(query.data)
            # ... process data
        ```
        """

        def filter_function(query: CallbackQuery) -> bool:
            try:
                data = self.parse(query.data)
                for key, value in config.items():
                    if data.get(key) != value:
                        return False
                return True
            except ValueError:
                return False

        return filter_function
