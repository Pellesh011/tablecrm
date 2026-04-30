import asyncio
from collections import OrderedDict


class AsyncLRU:
    def __init__(self, maxsize=128, cache_none: bool = True):
        self.cache = OrderedDict()
        self.maxsize = maxsize
        self.cache_none = cache_none
        self.lock = asyncio.Lock()

    async def get(self, key, func, *args, **kwargs):
        async with self.lock:
            if key in self.cache:
                self.cache.move_to_end(key)
                return self.cache[key]
            value = await func(*args, **kwargs)
            # Не кэшируем None, чтобы не "залипать" на разовых ошибках внешнего сервиса
            if value is None and not self.cache_none:
                return None
            self.cache[key] = value
            if len(self.cache) > self.maxsize:
                self.cache.popitem(last=False)
            return value
