from aio_pika.abc import AbstractRobustChannel


class IRabbitChannel:

    async def get_consumption_channel(self) -> AbstractRobustChannel:
        raise NotImplementedError()

    async def get_publication_channel(self) -> AbstractRobustChannel:
        raise NotImplementedError()

    async def close(self) -> None:
        raise NotImplementedError()
