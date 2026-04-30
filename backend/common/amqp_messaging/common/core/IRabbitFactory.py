class IRabbitFactory:

    async def __call__(
        self,
    ):
        raise NotImplementedError()

    async def close(self) -> None:
        raise NotImplementedError()
