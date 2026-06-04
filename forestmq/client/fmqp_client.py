class FMQPClient:

    HEADER

    def __init__(
            self,
            *,
            host: str = "127.0.0.1",
            port: int = 7171,
    ):
        self.host = host
        self.port = port

    async def connect(self) -> None:
        pass

    async def close(self) -> None:
        pass

    async def send(self) -> None:
        pass

    async def recv(self) -> None:
        pass
