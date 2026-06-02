import asyncio

from forestmq.logger import log


class TCP:

    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port

    async def handler(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        pass


    async def serve(self):
        s = await asyncio.start_server(self.handler, self.host, self.port)
        log.info(f"Starting server on http://{self.host}:{self.port}")

        async with s:
            await s.serve_forever()
