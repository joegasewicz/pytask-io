import asyncio
import json

from forestmq.logger import log
from forestmq.http import (
    Http,
    Response,
    Request,
    Status,
)
from forestmq.router import Router


class TCP:

    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.http = Http(
            request=Request(),
            response=Response(),
        )

    async def handler(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        request_line = await reader.readline()
        request_line_str = request_line.decode("iso-8859-1")
        method, path, version = request_line_str.rstrip("\r\n").split(" ", 2)

        router = Router(path=path, method=method)
        data, status = router.route()

        headers = self.http.set_headers(status_code=status, body=data)

        writer.write(headers)
        writer.close()
        await writer.wait_closed()

    async def serve(self):
        s = await asyncio.start_server(self.handler, self.host, self.port)
        log.info(f"Starting server on http://{self.host}:{self.port}")

        async with s:
            await s.serve_forever()
