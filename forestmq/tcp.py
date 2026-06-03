import asyncio

from forestmq.logger import log
from forestmq.http import (
    BaseHttp,
    Response,
    Request,
)
from forestmq.router import Router
from forestmq.protocol import Protocol


class TCP:

    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.http = BaseHttp(
            request=Request(),
            response=Response(),
        )


    async def handler(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        request_line = await reader.readline()
        headers = "".encode("utf-8")

        # Implement FMQP
        protocol = Protocol()

        if protocol.is_fmqp():
            headers = b""

        else:
            # ELSE implement Http
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
