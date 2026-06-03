import asyncio
import json

from forestmq.logger import log


class TCP:

    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port

    async def handler(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        request_line = await reader.readline()
        request_line_str = request_line.decode("iso-8859-1")
        method, path, version = request_line_str.rstrip("\r\n").split(" ", 2)

        json_data = {
            "status": "OK",
        }
        body = json.dumps(json_data).encode("utf-8")
        body_len = len(body)
        headers = "HTTP/1.1 200 OK\r\n"
        headers += "Server: ForestMQ\r\n"
        headers += "Content-Type: application/json\r\n"
        headers += f"Content-Length: {body_len}\r\n"
        headers += "Connection: close\r\n"
        headers += "\r\n"

        writer.write(headers.encode("utf-8") + body)
        writer.close()
        await writer.wait_closed()

    async def serve(self):
        s = await asyncio.start_server(self.handler, self.host, self.port)
        log.info(f"Starting server on http://{self.host}:{self.port}")

        async with s:
            await s.serve_forever()
