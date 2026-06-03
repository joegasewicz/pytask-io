import json

from forestmq.http.status import Status


class Response:

    def __init__(self):
        self.status = Status()

    def set_headers(self, *, status_code: int, body: dict) -> str:
        body_len = 0
        body_str = ""
        if body:
            body_str = self._marshal_body(data=body)
            body_len = len(body_str)

        status_code = self.status.get(status_code)

        self.headers = f"HTTP/1.1 {status_code}\r\n"
        self.headers += f"Server: ForestMQ\r\n"
        self.headers += f"Content-Type: application/json\r\n"
        self.headers += f"Content-Length: {body_len}\r\n"
        self.headers += f"Connection: close\r\n"
        self.headers += f"\r\n{body_str}"
        return self.headers

    def _marshal_body(self, *, data: dict) -> str:
       return json.dumps(data)
