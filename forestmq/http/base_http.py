from forestmq.http.response import Response
from forestmq.http.request import Request



class BaseHttp:

    headers: str = None

    def __init__(
        self,
        *,
        request: Request,
        response: Response,
    ):
        self.request = request
        self.response = response

    def set_headers(self, *, status_code: int, body=None) -> bytes:
        """
        :param body:
        :return:
        """
        if body is None:
            body = {}
        headers = self.response.set_headers(status_code=status_code, body=body)
        header_bytes = headers.encode("utf-8")
        return header_bytes
