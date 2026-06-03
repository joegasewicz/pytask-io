from typing import Tuple


class Router:

    def __init__(self, *, path: str, method: str):
        self.path = path
        self.method = method


    def route(self) -> Tuple[dict, int]:
        status = 404
        if self.path == "/health" and self.method == "GET":
            status = 200
            data = {
                "status": "OK",
            }
        else:
            data = {
                "error": "Please see https://forestmq.dev/ for usage guide."
            }
        return data, status