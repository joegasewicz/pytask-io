class PyTaskIOException(Exception):
    pass

class NotReadyException(PyTaskIOException):
    message = "Call `run()` first on your PyTaskIO instance."

    def __init__(self, message=None):
        super().__init__(self.message or message)