class Protocol:
    # Header data frame:
    # | PROTOCOL (4 bytes)     |
    # | VERSION (2 bytes)      |
    # | FLAGS (1 byte)         |
    # | TOPIC LENGTH (2 bytes) |
    # | BODY LENGTH (2 bytes)  |
    #
    # Body data frame:
    # | TOPIC                  |
    # | BODY                   |

    def __init__(self):
        pass

    def is_fmqp(self) -> bool:
        return True
