import logging
from enum import StrEnum

from forestmq.__version__ import VERSION


logging.basicConfig(level=logging.INFO)
log = logging.getLogger("forestmq")


class Color(StrEnum):
    RESET = "\033[0m"
    GREEN = "\033[32m"


def print_preamble() -> None:
    print("\n")
    print(f"🌲ForestMQ v{VERSION}")
    print(""
f"\n{Color.GREEN}   / ____/ __ \\/ __ \\/ ____/ ___/_  __/  /  |/  / __ \\\n"
"  / /_  / / / / /_/ / __/  \\__ \\ / /    / /|_/ / / / /\n"
" / __/ / /_/ / _, _/ /___ ___/ // /    / /  / / /_/ / \n"
f"/_/    \\____/_/ |_/_____//____//_/    /_/  /_/\\___\\_\\ {Color.RESET}\n\n"
)
