import asyncio

from forestmq.tcp import TCP
from forestmq.logger import print_preamble


if __name__ == "__main__":
   print_preamble()
   server = TCP(
      host="127.0.0.1",
      port=7171,
   )
   asyncio.run(server.serve())
