import asyncio
from db_manager.db_manager import DBManager

class DStorateDistribution:
    def __init__(self, queue_bc):
        self.queue_bc = queue_bc
        self.db_manager = DBManager()
        self.loop = asyncio.get_running_loop()
