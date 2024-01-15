from task_manager.core import TaskManager
from task_manager.storage import InMemoryStorage
from task_manager.servers import AioHttpServer

manager = TaskManager(InMemoryStorage())
server = AioHttpServer(manager, 8080)
server.run()