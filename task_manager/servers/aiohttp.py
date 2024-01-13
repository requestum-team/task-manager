from aiohttp import web
from aiohttp.web_request import Request
from task_manager.core import TaskManager


class AioHttpServer:
    def __init__(self, tm: TaskManager, port=8080):
        self.tm = tm
        self.port = port
        self.app = self._init_app()

    def _init_app(self):
        app = web.Application()
        # todo: register middlewares
        # todo: register route handlers
        # todo: register error handler
        return app

    async def _do_handler(self, req: Request):
        raise NotImplemented()

    async def _subscribe_handler(self, req: Request):
        raise NotImplemented()

    async def _consume_handler(self, req: Request):
        raise NotImplemented()

    def run(self):
        web.run_app(self.app, port=self.port)
