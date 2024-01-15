import asyncio
import json
from typing import Any

from aiohttp import web
from aiohttp.web_request import Request
from task_manager.core import TaskManager, Session, SessionClosedException
from pydantic import BaseModel, ValidationError


async def session_closer(req: web.Request, session: Session):
    while True:
        await asyncio.sleep(1)
        if req.transport is None:
            session.close()
            break


class TaskInput(BaseModel):
    topic: str
    payload: Any


class AioHttpServer:
    def __init__(self, tm: TaskManager, port=8080):
        self.tm = tm
        self.port = port
        self.app = self._init_app()

    def _init_app(self):
        app = web.Application(middlewares=[self._excpetions_middleware])
        app.add_routes([
            web.post('/publish', self._publish_task_handler),
            web.get('/consume/{topics}', self._consume_handler),
        ])
        return app

    async def _publish_task_handler(self, req: Request):
        body = await req.json()
        data = TaskInput.model_validate(body)
        session = self.tm.new_session()
        id = await session.publish_task(data.topic, data.payload)
        return web.json_response({"idn": id})

    async def _consume_handler(self, req: Request):
        topics = [t.strip() for t in req.match_info['topics'].split(',')]
        session = self.tm.new_session()
        res = web.StreamResponse()
        await res.prepare(req)
        asyncio.create_task(session_closer(req, session))
        try:
            while True:
                print('consuming')
                task = await session.consume_task(topics)
                body = json.dumps({
                    "idn": task.idn,
                    "payload": task.payload,
                })
                await res.write(f'{body}\n'.encode())
        except SessionClosedException as e:
            print('client disconnected')

        return res

    @web.middleware
    async def _excpetions_middleware(self, req: Request, handler):
        try:
            return await handler(req)
        except ValidationError as e:
            return web.Response(body=str(e), status=400)

    def run(self):
        web.run_app(self.app, port=self.port)
