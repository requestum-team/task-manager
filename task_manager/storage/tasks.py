import json
from dataclasses import dataclass

from task_manager.core import ConsumedTask
from task_manager.core.utils import UUIDEncoder


class TaskStatus:
    NEW = 0
    IN_PROGRESS = 1
    FINISHED = 2


@dataclass
class Task(ConsumedTask):
    status: int
    error: None | str
    description: str = ''


def create_task_from_dict(data: dict):
    if isinstance(data.get('payload'), str):
        data['payload'] = json.loads(data['payload'])
    return Task(**json.loads(json.dumps(data, cls=UUIDEncoder)))
