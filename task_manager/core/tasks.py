from dataclasses import dataclass


@dataclass
class ConsumedTask:
    idn: str
    topic: str
    payload: dict
