from dataclasses import dataclass
from typing import Optional

@dataclass
class KlinesRequest():
    symbol:  str
    interval: str
    start_time: int
    end_time: int
    limit: int = 1000