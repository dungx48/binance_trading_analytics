from dataclasses import dataclass

@dataclass
class KlinesRequest():
    base_symbol: str
    quote_symbol: str
    interval: str
    start_time: int
    end_time: int
    limit: int = 1000