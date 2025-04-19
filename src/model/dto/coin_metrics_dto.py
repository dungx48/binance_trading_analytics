from dataclasses import dataclass

@dataclass
class CoinMetricsDto():
    mode: str
    base_symbol: str
    quote_symbol: str
    data: list