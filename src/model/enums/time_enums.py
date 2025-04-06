from dataclasses import dataclass
from typing import Optional
import pandas as pd

@dataclass
class TimeEnums():
    START_TIME : int = int(pd.Timestamp("2017-01-01").timestamp() * 1000)
    CURRENT_TIME: int = int(pd.Timestamp.now().timestamp() * 1000)
    YESTERDAY: int = int((pd.Timestamp.now() - pd.Timedelta(days=1)).timestamp() * 1000)