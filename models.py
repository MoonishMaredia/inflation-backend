from optparse import Option
from typing import List, Optional, Dict
from pydantic.dataclasses import dataclass
from pydantic import (
    BaseModel
)

class TimeSeriesRequest(BaseModel):
    chartType: str
    seriesType: str
    yearStart: str
    monthStart: str
    yearEnd: str
    monthEnd: str
    seriesIds: List[str]

class CompareRequest(BaseModel):
    yearStart: str
    monthStart: str
    yearEnd: str
    monthEnd: str

class UpdateRequest(BaseModel):
    addDate: str
    weightTableUpdated: Optional[bool]