from pydantic import BaseModel
from typing import List

class TableQuery(BaseModel):
    id: str
    filters: dict

class JoinRequest(BaseModel):
    table: List[str]
    join_key: str
    