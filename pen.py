from pydantic import BaseModel


class Pen(BaseModel):
    id: int
    make: str
    type: str
    cost: float
