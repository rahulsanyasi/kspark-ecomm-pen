from pydantic import BaseModel


class Laptop(BaseModel):
    id: int
    make: str
    type: str
    cost: float
