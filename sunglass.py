from pydantic import BaseModel


class Sunglass(BaseModel):
    brand: str
    shape: str
    freamtype: str
    price : float
