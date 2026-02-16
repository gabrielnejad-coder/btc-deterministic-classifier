from pydantic import BaseModel

class FeatureObject(BaseModel):
    ts: str
    close: float
    ret_1: float
    ret_4: float
    ret_24: float
    vol_24: float
