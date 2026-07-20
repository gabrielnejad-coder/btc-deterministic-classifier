from pydantic import BaseModel


class FeatureObject(BaseModel):
    ts: str
    close: float

    ret_1: float
    ret_2: float
    ret_4: float
    ret_8: float
    ret_12: float
    ret_24: float

    mom_12: float

    vol_12: float
    vol_24: float
    vol_48: float

    range_1: float

    vol_chg_1: float
    vol_z_48: float
