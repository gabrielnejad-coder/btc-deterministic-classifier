from features.schema import FeatureObject

def classify(f: FeatureObject) -> dict:
    if f.ret_4 > 0:
        return {"direction": "up", "confidence": 1.0}
    else:
        return {"direction": "down", "confidence": 1.0}
