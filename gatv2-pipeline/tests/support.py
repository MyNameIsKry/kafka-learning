def paysim_record(**overrides):
    record = {
        "step": 1,
        "type": "PAYMENT",
        "amount": 25.5,
        "nameOrig": "C1",
        "oldbalanceOrg": 100.0,
        "newbalanceOrig": 74.5,
        "nameDest": "M1",
        "oldbalanceDest": 0.0,
        "newbalanceDest": 25.5,
        "isFraud": 0,
        "isFlaggedFraud": 0,
    }
    record.update(overrides)
    return record
