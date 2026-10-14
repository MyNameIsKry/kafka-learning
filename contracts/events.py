"""PaySim Kafka and inference contracts.

The raw event deliberately contains the four PaySim balance columns for
storage/reconciliation.  ``to_model_input`` is an explicit whitelist so raw
schema changes cannot silently leak labels or post-transaction balances into
the model pipeline.
"""

from datetime import datetime
from math import isfinite
from typing import Any, Literal, Mapping

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictInt, field_validator

TransactionType = Literal["CASH_IN", "CASH_OUT", "DEBIT", "PAYMENT", "TRANSFER"]
_FORBIDDEN_MODEL_FIELDS = frozenset(
    {
        "oldbalanceOrg",
        "newbalanceOrig",
        "oldbalanceDest",
        "newbalanceDest",
        "isFraud",
        "isFlaggedFraud",
    }
)


class ContractModel(BaseModel):
    """Common strictness rules for every external contract."""

    model_config = ConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
        validate_assignment=True,
    )

    def to_json_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible dict for Kafka serialization."""

        return self.model_dump(mode="json")

    def to_json(self) -> str:
        """Return a JSON string using the model's validated schema."""

        return self.model_dump_json()


def _require_non_empty(value: str) -> str:
    if not value:
        raise ValueError("must not be empty")
    return value


def _require_finite(value: float) -> float:
    if not isfinite(value):
        raise ValueError("must be finite")
    return value


def _require_json_number(value: Any) -> Any:
    """Accept JSON numbers, but reject numeric strings and booleans."""

    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("must be a JSON number")
    return value


class RawTransactionEvent(ContractModel):
    """PaySim business event published to ``transactions.raw``."""

    schemaVersion: Literal[2]
    transactionId: str = Field(min_length=1)
    sourceRowIndex: StrictInt = Field(ge=0)
    step: StrictInt = Field(ge=1)
    type: TransactionType
    amount: float = Field(ge=0)
    nameOrig: str = Field(min_length=1)
    oldbalanceOrg: float = Field(ge=0)
    newbalanceOrig: float = Field(ge=0)
    nameDest: str = Field(min_length=1)
    oldbalanceDest: float = Field(ge=0)
    newbalanceDest: float = Field(ge=0)

    _json_amount = field_validator("amount", mode="before")(_require_json_number)
    _json_balances = field_validator(
        "oldbalanceOrg", "newbalanceOrig", "oldbalanceDest", "newbalanceDest", mode="before"
    )(_require_json_number)
    _finite_amount = field_validator("amount")(_require_finite)
    _finite_balances = field_validator(
        "oldbalanceOrg", "newbalanceOrig", "oldbalanceDest", "newbalanceDest"
    )(_require_finite)


class ModelInputDTO(ContractModel):
    """Whitelisted transaction data passed to graph construction/model code."""

    transactionId: str = Field(min_length=1)
    sourceRowIndex: StrictInt = Field(ge=0)
    step: StrictInt = Field(ge=1)
    type: TransactionType
    amount: float = Field(ge=0)
    nameOrig: str = Field(min_length=1)
    nameDest: str = Field(min_length=1)

    _json_amount = field_validator("amount", mode="before")(_require_json_number)
    _finite_amount = field_validator("amount")(_require_finite)


# Alias used by the AI-facing documentation.
InferenceTransactionDTO = ModelInputDTO


class TransactionPredictionEvent(ContractModel):
    """Model output published to ``transactions.scored``."""

    schemaVersion: Literal[1]
    transactionId: str = Field(min_length=1)
    step: StrictInt = Field(ge=1)
    # The producer must publish a normalized finite score.  Its scale and
    # threshold remain an AI decision; no probability semantics are assumed.
    anomalyScore: float
    isAnomaly: StrictBool
    modelVersion: str = Field(min_length=1)
    processedAt: datetime
    reasonCodes: list[str] = Field(default_factory=list)

    _json_score = field_validator("anomalyScore", mode="before")(_require_json_number)
    _finite_score = field_validator("anomalyScore")(_require_finite)
    _non_empty_model_version = field_validator("modelVersion")(_require_non_empty)

    @field_validator("processedAt")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("processedAt must include a timezone")
        return value


class GroundTruthDTO(ContractModel):
    """Offline-only PaySim labels; never sent to inference."""

    transactionId: str = Field(min_length=1)
    step: StrictInt = Field(ge=1)
    isFraud: StrictInt = Field(ge=0, le=1)
    isFlaggedFraud: StrictInt = Field(ge=0, le=1)


class DLQEvent(ContractModel):
    """Record describing a permanently rejected Kafka input event."""

    sourceTopic: str = Field(min_length=1)
    sourcePartition: StrictInt = Field(ge=0)
    sourceOffset: StrictInt = Field(ge=0)
    errorCode: str = Field(min_length=1)
    errorMessage: str = Field(min_length=1)
    failedAt: datetime
    originalPayload: dict[str, Any]

    @field_validator("failedAt")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("failedAt must include a timezone")
        return value


def to_model_input(raw: RawTransactionEvent) -> ModelInputDTO:
    """Convert raw storage DTO to model DTO using an explicit whitelist."""

    model_input = ModelInputDTO(
        transactionId=raw.transactionId,
        sourceRowIndex=raw.sourceRowIndex,
        step=raw.step,
        type=raw.type,
        amount=raw.amount,
        nameOrig=raw.nameOrig,
        nameDest=raw.nameDest,
    )

    leaked_fields = _FORBIDDEN_MODEL_FIELDS.intersection(model_input.model_dump())
    if leaked_fields:
        raise AssertionError(f"model input contains forbidden fields: {sorted(leaked_fields)}")
    return model_input


def parse_raw_transaction(payload: Mapping[str, Any]) -> RawTransactionEvent:
    """Validate a decoded Kafka payload as a v2 raw event."""

    return RawTransactionEvent.model_validate(dict(payload))


def parse_prediction(payload: Mapping[str, Any]) -> TransactionPredictionEvent:
    """Validate a decoded scored-event payload."""

    return TransactionPredictionEvent.model_validate(dict(payload))
