"""Runtime parameter loader for Lab 06 External V2.

Recurring notebooks do not create widgets. Values are supplied by a Job task
(or by the optional development runner).
"""

from dataclasses import dataclass
from typing import Optional

from .config import Lab06ExternalConfig


@dataclass(frozen=True)
class Lab06ExternalRuntimeContext:
    config: Lab06ExternalConfig
    run_validation: Optional[bool] = None
    date_start: Optional[str] = None
    date_end: Optional[str] = None
    rebuild_dim_date: Optional[bool] = None
    drop_threshold_pct: Optional[float] = None
    simulated_volume_pct: Optional[float] = None


def _required(dbutils, name: str) -> str:
    try:
        value = dbutils.widgets.get(name)
    except Exception as exc:
        raise RuntimeError(
            f"Missing required Job/notebook parameter: {name}"
        ) from exc

    value = str(value).strip()
    if not value:
        raise ValueError(f"Parameter {name!r} must not be empty.")
    return value


def _bool(value: str, name: str) -> bool:
    normalized = value.lower()
    if normalized not in {"true", "false"}:
        raise ValueError(
            f"Parameter {name!r} must be true/false, got {value!r}"
        )
    return normalized == "true"


def load_runtime_context(
    dbutils,
    *,
    include_validation: bool = False,
    include_date_config: bool = False,
    include_alert_config: bool = False,
) -> Lab06ExternalRuntimeContext:
    config = Lab06ExternalConfig(
        catalog=_required(dbutils, "catalog"),
        source_schema=_required(dbutils, "source_schema"),
        source_volume_name=_required(dbutils, "source_volume_name"),
        target_schema=_required(dbutils, "target_schema"),
        external_gold_root=_required(dbutils, "external_gold_root"),
    )

    run_validation = None
    date_start = None
    date_end = None
    rebuild_dim_date = None
    drop_threshold_pct = None
    simulated_volume_pct = None

    if include_validation:
        run_validation = _bool(
            _required(dbutils, "run_validation"),
            "run_validation",
        )

    if include_date_config:
        date_start = _required(dbutils, "date_start")
        date_end = _required(dbutils, "date_end")
        rebuild_dim_date = _bool(
            _required(dbutils, "rebuild_dim_date"),
            "rebuild_dim_date",
        )

    if include_alert_config:
        drop_threshold_pct = float(
            _required(dbutils, "drop_threshold_pct")
        )
        simulated_volume_pct = float(
            _required(dbutils, "simulated_volume_pct")
        )

        for name, value in (
            ("drop_threshold_pct", drop_threshold_pct),
            ("simulated_volume_pct", simulated_volume_pct),
        ):
            if not 0 <= value <= 100:
                raise ValueError(
                    f"{name} must be between 0 and 100."
                )

    return Lab06ExternalRuntimeContext(
        config=config,
        run_validation=run_validation,
        date_start=date_start,
        date_end=date_end,
        rebuild_dim_date=rebuild_dim_date,
        drop_threshold_pct=drop_threshold_pct,
        simulated_volume_pct=simulated_volume_pct,
    )
