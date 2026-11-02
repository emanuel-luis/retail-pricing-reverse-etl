import json
import logging

from retail_pricing.logging import JsonFormatter, cycle_id_var, run_id_var


def test_json_formatter_adds_ids_and_extras() -> None:
    record = logging.makeLogRecord(
        {"name": "t", "levelname": "INFO", "msg": "hello %s", "args": ("x",), "rows": 3}
    )
    tok_c, tok_r = cycle_id_var.set(7), run_id_var.set("r-1")
    try:
        payload = json.loads(JsonFormatter().format(record))
    finally:
        cycle_id_var.reset(tok_c)
        run_id_var.reset(tok_r)
    assert payload["msg"] == "hello x"
    assert payload["cycle_id"] == 7
    assert payload["run_id"] == "r-1"
    assert payload["rows"] == 3
    assert payload["ts"].endswith("Z")
