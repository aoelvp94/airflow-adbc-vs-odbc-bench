"""Point pytest at include/ and keep Airflow from loading example/bench DAGs."""

from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
INCLUDE = ROOT / "include"
if str(INCLUDE) not in sys.path:
    sys.path.insert(0, str(INCLUDE))

os.environ.setdefault("AIRFLOW_HOME", str(ROOT / ".airflow_test_home"))
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "false")
os.environ.setdefault("AIRFLOW__CORE__DAGS_FOLDER", str(ROOT / "tests" / "_empty_dags"))
os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")
os.environ.setdefault("AIRFLOW__LOGGING__LOGGING_LEVEL", "ERROR")
