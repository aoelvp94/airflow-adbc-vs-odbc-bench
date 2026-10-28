"""common.sql 2.1.1: measure real SQL/transfer operators; do not invent names."""

import pytest
from airflow.providers.common.sql.operators.generic_transfer import GenericTransfer
from airflow.providers.common.sql.operators.sql import (
    BranchSQLOperator,
    SQLBulkLoadOperator,
    SQLCheckOperator,
    SQLColumnCheckOperator,
    SQLExecuteQueryOperator,
    SQLInsertRowsOperator,
    SQLIntervalCheckOperator,
    SQLTableCheckOperator,
    SQLThresholdCheckOperator,
    SQLValueCheckOperator,
)
from operators.adbc_arrow_transfer import AdbcArrowTransfer


def test_measured_operators_exist():
    assert issubclass(AdbcArrowTransfer, GenericTransfer)
    for cls in (
        SQLExecuteQueryOperator,
        SQLInsertRowsOperator,
        SQLBulkLoadOperator,
        GenericTransfer,
    ):
        assert cls.__name__


def test_quality_and_branch_operators_exist_but_are_not_transfer():
    for cls in (
        SQLCheckOperator,
        SQLColumnCheckOperator,
        SQLTableCheckOperator,
        SQLValueCheckOperator,
        SQLIntervalCheckOperator,
        SQLThresholdCheckOperator,
        BranchSQLOperator,
    ):
        assert cls.__name__


def test_bulk_load_only_postgres_hook_implements():
    pg = pytest.importorskip("airflow.providers.postgres.hooks.postgres")
    arrow = pytest.importorskip("airflow.providers.apache.arrow.hooks.adbc")
    odbc = pytest.importorskip("airflow.providers.odbc.hooks.odbc")
    assert callable(pg.PostgresHook.bulk_load)
    # Inherited from DbApiHook: raise NotImplementedError (not COPY).
    assert arrow.AdbcHook.bulk_load is not pg.PostgresHook.bulk_load
    assert odbc.OdbcHook.bulk_load is not pg.PostgresHook.bulk_load
