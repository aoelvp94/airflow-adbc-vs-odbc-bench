"""Unit tests for post-load COUNT / checksum helpers (no Postgres)."""

from __future__ import annotations

import pytest

from utils.verify_load import (
    LoadVerifyError,
    assert_count,
    assert_payload_equal,
    count_sql,
    digest_sql,
    except_both_ways_sqls,
    payload_column_sql,
    payload_select_sql,
    sql_ident,
    verify_copy_vs_source,
    verify_counts,
)


class FakeHook:
    def __init__(self, answers):
        self.answers = list(answers)
        self.sqls = []

    def get_first(self, sql):
        self.sqls.append(sql)
        if not self.answers:
            raise AssertionError(f"unexpected extra SQL: {sql}")
        return self.answers.pop(0)


def test_sql_ident_accepts_bench_tables():
    assert sql_ident("bench_source") == "bench_source"
    assert sql_ident("bench_op_xfer_arrow_from_pg") == "bench_op_xfer_arrow_from_pg"


def test_sql_ident_rejects_injection():
    with pytest.raises(ValueError, match="unsafe SQL identifier"):
        sql_ident("bench_source; DROP TABLE bench_source")
    with pytest.raises(ValueError, match="unsafe SQL identifier"):
        sql_ident("public.bench_source")


def test_count_sql():
    assert count_sql("bench_source") == "SELECT COUNT(*) FROM bench_source"


def test_payload_sql_casts_price():
    cols = payload_column_sql(include_price=True)
    assert "CAST(price AS numeric(10, 2)) AS price" in cols
    assert "quantity" in cols
    sql = payload_select_sql("bench_op_xfer_odbc", include_price=True)
    assert "CAST(price AS numeric(10, 2)) AS price" in sql
    assert sql.endswith("FROM bench_op_xfer_odbc")


def test_payload_sql_can_exclude_price():
    cols = payload_column_sql(include_price=False)
    assert all("price" not in c for c in cols)
    sql = digest_sql("bench_op_xfer_odbc", include_price=False)
    assert "price" not in sql
    assert "product_name" in sql
    assert "ORDER BY product_name" in sql


def test_digest_sql_is_server_side_aggregate():
    sql = digest_sql("bench_op_sql_traditional", include_price=True)
    assert "COUNT(*)" in sql
    assert "SUM(quantity)" in sql
    assert "string_agg(row_md5" in sql
    assert "CAST(price AS numeric(10, 2))" in sql
    assert "FROM bench_op_sql_traditional" in sql
    assert "fetchall" not in sql.lower()


def test_except_both_ways_sqls():
    left_sql, right_sql = except_both_ways_sqls(
        "bench_source", "bench_op_xfer_traditional"
    )
    assert "EXCEPT" in left_sql
    assert "EXCEPT" in right_sql
    assert "FROM bench_source" in left_sql
    assert "FROM bench_op_xfer_traditional" in left_sql
    assert left_sql.startswith("SELECT COUNT(*)")
    assert right_sql.startswith("SELECT COUNT(*)")


def test_assert_count_ok():
    hook = FakeHook([(1_000_000,)])
    assert assert_count(hook, "bench_source", 1_000_000) == 1_000_000
    assert "COUNT(*)" in hook.sqls[0]


def test_assert_count_mismatch_raises():
    hook = FakeHook([(12,)])
    with pytest.raises(LoadVerifyError, match="bench_op_sql_adbc COUNT\\(\\*\\)=12, expected 10"):
        assert_count(hook, "bench_op_sql_adbc", 10)


def test_assert_payload_equal_checksum_match_skips_except():
    digest = (100, 500, "abc123")
    hook = FakeHook([digest, digest])
    result = assert_payload_equal(hook, "bench_source", "bench_op_sql_traditional")
    assert result["left"]["digest"] == "abc123"
    assert len(hook.sqls) == 2
    assert all("EXCEPT" not in sql for sql in hook.sqls)


def test_assert_payload_equal_mismatch_runs_except_and_fails():
    hook = FakeHook(
        [
            (100, 500, "aaa"),
            (100, 500, "bbb"),
            (3,),
            (4,),
        ]
    )
    with pytest.raises(LoadVerifyError, match="EXCEPT 3 rows only in bench_source"):
        assert_payload_equal(hook, "bench_source", "bench_op_xfer_odbc")
    assert any("EXCEPT" in sql for sql in hook.sqls)


def test_verify_counts_fails_on_second_table():
    hook = FakeHook([(10,), (9,)])
    with pytest.raises(LoadVerifyError, match="bench_op_insert_odbc"):
        verify_counts(
            hook, ("bench_op_insert_traditional", "bench_op_insert_odbc"), 10
        )


def test_verify_copy_vs_source_payload_mismatch_fails():
    hook = FakeHook(
        [
            (50,),
            (50, 200, "src"),
            (50,),
            (50, 200, "dest"),
            (2,),
            (2,),
        ]
    )
    with pytest.raises(LoadVerifyError, match="EXCEPT 2 rows only in bench_source"):
        verify_copy_vs_source(hook, "bench_source", ("bench_op_sql_odbc",), 50)


def test_fetch_count_none_row_fails():
    hook = FakeHook([None])
    with pytest.raises(LoadVerifyError, match="no row returned"):
        assert_count(hook, "bench_source", 1)


def test_verify_copy_vs_source_happy_path():
    digest = (50, 200, "same")
    hook = FakeHook(
        [
            (50,),  # source count
            digest,  # source digest (once)
            (50,),  # dest count
            digest,  # dest digest
        ]
    )
    verify_copy_vs_source(hook, "bench_source", ("bench_op_sql_odbc",), 50)
    assert len(hook.answers) == 0
