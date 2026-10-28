"""Post-load dest checks: COUNT and SQL-side payload equality.

All comparisons run in PostgreSQL (COUNT, aggregate checksum, EXCEPT).
Rows are not fetched into Python, so N=1e6 stays on the server.
"""

from __future__ import annotations

import re
from typing import Any, Mapping, Sequence

# Payload copied by INSERT...SELECT / GenericTransfer / AdbcArrowTransfer / COPY.
PAYLOAD_COLUMNS = (
    "product_name",
    "category",
    "price",
    "quantity",
    "created_at",
)
NON_PRICE_COLUMNS = tuple(c for c in PAYLOAD_COLUMNS if c != "price")

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class LoadVerifyError(Exception):
    """Destination table failed post-load COUNT or payload verification."""


def sql_ident(name: str) -> str:
    """Accept only simple unquoted identifiers (bench table names)."""
    if not _IDENT.match(name):
        raise ValueError(f"unsafe SQL identifier: {name!r}")
    return name


def payload_column_sql(*, include_price: bool = True) -> tuple[str, ...]:
    """SELECT-list expressions for payload comparison.

    ``price`` is always CAST to numeric(10, 2) so float8 dests (ODBC/ADBC
    insert_rows) can be compared to NUMERIC dests / bench_source.
    """
    names = PAYLOAD_COLUMNS if include_price else NON_PRICE_COLUMNS
    cols: list[str] = []
    for name in names:
        if name == "price":
            cols.append("CAST(price AS numeric(10, 2)) AS price")
        else:
            cols.append(name)
    return tuple(cols)


def count_sql(table: str) -> str:
    return f"SELECT COUNT(*) FROM {sql_ident(table)}"


def payload_select_sql(table: str, *, include_price: bool = True) -> str:
    cols = ", ".join(payload_column_sql(include_price=include_price))
    return f"SELECT {cols} FROM {sql_ident(table)}"


def except_count_sql(left_select: str, right_select: str) -> str:
    """COUNT of rows in left SELECT that are not in right SELECT (set EXCEPT)."""
    return (
        "SELECT COUNT(*) FROM ("
        f"{left_select} EXCEPT {right_select}"
        ") AS diffs"
    )


def except_both_ways_sqls(
    left_table: str, right_table: str, *, include_price: bool = True
) -> tuple[str, str]:
    left = payload_select_sql(left_table, include_price=include_price)
    right = payload_select_sql(right_table, include_price=include_price)
    return except_count_sql(left, right), except_count_sql(right, left)


def digest_sql(table: str, *, include_price: bool = True) -> str:
    """COUNT + SUM(quantity) + md5 of ordered per-row md5s (one seq scan).

    ORDER BY product_name (unique in this bench: Product_{i}) so physical
    heap order does not affect the digest. price is CAST to numeric(10, 2).
    """
    ident = sql_ident(table)
    price_expr = ""
    if include_price:
        price_expr = "CAST(CAST(price AS numeric(10, 2)) AS text), "
    return (
        "SELECT COUNT(*)::bigint AS n, "
        "COALESCE(SUM(quantity), 0)::bigint AS sum_quantity, "
        "md5(COALESCE(string_agg(row_md5, '' ORDER BY product_name), '')) AS digest "
        "FROM ("
        "SELECT product_name, quantity, md5(CONCAT_WS(E'\\x1f', "
        "product_name, category, "
        f"{price_expr}"
        "CAST(quantity AS text), "
        "COALESCE(TO_CHAR(created_at, 'YYYY-MM-DD HH24:MI:SS.US'), '')"
        ")) AS row_md5 "
        f"FROM {ident}"
        ") AS hashed"
    )


def _one_row(hook: Any, sql: str) -> Sequence[Any]:
    row = hook.get_first(sql)
    if row is None:
        raise LoadVerifyError(f"no row returned for SQL: {sql.strip()[:160]}")
    return row


def fetch_count(hook: Any, table: str) -> int:
    return int(_one_row(hook, count_sql(table))[0])


def fetch_digest(
    hook: Any, table: str, *, include_price: bool = True
) -> dict[str, Any]:
    n, sum_quantity, digest = _one_row(
        hook, digest_sql(table, include_price=include_price)
    )
    return {
        "table": table,
        "n": int(n),
        "sum_quantity": int(sum_quantity),
        "digest": digest,
        "include_price": include_price,
    }


def fetch_except_counts(
    hook: Any,
    left_table: str,
    right_table: str,
    *,
    include_price: bool = True,
) -> tuple[int, int]:
    left_sql, right_sql = except_both_ways_sqls(
        left_table, right_table, include_price=include_price
    )
    only_left = int(_one_row(hook, left_sql)[0])
    only_right = int(_one_row(hook, right_sql)[0])
    return only_left, only_right


def assert_count(hook: Any, table: str, expected: int) -> int:
    n = fetch_count(hook, table)
    if n != expected:
        raise LoadVerifyError(
            f"{table} COUNT(*)={n}, expected {expected} (BENCH_ROW_COUNT)"
        )
    print(f"verify COUNT {table}: {n}")
    return n


def _digests_match(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    return (
        left["n"] == right["n"]
        and left["sum_quantity"] == right["sum_quantity"]
        and left["digest"] == right["digest"]
    )


def _assert_digests(
    hook: Any,
    left_table: str,
    right_table: str,
    left: Mapping[str, Any],
    right: Mapping[str, Any],
    *,
    include_price: bool,
) -> Mapping[str, Any]:
    if _digests_match(left, right):
        print(
            f"verify payload {left_table} == {right_table} "
            f"(n={left['n']}, sum_quantity={left['sum_quantity']}, "
            f"digest={left['digest']}, include_price={include_price})"
        )
        return {"left": left, "right": right}

    only_left, only_right = fetch_except_counts(
        hook, left_table, right_table, include_price=include_price
    )
    raise LoadVerifyError(
        f"payload mismatch {left_table} vs {right_table}: "
        f"digest {left} != {right}; "
        f"EXCEPT {only_left} rows only in {left_table}, "
        f"{only_right} rows only in {right_table}"
    )


def assert_payload_equal(
    hook: Any,
    left_table: str,
    right_table: str,
    *,
    include_price: bool = True,
) -> Mapping[str, Any]:
    """Fail if payload checksums differ; EXCEPT both ways explains the mismatch.

    On a checksum match we do not run EXCEPT (one seq scan per table).
    """
    left = fetch_digest(hook, left_table, include_price=include_price)
    right = fetch_digest(hook, right_table, include_price=include_price)
    return _assert_digests(
        hook,
        left_table,
        right_table,
        left,
        right,
        include_price=include_price,
    )


def verify_counts(hook: Any, tables: Sequence[str], expected: int) -> None:
    for table in tables:
        assert_count(hook, table, expected)


def verify_copy_vs_source(
    hook: Any,
    source_table: str,
    dest_tables: Sequence[str],
    expected: int,
    *,
    include_price: bool = True,
) -> None:
    """COUNT == expected on source and dests; dest payload equals source."""
    assert_count(hook, source_table, expected)
    source = fetch_digest(hook, source_table, include_price=include_price)
    for table in dest_tables:
        assert_count(hook, table, expected)
        dest = fetch_digest(hook, table, include_price=include_price)
        _assert_digests(
            hook,
            source_table,
            table,
            source,
            dest,
            include_price=include_price,
        )
