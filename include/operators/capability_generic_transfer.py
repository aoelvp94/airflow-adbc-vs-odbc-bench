"""GenericTransfer-style helper that switches on Arrow RecordBatch capabilities.

Local demo stub for AIP Phase-2. Not an apache/airflow change.

* If both wrapped hooks are ``RecordBatchReadable`` and ``RecordBatchWritable``,
  stream ``get_record_batches(sql)`` into ``insert_record_batches(...)``.
* Otherwise use stock GenericTransfer semantics: ``get_records`` → ``insert_rows``.

``AdbcHook`` instances are wrapped with ``AdbcArrowCapabilityAdapter`` so the
installed Arrow provider is not patched. Dest-only Arrow ingest remains
``AdbcArrowTransfer`` (kept as a comparison path).
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from airflow.providers.common.sql.operators.generic_transfer import GenericTransfer

from adapters.adbc import wrap_sql_hook
from protocols.arrow_capabilities import both_ends_arrow

COLUMNAR_PATH = "columnar"
ROW_PATH = "row"


def transfer_with_capabilities(
    source: Any,
    dest: Any,
    *,
    sql: str | Sequence[str],
    destination_table: str,
    insert_args: dict[str, Any] | None = None,
    preoperator: str | Sequence[str] | None = None,
    log: Any | None = None,
) -> str:
    """Run a GenericTransfer-like copy; return ``columnar`` or ``row``.

    ``source`` / ``dest`` may be raw hooks (AdbcHook is wrapped) or adapters
    that already advertise the RecordBatch protocols.
    """
    source = wrap_sql_hook(source)
    dest = wrap_sql_hook(dest)
    insert_args = dict(insert_args or {})

    if preoperator:
        if log is not None:
            log.info("Running preoperator")
            log.info(preoperator)
        dest.run(preoperator)

    sqls = [sql] if isinstance(sql, str) else list(sql)

    if both_ends_arrow(source, dest):
        if log is not None:
            log.info(
                "Columnar path: both ends advertise RecordBatch read/write "
                "(%s -> %s)",
                type(source).__name__,
                type(dest).__name__,
            )
        for statement in sqls:
            if log is not None:
                log.info("Executing:\n %s", statement)
            dest.insert_record_batches(
                destination_table,
                source.get_record_batches(statement),
                **insert_args,
            )
        return COLUMNAR_PATH

    if log is not None:
        log.info(
            "Row path fallback: get_records -> insert_rows (%s -> %s)",
            type(source).__name__,
            type(dest).__name__,
        )
    for statement in sqls:
        if log is not None:
            log.info("Executing:\n %s", statement)
        rows = source.get_records(statement)
        dest.insert_rows(table=destination_table, rows=rows, **insert_args)
    return ROW_PATH


class CapabilityGenericTransfer(GenericTransfer):
    """GenericTransfer subclass with a both-ends Arrow RecordBatch switch.

    Same operator interface as stock GenericTransfer (source/dest conn, SQL,
    preoperator, insert_args). The columnar path is taken only when **both**
    ends wrap to RecordBatch capabilities; dest-only Arrow is still
    ``AdbcArrowTransfer``.
    """

    ui_color = "#5b8c5a"

    def execute(self, context) -> str:
        return transfer_with_capabilities(
            self.source_hook,
            self.destination_hook,
            sql=self.sql,
            destination_table=self.destination_table,
            insert_args=self.insert_args or {},
            preoperator=self.preoperator,
            log=self.log,
        )
