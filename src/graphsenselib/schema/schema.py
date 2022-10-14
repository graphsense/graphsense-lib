import logging
from importlib.resources import files, read_text
from typing import Iterable, List

from parsy import forward_declaration, seq, string

from ..config import currency_to_schema_type
from ..db import DbFactory
from ..db.cassandra import build_create_stmt, normalize_cql_statement
from ..utils import flatten, split_list_on_condition
from ..utils.parsing import (
    anything,
    ci_str_token,
    identifier,
    lexeme,
    space,
    tableidentifier,
)

logger = logging.getLogger(__name__)

comma_sep = ci_str_token(",")
pk = ci_str_token("primary key")
ct = ci_str_token("create table")
ine = ci_str_token("if not exists")
wstmt = ci_str_token("with")
rb = ci_str_token(")")
lb = ci_str_token("(")
rpb = ci_str_token(">")
lpb = ci_str_token("<")
ma = ci_str_token("map")
fs = ci_str_token("frozen")
lst = ci_str_token("list")
id = lexeme(identifier)
tid = lexeme(tableidentifier)

types = forward_declaration()
types.become(
    (ma + lpb + types + string(",") + space + types + rpb)
    | (lst + lpb + types + rpb)
    | (fs + lpb + types + rpb)
    | id
)

nested_i_tuple = id | lb >> id.sep_by(comma_sep, min=1) << rb
column = seq(
    name=id,
    type=types,
    pk=(pk.result(True)).optional(),
)
pk_column = seq(
    pk=pk.result(True),
    compound_key=lb >> nested_i_tuple.sep_by(comma_sep) << rb,
)
column_expr = pk_column | column

# TODO: This is likely incomplete and needs some work, with statements support
# is missing
create_parser = seq(
    _select=ct,
    ine=ine.result(True).optional(),
    table=tid,
    columns=lb >> column_expr.sep_by(comma_sep, min=1) << rb,
    withstmt=(wstmt >> anything).optional(),
)


class CreateTableStatement:
    @classmethod
    def from_schema(Cls, schema_str):
        res = create_parser.parse(normalize_cql_statement(schema_str))
        keyspace = res["table"]["keyspace"] if "keyspace" in res["table"] else None
        cols = res["columns"]
        w_compound, wo_compound = split_list_on_condition(
            cols, lambda x: "compound_key" in x
        )
        pk_cols = [col for col in wo_compound if col["pk"] is True]
        ld = len(cols) - len(wo_compound)

        if ld == 1:
            pk = w_compound[0]["compound_key"]
        elif ld == 0:
            lpk = len(pk_cols)
            if lpk == 0:
                pk = []
            elif lpk == 1:
                pk = [pk_cols[0]["name"]]
            else:
                raise Exception("Table schema can't have more than one key column.")
        else:
            raise Exception(
                "Table schema can't have more than one compound key column."
            )

        return Cls(
            res["table"]["table"],
            {col["name"]: col["type"] for col in wo_compound},
            pk,
            if_not_exists=res["ine"] is True,
            keyspace=keyspace,
            withstmt=res["withstmt"],
        )

    def __init__(
        self,
        table,
        columns: dict[str, str],
        pks: list,
        if_not_exists=False,
        keyspace=None,
        withstmt=None,
    ):
        self.table = table
        self.columns = columns
        self.pk = pks
        self.if_not_exists = if_not_exists
        self.keyspace = keyspace
        self.with_stmt = withstmt

    def __eq__(self, other):
        return repr(self) == repr(other)

    def __repr__(self):
        return build_create_stmt(
            [f"{n} {t}" for n, t in self.columns.items()],
            [f"({','.join(pk)})" if type(pk) == list else pk for pk in self.pk],
            self.table,
            not self.if_not_exists,
            self.keyspace,
            with_stmt=self.with_stmt,
        )


class Schema:
    def __init__(self, schema_str):
        self.original_schema = schema_str
        self.statements_str = [
            normalize_cql_statement(s) for s in schema_str.split(";")
        ]

    def parse_create_table_statements(self) -> Iterable[CreateTableStatement]:
        return [
            CreateTableStatement.from_schema(x)
            for x in self.statements_str
            if x.startswith("create table")
        ]


class GraphsenseSchemas:

    RESOUCE_PATH = f"{__package__}.resources"

    def get_schema_files(self):
        return list(files(self.RESOUCE_PATH).iterdir())

    def load_schema_text(self, filename):
        return read_text(self.RESOUCE_PATH, filename)

    def get_by_currency(self, currency, keyspace_type=None) -> List[tuple[str, str]]:
        return self.get_by_schema_type(
            currency_to_schema_type[currency], keyspace_type=keyspace_type
        )

    def get_by_schema_type(
        self, schema_type, keyspace_type=None
    ) -> List[tuple[str, str]]:
        return [
            (f, s)
            for (f, s) in self.get_all_schemas()
            if (
                (
                    f.endswith(f"{schema_type}_schema.sql")
                    or f.endswith("generic_schema.sql")
                )
                and (
                    True
                    if keyspace_type is None
                    else (f.startswith(f"{keyspace_type}_"))
                )
            )
        ]

    def get_all_schemas(self) -> List[tuple[str, Schema]]:
        return [
            (file.name, Schema(self.load_schema_text(file.name)))
            for file in self.get_schema_files()
            if file.name.endswith("sql")
        ]

    def get_db_validation_report(self, env, currency):
        with DbFactory().from_config(env, currency) as db:
            raw_schemas = self.get_by_currency(currency, keyspace_type="raw")
            report_raw = self._validate_against_db_intermal(db.raw, raw_schemas)

            trans_schemas = self.get_by_currency(currency, keyspace_type="transformed")
            report_trans = self._validate_against_db_intermal(
                db.transformed, trans_schemas
            )
        return report_raw + report_trans

    def _validate_against_db_intermal(self, db, schemas):
        report = []
        creates_defs = flatten([s.parse_create_table_statements() for f, s in schemas])
        for x in creates_defs:
            pkeys = []
            ckeys = []
            result = db.get_columns_for_table(x.table)
            expected_cols = x.columns.copy()
            mres = list(result)
            ks_table = f"{db.get_keyspace()}.{x.table}"
            logger.info(f"Validating schema of {ks_table}.")
            if len(mres) == 0:
                report.append(f"Table {ks_table} not present in database.")
            for col in mres:
                assert col.table_name == x.table

                if col.column_name not in expected_cols:
                    report.append(
                        f"Column {ks_table}.{col.column_name} present in "
                        "database not present in schema."
                    )
                else:
                    expected_type = expected_cols[col.column_name]
                    if expected_type != col.type:
                        report.append(
                            f"Expected type {ks_table}{expected_cols} "
                            f"but was {col.type}"
                        )
                    expected_cols.pop(col.column_name)

                if col.kind == "partition_key":
                    pkeys.append((col.column_name, col.position))

                if col.kind == "clustering":
                    ckeys.append((col.column_name, col.position))

            if len(expected_cols) > 0:
                cm = ",".join([f"{k} {t}" for k, t in expected_cols.items()])
                report.append(
                    f"Table {ks_table} Some columns that where expected are "
                    f"missing in the database - {cm}"
                )

            expected_key = x.pk
            pkeys = [n for n, p in sorted(pkeys, key=lambda x: x[1])]
            ckeys = [n for n, p in sorted(ckeys, key=lambda x: x[1])]
            key = (
                ([pkeys[0] if len(pkeys) == 1 else pkeys]) if len(pkeys) > 0 else []
            ) + ckeys
            if expected_key != key:
                report.append(
                    f"The keys of the table {ks_table} does not match "
                    f"the expected keys. Expected: {expected_key} got {key}"
                )

        return report
