import logging
import re
from datetime import datetime
from importlib.resources import files
from typing import Iterable, List, Optional

from parsy import forward_declaration, seq, string

from ..config import currency_to_schema_type, get_config, keyspace_types
from ..datatypes import BadUserInputError
from ..db import DbFactory
from ..db.cassandra import (
    build_create_stmt,
    build_truncate_stmt,
    normalize_cql_statement,
)
from ..utils import flatten, split_list_on_condition
from ..utils.parsing import (
    anything,
    ci_str_token,
    identifier,
    lexeme,
    space,
    tableidentifier,
)

SCHEMA_TYPE_MAPPING_OVERWRITES = {("account_trx", "transformed"): "account"}

MAGIC_SCHEMA_CONSTANT = "0x8BADF00D"

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
ident_lm = lexeme(identifier)
tid = lexeme(tableidentifier)

types = forward_declaration()
types.become(
    (ma + lpb + types + string(",") + space + types + rpb)
    | (lst + lpb + types + rpb)
    | (fs + lpb + types + rpb)
    | ident_lm
)

nested_i_tuple = ident_lm | lb >> ident_lm.sep_by(comma_sep, min=1) << rb
column = seq(
    name=ident_lm,
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


def remove_eol_comments(statement: str) -> str:
    return re.sub(r"\s*?--.*?$", "", statement, 0, re.MULTILINE)


class CreateTableStatement:
    @classmethod
    def from_schema(Cls, schema_str):
        res = create_parser.parse(
            normalize_cql_statement(remove_eol_comments(schema_str))
        )
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
            [f"({','.join(pk)})" if isinstance(pk, list) else pk for pk in self.pk],
            self.table,
            not self.if_not_exists,
            self.keyspace,
            with_stmt=self.with_stmt,
        )


class Schema:
    def __init__(self, schema_str):
        self.original_schema = schema_str
        self.statements_str = [
            normalize_cql_statement(remove_eol_comments(s))
            for s in schema_str.split(";")
        ]

    def parse_create_table_statements(self) -> Iterable[CreateTableStatement]:
        return [
            CreateTableStatement.from_schema(x)
            for x in self.statements_str
            if x.startswith("create table")
        ]

    def get_schema_string(self, keyspace_name, replication_config):
        replication_factor_magic_constant = (
            MAGIC_SCHEMA_CONSTANT + "_REPLICATION_CONFIG"
        )
        if (
            self.original_schema.count(MAGIC_SCHEMA_CONSTANT) != 3
            or self.original_schema.count(replication_factor_magic_constant) != 1
        ):
            raise Exception(
                f"Expect three occurrences of {MAGIC_SCHEMA_CONSTANT} in schema file,"
                " two for the keyspace_name and one for the replication factor"
                f" config ({replication_factor_magic_constant})."
            )

        return self.original_schema.replace(
            replication_factor_magic_constant, replication_config
        ).replace(MAGIC_SCHEMA_CONSTANT, keyspace_name)


class GraphsenseSchemas:
    RESOUCE_PATH = f"{__package__}.resources"

    def is_extension(filename, schema_type):
        return not any(
            filename.endswith(f"{kst}_{schema_type}_schema.sql")
            for kst in keyspace_types
        )

    def get_schema_files(self):
        return list(files(self.RESOUCE_PATH).iterdir())

    def load_schema_text(self, filename):
        return files(self.RESOUCE_PATH).joinpath(filename).read_text()

    def get_by_currency(
        self, currency, keyspace_type=None, no_extensions=False
    ) -> List[tuple[str, Schema]]:
        schema_type = currency_to_schema_type[currency]
        if (schema_type, keyspace_type) in SCHEMA_TYPE_MAPPING_OVERWRITES:
            schema_type = SCHEMA_TYPE_MAPPING_OVERWRITES[(schema_type, keyspace_type)]
        return [
            x
            for x in self.get_by_schema_type(
                schema_type,
                keyspace_type=keyspace_type,
            )
            if (
                not no_extensions
                or not GraphsenseSchemas.is_extension(x[0], schema_type)
            )
        ]

    def create_keyspaces_if_not_exist(self, env, currency):
        for kstype in keyspace_types:
            self.create_keyspace_if_not_exist(env, currency, kstype)

    def create_keyspace_if_not_exist(self, env, currency, keyspace_type):
        config = get_config()
        with DbFactory().from_config(env, currency) as db:
            schema = self.get_by_currency(
                currency, keyspace_type=keyspace_type, no_extensions=True
            )
            if len(schema) > 0:
                schema = schema[0][1]
            else:
                raise Exception(
                    "No schema definition found for "
                    f"{env}, {currency}, type: {keyspace_type}"
                )
            keyspacedb = db.by_ks_type(keyspace_type)
            target_ks_name = keyspacedb.keyspace_name()
            if keyspacedb.exists():
                logger.info(
                    f"Keyspace {keyspace_type} for env "
                    f"{env}:{currency} exists: {target_ks_name}, nothing to do"
                )
            else:
                replication_config = (
                    config.get_keyspace_config(env, currency)
                    .keyspace_setup_config[keyspace_type]
                    .replication_config
                )
                schema_to_create = schema.get_schema_string(
                    target_ks_name, replication_config
                )
                db.db().setup_keyspace_using_schema(schema_to_create)
                logger.info(
                    f"Keyspace {keyspace_type} for env "
                    f"{env}:{currency} created on {target_ks_name} "
                    f"with replication config {replication_config}."
                )

                if not keyspacedb.is_configuration_populated():
                    config_defaults = (
                        config.get_keyspace_config(env, currency)
                        .keyspace_setup_config[keyspace_type]
                        .data_configuration
                    )
                    logger.warning(
                        "Config table in transformed not populated."
                        f" Setting default values {config_defaults}."
                    )
                    keyspacedb.ingest("configuration", [config_defaults])

    def create_new_transformed_ks_if_not_exist(
        self, env, currency, suffix=None, no_date=False
    ) -> Optional[str]:
        config = get_config()
        keyspace_type = "transformed"
        with DbFactory().from_config(env, currency) as db:
            schema = self.get_by_currency(
                currency, keyspace_type=keyspace_type, no_extensions=True
            )
            if len(schema) > 0:
                schema = schema[0][1]
            else:
                raise BadUserInputError(
                    "No schema definition found for "
                    f"{env}, {currency}, type: {keyspace_type}"
                )

            date_str = datetime.now().strftime("%Y%m%d")
            keyspace_name = f"{currency}_transformed"
            if not no_date:
                keyspace_name = f"{keyspace_name}_{date_str}"
            if suffix is not None:
                keyspace_name = f"{keyspace_name}_{suffix}"
            c_db = db.db()
            if c_db.has_keyspace(keyspace_name):
                logger.error(
                    f"Keyspace {keyspace_name} for env "
                    f"{env}:{currency} exists; please remove "
                    "or specify an fresh suffix."
                )
                return None
            else:
                replication_config = (
                    config.get_keyspace_config(env, currency)
                    .keyspace_setup_config[keyspace_type]
                    .replication_config
                )
                schema_to_create = schema.get_schema_string(
                    keyspace_name, replication_config
                )
                c_db.setup_keyspace_using_schema(schema_to_create)
                logger.info(
                    f"New transformed keyspace {keyspace_name} for env "
                    f"{env}:{currency} created "
                    f"with replication config {replication_config}."
                )
                return keyspace_name

    def get_by_schema_type(
        self, schema_type, keyspace_type=None
    ) -> List[tuple[str, Schema]]:
        if (schema_type, keyspace_type) in SCHEMA_TYPE_MAPPING_OVERWRITES:
            schema_type = SCHEMA_TYPE_MAPPING_OVERWRITES[(schema_type, keyspace_type)]
        return [
            (f, s)
            for (f, s) in self.get_all_schemas()
            if (
                (
                    f.endswith(f"{schema_type}_schema.sql")
                    or f.endswith("generic_schema.sql")
                )
                and (keyspace_type is None or (f.startswith(f"{keyspace_type}_")))
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
            raw_schemas = self.get_by_currency(
                currency, keyspace_type="raw", no_extensions=True
            )
            report_raw = self._validate_against_db_intermal(db.raw, raw_schemas)

            trans_schemas = self.get_by_currency(
                currency, keyspace_type="transformed", no_extensions=True
            )
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

    def get_table_columns_from_file(self, keyspace_type: str, table: str):
        schemas = self.get_by_schema_type(keyspace_type)[0][1].statements_str
        potential_schemas = [s for s in schemas if table in s]
        assert len(potential_schemas) == 1
        schema_str = potential_schemas[0]
        columns = schema_str.split("(")[1].split(")")[0].split(",")[:-1]
        columns = [c.strip() for c in columns]
        pk_columns = schema_str.split("primary key (")[1].split(")")[0].split(",")
        return columns, pk_columns

    def ensure_table_exists_by_name(
        self, db_transformed, table_name: str, truncate: bool = False
    ):
        keyspace = db_transformed.get_keyspace()
        columns, pk_columns = self.get_table_columns_from_file(keyspace, table_name)
        db_transformed._db.execute(
            build_create_stmt(
                columns,
                pk_columns,
                table_name,
                fail_if_exists=False,
                keyspace=keyspace,
            )
        )
        if truncate:
            db_transformed._db.execute(
                build_truncate_stmt(table_name, keyspace=keyspace)
            )
