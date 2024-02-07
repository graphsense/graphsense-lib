# flake8: noqa: W291

from graphsenselib.schema.schema import (
    CreateTableStatement,
    GraphsenseSchemas,
    create_parser,
    remove_eol_comments,
    types,
)


def test_create_parser_pkrow(capsys):
    create_parser.parse(
        "create table if not exists test.bla   (a b , b c, primary key (a)) with test;"
    )


def test_create_parser_nokey(capsys):
    ret = create_parser.parse("create table if not exists test.bla   (a b, c d)")

    assert ret["table"]["table"] == "bla"
    assert ret["table"]["keyspace"] == "test"
    assert ret["ine"] is True


def test_create_parser_nokeyNoIne(capsys):
    ret = create_parser.parse("create table test_demo (a b, c d)")

    assert ret["table"]["table"] == "test_demo"
    assert ret["ine"] is not True
    assert ret["withstmt"] is None


def test_create_parser_WithPK(capsys):
    ret = create_parser.parse(
        "create table test_demo (a_b b, c d primary key) with a=b and c={a:bn}"
    )

    assert ret["table"]["table"] == "test_demo"
    assert ret["ine"] is not True
    assert ret["withstmt"] == "a=b and c={a:bn}"
    assert ret["columns"][0]["pk"] is not True
    assert ret["columns"][0]["name"] == "a_b"
    assert ret["columns"][0]["type"] == "b"
    assert ret["columns"][1]["pk"] is True
    assert ret["columns"][1]["name"] == "c"
    assert ret["columns"][1]["type"] == "d"


def test_create_parser_PkSpaces(capsys):
    ret = create_parser.parse(
        "create table test_demo (a b, c d primary key) with a=b and c={a:bn}"
    )

    assert ret["table"]["table"] == "test_demo"
    assert ret["ine"] is not True
    assert ret["withstmt"] == "a=b and c={a:bn}"


def test_create_parser_compoundkey(capsys):
    ret = create_parser.parse(
        "create table test_demo (a b, c d, primary key (a, b)) with a=b and c={a:bn}"
    )

    assert ret["table"]["table"] == "test_demo"
    assert ret["ine"] is not True
    assert ret["withstmt"] == "a=b and c={a:bn}"

    assert ret["columns"][2]["pk"] is True
    assert ret["columns"][2]["compound_key"] == ["a", "b"]


def test_create_parser_compoundkeycomplex(capsys):
    ret = create_parser.parse(
        "CREATE table test_demo "
        "(a b, c d, primary key ((a, b,c), b)) with a=b and c={a:bn}"
    )

    assert ret["table"]["table"] == "test_demo"
    assert ret["ine"] is not True
    assert ret["withstmt"] == "a=b and c={a:bn}"

    assert ret["columns"][2]["pk"] is True
    assert ret["columns"][2]["compound_key"] == [["a", "b", "c"], "b"]


def test_CreateTableStatement_pkrow(capsys):
    CreateTableStatement.from_schema(
        "CREATE TABLE if not exists test.bla   (a b , b c, primary key (a)) with test;"
    )


def test_CreateTableStatement_nokey(capsys):
    ret = CreateTableStatement.from_schema(
        "create table if not exists test.bla   (a b, c d)"
    )

    assert ret.pk == []
    assert ret.columns == {"a": "b", "c": "d"}
    assert ret.if_not_exists is True
    assert ret.keyspace == "test"
    assert ret.table == "bla"
    assert ret.with_stmt is None


def test_CreateTableStatement_nokeyNoIne(capsys):
    ret = CreateTableStatement.from_schema("create table test_demo (a b, c d)")

    assert ret.pk == []
    assert ret.columns == {"a": "b", "c": "d"}
    assert ret.if_not_exists is False
    assert ret.keyspace is None
    assert ret.table == "test_demo"
    assert ret.with_stmt is None


def test_CreateTableStatement_WithPK(capsys):
    ret = CreateTableStatement.from_schema(
        "create table if    not   exists  blub.test_demo "
        "(a_b b, c d primary key) with a=b and c={a:bn}"
    )

    assert ret.pk == ["c"]
    assert ret.columns == {"a_b": "b", "c": "d"}
    assert ret.if_not_exists is True
    assert ret.keyspace == "blub"
    assert ret.table == "test_demo"
    assert ret.with_stmt == "a=b and c={a:bn}"


def test_CreateTableStatement_compoundkey(capsys):
    ret = CreateTableStatement.from_schema(
        "create table test_demo (a b, c d, primary key (a, b)) with a=b and c={a:bn}"
    )

    assert ret.pk == ["a", "b"]
    assert ret.columns == {"a": "b", "c": "d"}
    assert ret.if_not_exists is False
    assert ret.keyspace is None
    assert ret.table == "test_demo"
    assert ret.with_stmt == "a=b and c={a:bn}"


def test_CreateTableStatement_compoundkeycomplex(capsys):
    ret = CreateTableStatement.from_schema(
        "create table test_demo "
        "(a b, c d, primary key ((a, b,c), b)) with a=b and c={a:bn};   "
    )

    assert ret.pk == [["a", "b", "c"], "b"]
    assert ret.columns == {"a": "b", "c": "d"}
    assert ret.if_not_exists is False
    assert ret.keyspace is None
    assert ret.table == "test_demo"
    assert ret.with_stmt == "a=b and c={a:bn}"


def test_CreateTableStatement_strrepr(capsys):
    ret = CreateTableStatement.from_schema(
        "create table test_demo (a b, c d, primary key ((a, b,c), b))"
    )

    assert repr(ret) == "CREATE TABLE test_demo (a b, c d, PRIMARY KEY ((a,b,c),b));"


def test_load_and_parse_embedded_schemaFiles():
    schemas = GraphsenseSchemas().get_all_schemas()

    for f, s in schemas:
        stmts = s.parse_create_table_statements()

        for rs in stmts:
            rstmt = CreateTableStatement.from_schema(repr(rs))
            assert rs == rstmt


def test_get_schema_by_type():
    schemas = GraphsenseSchemas().get_by_schema_type("account_trx", "transformed")
    assert len(schemas) > 1

    schemas = GraphsenseSchemas().get_by_schema_type("account", "transformed")
    assert len(schemas) > 1

    schemas = GraphsenseSchemas().get_by_currency(
        "eth", "transformed", no_extensions=True
    )
    assert len(schemas) == 1

    schemas = GraphsenseSchemas().get_by_currency(
        "trx", "transformed", no_extensions=True
    )
    assert len(schemas) == 1


def test_types_parsing_map():
    res = types.parse("map<integer, float>")
    assert res == "map<integer, float>"


def test_types_parsing_listfrozenoutput():
    res = types.parse("list<FROZEN<tx_input_output>>")
    assert res == "list<frozen<tx_input_output>>"


def test_remove_inline_comments():
    schema = """
        CREATE TABLE trc10 (
            owner_address blob,
            name text,
            abbr text,
            total_supply varint, -- bigint probably barely enough, varint just to be safe
            trx_num varint, -- int probably barely enough, varint just to be safe
            num varint, -- int probably barely enough, varint just to be safe
            start_time varint, -- removed 3 last digits to conform with eth, not always zeros, but can be large 3 outliers
            end_time varint, -- removed 3 last digits to conform with eth, not always zeros, multiple outliers
            description text,
            url text,
            id int,
            frozen_supply list<FROZEN<trc10_frozen_supply>>,
            public_latest_free_net_time varint,
            vote_score smallint,
            free_asset_net_limit bigint,
            public_free_asset_net_limit bigint,
            precision smallint,
            PRIMARY KEY (id)
        );
    """

    schema_target = """
        CREATE TABLE trc10 (
            owner_address blob,
            name text,
            abbr text,
            total_supply varint,
            trx_num varint,
            num varint,
            start_time varint,
            end_time varint,
            description text,
            url text,
            id int,
            frozen_supply list<FROZEN<trc10_frozen_supply>>,
            public_latest_free_net_time varint,
            vote_score smallint,
            free_asset_net_limit bigint,
            public_free_asset_net_limit bigint,
            precision smallint,
            PRIMARY KEY (id)
        );
    """

    assert remove_eol_comments(schema) == schema_target


def test_schema_trx_loads():
    schema = [
        s
        for (n, s) in GraphsenseSchemas().get_by_schema_type(
            "account_trx", "transformed"
        )
        if n == "transformed_account_schema.sql"
    ]
    assert len(schema) == 1
    schema = schema[0]

    create_t_bt = [
        x
        for x in schema.parse_create_table_statements()
        if x.table == "block_transactions"
    ]
    assert len(create_t_bt) == 1
    assert create_t_bt[0].columns["tx_id"] == "bigint"
