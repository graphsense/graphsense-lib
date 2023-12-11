from graphsenselib.schema.schema import (
    CreateTableStatement,
    GraphsenseSchemas,
    create_parser,
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


def test_types_parsing_map():
    res = types.parse("map<integer, float>")
    assert res == "map<integer, float>"


def test_types_parsing_listfrozenoutput():
    res = types.parse("list<FROZEN<tx_input_output>>")
    assert res == "list<frozen<tx_input_output>>"
