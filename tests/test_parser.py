from cmp_sql.parser import parse_sql


def test_plain_create_table():
    r = parse_sql("CREATE TABLE T (A INT, B VARCHAR2(10))")
    assert r.ok
    assert not r.physical_stripped


def test_storage_clauses_are_stripped_on_retry():
    sql = "CREATE TABLE T (A INT) TABLESPACE USERS PCTFREE 10 STORAGE (INITIAL 64K) LOGGING"
    r = parse_sql(sql)
    assert r.ok
    assert r.physical_stripped


def test_pls_package_body_falls_back_to_text():
    sql = """
    CREATE OR REPLACE PACKAGE BODY PKG IS
      PROCEDURE p IS BEGIN NULL; END;
    END;
    /
    """
    r = parse_sql(sql)
    # Either ok (sqlglot might parse it) or text fallback — both are acceptable.
    # We only require it doesn't raise.
    assert r.ok or r.fallback_text is not None


def test_empty_input():
    r = parse_sql("")
    assert r.ok
    assert r.expressions == []
