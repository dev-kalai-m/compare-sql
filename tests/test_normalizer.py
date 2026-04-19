from cmp_sql.normalizer import normalize, render
from cmp_sql.parser import parse_sql


def _norm_render(sql: str) -> str:
    return render(normalize(parse_sql(sql).expressions))


def test_case_insensitive_match():
    a = "create table hr.t (a number, b varchar2(10))"
    b = "CREATE TABLE HR.T (A NUMBER, B VARCHAR2(10))"
    assert _norm_render(a) == _norm_render(b)


def test_storage_clauses_ignored():
    a = "CREATE TABLE T (A NUMBER) TABLESPACE USERS PCTFREE 10"
    b = "CREATE TABLE T (A NUMBER)"
    assert _norm_render(a) == _norm_render(b)


def test_sys_constraint_names_stripped():
    a = "CREATE TABLE T (A NUMBER, CONSTRAINT sys_c001 CHECK (A > 0))"
    b = "CREATE TABLE T (A NUMBER, CONSTRAINT sys_c999 CHECK (A > 0))"
    assert _norm_render(a) == _norm_render(b)


def test_constraint_order_insensitive():
    a = """CREATE TABLE T (
             A NUMBER,
             CONSTRAINT pk PRIMARY KEY (A),
             CONSTRAINT ck CHECK (A > 0)
           )"""
    b = """CREATE TABLE T (
             A NUMBER,
             CONSTRAINT ck CHECK (A > 0),
             CONSTRAINT pk PRIMARY KEY (A)
           )"""
    assert _norm_render(a) == _norm_render(b)


def test_byte_vs_char_stays_distinct():
    a = "CREATE TABLE T (C VARCHAR2(100 BYTE))"
    b = "CREATE TABLE T (C VARCHAR2(100 CHAR))"
    assert _norm_render(a) != _norm_render(b)
