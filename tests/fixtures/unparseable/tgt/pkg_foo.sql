-- same body, extra blank lines + different comment style
CREATE OR REPLACE PACKAGE BODY HR.PKG_FOO IS

  PROCEDURE greet(p_name IN VARCHAR2) IS
  BEGIN
    /* greet the user */
    DBMS_OUTPUT.PUT_LINE('Hello ' || p_name);
  END greet;

END PKG_FOO;
/
