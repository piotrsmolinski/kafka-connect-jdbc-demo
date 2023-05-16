ALTER SESSION SET CONTAINER=XEPDB1;

CREATE TABLE KAFKACONNECT.USERS (
  USER_ID VARCHAR2(32) PRIMARY KEY NOT NULL,
  FIRST_NAME VARCHAR2(32) NOT NULL,
  LAST_NAME VARCHAR2(32) NOT NULL
);

GRANT SELECT ON KAFKACONNECT.USERS to TESTER;