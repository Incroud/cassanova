-- USAGE: cqlsh localhost 9160 -f 'before.cql';

<cql>
CREATE KEYSPACE IF NOT EXISTS cassanova_test
   WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1};
</cql>

<cql>
USE cassanova_test;
</cql>

-- table
<cql>
CREATE TABLE IF NOT EXISTS cassanova_test.users (
  username text PRIMARY KEY,
  firstname text,
  lastname text,
  birthdate timestamp,
  email text,
  password text,
  session_token text
);
</cql>

<cql>
CREATE INDEX IF NOT EXISTS users_email
  ON cassanova_test.users (email);
</cql>
