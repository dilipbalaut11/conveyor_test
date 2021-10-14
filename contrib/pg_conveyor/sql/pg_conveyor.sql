CREATE EXTENSION pg_conveyor;

CREATE TABLE test(a int);

SELECT pg_conveyor_init('test'::regclass::oid, 4);
SELECT pg_conveyor_insert('test'::regclass::oid, 'test_data');
SELECT pg_conveyor_read('test'::regclass::oid, 0);

--CASE1
do $$
<<first_block>>
declare
  i int := 0;
  data varchar;
begin
  for i in 1..1000 loop
	data := 'test_dataaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' || i;
	PERFORM pg_conveyor_insert('test'::regclass::oid, data);
  end loop;
end first_block $$;

-- read from some random blocks
SELECT pg_conveyor_read('test'::regclass::oid, 100);
SELECT pg_conveyor_read('test'::regclass::oid, 800);

--CASE2
do $$
<<first_block>>
declare
  i int := 0;
  data varchar;
begin
  for i in 1..5000 loop
	data := 'test_dataaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' || i+1000;
	PERFORM pg_conveyor_insert('test'::regclass::oid, data);
  end loop;
end first_block $$;
SELECT pg_conveyor_read('test'::regclass::oid, 4000);
SELECT pg_conveyor_read('test'::regclass::oid, 3000);

--CASE3
DROP TABLE test;
CREATE TABLE test(a int);
SELECT pg_conveyor_init('test'::regclass::oid, 4);

do $$
<<first_block>>
declare
  i int := 0;
  data varchar;
begin
  for i in 1..50000 loop
	data := 'test_dataaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' || i;
	PERFORM pg_conveyor_insert('test'::regclass::oid, data);
  end loop;
end first_block $$;


--CASE4--(vacuum is failing)
DROP TABLE test;
CREATE TABLE test(a int);
SELECT pg_conveyor_init('test'::regclass::oid, 4);
do $$
<<first_block>>
declare
  i int := 0;
  data varchar;
begin
  for i in 1..5000 loop
	data := 'test_dataaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' || i;
	PERFORM pg_conveyor_insert('test'::regclass::oid, data);
  end loop;
end first_block $$;

SELECT pg_conveyor_truncate('test'::regclass::oid, 3000);
SELECT pg_conveyor_vacuum('test'::regclass::oid);

--CASE5
DROP TABLE test;
CREATE TABLE test(a int);
SELECT pg_conveyor_init('test'::regclass::oid, 4);

do $$
<<first_block>>
declare
  i int := 0;
  data varchar;
begin
  for i in 1..50000 loop
	data := 'test_dataaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' || i;
	PERFORM pg_conveyor_insert('test'::regclass::oid, data);
  end loop;
end first_block $$;

--CASE6 (multi truncate single vacuum)
DROP TABLE test;
CREATE TABLE test(a int);
SELECT pg_conveyor_init('test'::regclass::oid, 4);
do $$
<<first_block>>
declare
  i int := 0;
  data varchar;
begin
  for i in 1..1000 loop
	data := 'test_dataaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' || i;
	PERFORM pg_conveyor_insert('test'::regclass::oid, data);
  end loop;
end first_block $$;

--CASE7
SELECT pg_conveyor_truncate('test'::regclass::oid, 500);
do $$
<<first_block>>
declare
  i int := 0;
  data varchar;
begin
  for i in 1..1000 loop
	data := 'test_dataaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' || i;
	PERFORM pg_conveyor_insert('test'::regclass::oid, data);
  end loop;
end first_block $$;

SELECT pg_conveyor_truncate('test'::regclass::oid, 1800);
SELECT pg_conveyor_vacuum('test'::regclass::oid);

--CASE8
DROP TABLE test;
CREATE TABLE test(a int);
SELECT pg_conveyor_init('test'::regclass::oid, 4);

do $$
<<first_block>>
declare
  i int := 0;
  data varchar;
begin
  for i in 1..5000000 loop
	data := 'test_dataaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' || i;
	PERFORM pg_conveyor_insert('test'::regclass::oid, data);
  end loop;
end first_block $$;
