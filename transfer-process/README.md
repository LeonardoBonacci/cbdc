# Transfers and accounts

## Transfers



---

## Accounts 

```
docker run -itd -e POSTGRES_USER=baeldung -e POSTGRES_PASSWORD=baeldung -p 5432:5432 --name postgresql postgres
```

```
SHOW wal_level
ALTER SYSTEM SET wal_level = logical;

CREATE TABLE IF NOT EXISTS accounts (
  id VARCHAR ( 50 ) PRIMARY KEY,
  poolId VARCHAR ( 50 ) NOT NULL
);

insert into accounts values ('a', 'coro');
insert into accounts values ('b', 'coro');
insert into accounts values ('c', 'coro');
insert into accounts values ('d', 'coro');
insert into accounts values ('e', 'coro');
insert into accounts values ('f', 'coro');
insert into accounts values ('g', 'coro');
insert into accounts values ('h', 'coro');
insert into accounts values ('i', 'coro');
insert into accounts values ('j', 'coro');
insert into accounts values ('k', 'coro');
insert into accounts values ('l', 'coro');
insert into accounts values ('m', 'coro');
insert into accounts values ('n', 'coro');
insert into accounts values ('o', 'coro');
insert into accounts values ('p', 'coro');
insert into accounts values ('q', 'coro');
insert into accounts values ('r', 'coro');
insert into accounts values ('s', 'coro');
insert into accounts values ('t', 'coro');
insert into accounts values ('u', 'coro');
insert into accounts values ('v', 'coro');
insert into accounts values ('w', 'coro');
insert into accounts values ('x', 'coro');
insert into accounts values ('y', 'coro');
insert into accounts values ('z', 'coro');

```