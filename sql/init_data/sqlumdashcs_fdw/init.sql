DROP TABLE if EXISTS numbers;
DROP TABLE if EXISTS gloc1_1;
DROP TABLE if EXISTS gloc1_2;

create table numbers(a int primary key, b varchar(255) unique);
create table gloc1_1 (a int PRIMARY KEY, b int generated always as (a * 2) stored);
create table gloc1_2 (a int primary key, b int generated always as (a * 2) stored, c int generated always as (a * 3) stored, d int generated always as (a * 4) stored);

.q
