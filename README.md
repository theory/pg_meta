Meta: an extension for PostgreSQL
=================================

This extension turns DDL operations into DML operations. Think of it as a read-write system catalog where schema is manipulated by making changes to the data model directly.

Schema
------
```sql
insert into meta.schema (name) values ('bookstore');
update meta.schema set name = 'book_store' where name = 'bookstore';
delete from meta.schema where name = 'book_store';
```
Table
-----
```sql
insert into meta.table (schema, name) values ('bookstore', 'book');
update meta.table set name = 'books' where schema = 'bookstore' and name = 'book';
delete from meta.schema where name = 'books';
```
Column
------
```sql
insert into meta.column (schema, "table", name, type, nullable)
values ('bookstore', 'book', 'price', 'money', false);

update meta.column set "default" = 0
where schema = 'bookstore' and "table" = 'book' and name = 'price';
-- or alternatively
update meta.column set "default" = 0
where id = ('bookstore', 'book', 'price')::meta.column_id;

delete from meta.column where id = ('bookstore', 'book', 'price')::meta.column_id;
```
View
----
```sql
insert into meta.view (schema, name, query)
values ('bookstore', 'inexpensive_books', 'select * from bookstore.book where price < 5;');

update meta.view
set query = 'select * from bookstore.book where price < 10;'
where id = ('bookstore', 'inexpensive_books')::meta.view_id;
```
