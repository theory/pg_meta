/* contrib/meta/meta--0.1.sql */

/****************************************************************************************************
 * Schema modification using insert/update/delete                                                   *
 ****************************************************************************************************/

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION meta" to load this file. \quit



/****************************************************************************************************
 * SCHEMA meta
 ****************************************************************************************************/

create schema meta;



/****************************************************************************************************
 * VIEW meta.database                                                                               *
 ****************************************************************************************************/

create or replace view meta.database as
    select pg_database.oid as id,
           pg_database.datname as name,
           pg_database.datdba as owner_id
    from pg_database;

create function meta.database_insert() returns trigger as $$
    begin
        execute 'create database ' || quote_ident(NEW.name);
    end;
$$
language plpgsql;

create function meta.database_update() returns trigger as $$
    declare
        owner_name varchar;
    begin
        if (OLD.name != NEW.name) then
            execute 'alter database ' || quote_ident(OLD.name) || ' rename to ' || quote_ident(NEW.name);
        end if;

        if (OLD.owner_id != NEW.owner_id) then
            select into owner_name
                usename from pg_user where usesysid = NEW.owner_id;

            if (owner_name is NULL) then
                raise exception 'No such role.';
            end if;

            execute 'alter database ' || quote_ident(OLD.name) || ' owner to ' || quote_ident(owner_name);
        end if;
        return NEW;
    end
$$
language plpgsql;

create function meta.database_delete() returns trigger as $$
    begin
        execute 'drop database ' || quote_ident(OLD.name);
    end;
$$
language plpgsql;

create trigger meta_database_insert_trigger instead of insert on meta.database for each row execute procedure meta.database_insert();
create trigger meta_database_update_trigger instead of update on meta.database for each row execute procedure meta.database_update();
create trigger meta_database_delete_trigger instead of delete on meta.database for each row execute procedure meta.database_delete();



/****************************************************************************************************
 * VIEW meta.schema                                                                                 *
 ****************************************************************************************************/

create or replace view meta.schema as 
    select pg_namespace.oid as id,
           pg_namespace.nspname AS name
    from pg_namespace;

create function meta.schema_insert() returns trigger as $$
    declare
        schema_rec record;

    begin
        execute 'create schema ' || quote_ident(NEW.name);
        NEW.id := (select id from meta.schema where name = NEW.name);

        return NEW;
    end;
$$
language plpgsql;

create function meta.schema_update() returns trigger as $$
    begin
        execute 'alter schema ' || quote_ident(OLD.name) || ' rename to ' || quote_ident(NEW.name);
        return NEW;
    end;
$$
language plpgsql;

create function meta.schema_delete() returns trigger as $$
    begin
        raise notice 'drop_schema()';
        execute 'drop schema ' || quote_ident(OLD.name) || ' cascade';
        return OLD;
    end;
$$
language plpgsql;

create trigger meta_schema_insert_trigger instead of insert on meta.schema for each row execute procedure meta.schema_insert();
create trigger meta_schema_update_trigger instead of update on meta.schema for each row execute procedure meta.schema_update();
create trigger meta_schema_delete_trigger instead of delete on meta.schema for each row execute procedure meta.schema_delete();



/****************************************************************************************************
 * VIEW meta.table                                                                                  *
 ****************************************************************************************************/

create or replace view meta.table as
    select pg_class.oid as id,
           pg_class.relname as name,
           pg_namespace.oid as schema_id
    from pg_catalog.pg_class
    join pg_catalog.pg_namespace
      on pg_namespace.oid  = pg_class.relnamespace
    where pg_class.relkind = 'r';

create function meta.table_insert() returns trigger as $$
    declare
        schema_rec record;
        table_rec record;

    begin
        select * into schema_rec from meta.schema where id = NEW.schema_id;
        raise notice 'schema_rec: %', schema_rec;

        execute 'create table ' || quote_ident(schema_rec.name) || '.' || quote_ident(NEW.name) || ' ()';
        NEW.id := (select id from meta.table where schema_id = schema_rec.id and name = NEW.name);
        return NEW;
    end;
$$
language plpgsql;

create function meta.table_update() returns trigger as $$
    declare
        old_schema_rec record;
        new_schema_rec record;
    begin
        select * into old_schema_rec from meta.schema where id = OLD.schema_id;
        select * into new_schema_rec from meta.schema where id = NEW.schema_id;

        if NEW.schema_id != OLD.schema_id then
            execute 'alter table ' || quote_ident(old_schema_rec.name) || '.' || quote_ident(OLD.name) || ' set schema ' || quote_ident(new_schema_rec.name);
        end if;
        if NEW.name != OLD.name then
            execute 'alter table ' || quote_ident(new_schema_rec.name) || '.' || quote_ident(OLD.name) || ' rename to ' || quote_ident(NEW.name);
        end if;

        return NEW;
    end;
$$
language plpgsql;

create function meta.table_delete() returns trigger as $$
    declare
        schema_rec record;

    begin
        select * into schema_rec from meta.schema where id = OLD.schema_id;

        execute 'drop table ' || quote_ident(schema_rec.name) || '.' || quote_ident(OLD.name) || ' cascade';
        return OLD;
    end;
$$
language plpgsql;

create trigger meta_table_insert_trigger instead of insert on meta.table for each row execute procedure meta.table_insert(); 
create trigger meta_table_update_trigger instead of update on meta.table for each row execute procedure meta.table_update();
create trigger meta_table_delete_trigger instead of delete on meta.table for each row execute procedure meta.table_delete();



/****************************************************************************************************
 * VIEW meta.view                                                                                   *
 ****************************************************************************************************/

create or replace view meta.view as
    select c.oid as id,
           c.relname as name,
           n.oid as schema_id,
           pg_get_viewdef(c.oid) as query

    from pg_catalog.pg_class c

    join pg_catalog.pg_namespace n
      on n.oid  = c.relnamespace

    where c.relkind = 'v';

create function meta.view_insert() returns trigger as $$
    declare
        schema_rec record;
        view_id oid;
        stmt_id oid;

    begin
        select * into schema_rec from meta.schema where id = NEW.schema_id;
        execute 'create view ' || quote_ident(schema_rec.name) || '.' || quote_ident(NEW.name) || ' as ' || NEW.query;
        return NEW;
    end;
$$
language plpgsql;

create function meta.view_update() returns trigger as $$
    declare
        old_schema_rec record;
        new_schema_rec record;
        view_id oid;
        stmt_id oid;
    begin
        select * into old_schema_rec from meta.schema where id = OLD.schema_id;
        select * into new_schema_rec from meta.schema where id = NEW.schema_id;
        if NEW.schema_id != OLD.schema_id then
                execute 'alter view ' || quote_ident(old_schema_rec.name) || '.'
                    || quote_ident(OLD.name) || ' set schema ' || quote_ident(new_schema_rec.name);
        end if;
        if NEW.name != OLD.name then
            execute 'alter view ' || quote_ident(new_schema_rec.name) || '.'
                || quote_ident(OLD.name) || ' rename to ' || quote_ident(NEW.name);
        end if;
        if NEW.pg_code != OLD.pg_code then
            -- TODO: This won't actually do anything. Mike needs to make the delete function. Clutter for now.
            -- execute 'delete from sql.start where id = ' || quote_literal(OLD.statement_id) || ' cascade';
            execute 'create or replace view ' || quote_ident(schema_rec.name) || '.' || quote_ident(NEW.name) || ' as ' || NEW.pg_code;
            -- select oid into view_id from pg_class where relkind = 'v' and relname=NEW.name;
            -- select statement_id into stmt_id from sql.parse(NEW.pg_code);
            -- execute 'update meta.view_statement (statement_id, view_id) values (' || stmt_id || ',' || view_id || ') where view_id = ' || NEW.id;
        end if;
        -- if NEW.id != OLD.id then -- Can this ever happen?
        --     execute 'update meta.view_statement set (view_id) = (' || NEW.id || ') where view_id = ' || OLD.id;
        -- end if;
        -- if NEW.statement_id != OLD.statement_id then
        --     execute 'update meta.view_statement set statement_id = ' || NEW.statement_id || ' where view_id = ' || NEW.id;
        -- end if;
        return NEW;
    end;
$$
language plpgsql;

create function meta.view_delete() returns trigger as $$
    declare
        schema_rec record;

    begin
        select * into schema_rec from meta.schema where id = OLD.schema_id;
        -- TODO: This won't actually do anything. Mike needs to make the delete function. Clutter for now.
        --execute 'delete from sql.start where id = ' || quote_literal(OLD.statement_id) || ' cascade';
        -- execute 'delete from meta.view_statement where view_id = ' || OLD.id;
        execute 'drop view ' || quote_ident(schema_rec.name) || '.' || quote_ident(OLD.name) || ' cascade';
        return OLD;
    end;
$$
language plpgsql;

create trigger meta_view_insert_trigger instead of insert on meta.view for each row execute procedure meta.view_insert(); 
create trigger meta_view_update_trigger instead of update on meta.view for each row execute procedure meta.view_update(); 
create trigger meta_view_delete_trigger instead of delete on meta.view for each row execute procedure meta.view_delete(); 



/****************************************************************************************************
 * VIEW meta.column                                                                                 *
 ****************************************************************************************************/

create view meta.column as
    select pg_namespace.nspname || '__' || pg_class.relname || '__' || pg_attribute.attname as id,
           pg_attribute.attname as name,
           pg_class.oid as table_id,
           pg_attribute.attnum as number,
           pg_type.typname as type,
           (not pg_attribute.attnotnull) as nullable,
           pg_attrdef.adsrc as "default",
           (pg_constraint.conname is not null) as primary_key

    from pg_attribute

    left join pg_attrdef on pg_attribute.attrelid = pg_attrdef.adrelid
                        and pg_attribute.attnum = pg_attrdef.adnum
    left join pg_constraint on pg_attribute.attrelid = pg_constraint.conrelid
                           and pg_attribute.attnum = ANY(pg_constraint.conkey)
                           and pg_constraint.contype = 'p'
    join pg_class on pg_class.oid = pg_attribute.attrelid
    join pg_namespace on pg_namespace.oid = pg_class.relnamespace
    join pg_type on pg_attribute.atttypid = pg_type.oid

    where pg_attribute.attnum > 0;

create function meta.column_insert() returns trigger as $$
    declare
        alter_stmt varchar;
        schema_rec record;
        table_rec record;

    begin
        select * into table_rec from meta.table where id = NEW.table_id;
        select * into schema_rec from meta.schema where id = table_rec.schema_id;

        alter_stmt := 'alter table '|| quote_ident(schema_rec.name) || '.' || quote_ident(table_rec.name) || ' add column ' || quote_ident(NEW.name) || ' ' || NEW.type;

        if not NEW.nullable then
            alter_stmt := alter_stmt || ' not null';
        end if;

        if NEW."default" is not null then
            alter_stmt := alter_stmt || ' default ' || NEW."default" || '::' || NEW.type;
        end if;

        execute alter_stmt;

        if NEW.primary_key then
            execute 'alter table ' || quote_ident(schema_rec.name) || '.' || quote_ident(table_rec.name) || ' add primary key (' || quote_ident(NEW.name) || ')';
        end if;

        NEW.id := (select id from meta.column where table_id = table_rec.id and name = NEW.name);

        return NEW;
    end;
$$
language plpgsql;

create or replace function meta.column_update() returns trigger as $$
    declare
        alter_stmt varchar;
        schema_rec record;
        table_rec record;

    begin
        select * into table_rec from meta.table where id = NEW.table_id;
        select * into schema_rec from meta.schema where id = table_rec.schema_id;

        alter_stmt := 'alter table '|| quote_ident(schema_rec.name) || '.' || quote_ident(table_rec.name) || ' ';

        if NEW.name != OLD.name then
            execute alter_stmt || 'rename column ' || quote_ident(OLD.name) || ' to ' || quote_ident(NEW.name);
            alter_stmt := alter_stmt || 'alter column ' || quote_ident(NEW.name) || ' ';
        else
            alter_stmt := alter_stmt || 'alter column ' || quote_ident(OLD.name) || ' ';
        end if;

        if NEW.nullable then
           raise notice 'DROP NOT NULL';
           execute alter_stmt || 'drop not null';
        else
           execute 'alter table '|| quote_ident(schema_rec.name) || '.' || quote_ident(table_rec.name) || ' '|| 'alter column ' || quote_ident(NEW.name) || ' ' || 'set not null';
        end if;

        if NEW."default" != OLD."default" then
            if NEW."default" is not null then
                execute alter_stmt || ' set default ' || quote_literal(NEW."default") || '::' || NEW.type;
            else
                execute alter_stmt || ' drop default';
            end if;
        end if;

        if NEW.primary_key != OLD.primary_key then
            if NEW.primary_key then
                execute 'alter table ' || quote_ident(schema_rec.name) || '.' || quote_ident(table_rec.name) || ' add primary key (' || quote_ident(NEW.name) || ')';
            else
                execute 'alter table ' || quote_ident(schema_rec.name) || '.' || quote_ident(table_rec.name) || ' drop constraint ' || quote_ident(table_rec.name) || '_pkey';
            end if;
        end if;

        return NEW;
    end;
$$
language plpgsql;

create function meta.column_delete() returns trigger as $$
    declare
        schema_rec record;
        table_rec record;

    begin
        select * into table_rec from meta.table where id = OLD.table_id;
        select * into schema_rec from meta.schema where id = table_rec.schema_id;

        execute 'alter table ' || quote_ident(schema_rec.name) || '.' || quote_ident(table_rec.name) || ' drop column ' || quote_ident(OLD.name);

        return OLD;
    end;
$$
language plpgsql;

create trigger meta_column_insert_trigger instead of insert on meta.column for each row execute procedure meta.column_insert(); 
create trigger meta_column_update_trigger instead of update on meta.column for each row execute procedure meta.column_update();
create trigger meta_column_delete_trigger instead of delete on meta.column for each row execute procedure meta.column_delete();



/****************************************************************************************************
 * VIEW meta.foreign_key                                                                            *
 ****************************************************************************************************/

create or replace view meta.foreign_key as
    select q.id,
           q.name,
           array_agg(from_c.id) as from_column_ids,
           array_agg(to_c.id) as to_column_ids,
           q.on_update,
           q.on_delete

    from (
        select oid as id,
               conname as name,

               conrelid as from_table_id,
               unnest(conkey) as from_column_num,

               confrelid as to_table_id,
               unnest(confkey) as to_column_num,

               case pgc.confupdtype when 'a' then 'no action'
                                    when 'r' then 'restrict'
                                    when 'c' then 'cascade'
                                    when 'n' then 'set null'
                                    when 'd' then 'set default'
               end as on_update,

               case pgc.confupdtype when 'a' then 'no action'
                                    when 'r' then 'restrict'
                                    when 'c' then 'cascade'
                                    when 'n' then 'set null'
                                    when 'd' then 'set default'
               end as on_delete

        from pg_constraint pgc

        where pgc.contype = 'f'::"char"
    ) as q

    inner join meta.column from_c on
               from_c.table_id = q.from_table_id and
               from_c.number = q.from_column_num

    inner join meta.column to_c on
               to_c.table_id = q.to_table_id and
               to_c.number = q.to_column_num

    group by q.id,
             q.name,
             q.on_update,
             q.on_delete;

create or replace function meta.foreign_key_insert() returns trigger as $$
    declare
        fkey_create_stmt varchar;
        from_table_name varchar;
        to_table_name varchar;

    begin
        select schema.name || '.' || "table".name
        into from_table_name
        from meta."column"
        inner join meta."table" on
                   "table".id = "column".table_id
        inner join meta."schema" on
                   "schema".id = "table".schema_id
        where "column".id = NEW.from_column_ids[1];

        select schema.name || '.' || "table".name
        into to_table_name
        from meta."column"
        inner join meta."table" on
                   "table".id = "column".table_id
        inner join meta."schema" on
                   "schema".id = "table".schema_id
        where "column".id = NEW.to_column_ids[1];

        fkey_create_stmt := 'alter table ' || from_table_name || ' add constraint ' || quote_ident(NEW.name);
        fkey_create_stmt := fkey_create_stmt || ' foreign key (' ||
                             (
                                 select string_agg(name, ', ')
                                 from meta."column"
                                 where id = any(NEW.from_column_ids)
                             ) || ') references ';
        fkey_create_stmt := fkey_create_stmt || to_table_name || (
                                  select '(' || string_agg("column".name, ', ') || ')'
                                  from meta."column"
                                  where "column".id = any(NEW.to_column_ids)
                             );
        if (NEW.on_update != NULL) then
            fkey_create_stmt := fkey_create_stmt || ' match full on update ' || NEW.on_update;
        end if;
        if (NEW.on_delete != NULL) then
            fkey_create_stmt := fkey_create_stmt || ' on delete ' || NEW.on_delete;
        end if;

        raise notice 'fkey create statement: %', fkey_create_stmt;
        execute fkey_create_stmt;

        return NEW;
    end;
$$
language plpgsql;

create or replace function meta.foreign_key_update() returns trigger as $$
    declare
    begin
        raise exception 'NOT IMPLEMENTED.';
        return NEW;
    end;
$$
language plpgsql;

create or replace function meta.foreign_key_delete() returns trigger as $$
    declare
        table_name varchar;

    begin
        select schema.name || '.' || "table".name
        into table_name
        from meta."column"
        inner join meta."table" on
                   "table".id = "column".table_id
        inner join meta."schema" on
                   "schema".id = "table".schema_id
        where "column".id = OLD.from_column_ids[1];

        execute 'alter table ' || table_name || ' drop constraint ' || quote_ident(OLD.name);

        return OLD;
    end;
$$
language plpgsql;

create trigger meta_foreign_key_insert_trigger instead of insert on meta.foreign_key for each row execute procedure meta.foreign_key_insert();
create trigger meta_foreign_key_update_trigger instead of update on meta.foreign_key for each row execute procedure meta.foreign_key_update(); 
create trigger meta_foreign_key_delete_trigger instead of delete on meta.foreign_key for each row execute procedure meta.foreign_key_delete();



/****************************************************************************************************
 * VIEW meta.function                                                                               *
 ****************************************************************************************************/

create or replace view meta.function as
    with expanded_args as (
        select pgp.oid as id,
               pgp.proname as name,
               pgp.pronamespace as schema_id,
               unnest(pgp.proargmodes) as arg_mode,
               unnest(pgp.proargnames) as arg_name,
               unnest(pgp.proallargtypes) as arg_type,
               pgp.prosrc as code,
               pgp.prorettype as return_type,
               pgl.lanname as language

        from pg_proc pgp

        inner join pg_language pgl on
                   pgl.oid = pgp.prolang

        where pgp.proallargtypes is not null
    ),
    no_args as (
        select pgp.oid as id,
               pgp.proname as name,
               pgp.pronamespace as schema_id,
               null as arg_mode,
               null as arg_name,
               null as arg_type,
               pgp.prosrc as code,
               pgp.prorettype as return_type,
               pgl.lanname as language

        from pg_proc pgp

        inner join pg_language pgl on
                   pgl.oid = pgp.prolang

        where pgp.proallargtypes is null
    )
    select exp_a.id,
           exp_a.name,
           exp_a.schema_id,
           array_agg(case arg_mode when 'i' then 'in'
                                   when 'o' then 'out'
                                   when 'b' then 'inout'
                                   when 'v' then 'variadic'
                                   when 't' then 'table'
                     end || ' ' || quote_ident(arg_name) || ' ' || quote_ident(pgn.nspname) || '.' || quote_ident(pgt.typname)) as arguments,
           exp_a.code,
           exp_a.return_type::regtype,
           exp_a.language

    from expanded_args exp_a

    inner join pg_type pgt on
               pgt.oid = arg_type

    inner join pg_namespace pgn on
               pgn.oid = pgt.typnamespace

    group by exp_a.id,
             exp_a.name,
             exp_a.schema_id,
             exp_a.code,
             exp_a.return_type,
             exp_a.language

    union all

    select noa.id,
           noa.name,
           noa.schema_id,
           array[]::varchar[] as arguments,
           noa.code,
           noa.return_type::regtype,
           noa.language

    from no_args noa;

create or replace function meta.function_insert() returns trigger as $$
    begin
        execute 'create function ' || quote_ident((
            select name
            from meta.schema
            where id = NEW.schema_id
        )) || '.' || quote_ident(NEW.name) || '(' ||
            array_to_string(NEW.arguments, ',') ||
        ') returns ' || NEW.return_type || '
        as $body$
            ' || NEW.code || '
        $body$
        language ' || quote_ident(NEW.language) || ';';

        return NEW;
    end;
$$
language plpgsql;

create or replace function meta.function_update() returns trigger as $$
    begin
        execute 'create or replace function ' || quote_ident((
            select name
            from meta.schema
            where id = NEW.schema_id
        )) || '.' || quote_ident(NEW.name) || '(' ||
            array_to_string(NEW.arguments, ',') ||
        ') returns ' || NEW.return_type || '
        as $body$
            ' || NEW.code || '
        $body$
        language ' || quote_ident(NEW.language) || ';';

        return NEW;
    end;
$$
language plpgsql;

create or replace function meta.function_delete() returns trigger as $$
    begin
        execute 'drop function ' || quote_ident((
            select name
            from meta.schema
            where id = OLD.schema_id
        )) || '.' || quote_ident(OLD.name) || '(' ||
            array_to_string(OLD.arguments, ',') ||
        ')';

        return OLD;
    end;
$$
language plpgsql;

create trigger meta_function_insert_trigger instead of insert on meta.function for each row execute procedure meta.function_insert();
create trigger meta_function_update_trigger instead of update on meta.function for each row execute procedure meta.function_update(); 
create trigger meta_function_delete_trigger instead of delete on meta.function for each row execute procedure meta.function_delete();



/****************************************************************************************************
 * VIEW meta.trigger
 ****************************************************************************************************/

create view meta.trigger as
    select oid as id,
           tgname as name,
           tgrelid as table_id,
           tgfoid as function_id,
           case when (tgtype >> 1 & 1)::boolean then 'before'
                when (tgtype >> 6 & 1)::boolean then 'instead of'
                else 'after'
           end as "when",
           (tgtype >> 2 & 1)::boolean as "insert",
           (tgtype >> 3 & 1)::boolean as "delete",
           (tgtype >> 4 & 1)::boolean as "update",
           (tgtype >> 5 & 1)::boolean as "truncate",
           case when (tgtype & 1)::boolean then 'row'
                else 'statement'
           end as level
    from pg_trigger;

create or replace function meta.trigger_insert() returns trigger as $$
    begin
        execute 'create trigger ' || quote_ident(NEW.name) || ' ' || NEW."when" || ' ' ||
                    array_to_string(
                      array[]::text[]
                      || case NEW."insert" when true then 'insert'
                                           else null
                         end
                      || case NEW."update" when true then 'update'
                                           else null
                         end
                      || case NEW."delete" when true then 'delete'
                                           else null
                         end
                      || case NEW."truncate" when true then 'truncate'
                                             else null
                         end,
                    ' or ')

                || ' on ' || (
                    select quote_ident(s.name) || '.' || quote_ident(t.name)
                    from meta.table t
                    inner join meta.schema s on
                               s.id = t.schema_id
                    where t.id = NEW.table_id
                ) || ' for each ' || NEW."level" || ' execute procedure ' || (
                    select quote_ident(s.name) || '.' || quote_ident(f.name)
                    from meta.function f
                    inner join meta.schema s on
                               s.id = f.schema_id
                    where f.id = NEW.function_id
                ) || '()';

        return NEW;
    end;
$$
language plpgsql;

create or replace function meta.trigger_update() returns trigger as $$
    begin
        execute 'drop trigger ' || quote_ident(OLD.name) || ' on ' || (
            select quote_ident(s.name) || '.' || quote_ident(t.name)
            from meta.table t
            inner join meta.schema s on
                       s.id = t.schema_id
            where t.id = OLD.table_id
        );

        execute 'create trigger ' || quote_ident(NEW.name) || ' ' || NEW."when" || ' ' ||
                    array_to_string(
                      array[]::text[]
                      || case NEW."insert" when true then 'insert'
                                           else null
                         end
                      || case NEW."update" when true then 'update'
                                           else null
                         end
                      || case NEW."delete" when true then 'delete'
                                           else null
                         end
                      || case NEW."truncate" when true then 'truncate'
                                             else null
                         end,
                    ' or ')

                || ' on ' || (
                    select quote_ident(s.name) || '.' || quote_ident(t.name)
                    from meta.table t
                    inner join meta.schema s on
                               s.id = t.schema_id
                    where t.id = NEW.table_id
                ) || ' for each ' || NEW."level" || ' execute procedure ' || (
                    select quote_ident(s.name) || '.' || quote_ident(f.name)
                    from meta.function f
                    inner join meta.schema s on
                               s.id = f.schema_id
                    where f.id = NEW.function_id
                ) || '()';

        return NEW;
    end;
$$
language plpgsql;

create or replace function meta.trigger_delete() returns trigger as $$
    begin
        execute 'drop trigger ' || quote_ident(OLD.name) || ' on ' || (
            select quote_ident(s.name) || '.' || quote_ident(t.name)
            from meta.table t
            inner join meta.schema s on
                       s.id = t.schema_id
            where t.id = OLD.table_id
        );

        return OLD;
    end;
$$
language plpgsql;

create trigger meta_trigger_insert_trigger instead of insert on meta.trigger for each row execute procedure meta.trigger_insert();
create trigger meta_trigger_update_trigger instead of update on meta.trigger for each row execute procedure meta.trigger_update(); 
create trigger meta_trigger_delete_trigger instead of delete on meta.trigger for each row execute procedure meta.trigger_delete();
 


/****************************************************************************************************
 * VIEW meta.role                                                                                   *
 ****************************************************************************************************/

create or replace view meta.role as
   SELECT 
      pgr.oid::integer   AS id,
      pgr.rolname        AS name,
      pgr.rolsuper       AS superuser,
      pgr.rolinherit     AS inherit,
      pgr.rolcreaterole  AS create_role,
      pgr.rolcreatedb    AS create_db,
      -- Catalog update is a little weird, and you can't set it with create role.
      pgr.rolcatupdate   AS catalog_update,
      pgr.rolcanlogin    AS can_login,
      pgr.rolreplication AS replication,
      pgr.rolconnlimit   AS connection_limit,
      ''::text           AS password,
      pga.rolpassword    AS encrypted_password,
      pgr.rolvaliduntil  AS valid_until
   FROM pg_roles pgr
   JOIN pg_authid pga
     ON pgr.oid = pga.oid;

create function meta.role_insert() returns trigger as $$
    declare
        base_query varchar;
    begin
        --raise notice 'role_insert()';
        base_query := 'CREATE ROLE ' || quote_ident(NEW.name);
        if (NEW.superuser) then
            base_query := base_query || ' WITH SUPERUSER';
        else
            base_query := base_query || ' WITH NOSUPERUSER';
        end if;

        if (NEW.inherit) then
            base_query := base_query || ' INHERIT';
        else
            base_query := base_query || ' NOINHERIT';
        end if;

        if (NEW.create_role) then
            base_query := base_query || ' CREATEROLE';
        else
            base_query := base_query || ' NOCREATEROLE';
        end if;

        if (NEW.create_db) then
            base_query := base_query || ' CREATEDB';
        else
            base_query := base_query || ' NOCREATEDB';
        end if;

        if (NEW.can_login) then
            base_query := base_query || ' LOGIN';
        else
            base_query := base_query || ' NOLOGIN';
        end if;

        if (NEW.replication) then
            base_query := base_query || ' REPLICATION';
        else
            base_query := base_query || ' NOREPLICATION';
        end if;

        if (NEW.connection_limit is not NULL) then
            base_query := base_query || ' CONNECTION LIMIT ' || quote_literal(NEW.connection_limit);
        end if;

        if (NEW.password is not NULL) then
            base_query := base_query || ' PASSWORD ' || quote_literal(NEW.password);
        end if;

        if (NEW.encrypted_password is not NULL) then
            base_query := base_query || ' ENCRYPTED PASSWORD ' || quote_literal('md5' || NEW.encrypted_password);
        end if;

        if (NEW.valid_until is not NULL) then
            base_query := base_query || ' VALID UNTIL ' || quote_literal(NEW.valid_until);
        end if;

        execute base_query;
        SELECT oid INTO NEW.id FROM pg_roles WHERE rolname = NEW.name;
        return NEW;
    end;
$$
language plpgsql;

create function meta.role_update() returns trigger as $$
    declare
        base_query varchar;
    begin
        --raise notice 'role_update()';

        base_query := 'ALTER ROLE ' || quote_ident(OLD.name);

        -- We have to do a little more logic here because the user may not specify
        -- whether or not someone is a superuser. The same goes for all of these fields.
        -- Update: Actually postgres autofills in the previous values into the new field.
        if (NEW.superuser) then
            base_query := base_query || ' WITH SUPERUSER';
        else
            base_query := base_query || ' WITH NOSUPERUSER';
        end if;

        if (NEW.inherit) then
            base_query := base_query || ' INHERIT';
        else
            base_query := base_query || ' NOINHERIT';
        end if;

        if (NEW.create_role) then
            base_query := base_query || ' CREATEROLE';
        else
            base_query := base_query || ' NOCREATEROLE';
        end if;

        if (NEW.create_db) then
            base_query := base_query || ' CREATEDB';
        else
            base_query := base_query || ' NOCREATEDB';
        end if;

        if (NEW.can_login) then
            base_query := base_query || ' LOGIN';
        else
            base_query := base_query || ' NOLOGIN';
        end if;

        if (NEW.replication) then
            base_query := base_query || ' REPLICATION';
        else
            base_query := base_query || ' NOREPLICATION';
        end if;

        if (NEW.connection_limit != OLD.connection_limit) then
            base_query := base_query || ' CONNECTION LIMIT ' || quote_literal(NEW.connection_limit);
        end if;

        if (NEW.password != OLD.password) then
            base_query := base_query || ' PASSWORD ' || quote_literal(NEW.password);
        end if;

        if (NEW.encrypted_password != OLD.encrypted_password) then
            base_query := base_query || ' ENCRYPTED PASSWORD ' || quote_literal('md5' || NEW.encrypted_password);
        end if;

        if (NEW.valid_until != OLD.valid_until) then
            base_query := base_query || ' VALID UNTIL ' || quote_literal(NEW.valid_until);
        end if;

        --RAISE NOTICE 'Query %', base_query;
        execute base_query;

        if (NEW.catalog_update != OLD.catalog_update) then
            execute 'UPDATE pg_authid SET rolcatupdate=' ||
                quote_literal(NEW.catalog_update) ||
                ' where rolname=' || quote_literal(NEW.name);
        end if;

        if (NEW.name != OLD.name) then
            execute 'ALTER ROLE ' || quote_ident(OLD.name) || ' RENAME TO ' || quote_ident(NEW.name);
        end if;

        return NEW;
    end;
$$
language plpgsql;

create function meta.role_delete() returns trigger as $$
    begin
        execute 'drop role ' || quote_ident(OLD.name);
        return OLD;
    end;
$$
language plpgsql;

create trigger meta_role_insert_trigger instead of insert on meta.role for each row execute procedure meta.role_insert();
create trigger meta_role_update_trigger instead of update on meta.role for each row execute procedure meta.role_update();
create trigger meta_role_delete_trigger instead of delete on meta.role for each row execute procedure meta.role_delete();
