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

create view meta.database as
    select pg_database.datname as name,
           pg_roles.rolname as owner
    from pg_database
    inner join pg_roles
            on pg_roles.oid = pg_database.datdba;

create function meta.database_insert() returns trigger as $$
    begin
        if ((select rolname from pg_roles where rolname = NEW.owner) is NULL) then
            raise exception 'No such role.';
        end if;

        execute 'create database ' || quote_ident(NEW.name) || ' owner ' || NEW.owner;
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

        if (OLD.owner != NEW.owner) then
            if ((select rolname from pg_roles where rolname = NEW.owner) is NULL) then
                raise exception 'No such role.';
            end if;

            execute 'alter database ' || quote_ident(OLD.name) || ' owner to ' || quote_ident(NEW.owner);
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
    select pg_namespace.nspname AS name
    from pg_namespace;

create function meta.schema_insert() returns trigger as $$
    begin
        execute 'create schema ' || quote_ident(NEW.name);
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

create type meta.table_id as (
    "schema" pg_catalog.name,
    "table" pg_catalog.name
);

create or replace view meta.table as
    select ROW(pg_namespace.nspname, pg_class.relname)::meta.table_id as id,
           pg_namespace.nspname as schema,
           pg_class.relname as name
    from pg_catalog.pg_class
    join pg_catalog.pg_namespace
      on pg_namespace.oid  = pg_class.relnamespace
    where pg_class.relkind = 'r';

create function meta.table_insert() returns trigger as $$
    begin
        execute 'create table ' || quote_ident(NEW.schema) || '.' || quote_ident(NEW.name) || ' ()';
        return NEW;
    end;
$$
language plpgsql;

create function meta.table_update() returns trigger as $$
    begin
        if NEW.schema != OLD.schema then
            execute 'alter table ' || quote_ident(OLD.schema) || '.' || quote_ident(OLD.name) || ' set schema ' || quote_ident(NEW.schema);
        end if;

        if NEW.name != OLD.name then
            execute 'alter table ' || quote_ident(NEW.schema) || '.' || quote_ident(OLD.name) || ' rename to ' || quote_ident(NEW.name);
        end if;

        return NEW;
    end;
$$
language plpgsql;

create function meta.table_delete() returns trigger as $$
    begin
        execute 'drop table ' || quote_ident(OLD.schema) || '.' || quote_ident(OLD.name) || ' cascade';
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

create type meta.view_id as (
    "schema" pg_catalog.name,
    "view" pg_catalog.name
);

create view meta.view as
    select ROW(n.nspname, c.relname)::meta.view_id as id,
           n.nspname as schema,
           c.relname as name,
           pg_get_viewdef(c.oid) as query

    from pg_catalog.pg_class c

    join pg_catalog.pg_namespace n
      on n.oid  = c.relnamespace

    where c.relkind = 'v';

create function meta.view_insert() returns trigger as $$
    begin
        execute 'create view ' || quote_ident(NEW.schema) || '.' || quote_ident(NEW.name) || ' as ' || NEW.query;
        return NEW;
    end;
$$
language plpgsql;

create function meta.view_update() returns trigger as $$
    begin
        if NEW.schema != OLD.schema then
            execute 'alter view ' || quote_ident(OLD.schema) || '.' || quote_ident(OLD.name) || ' set schema ' || quote_ident(NEW.schema);
        end if;

        if NEW.name != OLD.name then
            execute 'alter view ' || quote_ident(NEW.schema) || '.' || quote_ident(OLD.name) || ' rename to ' || quote_ident(NEW.name);
        end if;

        if NEW.query != OLD.query then
            execute 'create or replace view ' || quote_ident(NEW.schema) || '.' || quote_ident(NEW.name) || ' as ' || NEW.query;
        end if;

        return NEW;
    end;
$$
language plpgsql;

create function meta.view_delete() returns trigger as $$
    begin
        execute 'drop view ' || quote_ident(OLD.schema) || '.' || quote_ident(OLD.name) || ' cascade';
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

create type meta.column_id as (
    "schema" pg_catalog.name,
    "table" pg_catalog.name,
    "column" pg_catalog.name
);

create view meta.column as
    select ROW(pg_namespace.nspname, pg_class.relname, pg_attribute.attname)::meta.column_id as id,
           pg_namespace.nspname as schema,
           pg_class.relname as table,
           pg_attribute.attname as name,
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

    begin
        alter_stmt := 'alter table '|| quote_ident(NEW.schema) || '.' || quote_ident(NEW.table)
                      || ' add column ' || quote_ident(NEW.name) || ' ' || NEW.type;

        if not NEW.nullable then
            alter_stmt := alter_stmt || ' not null';
        end if;

        if NEW."default" is not null then
            alter_stmt := alter_stmt || ' default ' || NEW."default" || '::' || NEW.type;
        end if;

        execute alter_stmt;

        if NEW.primary_key then
            execute 'alter table ' || quote_ident(NEW.schema) || '.' || quote_ident(NEW.table)
                    || ' add primary key (' || quote_ident(NEW.name) || ')';
        end if;

        return NEW;
    end;
$$
language plpgsql;

create or replace function meta.column_update() returns trigger as $$
    declare
        alter_stmt varchar;

    begin
        alter_stmt := 'alter table ' || quote_ident(OLD.schema) || '.' || quote_ident(OLD.table) || ' ';

        if NEW.name != OLD.name then
            execute alter_stmt || 'rename column ' || quote_ident(OLD.name) || ' to ' || quote_ident(NEW.name);
            alter_stmt := alter_stmt || 'alter column ' || quote_ident(NEW.name) || ' ';
        else
            alter_stmt := alter_stmt || 'alter column ' || quote_ident(OLD.name) || ' ';
        end if;

        if NEW.nullable then
           execute alter_stmt || 'drop not null';
        else
           execute 'alter table '|| quote_ident(OLD.schema) || '.' || quote_ident(OLD.table) || ' '|| 'alter column ' || quote_ident(NEW.name) || ' ' || 'set not null';
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
                execute 'alter table ' || quote_ident(OLD.schema) || '.' || quote_ident(OLD.table) || ' add primary key (' || quote_ident(NEW.name) || ')';
            else
                execute 'alter table ' || quote_ident(OLD.schema) || '.' || quote_ident(OLD.table) || ' drop constraint ' || quote_ident(OLD.table) || '_pkey';
            end if;
        end if;

        return NEW;
    end;
$$
language plpgsql;

create function meta.column_delete() returns trigger as $$
    begin
        execute 'alter table ' || quote_ident(OLD.schema) || '.' || quote_ident(OLD.table) || ' drop column ' || quote_ident(OLD.name);
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
    select q.name,
           array_agg((f_pgn.nspname, f_pgc.relname, f_pga.attname)::meta.column_id) as from_column_ids,
           array_agg((t_pgn.nspname, t_pgc.relname, t_pga.attname)::meta.column_id) as to_column_ids,
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

    inner join pg_attribute f_pga
            on f_pga.attnum = q.from_column_num and
               f_pga.attrelid = q.from_table_id

    inner join pg_class f_pgc
            on f_pgc.oid = q.from_table_id

    inner join pg_namespace f_pgn
            on f_pgn.oid = f_pgc.relnamespace

    inner join pg_attribute t_pga
            on t_pga.attnum = q.to_column_num and
               t_pga.attrelid = q.to_table_id

    inner join pg_class t_pgc
            on t_pgc.oid = q.to_table_id

    inner join pg_namespace t_pgn
            on t_pgn.oid = t_pgc.relnamespace

    group by q.name, q.on_update, q.on_delete;        

create or replace function meta.foreign_key_insert() returns trigger as $$
    declare
        fkey_create_stmt varchar;

    begin
        fkey_create_stmt := 'alter table ' || (
            select c.schema || '.' || c."table"
            from meta."column" c
            where c.id = NEW.from_column_ids[1]
        ) || ' add constraint ' || quote_ident(NEW.name)

        || ' foreign key (' || (
            select string_agg(name, ', ')
            from meta."column"
            where id = any(NEW.from_column_ids)

        ) || ') references ' || (
            select c.schema || '.' || c."table"
            from meta."column" c
            where c.id = NEW.to_column_ids[1]
        )

        || (
            select '(' || string_agg(c.name, ', ') || ')'
            from meta."column" c
            where c.id = any(NEW.to_column_ids)
        );

        if fkey_create_stmt is null then
            raise exception 'A provided column identifier didn''t match an existing column';
        end if;

        if (NEW.on_update != NULL) then
            fkey_create_stmt := fkey_create_stmt || ' match full on update ' || NEW.on_update;
        end if;

        if (NEW.on_delete != NULL) then
            fkey_create_stmt := fkey_create_stmt || ' on delete ' || NEW.on_delete;
        end if;

        execute fkey_create_stmt;

        return NEW;
    end;
$$
language plpgsql;

create or replace function meta.foreign_key_update() returns trigger as $$
    declare
        fkey_create_stmt varchar;

    begin
        execute 'alter table ' || (
            select c.schema || '.' || c."table"
            from meta."column" c
            where c.id = OLD.from_column_ids[1]
        ) || ' drop constraint ' || quote_ident(OLD.name);

        fkey_create_stmt := 'alter table ' || (
            select c.schema || '.' || c."table"
            from meta."column" c
            where c.id = NEW.from_column_ids[1]
        ) || ' add constraint ' || quote_ident(NEW.name)

        || ' foreign key (' || (
            select string_agg(name, ', ')
            from meta."column"
            where id = any(NEW.from_column_ids)

        ) || ') references ' || (
            select c.schema || '.' || c."table"
            from meta."column" c
            where c.id = NEW.to_column_ids[1]
        )

        || (
            select '(' || string_agg(c.name, ', ') || ')'
            from meta."column" c
            where c.id = any(NEW.to_column_ids)
        );

        if fkey_create_stmt is null then
            raise exception 'A provided column identifier didn''t match an existing column';
        end if;

        if (NEW.on_update != NULL) then
            fkey_create_stmt := fkey_create_stmt || ' match full on update ' || NEW.on_update;
        end if;

        if (NEW.on_delete != NULL) then
            fkey_create_stmt := fkey_create_stmt || ' on delete ' || NEW.on_delete;
        end if;

        execute fkey_create_stmt;

        return NEW;
    end;
$$
language plpgsql;

create or replace function meta.foreign_key_delete() returns trigger as $$
    declare
        table_name varchar;

    begin
        execute 'alter table ' || (
            select c.schema || '.' || c."table"
            from meta."column" c
            where c.id = OLD.from_column_ids[1]
        ) || ' drop constraint ' || quote_ident(OLD.name);

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

create type meta.function_id as (
    "schema" pg_catalog.name,
    "function" pg_catalog.name
);

create or replace view meta.function as
    with expanded_args as (
        select pgp.oid as id,
               pgn.nspname as schema,
               pgp.proname as name,
               unnest(pgp.proargnames) as arg_name,
               unnest(pgp.proargtypes) as arg_type,
               pgp.prosrc as code,
               pgp.prorettype as return_type,
               pgl.lanname as language

        from pg_proc pgp

        inner join pg_namespace pgn on
                   pgn.oid = pgp.pronamespace

        inner join pg_language pgl on
                   pgl.oid = pgp.prolang

        where pgp.proargnames is not null
    ),
    no_args as (
        select pgp.oid as id,
               pgn.nspname as schema,
               pgp.proname as name,
               null as arg_name,
               null as arg_type,
               pgp.prosrc as code,
               pgp.prorettype as return_type,
               pgl.lanname as language

        from pg_proc pgp

        inner join pg_namespace pgn on
                   pgn.oid = pgp.pronamespace

        inner join pg_language pgl on
                   pgl.oid = pgp.prolang

        where pgp.proargnames is null
    )
    select ROW(exp_a.schema, exp_a.name)::meta.function_id as id,
           exp_a.schema,
           exp_a.name,
           array_agg(quote_ident(arg_name) || ' ' || quote_ident(pgn.nspname) || '.' || quote_ident(pgt.typname)) as arguments,
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
             exp_a.schema,
             exp_a.code,
             exp_a.return_type,
             exp_a.language

    union all

    select ROW(noa.schema, noa.name)::meta.function_id,
           noa.schema,
           noa.name,
           array[]::varchar[] as arguments,
           noa.code,
           noa.return_type::regtype,
           noa.language

    from no_args noa;

create or replace function meta.function_insert() returns trigger as $$
    begin
        execute 'create function ' || quote_ident(NEW.schema) || '.' || quote_ident(NEW.name) || '(' ||
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
        execute 'create or replace function ' || quote_ident(NEW.schema) || '.' || quote_ident(NEW.name) || '(' ||
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
        execute 'drop function ' || quote_ident(OLD.schema) || '.' || quote_ident(OLD.name) || '(' ||
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
    select t_pgn.nspname as t_schema,
           pgc.relname as t_name,
           f_pgn.nspname as f_schema,
           pgp.proname as f_name,
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

    from pg_trigger

    inner join pg_proc pgp
            on pgp.oid = tgfoid

    inner join pg_namespace f_pgn
            on f_pgn.oid = pgp.pronamespace

    inner join pg_class pgc
            on pgc.oid = tgrelid

    inner join pg_namespace t_pgn
            on t_pgn.oid = pgc.relnamespace;

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

                || ' on ' || quote_ident(NEW.t_schema) || '.' || quote_ident(NEW.t_name)

                || ' for each ' || NEW."level" || ' execute procedure '
                || quote_ident(NEW.f_schema) || '.' || quote_ident(NEW.f_name) || '()';

        return NEW;
    end;
$$
language plpgsql;

create or replace function meta.trigger_update() returns trigger as $$
    begin
        execute 'drop trigger ' || quote_ident(OLD.name) || ' on ' || quote_ident(OLD.t_schema) || '.' || quote_ident(OLD.t_name);

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

                || ' on ' || quote_ident(NEW.t_schema) || '.' || quote_ident(NEW.t_name)

                || ' for each ' || NEW."level" || ' execute procedure '
                || quote_ident(NEW.f_schema) || '.' || quote_ident(NEW.f_name) || '()';

        return NEW;
    end;
$$
language plpgsql;

create or replace function meta.trigger_delete() returns trigger as $$
    begin
        execute 'drop trigger ' || quote_ident(OLD.name) || ' on '
                || ' on ' || quote_ident(NEW.t_schema) || '.' || quote_ident(NEW.t_name);

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

create or replace function meta.role_insert() returns trigger as $$
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



/****************************************************************************************************
 * VIEW meta.constraint_unique                                                                      *
 ****************************************************************************************************/

create view meta.constraint_unique as 
    select pg_namespace.nspname as schema,
           pg_class.relname as table,
           pgc.conname as name,
           array_agg(pga.attname) as columns
    from pg_constraint pgc
    inner join pg_class
            on pg_class.oid = conrelid
    inner join pg_namespace
            on pg_namespace.oid = pg_class.relnamespace
    inner join pg_class pgcl
            on pgcl.oid = conindid
    inner join pg_attribute pga
            on pga.attrelid = pgcl.oid
    where contype = 'u'
    group by pg_namespace.nspname,
             pgc.oid,
             pgc.conname,
             pg_class.relname;

create or replace function meta.constraint_unique_insert() returns trigger as $$
    begin
        if NEW.schema is null then
             raise exception 'A schema is required.';
        end if;

        if NEW.table is null then
             raise exception 'A table is required.';
        end if;

        if NEW.name is null then
             raise exception 'A name is required.';
        end if;

        if NEW.columns is null or array_length(NEW.columns, 1) = 0 then
             raise exception 'Columns are required.';
        end if;

        execute 'alter table ' || quote_ident(NEW.schema) || '.' || quote_ident(NEW.table)
                || ' add constraint ' || NEW.name || ' unique (' || array_to_string(NEW.columns, ',') || ')';

        return NEW;
    end;
$$
language plpgsql;

create or replace function meta.constraint_unique_update() returns trigger as $$
    begin
        execute 'alter table ' || quote_ident(OLD.schema) || '.' || quote_ident(OLD.table) || ' drop constraint ' || OLD.name;
        
        if NEW.schema is null then
             raise exception 'A schema is required.';
        end if;

        if NEW.table is null then
             raise exception 'A table is required.';
        end if;

        if NEW.name is null then
             raise exception 'A name is required.';
        end if;

        if NEW.columns is null or array_length(NEW.columns, 1) = 0 then
             raise exception 'Columns are required.';
        end if;

        execute 'alter table ' || quote_ident(NEW.schema) || '.' || quote_ident(NEW.table)
                || ' add constraint ' || NEW.name || ' unique (' || array_to_string(NEW.columns, ',') || ')';

        return NEW;
    end;
$$
language plpgsql;

create or replace function meta.constraint_unique_delete() returns trigger as $$
    begin
        execute 'alter table ' || quote_ident(OLD.schema) || '.' || quote_ident(OLD.table) || ' drop constraint ' || OLD.name;
        return OLD;
    end;
$$
language plpgsql;

create trigger meta_constraint_unique_insert_trigger instead of insert on meta.constraint_unique for each row execute procedure meta.constraint_unique_insert();
create trigger meta_constraint_unique_update_trigger instead of update on meta.constraint_unique for each row execute procedure meta.constraint_unique_update();
create trigger meta_constraint_unique_delete_trigger instead of delete on meta.constraint_unique for each row execute procedure meta.constraint_unique_delete();



/****************************************************************************************************
 * VIEW meta.constraint_check                                                                       *
 ****************************************************************************************************/

create view meta.constraint_check as 
    select pg_namespace.nspname as schema,
           pg_class.relname as table,
           pgc.conname as name,
           pgc.consrc as check
    from pg_constraint pgc
    inner join pg_class
            on pg_class.oid = conrelid
    inner join pg_namespace
            on pg_namespace.oid = pg_class.relnamespace
    where contype = 'c'
    group by pg_namespace.nspname,
             pgc.oid,
             pgc.conname,
             pg_class.relname,
             pgc.consrc;

create or replace function meta.constraint_check_insert() returns trigger as $$
    begin
        if NEW.schema is null then
             raise exception 'A schema is required.';
        end if;

        if NEW.table is null then
             raise exception 'A table is required.';
        end if;

        if NEW.name is null then
             raise exception 'A name is required.';
        end if;

        if NEW.check is null then
             raise exception 'A check expression is required.';
        end if;

        execute 'alter table ' || quote_ident(NEW.schema) || '.' || quote_ident(NEW.table)
                || ' add constraint ' || NEW.name || ' check (' || NEW.check || ')';

        return NEW;
    end;
$$
language plpgsql;

create or replace function meta.constraint_check_update() returns trigger as $$
    begin
        execute 'alter table ' || quote_ident(OLD.schema) || '.' || quote_ident(OLD.table) || ' drop constraint ' || OLD.name;
        
        if NEW.schema is null then
             raise exception 'A schema is required.';
        end if;

        if NEW.table is null then
             raise exception 'A table is required.';
        end if;

        if NEW.name is null then
             raise exception 'A name is required.';
        end if;

        if NEW.check is null then
             raise exception 'A check expression is required.';
        end if;


        execute 'alter table ' || quote_ident(NEW.schema) || '.' || quote_ident(NEW.table)
                || ' add constraint ' || NEW.name || ' check (' || NEW.check || ')';

        return NEW;
    end;
$$
language plpgsql;

create or replace function meta.constraint_check_delete() returns trigger as $$
    begin
        execute 'alter table ' || quote_ident(OLD.schema) || '.' || quote_ident(OLD.table) || ' drop constraint ' || OLD.name;
        return OLD;
    end;
$$
language plpgsql;

create trigger meta_constraint_check_insert_trigger instead of insert on meta.constraint_check for each row execute procedure meta.constraint_check_insert();
create trigger meta_constraint_check_update_trigger instead of update on meta.constraint_check for each row execute procedure meta.constraint_check_update();
create trigger meta_constraint_check_delete_trigger instead of delete on meta.constraint_check for each row execute procedure meta.constraint_check_delete();
