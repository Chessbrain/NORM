/*
    SYSTEM FUNCTIONS AND DATA TYPES REQUIRED FOR EXTENSION TO WORK
*/
-- unnests 2d arrays into Nx1D arrays (N - number of elements in the 2D array)
create or replace function unnest_2d (anyarray, out a anyarray)
  returns setof anyarray
  language plpgsql
  immutable
  strict
as
$$
begin
    foreach a slice 1 in array $1 loop
        return next;
    end loop;
end;
$$;


-- creates function input parameters with p_ prefix
create or replace function _parameter_generator(text[])
  returns text
  language sql
  immutable
as
$$
    select array_to_string(array_agg(concat('p_',x)),',') from unnest($1) x;
$$;


-- generates input parameter for functions
create or replace function _parameter_generator(
    p_tables text[],
    p_columns text[],
    p_is_update boolean default false
)
  returns text
  language plpgsql
as
$$
declare
    v_input_parameters text[];
begin
    if p_is_update then
        select array_agg(E'\n\t\t'||c.column_name||' = '||'coalesce('||'p_'||c.column_name||','||c.column_name||')') into v_input_parameters
        from information_schema.columns c
        where c.table_name = any(p_tables)
            and c.column_name = any(p_columns);
    else
        select array_agg('p_'||c.column_name||' '||c.data_type) into v_input_parameters
        from information_schema.columns c
        where c.table_name = any(p_tables)
            and c.column_name = any(p_columns);
    end if;

    return array_to_string(v_input_parameters,',','null');
end;
$$;


-- overload for scalar version of _parameter_generator
create or replace function _parameter_generator(
    p_table_name text,
    p_columns text[],
    p_is_update boolean default false
)
  returns text
  language plpgsql
as
$$
begin
    return _parameter_generator(array[p_table_name],p_columns,p_is_update);
end;
$$;


-- mapper, returns which columns belong to which table
create or replace function _table_columns(
    p_tables text[],
    p_columns text[]
)
  returns table(
    table_name text,
    column_name text
  )
  language plpgsql
as
$$
begin
    return query
    select c.table_name::text, c.column_name::text
    from information_schema.columns c
    where c.table_name = any(p_tables)
        and c.column_name = any(p_columns);
end;
$$;


-- where clause generator, p_is_null - turns all parameters into optional filters, p_and - true = and, false = or
create or replace function _where_generator(
    p_columns text[],
    p_is_null boolean default false,
    p_and boolean default true
)
  returns text
  language plpgsql
  immutable
as
$$
declare
    v_where_clause text[];
    v_condition_type text := case when p_and then E'\tand' else E'\tor' end;
begin
    if p_is_null then
        select array_agg('(p_'||c||' is null or '||c||' = '||'p_'||c||E')\n\t') into v_where_clause
        from unnest(p_columns) c;
    else
        select array_agg('('||c||' = '||'p_'||c||E')\n\t') into v_where_clause
        from unnest(p_columns) c;
    end if;

    return rtrim(array_to_string(v_where_clause,v_condition_type,'null'),E'\n\t');
end;
$$;


-- include table in condition generation (for get function generator mainly)
create or replace function _where_generator(
    p_tables text[],
    p_columns text[],
    p_is_null boolean default false,
    p_and boolean default true
)
  returns text
  language plpgsql
as
$$
declare
    v_where_clause text[];
    v_condition_type text := case when p_and then E'\tand' else E'\tor' end;
begin
    select
        case
            when p_is_null then
                array_agg('(p_'||x.column_name||' is null or '||x.table_name||'.'||x.column_name||' = '||'p_'||x.column_name||E')\n\t')
            else
                array_agg('('||x.table_name||'.'||x.column_name||' = '||'p_'||x.column_name||E')\n\t')
        end
    into v_where_clause
    from (
        select distinct on(tc.column_name) tc.table_name, tc.column_name
        from _table_columns(p_tables,p_columns) tc
        order by tc.column_name
    ) x;

    return rtrim(array_to_string(v_where_clause,v_condition_type,'null'),E'\n\t');
end;
$$;


-- checks if columns specified exist for this table (p_not_null checks for non-nullable columns)
create or replace function _check_columns(
    p_tables text[],
    p_columns text[],
    p_not_null boolean default false
)
  returns boolean
  language plpgsql
  security definer
as
$$
declare
    a_missing_columns text[];
    v_success boolean := true;
begin
    if p_tables is null or p_columns is null or array_length(p_tables,1) is null or array_length(p_columns,1) is null
    then
        raise 'Please specify valid table(s) and column(s).' using errcode = 'N0000';
    end if;

    if not exists(select from information_schema.tables t where t.table_name = any(p_tables)) then
        raise 'Specified table(s) does not exist, please create it first' using errcode = 'N0001';
    end if;

    if not p_not_null then
        select array_agg(ic) into a_missing_columns
        from unnest(p_columns) ic
            left join information_schema.columns c on c.column_name = ic
                and c.table_name = any(p_tables)
        where c.column_name is null;

        if exists (select from unnest(a_missing_columns) mc where mc is not null) then
            raise 'Specified column(s): %, do not exist.', array_to_string(a_missing_columns,',','null') using errcode = 'N0002';
        end if;
    end if;

    if p_not_null then
        select array_agg(c.column_name) into a_missing_columns
        from information_schema.columns c
            left join unnest(p_columns) ic on ic = c.column_name
        where c.table_name = any(p_tables)
            and c.is_nullable = 'NO'
            and ic is null
            and pg_get_serial_sequence(c.table_name,c.column_name) is null;

        if exists (select from unnest(a_missing_columns) mc where mc is not null) then
            raise 'Missing non-nullable column(s): %.', array_to_string(a_missing_columns,',','null') using errcode = 'N0003';
        end if;
    end if;

    return v_success;
end;
$$;


-- overload for scalar version of _check_columns
create or replace function _check_columns(
    p_tables text,
    p_columns text[],
    p_not_null boolean default false
)
  returns boolean
  language plpgsql
  security definer
as
$$
begin
    return _check_columns(array[p_tables],p_columns,p_not_null);
end;
$$;


-- generates returns table columns (in the same order as _select_generator)
create or replace function _return_generator(
    p_tables text[],
    p_columns text[]
)
  returns text
  language plpgsql
as
$$
declare
    v_return text[];
begin
    select array_agg(E'\n\t'||x.column_name||' '||x.data_type order by x.column_name) into v_return
    from (
        select distinct c.column_name, c.data_type
        from information_schema.columns c
        where c.table_name = any(p_tables)
            and c.column_name = any(p_columns)
    ) x;

    return array_to_string(v_return,',');
end;
$$;


-- generates from clause
create or replace function _from_generator(text)
  returns text
  language sql
  immutable
  strict
as
$$
    select 'from '||$1;
$$;


-- generates from and join clauses (cannot handle left/right joins and multi-fk tables, separate functions will be created for such cases)
create or replace function _join_generator(
    p_tables text[]
)
  returns text
  language plpgsql
as
$$
declare
    v_from text;
    a_tables_ordered text[];
    a_joins text[];
    a_query_string text[];
begin
    if array_length(p_tables,1) > 1 and exists (select from information_schema.table_constraints c where c.table_name = any(p_tables) and c.constraint_type = 'FOREIGN KEY' group by table_name having count(*) > 1) then
        raise 'Function currently doesn''t support tables with more than a single FK. Will be implemented in future versions.' using errcode ='N0005';
    end if;

    select array_agg(x.table_name order by importance) into a_tables_ordered
    from (
        select c.table_name, count(*) as importance
        from information_schema.table_constraints c
            join information_schema.key_column_usage kcu on kcu.constraint_name = c.constraint_name
            join information_schema.constraint_column_usage ccu on ccu.constraint_name = c.constraint_name
        where kcu.table_name = any(p_tables)
            and ccu.table_name = any(p_tables)
        group by c.table_name
    ) x;

    v_from = _from_generator(a_tables_ordered[1]);
    a_query_string = array_agg(v_from);

    select
        array_agg('join '||kcu.table_name||' on '||kcu.table_name||'.'||kcu.column_name ||' = '|| ccu.table_name||'.'||ccu.column_name order by a.ordinality)
    into a_joins
    from unnest(a_tables_ordered) with ordinality a
        join information_schema.table_constraints c on c.table_name = a
        join information_schema.key_column_usage kcu on kcu.constraint_name = c.constraint_name
        join information_schema.constraint_column_usage ccu on ccu.constraint_name = c.constraint_name
    where c.constraint_type = 'FOREIGN KEY'
        and kcu.table_name = any(p_tables)
        and ccu.table_name = any(p_tables);

    a_query_string := array_cat(a_query_string,a_joins);

    return array_to_string(a_query_string,E'\n\t\t');
end;
$$;


-- generates select clause columns (ordered in the same way as _return_generator)
create or replace function _select_generator(
    p_tables text[],
    p_columns text[]
)
  returns text
  language plpgsql
as
$$
declare
    v_select text[];
begin
    select array_agg(E'\n\t\t'||x.table_name||'.'||x.column_name order by x.column_name) into v_select
    from (
        select distinct on(c.column_name) c.table_name, c.column_name
        from information_schema.columns c
        where c.table_name = any(p_tables)
            and c.column_name = any(p_columns)
        order by c.column_name,c.table_name
    ) x;

    return array_to_string(v_select,',');
end;
$$;



/*
    IMPLEMENTATION
*/


-- creates insert function (cannot handle multiple tables for now)
create or replace function norm_insert(
    p_table_name text,
    p_columns text[]
)
  returns boolean
  language plpgsql
as
$$
declare
    v_input_parameters text;
    v_insert_parameters text;
    v_column_names text := array_to_string(p_columns,',');
    v_success boolean := true;
    v_query text;
begin
    perform _check_columns(p_table_name,p_columns,true);

    v_input_parameters = _parameter_generator(p_table_name,p_columns);
    v_insert_parameters = _parameter_generator(p_columns);

    v_query =
'create or replace function insert_'||p_table_name||'('||v_input_parameters||')
  returns boolean
  language plpgsql
  security definer
as
$func$
begin
    insert into '||p_table_name||'('||v_column_names||')
    values('||v_insert_parameters||');

    return found;
end;
$func$;';

    execute v_query;

    return v_success;
end;
$$;


-- creates update function (cannot handle multiple tables for now)
create or replace function norm_update(
    p_table_name text,
    p_columns text[],
    p_filter_columns text[]
)
  returns boolean
  language plpgsql
as
$$
declare
    v_generator_input text[] := p_filter_columns || p_columns;
    v_input_parameters text;
    v_update_parameters text;
    v_where_clause text;
    v_success boolean := true;
    v_query text;
begin
    if p_filter_columns is null then raise 'Cannot create update function without filter(s).' using errcode = 'N0004'; end if;

    perform _check_columns(p_table_name,p_columns);
    perform _check_columns(p_table_name,p_filter_columns);

    v_input_parameters = _parameter_generator(p_table_name,v_generator_input);
    v_update_parameters = _parameter_generator(p_table_name,p_columns,true);
    v_where_clause = _where_generator(p_filter_columns);

    v_query =
'create or replace function update_'||p_table_name||'('||v_input_parameters||')
  returns boolean
  language plpgsql
  security definer
as
$func$
begin
    update '||p_table_name||' set'||v_update_parameters||
    E'\n\twhere '||v_where_clause||E'\t;

    return found;
end;
$func$;';

    execute v_query;

    return v_success;
end;
$$;


-- creates delete function (cannot handle multiple tables for now)
create or replace function norm_delete(
    p_table_name text,
    p_filters text[]
)
  returns boolean
  language plpgsql
as
$$
declare
    v_input_parameters text;
    v_where_clause text;
    v_query text;
    v_success boolean := true;
begin
    perform _check_columns(p_table_name,p_filters);

    v_input_parameters = _parameter_generator(p_table_name,p_filters);
    v_where_clause = _where_generator(p_filters);

    v_query =
'create or replace function delete_'||p_table_name||'('||v_input_parameters||')
  returns boolean
  language plpgsql
  security definer
as
$func$
begin
    delete from '||p_table_name||
    E'\n\twhere '||v_where_clause||';

    return found;
end;
$func$;';

    execute v_query;

    return v_success;

end;
$$;


-- creates get function with inner join capabilities only (cannot handle multi-fk tables)
create or replace function norm_get(
    p_tables text[],
    p_columns text[],
    p_filters text[] default null,
    p_function_name text default null
)
  returns boolean
  language plpgsql
as
$$
declare
    v_function_name text := coalesce(p_function_name,'get_'||array_to_string(p_tables,'_'));
    v_input_parameters text := '';
    v_returns_table text;
    v_select_clause text;
    v_from_join_clause text;
    v_where_clause text;
    v_query text;
    v_success boolean := true;
begin
    perform _check_columns(p_tables,p_columns);

    if p_filters is not null then
        perform _check_columns(p_tables,p_filters);
        v_input_parameters = _parameter_generator(p_tables,p_filters);
    end if;

    v_returns_table = _return_generator(p_tables,p_columns);
    v_select_clause = _select_generator(p_tables,p_columns);
    v_from_join_clause = _join_generator(p_tables);
    v_where_clause = coalesce(E'\n\twhere '||_where_generator(p_tables,p_filters),'');

    v_query =
'create or replace function '||v_function_name||'('||v_input_parameters||E')
  returns table (\n\t' ||
v_returns_table||')
  language plpgsql
  security definer
as
$func$
begin
    return query
    select '||v_select_clause||E'\n\t'||v_from_join_clause||
    v_where_clause||';
end;
$func$;';

    execute v_query;

    return v_success;
end;
$$;



