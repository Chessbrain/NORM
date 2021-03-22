/*
    SYSTEM FUNCTIONS AND DATA TYPES REQUIRED FOR EXTENSION TO WORK
*/
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

/*create type input_parameters as (
    parameter_name text,
    data_type text
);*/

/*
    IMPLEMENTATION
*/
-- creates function input parameters with p_ prefix
create or replace function _parameter_generator(text[])
  returns text
  language sql
  immutable
as
$$
    select array_to_string(array_agg(concat('p_',x)),',') from unnest($1) x;
$$;



create or replace function _parameter_generator(
    p_table_name text,
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
        where c.table_name = p_table_name
            and c.column_name = any(p_columns);
    else
        select array_agg('p_'||c.column_name||' '||c.data_type) into v_input_parameters
        from information_schema.columns c
        where c.table_name = p_table_name
            and c.column_name = any(p_columns);
    end if;

    return array_to_string(v_input_parameters,',','null');
end;
$$;



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

    return array_to_string(v_where_clause,v_condition_type,'null');
end;
$$;



-- checks if columns specified exist for this table (p_not_null checks for non-nullable columns)
create or replace function _check_columns(
    p_table_name text,
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
    if not exists(select from information_schema.tables t where t.table_name = p_table_name) then
        raise 'Specified table does not exist, please create it first' using errcode = 'N0001';
    end if;

    select array_agg(ic) into a_missing_columns
    from unnest(p_columns) ic
        left join information_schema.columns c on c.column_name = ic and c.table_name = p_table_name
    where c.column_name is null;

    if exists (select from unnest(a_missing_columns) mc where mc is not null) then
        raise 'Specified column(s): %, do not exist in table %.', array_to_string(a_missing_columns,',','null'),p_table_name using errcode = 'N0002';
    end if;

    if p_not_null then
        select array_agg(c.column_name) into a_missing_columns
        from information_schema.columns c
            left join unnest(p_columns) ic on ic = c.column_name
        where c.table_name = p_table_name
            and c.is_nullable = 'NO'
            and ic is null;

        if exists (select from unnest(a_missing_columns) mc where mc is not null) then
            raise 'Missing non-nullable column(s): %.', array_to_string(a_missing_columns,',','null') using errcode = 'N0003';
        end if;
    end if;

    return v_success;
end;
$$;



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
    v_filters text;
    v_query text;
    v_success boolean := true;
begin
    perform _check_columns(p_table_name,p_filters);

    v_input_parameters = _parameter_generator(p_table_name,p_filters);
    v_filters = _where_generator(p_filters);

    v_query =
'create or replace function delete_'||p_table_name||'('||v_input_parameters||')
  returns boolean
  language plpgsql
  security definer
as
$func$
begin
    delete from '||p_table_name||
    E'\n\twhere '||v_filters||';

    return found;
end;
$func$;';

    execute v_query;

    return v_success;

end;
$$;




