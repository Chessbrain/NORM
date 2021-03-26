# NORM
No-ORM. Removing ORM one function at a time


# System functions

## norm_insert
```postgresql
norm_insert(
    p_table_name text,
    p_columns text[]
)
  returns boolean
```
This generates a simple insert into function.

As an example, for the following function call:
```postgresql
select norm_insert('games',array['game_name']);
```
The following function gets generated:
```postgresql
create function insert_games(p_game_name text ) returns boolean
    security definer
    language plpgsql
as
$$
begin
    insert into games(game_name)
    values(p_game_name);

    return found;
end;
$$;

alter function insert_games(text) owner to postgres;
```

## norm_update
```postgresql
norm_update(
    p_table_name text,
    p_columns text[],
    p_filter_columns text[]
)
  returns boolean
```
This generates a simple update function.

As an example, for the following input:
```postgresql
select norm_update('games',array['game_name'],array['game_id']);
```
The following function gets generated:
```postgresql
create function update_games(p_game_id bigint, p_game_name text ) returns boolean
    security definer
    language plpgsql
as
$$
begin
    update games set
        game_name = coalesce(p_game_name,game_name)
    where (game_id = p_game_id)	;

    return found;
end;
$$;

alter function update_games(bigint, text) owner to postgres;
```
This function may return the following error code(s):

| Error code      | Error Message | Description     |
| :---        |    :----:   |          :---: |
| N0004      | Cannot create update function without filter(s).       | User sent `null` for `p_filter_columns` parameter    |


## norm_delete
```postgresql
norm_delete(
    p_table_name text,
    p_filters text[]
)
  returns boolean
```
This generates a simple delete function.

As an example, for the following input:
```postgresql
select norm_delete('games',array['game_id']);
```
The following function gets generated:
```postgresql
create function delete_games(p_game_id bigint ) returns boolean
    security definer
    language plpgsql
as
$$
begin
    delete from games
    where (game_id = p_game_id);

    return found;
end;
$$;

alter function delete_games(bigint) owner to postgres;
```

## norm_get
```postgresql
norm_get(
    p_tables text[],
    p_columns text[],
    p_filters text[] default null,
    p_function_name text default null
)
  returns boolean
```
This generates a get function.

As an example, for the following input:
```postgresql
create function get_users_deposits(p_first_name text )
    returns TABLE(amount numeric, first_name text)
    security definer
    language plpgsql
as
$$
begin
    return query
    select 
        deposits.amount,
        users.first_name
    from users
    	join deposits on deposits.user_id = users.user_id
    where (users.first_name = p_first_name);
end;
$$;

alter function get_users_deposits(text) owner to postgres;
```


# Implementation
_usually not accessed by user, check **System functions** section for user side functions_

## unnest_2d
```postgresql
unnest_2d (anyarray, out a anyarray) returns setof anyarray
```

Function unnests 2D arrays into Nx1D arrays, example:

`[['International','Master'],['Grand','Master']]`

turns into:
```
['International','Master']
['Grand','Master']
```

Which allows us to select individual elements of each 1D array using `array[n]` syntax (n - starts with index 1 not 0).

Learned this approach from here: https://stackoverflow.com/questions/8137112/unnest-array-by-one-level

## _parameter_generator
```postgresql
_parameter_generator(text[]) returns text
```

Generates input parameter syntax for use in simple insert into clause, returns `p_<column_name>` syntax.

```postgresql
_parameter_generator(
    p_tables text[],
    p_columns text[],
    p_is_update boolean default false
)
  returns text
```
This overload returns `p_<column_name> <data_type>` used for input parameter definition of functions. The above function also has a scalar overload which just calls this same function (simply made for convenience).
1. If `p_is_update` is set to true it returns `<column_name> = coalesce(p_<column_name>,<column_name>)` for the "set" portion of the update clause

## _table_columns

```postgresql
_table_columns(
    p_tables text[],
    p_columns text[]
)
  returns table(
    table_name text,
    column_name text
  )
```

This functions as a mapper, as some functions (and many in the future) take arrays of tables and columns. This maps which column belongs to which table.

## _where_generator

```postgresql
_where_generator(
    p_columns text[],
    p_is_null boolean default false,
    p_and boolean default true
)
  returns text
```

This generates the simplest form of a `where` clause. It is not table aware and thus, used only for simple functions.
1. If `p_is_null` is set to true it will switch all conditions to optional `(<parameter> is null or <column> = <parameter>)`
2. If `p_and` is set to false, it will switch all conditions to `or` instead.

```postgresql
_where_generator(
    p_tables text[],
    p_columns text[],
    p_is_null boolean default false,
    p_and boolean default true
)
  returns text
```

Overload of the above function which is table aware

## _check_columns
```postgresql
_check_columns(
    p_tables text[],
    p_columns text[],
    p_not_null boolean default false
)
  returns boolean
```

Most used function in the extension so far. Handles checking of valid tables and columns sent to all functions.

1. `p_not_null` set to true forces check for non-nullable columns, this is mainly used for insert functions.
2. Returns true if everything passes, otherwise has a few implemented error codes:

| Error code      | Error Message | Description     |
| :---        |    :----:   |          :---: |
| N0000      | Please specify valid table(s) and column(s).       | No table or column specified or empty arrays   |
| N0001   | Specified table(s) does not exist, please create it first        | Table or one of the tables in the array do not exist      ||
| N0002   | Specified column(s): %, do not exist.        | Column or one of the columns specified in the array do not exist (specifies columns)      ||
| N0003   | Missing non-nullable column(s): %.        | Column(s) that are non-nullable are missing (specifies columns)     ||

```postgresql
_check_columns(
    p_tables text,
    p_columns text[],
    p_not_null boolean default false
)
  returns boolean
```
Scalar overload of the function, calls array implementation (simply for convenience).

## _return_generator
```postgresql
_return_generator(
    p_tables text[],
    p_columns text[]
)
  returns text
```
Generates `returns table` list of columns and their data types. This uses explicit ordering to guarantee matching with `_select_generator` so functions don't throw errors.

## _from_generator
```postgresql
_from_generator(text) returns text
```
Simple function that returns `from <table_name>`.

## _join_generator
```postgresql
_join_generator(
    p_tables text[]
)
  returns text
```
Function creates entire query string `from - join` based on array of tables forwarded to it
1. This function returns the following possible error code(s):

| Error code      | Error Message | Description     |
| :---        |    :----:   |          :---: |
| N0005      | Function currently doesn't support tables with more than a single FK. Will be implemented in future versions.       | As of version 0.0.1, left join and tables with multiple FKs are not supported yet.   |
2. Make sure to send this function tables with only one FK max.
3. If only a single table is specified, it will only generate `from <table>` text.

## _select_generator
```postgresql
_select_generator(
    p_tables text[],
    p_columns text[]
)
  returns text
```

Generates select columns with `<table_name>.<column_name>` syntax. This uses explicit ordering to guarantee matching with `_return_generator` so functions don't throw errors.


# To be implemented

- Allow optional parameter input in get function
- Tables with multiple FKs join
- Left joins
- Aggregates
- Create table
- Create index
- Subqueries
- Multi-table insert/update/delete