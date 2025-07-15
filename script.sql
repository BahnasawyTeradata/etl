--sqlfluff:dialect:duckdb

with pks as (
    select
        table_name,
        column_name
    from smx_tables
    where
        (
            trim(smx_tables."Table Status") not in ('DELETED', 'PENDING')
            or smx_tables."Table Status" is NULL
        )
        and trim(upper(smx_tables."DROP NUMBER")) = 'DROP 0'
        and upper(trim(pk)) = 'Y'
),

non_pks as (
    select
        table_name,
        column_name
    from smx_tables
    where
        (
            trim(smx_tables."Table Status") not in ('DELETED', 'PENDING')
            or smx_tables."Table Status" is NULL
        )
        and trim(upper(smx_tables."DROP NUMBER")) = 'DROP 0'
        and upper(trim(pk)) = 'N'
),

coalesce_pks as (
    select
        table_name,
        string_agg(printf('COALESCE(X.%1$s, Y.%1$s) AS %1$s', column_name), ', ')
            as columns
    from pks
  group by table_name
) ,

inserts as (
    select
        table_name,
        string_agg(printf('Y.%1$s IS NULL', column_name), ' AND ') as columns
    from pks
  group by table_name
),

multiple_pks as (
    select
        table_name,
        string_agg(printf('X.%1$s = Y.%1$s', column_name), ' AND ') as columns
    from pks
  group by table_name
),

updates as (
    select
        table_name,
            string_agg(printf(
                '(X.%1$s IS NULL AND Y.%1$s IS NOT NULL) OR (X.%1$s IS NOT NULL AND Y.%1$s IS NULL) OR (X.%1$s <> Y.%1$s))',
                column_name
            ), ' OR ')
         as columns
    from non_pks
  group by table_name
),

deletes as (
    select
        table_name,
        string_agg(printf('X.%1$s IS NULL', column_name), ' AND ') as columns
    from pks
  group by table_name
)

 select
  non_pks.table_name,
  printf($$
  REPLACE VIEW DEVV_STG_FLX.%1$s AS
  LOCK ROW FOR ACCESS
  SELECT
  %2$s,
  %3$s,
  -------NO_CHANGE-----------
  COALESCE(X.ETL_START_DT,Y.ETL_START_DT) AS ETL_START_DT,
  X.ETL_END_DT,
  X.RECORD_DELETED_FLAG,
  X.PROCESS_NAME,
  X.UPDATE_PROCESS_NAME,
  X.EXT_SS_ID,
  X.EXE_PROCESS_ID,
  X.UPDATE_PROCESS_ID,
  COALESCE(X.ETL_START_TS,Y.ETL_START_TS) AS ETL_START_TS,
  X.ETL_END_TS,
  CASE WHEN %4$s THEN 'INS' --- IF MULTIPLE PKs are there then concatenate them with AND
  WHEN %5$s  --- IF MULTIPLE PKs are there then concatenate them with AND
  AND(
  ( %6$s ) THEN 'UPD'
  WHEN %7$s THEN 'DEL' --- IF MULTIPLE PKs are there then concatenate them with AND
  END AS OPT_FLAG
  FROM DEVT_STG_FLX.%1$s X
  FULL JOIN DEVT_STG_FLX.%1$s_BACKUP Y
  ON %5$s --- IF MULTIPLE PKs are there then concatenate them with AND
  WHERE OPT_FLAG IS NOT NULL;
 $$, 
  non_pks.table_name, 
  ANY_VALUE(coalesce_pks.columns),
  string_agg(printf('X.%1$s', non_pks.column_name), ', '),
  ANY_VALUE(inserts.columns),
  ANY_VALUE(multiple_pks.columns),
  ANY_VALUE(updates.columns),
  ANY_VALUE(deletes.columns)
   ) as scripts
 from non_pks
 inner join coalesce_pks on non_pks.table_name = coalesce_pks.table_name
 inner join inserts on non_pks.table_name = inserts.table_name
 inner join multiple_pks on non_pks.table_name = multiple_pks.table_name
 inner join updates on non_pks.table_name = updates.table_name
 inner join deletes on non_pks.table_name = deletes.table_name
 group by non_pks.table_name
