CREATE OR REPLACE VIEW DEVV_STG_FLX.CSTM_PRODUCT_GROUP_get_delta AS LOCKING ROW FOR ACCESS
SELECT
  COALESCE(X.PRODUCT_GROUP, Y.PRODUCT_GROUP) AS PRODUCT_GROUP,
  X.AUTH_STAT,
  X.BM_PRODUCT_GROUP,
  X.CHECKER_DT_STAMP,
  X.CHECKER_ID,
  X.MAKER_DT_STAMP,
  X.MAKER_ID,
  X.MOD_NO,
  X.ONCE_AUTH,
  X.PRODUCT_GROUP_DESCRIPTION,
  X.RECORD_STAT,
  COALESCE(X.ETL_START_DT, Y.ETL_START_DT) AS ETL_START_DT, /* -----NO_CHANGE----------- */
  X.ETL_END_DT,
  X.RECORD_DELETED_FLAG,
  X.PROCESS_NAME,
  X.UPDATE_PROCESS_NAME,
  X.EXT_SS_ID,
  X.EXE_PROCESS_ID,
  X.UPDATE_PROCESS_ID,
  COALESCE(X.ETL_START_TS, Y.ETL_START_TS) AS ETL_START_TS,
  X.ETL_END_TS,
  CASE
    WHEN Y.PRODUCT_GROUP IS NULL
    THEN 'INS' /* - IF MULTIPLE PKs are there then concatenate them with AND */
    WHEN X.PRODUCT_GROUP = Y.PRODUCT_GROUP /* - IF MULTIPLE PKs are there then concatenate them with AND */
    AND (
      (
        (
          X.AUTH_STAT IS NULL AND NOT Y.AUTH_STAT IS NULL
        )
        OR (
          NOT X.AUTH_STAT IS NULL AND Y.AUTH_STAT IS NULL
        )
        OR (
          X.AUTH_STAT <> Y.AUTH_STAT
        )
      )
      OR (
        (
          X.BM_PRODUCT_GROUP IS NULL AND NOT Y.BM_PRODUCT_GROUP IS NULL
        )
        OR (
          NOT X.BM_PRODUCT_GROUP IS NULL AND Y.BM_PRODUCT_GROUP IS NULL
        )
        OR (
          X.BM_PRODUCT_GROUP <> Y.BM_PRODUCT_GROUP
        )
      )
      OR (
        (
          X.CHECKER_DT_STAMP IS NULL AND NOT Y.CHECKER_DT_STAMP IS NULL
        )
        OR (
          NOT X.CHECKER_DT_STAMP IS NULL AND Y.CHECKER_DT_STAMP IS NULL
        )
        OR (
          X.CHECKER_DT_STAMP <> Y.CHECKER_DT_STAMP
        )
      )
      OR (
        (
          X.CHECKER_ID IS NULL AND NOT Y.CHECKER_ID IS NULL
        )
        OR (
          NOT X.CHECKER_ID IS NULL AND Y.CHECKER_ID IS NULL
        )
        OR (
          X.CHECKER_ID <> Y.CHECKER_ID
        )
      )
      OR (
        (
          X.MAKER_DT_STAMP IS NULL AND NOT Y.MAKER_DT_STAMP IS NULL
        )
        OR (
          NOT X.MAKER_DT_STAMP IS NULL AND Y.MAKER_DT_STAMP IS NULL
        )
        OR (
          X.MAKER_DT_STAMP <> Y.MAKER_DT_STAMP
        )
      )
      OR (
        (
          X.MAKER_ID IS NULL AND NOT Y.MAKER_ID IS NULL
        )
        OR (
          NOT X.MAKER_ID IS NULL AND Y.MAKER_ID IS NULL
        )
        OR (
          X.MAKER_ID <> Y.MAKER_ID
        )
      )
      OR (
        (
          X.MOD_NO IS NULL AND NOT Y.MOD_NO IS NULL
        )
        OR (
          NOT X.MOD_NO IS NULL AND Y.MOD_NO IS NULL
        )
        OR (
          X.MOD_NO <> Y.MOD_NO
        )
      )
      OR (
        (
          X.ONCE_AUTH IS NULL AND NOT Y.ONCE_AUTH IS NULL
        )
        OR (
          NOT X.ONCE_AUTH IS NULL AND Y.ONCE_AUTH IS NULL
        )
        OR (
          X.ONCE_AUTH <> Y.ONCE_AUTH
        )
      )
      OR (
        (
          X.PRODUCT_GROUP_DESCRIPTION IS NULL AND NOT Y.PRODUCT_GROUP_DESCRIPTION IS NULL
        )
        OR (
          NOT X.PRODUCT_GROUP_DESCRIPTION IS NULL AND Y.PRODUCT_GROUP_DESCRIPTION IS NULL
        )
        OR (
          X.PRODUCT_GROUP_DESCRIPTION <> Y.PRODUCT_GROUP_DESCRIPTION
        )
      )
      OR (
        (
          X.RECORD_STAT IS NULL AND NOT Y.RECORD_STAT IS NULL
        )
        OR (
          NOT X.RECORD_STAT IS NULL AND Y.RECORD_STAT IS NULL
        )
        OR (
          X.RECORD_STAT <> Y.RECORD_STAT
        )
      )
    )
    THEN 'UPD'
    WHEN X.PRODUCT_GROUP IS NULL
    THEN 'DEL' /* - IF MULTIPLE PKs are there then concatenate them with AND */
  END AS OPT_FLAG
FROM DEVT_STG_FLX.CSTM_PRODUCT_GROUP AS X
FULL JOIN DEVT_STG_FLX.CSTM_PRODUCT_GROUP_BACKUP AS Y
  ON X.PRODUCT_GROUP = Y.PRODUCT_GROUP /* - IF MULTIPLE PKs are there then concatenate them with AND */
WHERE
  NOT OPT_FLAG IS NULL