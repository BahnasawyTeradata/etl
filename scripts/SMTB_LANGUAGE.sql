CREATE OR REPLACE VIEW DEVV_STG_FLX.SMTB_LANGUAGE_get_delta AS LOCKING ROW FOR ACCESS
SELECT
  COALESCE(X.LANG_CODE, Y.LANG_CODE) AS LANG_CODE,
  X.AUTH_STAT,
  X.BM_LANG_CODE,
  X.CHECKER_DT_STAMP,
  X.CHECKER_ID,
  X.DISPLAY_DIRECTION,
  X.LANG_ISO_CODE,
  X.LANG_NAME,
  X.MAKER_DT_STAMP,
  X.MAKER_ID,
  X.MOD_NO,
  X.ONCE_AUTH,
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
    WHEN Y.LANG_CODE IS NULL
    THEN 'INS' /* - IF MULTIPLE PKs are there then concatenate them with AND */
    WHEN X.LANG_CODE = Y.LANG_CODE /* - IF MULTIPLE PKs are there then concatenate them with AND */
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
          X.BM_LANG_CODE IS NULL AND NOT Y.BM_LANG_CODE IS NULL
        )
        OR (
          NOT X.BM_LANG_CODE IS NULL AND Y.BM_LANG_CODE IS NULL
        )
        OR (
          X.BM_LANG_CODE <> Y.BM_LANG_CODE
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
          X.DISPLAY_DIRECTION IS NULL AND NOT Y.DISPLAY_DIRECTION IS NULL
        )
        OR (
          NOT X.DISPLAY_DIRECTION IS NULL AND Y.DISPLAY_DIRECTION IS NULL
        )
        OR (
          X.DISPLAY_DIRECTION <> Y.DISPLAY_DIRECTION
        )
      )
      OR (
        (
          X.LANG_ISO_CODE IS NULL AND NOT Y.LANG_ISO_CODE IS NULL
        )
        OR (
          NOT X.LANG_ISO_CODE IS NULL AND Y.LANG_ISO_CODE IS NULL
        )
        OR (
          X.LANG_ISO_CODE <> Y.LANG_ISO_CODE
        )
      )
      OR (
        (
          X.LANG_NAME IS NULL AND NOT Y.LANG_NAME IS NULL
        )
        OR (
          NOT X.LANG_NAME IS NULL AND Y.LANG_NAME IS NULL
        )
        OR (
          X.LANG_NAME <> Y.LANG_NAME
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
    WHEN X.LANG_CODE IS NULL
    THEN 'DEL' /* - IF MULTIPLE PKs are there then concatenate them with AND */
  END AS OPT_FLAG
FROM DEVT_STG_FLX.SMTB_LANGUAGE AS X
FULL JOIN DEVT_STG_FLX.SMTB_LANGUAGE_BACKUP AS Y
  ON X.LANG_CODE = Y.LANG_CODE /* - IF MULTIPLE PKs are there then concatenate them with AND */
WHERE
  NOT OPT_FLAG IS NULL