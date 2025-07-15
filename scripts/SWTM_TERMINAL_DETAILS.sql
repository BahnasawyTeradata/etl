CREATE OR REPLACE VIEW DEVV_STG_FLX.SWTM_TERMINAL_DETAILS_get_delta AS LOCKING ROW FOR ACCESS
SELECT
  COALESCE(X.TERM_ID, Y.TERM_ID) AS TERM_ID,
  X.AUTH_STAT,
  X.BM_CHANNEL,
  X.CASH_GL,
  X.CHANNEL,
  X.CHECKER_DT_STAMP,
  X.CHECKER_ID,
  X.INTELLIGENT_DEPOSIT,
  X.MAKER_DT_STAMP,
  X.MAKER_ID,
  X.MERCH_ID,
  X.MOD_NO,
  X.ONCE_AUTH,
  X.ORG_BRN,
  X.RECORD_STAT,
  X.SK_TERM_ID,
  X.TERM_ADDR,
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
    WHEN Y.TERM_ID IS NULL
    THEN 'INS' /* - IF MULTIPLE PKs are there then concatenate them with AND */
    WHEN X.TERM_ID = Y.TERM_ID /* - IF MULTIPLE PKs are there then concatenate them with AND */
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
          X.BM_CHANNEL IS NULL AND NOT Y.BM_CHANNEL IS NULL
        )
        OR (
          NOT X.BM_CHANNEL IS NULL AND Y.BM_CHANNEL IS NULL
        )
        OR (
          X.BM_CHANNEL <> Y.BM_CHANNEL
        )
      )
      OR (
        (
          X.CASH_GL IS NULL AND NOT Y.CASH_GL IS NULL
        )
        OR (
          NOT X.CASH_GL IS NULL AND Y.CASH_GL IS NULL
        )
        OR (
          X.CASH_GL <> Y.CASH_GL
        )
      )
      OR (
        (
          X.CHANNEL IS NULL AND NOT Y.CHANNEL IS NULL
        )
        OR (
          NOT X.CHANNEL IS NULL AND Y.CHANNEL IS NULL
        )
        OR (
          X.CHANNEL <> Y.CHANNEL
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
          X.INTELLIGENT_DEPOSIT IS NULL AND NOT Y.INTELLIGENT_DEPOSIT IS NULL
        )
        OR (
          NOT X.INTELLIGENT_DEPOSIT IS NULL AND Y.INTELLIGENT_DEPOSIT IS NULL
        )
        OR (
          X.INTELLIGENT_DEPOSIT <> Y.INTELLIGENT_DEPOSIT
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
          X.MERCH_ID IS NULL AND NOT Y.MERCH_ID IS NULL
        )
        OR (
          NOT X.MERCH_ID IS NULL AND Y.MERCH_ID IS NULL
        )
        OR (
          X.MERCH_ID <> Y.MERCH_ID
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
          X.ORG_BRN IS NULL AND NOT Y.ORG_BRN IS NULL
        )
        OR (
          NOT X.ORG_BRN IS NULL AND Y.ORG_BRN IS NULL
        )
        OR (
          X.ORG_BRN <> Y.ORG_BRN
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
      OR (
        (
          X.SK_TERM_ID IS NULL AND NOT Y.SK_TERM_ID IS NULL
        )
        OR (
          NOT X.SK_TERM_ID IS NULL AND Y.SK_TERM_ID IS NULL
        )
        OR (
          X.SK_TERM_ID <> Y.SK_TERM_ID
        )
      )
      OR (
        (
          X.TERM_ADDR IS NULL AND NOT Y.TERM_ADDR IS NULL
        )
        OR (
          NOT X.TERM_ADDR IS NULL AND Y.TERM_ADDR IS NULL
        )
        OR (
          X.TERM_ADDR <> Y.TERM_ADDR
        )
      )
    )
    THEN 'UPD'
    WHEN X.TERM_ID IS NULL
    THEN 'DEL' /* - IF MULTIPLE PKs are there then concatenate them with AND */
  END AS OPT_FLAG
FROM DEVT_STG_FLX.SWTM_TERMINAL_DETAILS AS X
FULL JOIN DEVT_STG_FLX.SWTM_TERMINAL_DETAILS_BACKUP AS Y
  ON X.TERM_ID = Y.TERM_ID /* - IF MULTIPLE PKs are there then concatenate them with AND */
WHERE
  NOT OPT_FLAG IS NULL