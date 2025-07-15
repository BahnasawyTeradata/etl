CREATE OR REPLACE VIEW DEVV_STG_FLX.ICTM_RATES_get_delta AS LOCKING ROW FOR ACCESS
SELECT
  COALESCE(X.CCY_CODE, Y.CCY_CODE) AS CCY_CODE,
  COALESCE(X.EFF_DT, Y.EFF_DT) AS EFF_DT,
  COALESCE(X.RATE_CODE, Y.RATE_CODE) AS RATE_CODE,
  X.AUTH_STAT,
  X.BM_AUTH_STAT,
  X.BM_CCY_CODE,
  X.BM_ONCE_AUTH,
  X.BM_RECORD_STAT,
  X.BRANCH_CODE,
  X.ONCE_AUTH,
  X.RATE,
  X.RECORD_STAT,
  X.SK_RATE_CODE,
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
    WHEN Y.CCY_CODE IS NULL AND Y.EFF_DT IS NULL AND Y.RATE_CODE IS NULL
    THEN 'INS' /* - IF MULTIPLE PKs are there then concatenate them with AND */
    WHEN X.CCY_CODE = Y.CCY_CODE
    AND X.EFF_DT = Y.EFF_DT
    AND X.RATE_CODE = Y.RATE_CODE /* - IF MULTIPLE PKs are there then concatenate them with AND */
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
          X.BM_AUTH_STAT IS NULL AND NOT Y.BM_AUTH_STAT IS NULL
        )
        OR (
          NOT X.BM_AUTH_STAT IS NULL AND Y.BM_AUTH_STAT IS NULL
        )
        OR (
          X.BM_AUTH_STAT <> Y.BM_AUTH_STAT
        )
      )
      OR (
        (
          X.BM_CCY_CODE IS NULL AND NOT Y.BM_CCY_CODE IS NULL
        )
        OR (
          NOT X.BM_CCY_CODE IS NULL AND Y.BM_CCY_CODE IS NULL
        )
        OR (
          X.BM_CCY_CODE <> Y.BM_CCY_CODE
        )
      )
      OR (
        (
          X.BM_ONCE_AUTH IS NULL AND NOT Y.BM_ONCE_AUTH IS NULL
        )
        OR (
          NOT X.BM_ONCE_AUTH IS NULL AND Y.BM_ONCE_AUTH IS NULL
        )
        OR (
          X.BM_ONCE_AUTH <> Y.BM_ONCE_AUTH
        )
      )
      OR (
        (
          X.BM_RECORD_STAT IS NULL AND NOT Y.BM_RECORD_STAT IS NULL
        )
        OR (
          NOT X.BM_RECORD_STAT IS NULL AND Y.BM_RECORD_STAT IS NULL
        )
        OR (
          X.BM_RECORD_STAT <> Y.BM_RECORD_STAT
        )
      )
      OR (
        (
          X.BRANCH_CODE IS NULL AND NOT Y.BRANCH_CODE IS NULL
        )
        OR (
          NOT X.BRANCH_CODE IS NULL AND Y.BRANCH_CODE IS NULL
        )
        OR (
          X.BRANCH_CODE <> Y.BRANCH_CODE
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
          X.RATE IS NULL AND NOT Y.RATE IS NULL
        )
        OR (
          NOT X.RATE IS NULL AND Y.RATE IS NULL
        )
        OR (
          X.RATE <> Y.RATE
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
          X.SK_RATE_CODE IS NULL AND NOT Y.SK_RATE_CODE IS NULL
        )
        OR (
          NOT X.SK_RATE_CODE IS NULL AND Y.SK_RATE_CODE IS NULL
        )
        OR (
          X.SK_RATE_CODE <> Y.SK_RATE_CODE
        )
      )
    )
    THEN 'UPD'
    WHEN X.CCY_CODE IS NULL AND X.EFF_DT IS NULL AND X.RATE_CODE IS NULL
    THEN 'DEL' /* - IF MULTIPLE PKs are there then concatenate them with AND */
  END AS OPT_FLAG
FROM DEVT_STG_FLX.ICTM_RATES AS X
FULL JOIN DEVT_STG_FLX.ICTM_RATES_BACKUP AS Y
  ON X.CCY_CODE = Y.CCY_CODE
  AND X.EFF_DT = Y.EFF_DT
  AND X.RATE_CODE = Y.RATE_CODE /* - IF MULTIPLE PKs are there then concatenate them with AND */
WHERE
  NOT OPT_FLAG IS NULL