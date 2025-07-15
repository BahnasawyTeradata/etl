CREATE OR REPLACE VIEW DEVV_STG_FLX.ICTM_ACC_UDEVALS_get_delta AS LOCKING ROW FOR ACCESS
SELECT
  COALESCE(X.ACC, Y.ACC) AS ACC,
  COALESCE(X.PROD, Y.PROD) AS PROD,
  COALESCE(X.UDE_EFF_DT, Y.UDE_EFF_DT) AS UDE_EFF_DT,
  COALESCE(X.UDE_ID, Y.UDE_ID) AS UDE_ID,
  X.AUTH_STAT,
  X.BASE_RATE,
  X.BASE_SPREAD,
  X.BRN,
  X.RATE_CODE,
  X.RECORD_STAT,
  X.TD_RATE_CODE,
  X.UDE_VALUE,
  X.UDE_VARIANCE,
  X.SK_ACC,
  X.SK_PROD,
  X.SK_RATE_CODE_CCY_CODE,
  X.SK_UDE_ID,
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
    WHEN Y.ACC IS NULL AND Y.PROD IS NULL AND Y.UDE_EFF_DT IS NULL AND Y.UDE_ID IS NULL
    THEN 'INS' /* - IF MULTIPLE PKs are there then concatenate them with AND */
    WHEN X.ACC = Y.ACC
    AND X.PROD = Y.PROD
    AND X.UDE_EFF_DT = Y.UDE_EFF_DT
    AND X.UDE_ID = Y.UDE_ID /* - IF MULTIPLE PKs are there then concatenate them with AND */
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
          X.BASE_RATE IS NULL AND NOT Y.BASE_RATE IS NULL
        )
        OR (
          NOT X.BASE_RATE IS NULL AND Y.BASE_RATE IS NULL
        )
        OR (
          X.BASE_RATE <> Y.BASE_RATE
        )
      )
      OR (
        (
          X.BASE_SPREAD IS NULL AND NOT Y.BASE_SPREAD IS NULL
        )
        OR (
          NOT X.BASE_SPREAD IS NULL AND Y.BASE_SPREAD IS NULL
        )
        OR (
          X.BASE_SPREAD <> Y.BASE_SPREAD
        )
      )
      OR (
        (
          X.BRN IS NULL AND NOT Y.BRN IS NULL
        )
        OR (
          NOT X.BRN IS NULL AND Y.BRN IS NULL
        )
        OR (
          X.BRN <> Y.BRN
        )
      )
      OR (
        (
          X.RATE_CODE IS NULL AND NOT Y.RATE_CODE IS NULL
        )
        OR (
          NOT X.RATE_CODE IS NULL AND Y.RATE_CODE IS NULL
        )
        OR (
          X.RATE_CODE <> Y.RATE_CODE
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
          X.TD_RATE_CODE IS NULL AND NOT Y.TD_RATE_CODE IS NULL
        )
        OR (
          NOT X.TD_RATE_CODE IS NULL AND Y.TD_RATE_CODE IS NULL
        )
        OR (
          X.TD_RATE_CODE <> Y.TD_RATE_CODE
        )
      )
      OR (
        (
          X.UDE_VALUE IS NULL AND NOT Y.UDE_VALUE IS NULL
        )
        OR (
          NOT X.UDE_VALUE IS NULL AND Y.UDE_VALUE IS NULL
        )
        OR (
          X.UDE_VALUE <> Y.UDE_VALUE
        )
      )
      OR (
        (
          X.UDE_VARIANCE IS NULL AND NOT Y.UDE_VARIANCE IS NULL
        )
        OR (
          NOT X.UDE_VARIANCE IS NULL AND Y.UDE_VARIANCE IS NULL
        )
        OR (
          X.UDE_VARIANCE <> Y.UDE_VARIANCE
        )
      )
      OR (
        (
          X.SK_ACC IS NULL AND NOT Y.SK_ACC IS NULL
        )
        OR (
          NOT X.SK_ACC IS NULL AND Y.SK_ACC IS NULL
        )
        OR (
          X.SK_ACC <> Y.SK_ACC
        )
      )
      OR (
        (
          X.SK_PROD IS NULL AND NOT Y.SK_PROD IS NULL
        )
        OR (
          NOT X.SK_PROD IS NULL AND Y.SK_PROD IS NULL
        )
        OR (
          X.SK_PROD <> Y.SK_PROD
        )
      )
      OR (
        (
          X.SK_RATE_CODE_CCY_CODE IS NULL AND NOT Y.SK_RATE_CODE_CCY_CODE IS NULL
        )
        OR (
          NOT X.SK_RATE_CODE_CCY_CODE IS NULL AND Y.SK_RATE_CODE_CCY_CODE IS NULL
        )
        OR (
          X.SK_RATE_CODE_CCY_CODE <> Y.SK_RATE_CODE_CCY_CODE
        )
      )
      OR (
        (
          X.SK_UDE_ID IS NULL AND NOT Y.SK_UDE_ID IS NULL
        )
        OR (
          NOT X.SK_UDE_ID IS NULL AND Y.SK_UDE_ID IS NULL
        )
        OR (
          X.SK_UDE_ID <> Y.SK_UDE_ID
        )
      )
    )
    THEN 'UPD'
    WHEN X.ACC IS NULL AND X.PROD IS NULL AND X.UDE_EFF_DT IS NULL AND X.UDE_ID IS NULL
    THEN 'DEL' /* - IF MULTIPLE PKs are there then concatenate them with AND */
  END AS OPT_FLAG
FROM DEVT_STG_FLX.ICTM_ACC_UDEVALS AS X
FULL JOIN DEVT_STG_FLX.ICTM_ACC_UDEVALS_BACKUP AS Y
  ON X.ACC = Y.ACC
  AND X.PROD = Y.PROD
  AND X.UDE_EFF_DT = Y.UDE_EFF_DT
  AND X.UDE_ID = Y.UDE_ID /* - IF MULTIPLE PKs are there then concatenate them with AND */
WHERE
  NOT OPT_FLAG IS NULL