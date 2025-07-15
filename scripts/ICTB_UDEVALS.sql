CREATE OR REPLACE VIEW DEVV_STG_FLX.ICTB_UDEVALS_get_delta AS LOCKING ROW FOR ACCESS
SELECT
  COALESCE(X.COND_KEY, Y.COND_KEY) AS COND_KEY,
  COALESCE(X.PROD, Y.PROD) AS PROD,
  COALESCE(X.UDE_EFF_DT, Y.UDE_EFF_DT) AS UDE_EFF_DT,
  COALESCE(X.UDE_ID, Y.UDE_ID) AS UDE_ID,
  X.AMT,
  X.BM_RATE_CCY,
  X.COND_TYPE,
  X.RATE,
  X.RATE_CCY,
  X.RATE_CODE,
  X.RATE_DT,
  X.SK_ACC_COND_KEY,
  X.SK_PROD,
  X.SK_RATE_CODE_CCY_CODE,
  X.SK_UDE_ID,
  X.UDE_DT,
  X.UDE_VARIANCE,
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
    WHEN Y.COND_KEY IS NULL AND Y.PROD IS NULL AND Y.UDE_EFF_DT IS NULL AND Y.UDE_ID IS NULL
    THEN 'INS' /* - IF MULTIPLE PKs are there then concatenate them with AND */
    WHEN X.COND_KEY = Y.COND_KEY
    AND X.PROD = Y.PROD
    AND X.UDE_EFF_DT = Y.UDE_EFF_DT
    AND X.UDE_ID = Y.UDE_ID /* - IF MULTIPLE PKs are there then concatenate them with AND */
    AND (
      (
        (
          X.AMT IS NULL AND NOT Y.AMT IS NULL
        )
        OR (
          NOT X.AMT IS NULL AND Y.AMT IS NULL
        )
        OR (
          X.AMT <> Y.AMT
        )
      )
      OR (
        (
          X.BM_RATE_CCY IS NULL AND NOT Y.BM_RATE_CCY IS NULL
        )
        OR (
          NOT X.BM_RATE_CCY IS NULL AND Y.BM_RATE_CCY IS NULL
        )
        OR (
          X.BM_RATE_CCY <> Y.BM_RATE_CCY
        )
      )
      OR (
        (
          X.COND_TYPE IS NULL AND NOT Y.COND_TYPE IS NULL
        )
        OR (
          NOT X.COND_TYPE IS NULL AND Y.COND_TYPE IS NULL
        )
        OR (
          X.COND_TYPE <> Y.COND_TYPE
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
          X.RATE_CCY IS NULL AND NOT Y.RATE_CCY IS NULL
        )
        OR (
          NOT X.RATE_CCY IS NULL AND Y.RATE_CCY IS NULL
        )
        OR (
          X.RATE_CCY <> Y.RATE_CCY
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
          X.RATE_DT IS NULL AND NOT Y.RATE_DT IS NULL
        )
        OR (
          NOT X.RATE_DT IS NULL AND Y.RATE_DT IS NULL
        )
        OR (
          X.RATE_DT <> Y.RATE_DT
        )
      )
      OR (
        (
          X.SK_ACC_COND_KEY IS NULL AND NOT Y.SK_ACC_COND_KEY IS NULL
        )
        OR (
          NOT X.SK_ACC_COND_KEY IS NULL AND Y.SK_ACC_COND_KEY IS NULL
        )
        OR (
          X.SK_ACC_COND_KEY <> Y.SK_ACC_COND_KEY
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
      OR (
        (
          X.UDE_DT IS NULL AND NOT Y.UDE_DT IS NULL
        )
        OR (
          NOT X.UDE_DT IS NULL AND Y.UDE_DT IS NULL
        )
        OR (
          X.UDE_DT <> Y.UDE_DT
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
    )
    THEN 'UPD'
    WHEN X.COND_KEY IS NULL AND X.PROD IS NULL AND X.UDE_EFF_DT IS NULL AND X.UDE_ID IS NULL
    THEN 'DEL' /* - IF MULTIPLE PKs are there then concatenate them with AND */
  END AS OPT_FLAG
FROM DEVT_STG_FLX.ICTB_UDEVALS AS X
FULL JOIN DEVT_STG_FLX.ICTB_UDEVALS_BACKUP AS Y
  ON X.COND_KEY = Y.COND_KEY
  AND X.PROD = Y.PROD
  AND X.UDE_EFF_DT = Y.UDE_EFF_DT
  AND X.UDE_ID = Y.UDE_ID /* - IF MULTIPLE PKs are there then concatenate them with AND */
WHERE
  NOT OPT_FLAG IS NULL