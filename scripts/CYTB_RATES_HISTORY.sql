CREATE OR REPLACE VIEW DEVV_STG_FLX.CYTB_RATES_HISTORY_get_delta AS LOCKING ROW FOR ACCESS
SELECT
  COALESCE(X.BRANCH_CODE, Y.BRANCH_CODE) AS BRANCH_CODE,
  COALESCE(X.CCY1, Y.CCY1) AS CCY1,
  COALESCE(X.CCY2, Y.CCY2) AS CCY2,
  COALESCE(X.RATE_DT_STAMP, Y.RATE_DT_STAMP) AS RATE_DT_STAMP,
  COALESCE(X.RATE_TYPE, Y.RATE_TYPE) AS RATE_TYPE,
  X.BM_CCY1,
  X.BM_CCY2,
  X.BM_RATE_TYPE,
  X.BUY_RATE,
  X.MID_RATE,
  X.RATE_DATE,
  X.RATE_SERIAL,
  X.SALE_RATE,
  X.SK_BRANCH_CODE,
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
    WHEN Y.BRANCH_CODE IS NULL
    AND Y.CCY1 IS NULL
    AND Y.CCY2 IS NULL
    AND Y.RATE_DT_STAMP IS NULL
    AND Y.RATE_TYPE IS NULL
    THEN 'INS' /* - IF MULTIPLE PKs are there then concatenate them with AND */
    WHEN X.BRANCH_CODE = Y.BRANCH_CODE
    AND X.CCY1 = Y.CCY1
    AND X.CCY2 = Y.CCY2
    AND X.RATE_DT_STAMP = Y.RATE_DT_STAMP
    AND X.RATE_TYPE = Y.RATE_TYPE /* - IF MULTIPLE PKs are there then concatenate them with AND */
    AND (
      (
        (
          X.BM_CCY1 IS NULL AND NOT Y.BM_CCY1 IS NULL
        )
        OR (
          NOT X.BM_CCY1 IS NULL AND Y.BM_CCY1 IS NULL
        )
        OR (
          X.BM_CCY1 <> Y.BM_CCY1
        )
      )
      OR (
        (
          X.BM_CCY2 IS NULL AND NOT Y.BM_CCY2 IS NULL
        )
        OR (
          NOT X.BM_CCY2 IS NULL AND Y.BM_CCY2 IS NULL
        )
        OR (
          X.BM_CCY2 <> Y.BM_CCY2
        )
      )
      OR (
        (
          X.BM_RATE_TYPE IS NULL AND NOT Y.BM_RATE_TYPE IS NULL
        )
        OR (
          NOT X.BM_RATE_TYPE IS NULL AND Y.BM_RATE_TYPE IS NULL
        )
        OR (
          X.BM_RATE_TYPE <> Y.BM_RATE_TYPE
        )
      )
      OR (
        (
          X.BUY_RATE IS NULL AND NOT Y.BUY_RATE IS NULL
        )
        OR (
          NOT X.BUY_RATE IS NULL AND Y.BUY_RATE IS NULL
        )
        OR (
          X.BUY_RATE <> Y.BUY_RATE
        )
      )
      OR (
        (
          X.MID_RATE IS NULL AND NOT Y.MID_RATE IS NULL
        )
        OR (
          NOT X.MID_RATE IS NULL AND Y.MID_RATE IS NULL
        )
        OR (
          X.MID_RATE <> Y.MID_RATE
        )
      )
      OR (
        (
          X.RATE_DATE IS NULL AND NOT Y.RATE_DATE IS NULL
        )
        OR (
          NOT X.RATE_DATE IS NULL AND Y.RATE_DATE IS NULL
        )
        OR (
          X.RATE_DATE <> Y.RATE_DATE
        )
      )
      OR (
        (
          X.RATE_SERIAL IS NULL AND NOT Y.RATE_SERIAL IS NULL
        )
        OR (
          NOT X.RATE_SERIAL IS NULL AND Y.RATE_SERIAL IS NULL
        )
        OR (
          X.RATE_SERIAL <> Y.RATE_SERIAL
        )
      )
      OR (
        (
          X.SALE_RATE IS NULL AND NOT Y.SALE_RATE IS NULL
        )
        OR (
          NOT X.SALE_RATE IS NULL AND Y.SALE_RATE IS NULL
        )
        OR (
          X.SALE_RATE <> Y.SALE_RATE
        )
      )
      OR (
        (
          X.SK_BRANCH_CODE IS NULL AND NOT Y.SK_BRANCH_CODE IS NULL
        )
        OR (
          NOT X.SK_BRANCH_CODE IS NULL AND Y.SK_BRANCH_CODE IS NULL
        )
        OR (
          X.SK_BRANCH_CODE <> Y.SK_BRANCH_CODE
        )
      )
    )
    THEN 'UPD'
    WHEN X.BRANCH_CODE IS NULL
    AND X.CCY1 IS NULL
    AND X.CCY2 IS NULL
    AND X.RATE_DT_STAMP IS NULL
    AND X.RATE_TYPE IS NULL
    THEN 'DEL' /* - IF MULTIPLE PKs are there then concatenate them with AND */
  END AS OPT_FLAG
FROM DEVT_STG_FLX.CYTB_RATES_HISTORY AS X
FULL JOIN DEVT_STG_FLX.CYTB_RATES_HISTORY_BACKUP AS Y
  ON X.BRANCH_CODE = Y.BRANCH_CODE
  AND X.CCY1 = Y.CCY1
  AND X.CCY2 = Y.CCY2
  AND X.RATE_DT_STAMP = Y.RATE_DT_STAMP
  AND X.RATE_TYPE = Y.RATE_TYPE /* - IF MULTIPLE PKs are there then concatenate them with AND */
WHERE
  NOT OPT_FLAG IS NULL