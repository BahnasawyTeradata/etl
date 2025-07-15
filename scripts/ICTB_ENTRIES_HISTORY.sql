CREATE OR REPLACE VIEW DEVV_STG_FLX.ICTB_ENTRIES_HISTORY_get_delta AS LOCKING ROW FOR ACCESS
SELECT
  COALESCE(X.ACC, Y.ACC) AS ACC,
  COALESCE(X.BRN, Y.BRN) AS BRN,
  COALESCE(X.ENT_DT, Y.ENT_DT) AS ENT_DT,
  COALESCE(X.FRM_NO, Y.FRM_NO) AS FRM_NO,
  COALESCE(X.PROD, Y.PROD) AS PROD,
  X.ACCR,
  X.ACCRUED_AMT,
  X.AMT,
  X.AMT_TO_ACCRUE,
  X.CCY,
  X.CUR_RUN_ACCR,
  X.CUR_RUN_ACCR_LCY,
  X.DRCR,
  X.ENTRY_PASSED,
  X.ENTRY_TYPE,
  X.LAST_CUR_RUN_ACCR,
  X.LCY_AMT,
  X.LIQN,
  X.PROCESS,
  X.RUN_DATE,
  X.SK_ACC,
  X.SK_BRN,
  X.SK_PROD,
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
    WHEN Y.ACC IS NULL
    AND Y.BRN IS NULL
    AND Y.ENT_DT IS NULL
    AND Y.FRM_NO IS NULL
    AND Y.PROD IS NULL
    THEN 'INS' /* - IF MULTIPLE PKs are there then concatenate them with AND */
    WHEN X.ACC = Y.ACC
    AND X.BRN = Y.BRN
    AND X.ENT_DT = Y.ENT_DT
    AND X.FRM_NO = Y.FRM_NO
    AND X.PROD = Y.PROD /* - IF MULTIPLE PKs are there then concatenate them with AND */
    AND (
      (
        (
          X.ACCR IS NULL AND NOT Y.ACCR IS NULL
        )
        OR (
          NOT X.ACCR IS NULL AND Y.ACCR IS NULL
        )
        OR (
          X.ACCR <> Y.ACCR
        )
      )
      OR (
        (
          X.ACCRUED_AMT IS NULL AND NOT Y.ACCRUED_AMT IS NULL
        )
        OR (
          NOT X.ACCRUED_AMT IS NULL AND Y.ACCRUED_AMT IS NULL
        )
        OR (
          X.ACCRUED_AMT <> Y.ACCRUED_AMT
        )
      )
      OR (
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
          X.AMT_TO_ACCRUE IS NULL AND NOT Y.AMT_TO_ACCRUE IS NULL
        )
        OR (
          NOT X.AMT_TO_ACCRUE IS NULL AND Y.AMT_TO_ACCRUE IS NULL
        )
        OR (
          X.AMT_TO_ACCRUE <> Y.AMT_TO_ACCRUE
        )
      )
      OR (
        (
          X.CCY IS NULL AND NOT Y.CCY IS NULL
        )
        OR (
          NOT X.CCY IS NULL AND Y.CCY IS NULL
        )
        OR (
          X.CCY <> Y.CCY
        )
      )
      OR (
        (
          X.CUR_RUN_ACCR IS NULL AND NOT Y.CUR_RUN_ACCR IS NULL
        )
        OR (
          NOT X.CUR_RUN_ACCR IS NULL AND Y.CUR_RUN_ACCR IS NULL
        )
        OR (
          X.CUR_RUN_ACCR <> Y.CUR_RUN_ACCR
        )
      )
      OR (
        (
          X.CUR_RUN_ACCR_LCY IS NULL AND NOT Y.CUR_RUN_ACCR_LCY IS NULL
        )
        OR (
          NOT X.CUR_RUN_ACCR_LCY IS NULL AND Y.CUR_RUN_ACCR_LCY IS NULL
        )
        OR (
          X.CUR_RUN_ACCR_LCY <> Y.CUR_RUN_ACCR_LCY
        )
      )
      OR (
        (
          X.DRCR IS NULL AND NOT Y.DRCR IS NULL
        )
        OR (
          NOT X.DRCR IS NULL AND Y.DRCR IS NULL
        )
        OR (
          X.DRCR <> Y.DRCR
        )
      )
      OR (
        (
          X.ENTRY_PASSED IS NULL AND NOT Y.ENTRY_PASSED IS NULL
        )
        OR (
          NOT X.ENTRY_PASSED IS NULL AND Y.ENTRY_PASSED IS NULL
        )
        OR (
          X.ENTRY_PASSED <> Y.ENTRY_PASSED
        )
      )
      OR (
        (
          X.ENTRY_TYPE IS NULL AND NOT Y.ENTRY_TYPE IS NULL
        )
        OR (
          NOT X.ENTRY_TYPE IS NULL AND Y.ENTRY_TYPE IS NULL
        )
        OR (
          X.ENTRY_TYPE <> Y.ENTRY_TYPE
        )
      )
      OR (
        (
          X.LAST_CUR_RUN_ACCR IS NULL AND NOT Y.LAST_CUR_RUN_ACCR IS NULL
        )
        OR (
          NOT X.LAST_CUR_RUN_ACCR IS NULL AND Y.LAST_CUR_RUN_ACCR IS NULL
        )
        OR (
          X.LAST_CUR_RUN_ACCR <> Y.LAST_CUR_RUN_ACCR
        )
      )
      OR (
        (
          X.LCY_AMT IS NULL AND NOT Y.LCY_AMT IS NULL
        )
        OR (
          NOT X.LCY_AMT IS NULL AND Y.LCY_AMT IS NULL
        )
        OR (
          X.LCY_AMT <> Y.LCY_AMT
        )
      )
      OR (
        (
          X.LIQN IS NULL AND NOT Y.LIQN IS NULL
        )
        OR (
          NOT X.LIQN IS NULL AND Y.LIQN IS NULL
        )
        OR (
          X.LIQN <> Y.LIQN
        )
      )
      OR (
        (
          X.PROCESS IS NULL AND NOT Y.PROCESS IS NULL
        )
        OR (
          NOT X.PROCESS IS NULL AND Y.PROCESS IS NULL
        )
        OR (
          X.PROCESS <> Y.PROCESS
        )
      )
      OR (
        (
          X.RUN_DATE IS NULL AND NOT Y.RUN_DATE IS NULL
        )
        OR (
          NOT X.RUN_DATE IS NULL AND Y.RUN_DATE IS NULL
        )
        OR (
          X.RUN_DATE <> Y.RUN_DATE
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
          X.SK_BRN IS NULL AND NOT Y.SK_BRN IS NULL
        )
        OR (
          NOT X.SK_BRN IS NULL AND Y.SK_BRN IS NULL
        )
        OR (
          X.SK_BRN <> Y.SK_BRN
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
    )
    THEN 'UPD'
    WHEN X.ACC IS NULL
    AND X.BRN IS NULL
    AND X.ENT_DT IS NULL
    AND X.FRM_NO IS NULL
    AND X.PROD IS NULL
    THEN 'DEL' /* - IF MULTIPLE PKs are there then concatenate them with AND */
  END AS OPT_FLAG
FROM DEVT_STG_FLX.ICTB_ENTRIES_HISTORY AS X
FULL JOIN DEVT_STG_FLX.ICTB_ENTRIES_HISTORY_BACKUP AS Y
  ON X.ACC = Y.ACC
  AND X.BRN = Y.BRN
  AND X.ENT_DT = Y.ENT_DT
  AND X.FRM_NO = Y.FRM_NO
  AND X.PROD = Y.PROD /* - IF MULTIPLE PKs are there then concatenate them with AND */
WHERE
  NOT OPT_FLAG IS NULL