CREATE OR REPLACE VIEW DEVV_STG_FLX.ICTB_ENTRIES_get_delta AS LOCKING ROW FOR ACCESS
SELECT
  COALESCE(X.ACC, Y.ACC) AS ACC,
  COALESCE(X.FRM_NO, Y.FRM_NO) AS FRM_NO,
  COALESCE(X.PROD, Y.PROD) AS PROD,
  X.ACCRUED_AMT,
  X.ACQUIRED_ADJ,
  X.ACQUIRED_AMT,
  X.AMT,
  X.AMT_TO_ACCRUE,
  X.BRN,
  X.CUR_RUN_ACCR,
  X.DAILY_AMT,
  X.DRCR,
  X.ENT_DT,
  X.HAS_ACCR,
  X.NEXT_CALC_DUE_DATE,
  X.PROCESS,
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
    WHEN Y.ACC IS NULL AND Y.FRM_NO IS NULL AND Y.PROD IS NULL
    THEN 'INS' /* - IF MULTIPLE PKs are there then concatenate them with AND */
    WHEN X.ACC = Y.ACC
    AND X.FRM_NO = Y.FRM_NO
    AND X.PROD = Y.PROD /* - IF MULTIPLE PKs are there then concatenate them with AND */
    AND (
      (
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
          X.ACQUIRED_ADJ IS NULL AND NOT Y.ACQUIRED_ADJ IS NULL
        )
        OR (
          NOT X.ACQUIRED_ADJ IS NULL AND Y.ACQUIRED_ADJ IS NULL
        )
        OR (
          X.ACQUIRED_ADJ <> Y.ACQUIRED_ADJ
        )
      )
      OR (
        (
          X.ACQUIRED_AMT IS NULL AND NOT Y.ACQUIRED_AMT IS NULL
        )
        OR (
          NOT X.ACQUIRED_AMT IS NULL AND Y.ACQUIRED_AMT IS NULL
        )
        OR (
          X.ACQUIRED_AMT <> Y.ACQUIRED_AMT
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
          X.DAILY_AMT IS NULL AND NOT Y.DAILY_AMT IS NULL
        )
        OR (
          NOT X.DAILY_AMT IS NULL AND Y.DAILY_AMT IS NULL
        )
        OR (
          X.DAILY_AMT <> Y.DAILY_AMT
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
          X.ENT_DT IS NULL AND NOT Y.ENT_DT IS NULL
        )
        OR (
          NOT X.ENT_DT IS NULL AND Y.ENT_DT IS NULL
        )
        OR (
          X.ENT_DT <> Y.ENT_DT
        )
      )
      OR (
        (
          X.HAS_ACCR IS NULL AND NOT Y.HAS_ACCR IS NULL
        )
        OR (
          NOT X.HAS_ACCR IS NULL AND Y.HAS_ACCR IS NULL
        )
        OR (
          X.HAS_ACCR <> Y.HAS_ACCR
        )
      )
      OR (
        (
          X.NEXT_CALC_DUE_DATE IS NULL AND NOT Y.NEXT_CALC_DUE_DATE IS NULL
        )
        OR (
          NOT X.NEXT_CALC_DUE_DATE IS NULL AND Y.NEXT_CALC_DUE_DATE IS NULL
        )
        OR (
          X.NEXT_CALC_DUE_DATE <> Y.NEXT_CALC_DUE_DATE
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
    WHEN X.ACC IS NULL AND X.FRM_NO IS NULL AND X.PROD IS NULL
    THEN 'DEL' /* - IF MULTIPLE PKs are there then concatenate them with AND */
  END AS OPT_FLAG
FROM DEVT_STG_FLX.ICTB_ENTRIES AS X
FULL JOIN DEVT_STG_FLX.ICTB_ENTRIES_BACKUP AS Y
  ON X.ACC = Y.ACC
  AND X.FRM_NO = Y.FRM_NO
  AND X.PROD = Y.PROD /* - IF MULTIPLE PKs are there then concatenate them with AND */
WHERE
  NOT OPT_FLAG IS NULL