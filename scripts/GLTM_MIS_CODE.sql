CREATE OR REPLACE VIEW DEVV_STG_FLX.GLTM_MIS_CODE_get_delta AS LOCKING ROW FOR ACCESS
SELECT
  COALESCE(X.MIS_CLASS, Y.MIS_CLASS) AS MIS_CLASS,
  COALESCE(X.MIS_CODE, Y.MIS_CODE) AS MIS_CODE,
  X.ACTIVE,
  X.BM_MIS_CODE_ACTIVITY,
  X.BM_MIS_CODE_BANKS,
  X.BM_MIS_CODE_CBE_SECT,
  X.BM_MIS_CODE_CITY,
  X.BM_MIS_CODE_CONS_NPL,
  X.BM_MIS_CODE_INTITY,
  X.BM_MIS_CODE_INTITY,
  X.BM_MIS_CODE_LOB,
  X.BM_MIS_CODE_OTHER_NATIONALITY,
  X.BM_MIS_CODE_RELA_RISK,
  X.BM_MIS_CODE_RISK,
  X.BM_MIS_CODE_SECT,
  X.CODE_DESC,
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
    WHEN Y.MIS_CLASS IS NULL AND Y.MIS_CODE IS NULL
    THEN 'INS' /* - IF MULTIPLE PKs are there then concatenate them with AND */
    WHEN X.MIS_CLASS = Y.MIS_CLASS
    AND X.MIS_CODE = Y.MIS_CODE /* - IF MULTIPLE PKs are there then concatenate them with AND */
    AND (
      (
        (
          X.ACTIVE IS NULL AND NOT Y.ACTIVE IS NULL
        )
        OR (
          NOT X.ACTIVE IS NULL AND Y.ACTIVE IS NULL
        )
        OR (
          X.ACTIVE <> Y.ACTIVE
        )
      )
      OR (
        (
          X.BM_MIS_CODE_ACTIVITY IS NULL AND NOT Y.BM_MIS_CODE_ACTIVITY IS NULL
        )
        OR (
          NOT X.BM_MIS_CODE_ACTIVITY IS NULL AND Y.BM_MIS_CODE_ACTIVITY IS NULL
        )
        OR (
          X.BM_MIS_CODE_ACTIVITY <> Y.BM_MIS_CODE_ACTIVITY
        )
      )
      OR (
        (
          X.BM_MIS_CODE_BANKS IS NULL AND NOT Y.BM_MIS_CODE_BANKS IS NULL
        )
        OR (
          NOT X.BM_MIS_CODE_BANKS IS NULL AND Y.BM_MIS_CODE_BANKS IS NULL
        )
        OR (
          X.BM_MIS_CODE_BANKS <> Y.BM_MIS_CODE_BANKS
        )
      )
      OR (
        (
          X.BM_MIS_CODE_CBE_SECT IS NULL AND NOT Y.BM_MIS_CODE_CBE_SECT IS NULL
        )
        OR (
          NOT X.BM_MIS_CODE_CBE_SECT IS NULL AND Y.BM_MIS_CODE_CBE_SECT IS NULL
        )
        OR (
          X.BM_MIS_CODE_CBE_SECT <> Y.BM_MIS_CODE_CBE_SECT
        )
      )
      OR (
        (
          X.BM_MIS_CODE_CITY IS NULL AND NOT Y.BM_MIS_CODE_CITY IS NULL
        )
        OR (
          NOT X.BM_MIS_CODE_CITY IS NULL AND Y.BM_MIS_CODE_CITY IS NULL
        )
        OR (
          X.BM_MIS_CODE_CITY <> Y.BM_MIS_CODE_CITY
        )
      )
      OR (
        (
          X.BM_MIS_CODE_CONS_NPL IS NULL AND NOT Y.BM_MIS_CODE_CONS_NPL IS NULL
        )
        OR (
          NOT X.BM_MIS_CODE_CONS_NPL IS NULL AND Y.BM_MIS_CODE_CONS_NPL IS NULL
        )
        OR (
          X.BM_MIS_CODE_CONS_NPL <> Y.BM_MIS_CODE_CONS_NPL
        )
      )
      OR (
        (
          X.BM_MIS_CODE_INTITY IS NULL AND NOT Y.BM_MIS_CODE_INTITY IS NULL
        )
        OR (
          NOT X.BM_MIS_CODE_INTITY IS NULL AND Y.BM_MIS_CODE_INTITY IS NULL
        )
        OR (
          X.BM_MIS_CODE_INTITY <> Y.BM_MIS_CODE_INTITY
        )
      )
      OR (
        (
          X.BM_MIS_CODE_INTITY IS NULL AND NOT Y.BM_MIS_CODE_INTITY IS NULL
        )
        OR (
          NOT X.BM_MIS_CODE_INTITY IS NULL AND Y.BM_MIS_CODE_INTITY IS NULL
        )
        OR (
          X.BM_MIS_CODE_INTITY <> Y.BM_MIS_CODE_INTITY
        )
      )
      OR (
        (
          X.BM_MIS_CODE_LOB IS NULL AND NOT Y.BM_MIS_CODE_LOB IS NULL
        )
        OR (
          NOT X.BM_MIS_CODE_LOB IS NULL AND Y.BM_MIS_CODE_LOB IS NULL
        )
        OR (
          X.BM_MIS_CODE_LOB <> Y.BM_MIS_CODE_LOB
        )
      )
      OR (
        (
          X.BM_MIS_CODE_OTHER_NATIONALITY IS NULL
          AND NOT Y.BM_MIS_CODE_OTHER_NATIONALITY IS NULL
        )
        OR (
          NOT X.BM_MIS_CODE_OTHER_NATIONALITY IS NULL
          AND Y.BM_MIS_CODE_OTHER_NATIONALITY IS NULL
        )
        OR (
          X.BM_MIS_CODE_OTHER_NATIONALITY <> Y.BM_MIS_CODE_OTHER_NATIONALITY
        )
      )
      OR (
        (
          X.BM_MIS_CODE_RELA_RISK IS NULL AND NOT Y.BM_MIS_CODE_RELA_RISK IS NULL
        )
        OR (
          NOT X.BM_MIS_CODE_RELA_RISK IS NULL AND Y.BM_MIS_CODE_RELA_RISK IS NULL
        )
        OR (
          X.BM_MIS_CODE_RELA_RISK <> Y.BM_MIS_CODE_RELA_RISK
        )
      )
      OR (
        (
          X.BM_MIS_CODE_RISK IS NULL AND NOT Y.BM_MIS_CODE_RISK IS NULL
        )
        OR (
          NOT X.BM_MIS_CODE_RISK IS NULL AND Y.BM_MIS_CODE_RISK IS NULL
        )
        OR (
          X.BM_MIS_CODE_RISK <> Y.BM_MIS_CODE_RISK
        )
      )
      OR (
        (
          X.BM_MIS_CODE_SECT IS NULL AND NOT Y.BM_MIS_CODE_SECT IS NULL
        )
        OR (
          NOT X.BM_MIS_CODE_SECT IS NULL AND Y.BM_MIS_CODE_SECT IS NULL
        )
        OR (
          X.BM_MIS_CODE_SECT <> Y.BM_MIS_CODE_SECT
        )
      )
      OR (
        (
          X.CODE_DESC IS NULL AND NOT Y.CODE_DESC IS NULL
        )
        OR (
          NOT X.CODE_DESC IS NULL AND Y.CODE_DESC IS NULL
        )
        OR (
          X.CODE_DESC <> Y.CODE_DESC
        )
      )
    )
    THEN 'UPD'
    WHEN X.MIS_CLASS IS NULL AND X.MIS_CODE IS NULL
    THEN 'DEL' /* - IF MULTIPLE PKs are there then concatenate them with AND */
  END AS OPT_FLAG
FROM DEVT_STG_FLX.GLTM_MIS_CODE AS X
FULL JOIN DEVT_STG_FLX.GLTM_MIS_CODE_BACKUP AS Y
  ON X.MIS_CLASS = Y.MIS_CLASS
  AND X.MIS_CODE = Y.MIS_CODE /* - IF MULTIPLE PKs are there then concatenate them with AND */
WHERE
  NOT OPT_FLAG IS NULL