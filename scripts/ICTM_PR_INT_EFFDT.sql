CREATE OR REPLACE VIEW DEVV_STG_FLX.ICTM_PR_INT_EFFDT_get_delta AS LOCKING ROW FOR ACCESS
SELECT
  COALESCE(X.ACLASS, Y.ACLASS) AS ACLASS,
  COALESCE(X.BRANCH_CODE, Y.BRANCH_CODE) AS BRANCH_CODE,
  COALESCE(X.CCY_CODE, Y.CCY_CODE) AS CCY_CODE,
  COALESCE(X.PRODUCT_CODE, Y.PRODUCT_CODE) AS PRODUCT_CODE,
  COALESCE(X.UDE_EFF_DT, Y.UDE_EFF_DT) AS UDE_EFF_DT,
  X.AUTH_STAT,
  X.BM_AUTH_STAT,
  X.BM_CCY_CODE,
  X.BM_RECORD_STAT,
  X.CHECKER_DT_STAMP,
  X.CHECKER_ID,
  X.MAKER_DT_STAMP,
  X.MAKER_ID,
  X.MOD_NO,
  X.ONCE_AUTH,
  X.RECORD_STAT,
  X.SK_ACLASS,
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
    WHEN Y.ACLASS IS NULL
    AND Y.BRANCH_CODE IS NULL
    AND Y.CCY_CODE IS NULL
    AND Y.PRODUCT_CODE IS NULL
    AND Y.UDE_EFF_DT IS NULL
    THEN 'INS' /* - IF MULTIPLE PKs are there then concatenate them with AND */
    WHEN X.ACLASS = Y.ACLASS
    AND X.BRANCH_CODE = Y.BRANCH_CODE
    AND X.CCY_CODE = Y.CCY_CODE
    AND X.PRODUCT_CODE = Y.PRODUCT_CODE
    AND X.UDE_EFF_DT = Y.UDE_EFF_DT /* - IF MULTIPLE PKs are there then concatenate them with AND */
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
          X.SK_ACLASS IS NULL AND NOT Y.SK_ACLASS IS NULL
        )
        OR (
          NOT X.SK_ACLASS IS NULL AND Y.SK_ACLASS IS NULL
        )
        OR (
          X.SK_ACLASS <> Y.SK_ACLASS
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
    WHEN X.ACLASS IS NULL
    AND X.BRANCH_CODE IS NULL
    AND X.CCY_CODE IS NULL
    AND X.PRODUCT_CODE IS NULL
    AND X.UDE_EFF_DT IS NULL
    THEN 'DEL' /* - IF MULTIPLE PKs are there then concatenate them with AND */
  END AS OPT_FLAG
FROM DEVT_STG_FLX.ICTM_PR_INT_EFFDT AS X
FULL JOIN DEVT_STG_FLX.ICTM_PR_INT_EFFDT_BACKUP AS Y
  ON X.ACLASS = Y.ACLASS
  AND X.BRANCH_CODE = Y.BRANCH_CODE
  AND X.CCY_CODE = Y.CCY_CODE
  AND X.PRODUCT_CODE = Y.PRODUCT_CODE
  AND X.UDE_EFF_DT = Y.UDE_EFF_DT /* - IF MULTIPLE PKs are there then concatenate them with AND */
WHERE
  NOT OPT_FLAG IS NULL