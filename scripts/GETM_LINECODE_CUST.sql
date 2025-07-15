CREATE OR REPLACE VIEW DEVV_STG_FLX.GETM_LINECODE_CUST_get_delta AS LOCKING ROW FOR ACCESS
SELECT
  COALESCE(X.LINE_CODE, Y.LINE_CODE) AS LINE_CODE,
  X.AUTH_STAT,
  X.BM_LINE_CODE,
  X.BM_LINE_CODE_1,
  X.CHECKER_DT_STAMP,
  X.CHECKER_ID,
  X.DESCRIPTION,
  X.MAKER_DT_STAMP,
  X.MAKER_ID,
  X.MOD_NO,
  X.ONCE_AUTH,
  X.RECORD_STAT,
  X.REST_TYPE_BRN,
  X.REST_TYPE_CCY,
  X.REST_TYPE_CUST,
  X.REST_TYPE_EXP,
  X.REST_TYPE_PRD,
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
    WHEN Y.LINE_CODE IS NULL
    THEN 'INS' /* - IF MULTIPLE PKs are there then concatenate them with AND */
    WHEN X.LINE_CODE = Y.LINE_CODE /* - IF MULTIPLE PKs are there then concatenate them with AND */
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
          X.BM_LINE_CODE IS NULL AND NOT Y.BM_LINE_CODE IS NULL
        )
        OR (
          NOT X.BM_LINE_CODE IS NULL AND Y.BM_LINE_CODE IS NULL
        )
        OR (
          X.BM_LINE_CODE <> Y.BM_LINE_CODE
        )
      )
      OR (
        (
          X.BM_LINE_CODE_1 IS NULL AND NOT Y.BM_LINE_CODE_1 IS NULL
        )
        OR (
          NOT X.BM_LINE_CODE_1 IS NULL AND Y.BM_LINE_CODE_1 IS NULL
        )
        OR (
          X.BM_LINE_CODE_1 <> Y.BM_LINE_CODE_1
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
          X.DESCRIPTION IS NULL AND NOT Y.DESCRIPTION IS NULL
        )
        OR (
          NOT X.DESCRIPTION IS NULL AND Y.DESCRIPTION IS NULL
        )
        OR (
          X.DESCRIPTION <> Y.DESCRIPTION
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
          X.REST_TYPE_BRN IS NULL AND NOT Y.REST_TYPE_BRN IS NULL
        )
        OR (
          NOT X.REST_TYPE_BRN IS NULL AND Y.REST_TYPE_BRN IS NULL
        )
        OR (
          X.REST_TYPE_BRN <> Y.REST_TYPE_BRN
        )
      )
      OR (
        (
          X.REST_TYPE_CCY IS NULL AND NOT Y.REST_TYPE_CCY IS NULL
        )
        OR (
          NOT X.REST_TYPE_CCY IS NULL AND Y.REST_TYPE_CCY IS NULL
        )
        OR (
          X.REST_TYPE_CCY <> Y.REST_TYPE_CCY
        )
      )
      OR (
        (
          X.REST_TYPE_CUST IS NULL AND NOT Y.REST_TYPE_CUST IS NULL
        )
        OR (
          NOT X.REST_TYPE_CUST IS NULL AND Y.REST_TYPE_CUST IS NULL
        )
        OR (
          X.REST_TYPE_CUST <> Y.REST_TYPE_CUST
        )
      )
      OR (
        (
          X.REST_TYPE_EXP IS NULL AND NOT Y.REST_TYPE_EXP IS NULL
        )
        OR (
          NOT X.REST_TYPE_EXP IS NULL AND Y.REST_TYPE_EXP IS NULL
        )
        OR (
          X.REST_TYPE_EXP <> Y.REST_TYPE_EXP
        )
      )
      OR (
        (
          X.REST_TYPE_PRD IS NULL AND NOT Y.REST_TYPE_PRD IS NULL
        )
        OR (
          NOT X.REST_TYPE_PRD IS NULL AND Y.REST_TYPE_PRD IS NULL
        )
        OR (
          X.REST_TYPE_PRD <> Y.REST_TYPE_PRD
        )
      )
    )
    THEN 'UPD'
    WHEN X.LINE_CODE IS NULL
    THEN 'DEL' /* - IF MULTIPLE PKs are there then concatenate them with AND */
  END AS OPT_FLAG
FROM DEVT_STG_FLX.GETM_LINECODE_CUST AS X
FULL JOIN DEVT_STG_FLX.GETM_LINECODE_CUST_BACKUP AS Y
  ON X.LINE_CODE = Y.LINE_CODE /* - IF MULTIPLE PKs are there then concatenate them with AND */
WHERE
  NOT OPT_FLAG IS NULL