CREATE OR REPLACE VIEW DEVV_STG_FLX.CSTB_CONTRACT_EVENT_LOG_get_delta AS LOCKING ROW FOR ACCESS
SELECT
  COALESCE(X.CONTRACT_REF_NO, Y.CONTRACT_REF_NO) AS CONTRACT_REF_NO,
  COALESCE(X.EVENT_SEQ_NO, Y.EVENT_SEQ_NO) AS EVENT_SEQ_NO,
  X.AUTH_STATUS,
  X.BM_AUTH_STATUS,
  X.BM_CONTRACT_STATUS,
  X.BM_CONTRACT_STATUS_1,
  X.BM_EVENT_CODE,
  X.BM_EVENT_CODE_1,
  X.BM_MODULE,
  X.BM_NEW_VERSION_INDICATOR,
  X.CHECKER_DT_STAMP,
  X.CHECKER_ID,
  X.CONTRACT_STATUS,
  X.EVENT_CODE,
  X.EVENT_DATE,
  X.EVENT_VALUE_DATE,
  X.MAKER_DT_STAMP,
  X.MAKER_ID,
  X.MODULE,
  X.NEW_VERSION_INDICATOR,
  X.REVERSED_EVENT_SEQ_NO,
  X.SK_CONTRACT_REF_NO,
  X.SK_CONTRACT_REF_NO_EVENT_SEQ_NO,
  X.SK_CHECKER_ID,
  X.SK_MAKER_ID,
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
    WHEN Y.CONTRACT_REF_NO IS NULL AND Y.EVENT_SEQ_NO IS NULL
    THEN 'INS' /* - IF MULTIPLE PKs are there then concatenate them with AND */
    WHEN X.CONTRACT_REF_NO = Y.CONTRACT_REF_NO
    AND X.EVENT_SEQ_NO = Y.EVENT_SEQ_NO /* - IF MULTIPLE PKs are there then concatenate them with AND */
    AND (
      (
        (
          X.AUTH_STATUS IS NULL AND NOT Y.AUTH_STATUS IS NULL
        )
        OR (
          NOT X.AUTH_STATUS IS NULL AND Y.AUTH_STATUS IS NULL
        )
        OR (
          X.AUTH_STATUS <> Y.AUTH_STATUS
        )
      )
      OR (
        (
          X.BM_AUTH_STATUS IS NULL AND NOT Y.BM_AUTH_STATUS IS NULL
        )
        OR (
          NOT X.BM_AUTH_STATUS IS NULL AND Y.BM_AUTH_STATUS IS NULL
        )
        OR (
          X.BM_AUTH_STATUS <> Y.BM_AUTH_STATUS
        )
      )
      OR (
        (
          X.BM_CONTRACT_STATUS IS NULL AND NOT Y.BM_CONTRACT_STATUS IS NULL
        )
        OR (
          NOT X.BM_CONTRACT_STATUS IS NULL AND Y.BM_CONTRACT_STATUS IS NULL
        )
        OR (
          X.BM_CONTRACT_STATUS <> Y.BM_CONTRACT_STATUS
        )
      )
      OR (
        (
          X.BM_CONTRACT_STATUS_1 IS NULL AND NOT Y.BM_CONTRACT_STATUS_1 IS NULL
        )
        OR (
          NOT X.BM_CONTRACT_STATUS_1 IS NULL AND Y.BM_CONTRACT_STATUS_1 IS NULL
        )
        OR (
          X.BM_CONTRACT_STATUS_1 <> Y.BM_CONTRACT_STATUS_1
        )
      )
      OR (
        (
          X.BM_EVENT_CODE IS NULL AND NOT Y.BM_EVENT_CODE IS NULL
        )
        OR (
          NOT X.BM_EVENT_CODE IS NULL AND Y.BM_EVENT_CODE IS NULL
        )
        OR (
          X.BM_EVENT_CODE <> Y.BM_EVENT_CODE
        )
      )
      OR (
        (
          X.BM_EVENT_CODE_1 IS NULL AND NOT Y.BM_EVENT_CODE_1 IS NULL
        )
        OR (
          NOT X.BM_EVENT_CODE_1 IS NULL AND Y.BM_EVENT_CODE_1 IS NULL
        )
        OR (
          X.BM_EVENT_CODE_1 <> Y.BM_EVENT_CODE_1
        )
      )
      OR (
        (
          X.BM_MODULE IS NULL AND NOT Y.BM_MODULE IS NULL
        )
        OR (
          NOT X.BM_MODULE IS NULL AND Y.BM_MODULE IS NULL
        )
        OR (
          X.BM_MODULE <> Y.BM_MODULE
        )
      )
      OR (
        (
          X.BM_NEW_VERSION_INDICATOR IS NULL AND NOT Y.BM_NEW_VERSION_INDICATOR IS NULL
        )
        OR (
          NOT X.BM_NEW_VERSION_INDICATOR IS NULL AND Y.BM_NEW_VERSION_INDICATOR IS NULL
        )
        OR (
          X.BM_NEW_VERSION_INDICATOR <> Y.BM_NEW_VERSION_INDICATOR
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
          X.CONTRACT_STATUS IS NULL AND NOT Y.CONTRACT_STATUS IS NULL
        )
        OR (
          NOT X.CONTRACT_STATUS IS NULL AND Y.CONTRACT_STATUS IS NULL
        )
        OR (
          X.CONTRACT_STATUS <> Y.CONTRACT_STATUS
        )
      )
      OR (
        (
          X.EVENT_CODE IS NULL AND NOT Y.EVENT_CODE IS NULL
        )
        OR (
          NOT X.EVENT_CODE IS NULL AND Y.EVENT_CODE IS NULL
        )
        OR (
          X.EVENT_CODE <> Y.EVENT_CODE
        )
      )
      OR (
        (
          X.EVENT_DATE IS NULL AND NOT Y.EVENT_DATE IS NULL
        )
        OR (
          NOT X.EVENT_DATE IS NULL AND Y.EVENT_DATE IS NULL
        )
        OR (
          X.EVENT_DATE <> Y.EVENT_DATE
        )
      )
      OR (
        (
          X.EVENT_VALUE_DATE IS NULL AND NOT Y.EVENT_VALUE_DATE IS NULL
        )
        OR (
          NOT X.EVENT_VALUE_DATE IS NULL AND Y.EVENT_VALUE_DATE IS NULL
        )
        OR (
          X.EVENT_VALUE_DATE <> Y.EVENT_VALUE_DATE
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
          X.MODULE IS NULL AND NOT Y.MODULE IS NULL
        )
        OR (
          NOT X.MODULE IS NULL AND Y.MODULE IS NULL
        )
        OR (
          X.MODULE <> Y.MODULE
        )
      )
      OR (
        (
          X.NEW_VERSION_INDICATOR IS NULL AND NOT Y.NEW_VERSION_INDICATOR IS NULL
        )
        OR (
          NOT X.NEW_VERSION_INDICATOR IS NULL AND Y.NEW_VERSION_INDICATOR IS NULL
        )
        OR (
          X.NEW_VERSION_INDICATOR <> Y.NEW_VERSION_INDICATOR
        )
      )
      OR (
        (
          X.REVERSED_EVENT_SEQ_NO IS NULL AND NOT Y.REVERSED_EVENT_SEQ_NO IS NULL
        )
        OR (
          NOT X.REVERSED_EVENT_SEQ_NO IS NULL AND Y.REVERSED_EVENT_SEQ_NO IS NULL
        )
        OR (
          X.REVERSED_EVENT_SEQ_NO <> Y.REVERSED_EVENT_SEQ_NO
        )
      )
      OR (
        (
          X.SK_CONTRACT_REF_NO IS NULL AND NOT Y.SK_CONTRACT_REF_NO IS NULL
        )
        OR (
          NOT X.SK_CONTRACT_REF_NO IS NULL AND Y.SK_CONTRACT_REF_NO IS NULL
        )
        OR (
          X.SK_CONTRACT_REF_NO <> Y.SK_CONTRACT_REF_NO
        )
      )
      OR (
        (
          X.SK_CONTRACT_REF_NO_EVENT_SEQ_NO IS NULL
          AND NOT Y.SK_CONTRACT_REF_NO_EVENT_SEQ_NO IS NULL
        )
        OR (
          NOT X.SK_CONTRACT_REF_NO_EVENT_SEQ_NO IS NULL
          AND Y.SK_CONTRACT_REF_NO_EVENT_SEQ_NO IS NULL
        )
        OR (
          X.SK_CONTRACT_REF_NO_EVENT_SEQ_NO <> Y.SK_CONTRACT_REF_NO_EVENT_SEQ_NO
        )
      )
      OR (
        (
          X.SK_CHECKER_ID IS NULL AND NOT Y.SK_CHECKER_ID IS NULL
        )
        OR (
          NOT X.SK_CHECKER_ID IS NULL AND Y.SK_CHECKER_ID IS NULL
        )
        OR (
          X.SK_CHECKER_ID <> Y.SK_CHECKER_ID
        )
      )
      OR (
        (
          X.SK_MAKER_ID IS NULL AND NOT Y.SK_MAKER_ID IS NULL
        )
        OR (
          NOT X.SK_MAKER_ID IS NULL AND Y.SK_MAKER_ID IS NULL
        )
        OR (
          X.SK_MAKER_ID <> Y.SK_MAKER_ID
        )
      )
    )
    THEN 'UPD'
    WHEN X.CONTRACT_REF_NO IS NULL AND X.EVENT_SEQ_NO IS NULL
    THEN 'DEL' /* - IF MULTIPLE PKs are there then concatenate them with AND */
  END AS OPT_FLAG
FROM DEVT_STG_FLX.CSTB_CONTRACT_EVENT_LOG AS X
FULL JOIN DEVT_STG_FLX.CSTB_CONTRACT_EVENT_LOG_BACKUP AS Y
  ON X.CONTRACT_REF_NO = Y.CONTRACT_REF_NO
  AND X.EVENT_SEQ_NO = Y.EVENT_SEQ_NO /* - IF MULTIPLE PKs are there then concatenate them with AND */
WHERE
  NOT OPT_FLAG IS NULL