CREATE OR REPLACE VIEW DEVV_STG_FLX.CSTB_CONTRACT_get_delta AS LOCKING ROW FOR ACCESS
SELECT
  COALESCE(X.CONTRACT_REF_NO, Y.CONTRACT_REF_NO) AS CONTRACT_REF_NO,
  X.AUTH_STATUS,
  X.AUTO_MANUAL_FLAG,
  X.BM_AUTH_STATUS,
  X.BM_CONTRACT_CCY,
  X.BM_CONTRACT_STATUS,
  X.BM_CURR_EVENT_CODE,
  X.BM_CURR_EVENT_CODE,
  X.BM_MODULE_CODE,
  X.BM_MODULE_CODE_1,
  X.BM_PRODUCT_TYPE,
  X.BM_USER_DEFINED_STATUS,
  X.BOOK_DATE,
  X.BRANCH,
  X.CONTRACT_CCY,
  X.CONTRACT_STATUS,
  X.COUNTERPARTY,
  X.CURR_EVENT_CODE,
  X.EXTERNAL_REF_NO,
  X.FUND_REF_NO,
  X.LATEST_EVENT_DATE,
  X.LATEST_EVENT_SEQ_NO,
  X.LATEST_VERSION_NO,
  X.LIABILITY_CIF,
  X.MODULE_CODE,
  X.SK_COUNTERPARTY,
  X.PRODUCT_CODE,
  X.PRODUCT_TYPE,
  X.RESPONSE_STAT,
  X.REVERSED_CONT_REF,
  X.SECURITY_UPLOAD_STATUS,
  X.SERIAL_NO,
  X.SK_BRANCH,
  X.SK_CONTRACT_REF_NO,
  X.SK_PRODUCT_CODE,
  X.SOURCE,
  X.STOP_DATE,
  X.TEMPLATE_STATUS,
  X.USER_DEFINED_STATUS,
  X.USER_REF_NO,
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
    WHEN Y.CONTRACT_REF_NO IS NULL
    THEN 'INS' /* - IF MULTIPLE PKs are there then concatenate them with AND */
    WHEN X.CONTRACT_REF_NO = Y.CONTRACT_REF_NO /* - IF MULTIPLE PKs are there then concatenate them with AND */
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
          X.AUTO_MANUAL_FLAG IS NULL AND NOT Y.AUTO_MANUAL_FLAG IS NULL
        )
        OR (
          NOT X.AUTO_MANUAL_FLAG IS NULL AND Y.AUTO_MANUAL_FLAG IS NULL
        )
        OR (
          X.AUTO_MANUAL_FLAG <> Y.AUTO_MANUAL_FLAG
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
          X.BM_CONTRACT_CCY IS NULL AND NOT Y.BM_CONTRACT_CCY IS NULL
        )
        OR (
          NOT X.BM_CONTRACT_CCY IS NULL AND Y.BM_CONTRACT_CCY IS NULL
        )
        OR (
          X.BM_CONTRACT_CCY <> Y.BM_CONTRACT_CCY
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
          X.BM_CURR_EVENT_CODE IS NULL AND NOT Y.BM_CURR_EVENT_CODE IS NULL
        )
        OR (
          NOT X.BM_CURR_EVENT_CODE IS NULL AND Y.BM_CURR_EVENT_CODE IS NULL
        )
        OR (
          X.BM_CURR_EVENT_CODE <> Y.BM_CURR_EVENT_CODE
        )
      )
      OR (
        (
          X.BM_CURR_EVENT_CODE IS NULL AND NOT Y.BM_CURR_EVENT_CODE IS NULL
        )
        OR (
          NOT X.BM_CURR_EVENT_CODE IS NULL AND Y.BM_CURR_EVENT_CODE IS NULL
        )
        OR (
          X.BM_CURR_EVENT_CODE <> Y.BM_CURR_EVENT_CODE
        )
      )
      OR (
        (
          X.BM_MODULE_CODE IS NULL AND NOT Y.BM_MODULE_CODE IS NULL
        )
        OR (
          NOT X.BM_MODULE_CODE IS NULL AND Y.BM_MODULE_CODE IS NULL
        )
        OR (
          X.BM_MODULE_CODE <> Y.BM_MODULE_CODE
        )
      )
      OR (
        (
          X.BM_MODULE_CODE_1 IS NULL AND NOT Y.BM_MODULE_CODE_1 IS NULL
        )
        OR (
          NOT X.BM_MODULE_CODE_1 IS NULL AND Y.BM_MODULE_CODE_1 IS NULL
        )
        OR (
          X.BM_MODULE_CODE_1 <> Y.BM_MODULE_CODE_1
        )
      )
      OR (
        (
          X.BM_PRODUCT_TYPE IS NULL AND NOT Y.BM_PRODUCT_TYPE IS NULL
        )
        OR (
          NOT X.BM_PRODUCT_TYPE IS NULL AND Y.BM_PRODUCT_TYPE IS NULL
        )
        OR (
          X.BM_PRODUCT_TYPE <> Y.BM_PRODUCT_TYPE
        )
      )
      OR (
        (
          X.BM_USER_DEFINED_STATUS IS NULL AND NOT Y.BM_USER_DEFINED_STATUS IS NULL
        )
        OR (
          NOT X.BM_USER_DEFINED_STATUS IS NULL AND Y.BM_USER_DEFINED_STATUS IS NULL
        )
        OR (
          X.BM_USER_DEFINED_STATUS <> Y.BM_USER_DEFINED_STATUS
        )
      )
      OR (
        (
          X.BOOK_DATE IS NULL AND NOT Y.BOOK_DATE IS NULL
        )
        OR (
          NOT X.BOOK_DATE IS NULL AND Y.BOOK_DATE IS NULL
        )
        OR (
          X.BOOK_DATE <> Y.BOOK_DATE
        )
      )
      OR (
        (
          X.BRANCH IS NULL AND NOT Y.BRANCH IS NULL
        )
        OR (
          NOT X.BRANCH IS NULL AND Y.BRANCH IS NULL
        )
        OR (
          X.BRANCH <> Y.BRANCH
        )
      )
      OR (
        (
          X.CONTRACT_CCY IS NULL AND NOT Y.CONTRACT_CCY IS NULL
        )
        OR (
          NOT X.CONTRACT_CCY IS NULL AND Y.CONTRACT_CCY IS NULL
        )
        OR (
          X.CONTRACT_CCY <> Y.CONTRACT_CCY
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
          X.COUNTERPARTY IS NULL AND NOT Y.COUNTERPARTY IS NULL
        )
        OR (
          NOT X.COUNTERPARTY IS NULL AND Y.COUNTERPARTY IS NULL
        )
        OR (
          X.COUNTERPARTY <> Y.COUNTERPARTY
        )
      )
      OR (
        (
          X.CURR_EVENT_CODE IS NULL AND NOT Y.CURR_EVENT_CODE IS NULL
        )
        OR (
          NOT X.CURR_EVENT_CODE IS NULL AND Y.CURR_EVENT_CODE IS NULL
        )
        OR (
          X.CURR_EVENT_CODE <> Y.CURR_EVENT_CODE
        )
      )
      OR (
        (
          X.EXTERNAL_REF_NO IS NULL AND NOT Y.EXTERNAL_REF_NO IS NULL
        )
        OR (
          NOT X.EXTERNAL_REF_NO IS NULL AND Y.EXTERNAL_REF_NO IS NULL
        )
        OR (
          X.EXTERNAL_REF_NO <> Y.EXTERNAL_REF_NO
        )
      )
      OR (
        (
          X.FUND_REF_NO IS NULL AND NOT Y.FUND_REF_NO IS NULL
        )
        OR (
          NOT X.FUND_REF_NO IS NULL AND Y.FUND_REF_NO IS NULL
        )
        OR (
          X.FUND_REF_NO <> Y.FUND_REF_NO
        )
      )
      OR (
        (
          X.LATEST_EVENT_DATE IS NULL AND NOT Y.LATEST_EVENT_DATE IS NULL
        )
        OR (
          NOT X.LATEST_EVENT_DATE IS NULL AND Y.LATEST_EVENT_DATE IS NULL
        )
        OR (
          X.LATEST_EVENT_DATE <> Y.LATEST_EVENT_DATE
        )
      )
      OR (
        (
          X.LATEST_EVENT_SEQ_NO IS NULL AND NOT Y.LATEST_EVENT_SEQ_NO IS NULL
        )
        OR (
          NOT X.LATEST_EVENT_SEQ_NO IS NULL AND Y.LATEST_EVENT_SEQ_NO IS NULL
        )
        OR (
          X.LATEST_EVENT_SEQ_NO <> Y.LATEST_EVENT_SEQ_NO
        )
      )
      OR (
        (
          X.LATEST_VERSION_NO IS NULL AND NOT Y.LATEST_VERSION_NO IS NULL
        )
        OR (
          NOT X.LATEST_VERSION_NO IS NULL AND Y.LATEST_VERSION_NO IS NULL
        )
        OR (
          X.LATEST_VERSION_NO <> Y.LATEST_VERSION_NO
        )
      )
      OR (
        (
          X.LIABILITY_CIF IS NULL AND NOT Y.LIABILITY_CIF IS NULL
        )
        OR (
          NOT X.LIABILITY_CIF IS NULL AND Y.LIABILITY_CIF IS NULL
        )
        OR (
          X.LIABILITY_CIF <> Y.LIABILITY_CIF
        )
      )
      OR (
        (
          X.MODULE_CODE IS NULL AND NOT Y.MODULE_CODE IS NULL
        )
        OR (
          NOT X.MODULE_CODE IS NULL AND Y.MODULE_CODE IS NULL
        )
        OR (
          X.MODULE_CODE <> Y.MODULE_CODE
        )
      )
      OR (
        (
          X.SK_COUNTERPARTY IS NULL AND NOT Y.SK_COUNTERPARTY IS NULL
        )
        OR (
          NOT X.SK_COUNTERPARTY IS NULL AND Y.SK_COUNTERPARTY IS NULL
        )
        OR (
          X.SK_COUNTERPARTY <> Y.SK_COUNTERPARTY
        )
      )
      OR (
        (
          X.PRODUCT_CODE IS NULL AND NOT Y.PRODUCT_CODE IS NULL
        )
        OR (
          NOT X.PRODUCT_CODE IS NULL AND Y.PRODUCT_CODE IS NULL
        )
        OR (
          X.PRODUCT_CODE <> Y.PRODUCT_CODE
        )
      )
      OR (
        (
          X.PRODUCT_TYPE IS NULL AND NOT Y.PRODUCT_TYPE IS NULL
        )
        OR (
          NOT X.PRODUCT_TYPE IS NULL AND Y.PRODUCT_TYPE IS NULL
        )
        OR (
          X.PRODUCT_TYPE <> Y.PRODUCT_TYPE
        )
      )
      OR (
        (
          X.RESPONSE_STAT IS NULL AND NOT Y.RESPONSE_STAT IS NULL
        )
        OR (
          NOT X.RESPONSE_STAT IS NULL AND Y.RESPONSE_STAT IS NULL
        )
        OR (
          X.RESPONSE_STAT <> Y.RESPONSE_STAT
        )
      )
      OR (
        (
          X.REVERSED_CONT_REF IS NULL AND NOT Y.REVERSED_CONT_REF IS NULL
        )
        OR (
          NOT X.REVERSED_CONT_REF IS NULL AND Y.REVERSED_CONT_REF IS NULL
        )
        OR (
          X.REVERSED_CONT_REF <> Y.REVERSED_CONT_REF
        )
      )
      OR (
        (
          X.SECURITY_UPLOAD_STATUS IS NULL AND NOT Y.SECURITY_UPLOAD_STATUS IS NULL
        )
        OR (
          NOT X.SECURITY_UPLOAD_STATUS IS NULL AND Y.SECURITY_UPLOAD_STATUS IS NULL
        )
        OR (
          X.SECURITY_UPLOAD_STATUS <> Y.SECURITY_UPLOAD_STATUS
        )
      )
      OR (
        (
          X.SERIAL_NO IS NULL AND NOT Y.SERIAL_NO IS NULL
        )
        OR (
          NOT X.SERIAL_NO IS NULL AND Y.SERIAL_NO IS NULL
        )
        OR (
          X.SERIAL_NO <> Y.SERIAL_NO
        )
      )
      OR (
        (
          X.SK_BRANCH IS NULL AND NOT Y.SK_BRANCH IS NULL
        )
        OR (
          NOT X.SK_BRANCH IS NULL AND Y.SK_BRANCH IS NULL
        )
        OR (
          X.SK_BRANCH <> Y.SK_BRANCH
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
          X.SK_PRODUCT_CODE IS NULL AND NOT Y.SK_PRODUCT_CODE IS NULL
        )
        OR (
          NOT X.SK_PRODUCT_CODE IS NULL AND Y.SK_PRODUCT_CODE IS NULL
        )
        OR (
          X.SK_PRODUCT_CODE <> Y.SK_PRODUCT_CODE
        )
      )
      OR (
        (
          X.SOURCE IS NULL AND NOT Y.SOURCE IS NULL
        )
        OR (
          NOT X.SOURCE IS NULL AND Y.SOURCE IS NULL
        )
        OR (
          X.SOURCE <> Y.SOURCE
        )
      )
      OR (
        (
          X.STOP_DATE IS NULL AND NOT Y.STOP_DATE IS NULL
        )
        OR (
          NOT X.STOP_DATE IS NULL AND Y.STOP_DATE IS NULL
        )
        OR (
          X.STOP_DATE <> Y.STOP_DATE
        )
      )
      OR (
        (
          X.TEMPLATE_STATUS IS NULL AND NOT Y.TEMPLATE_STATUS IS NULL
        )
        OR (
          NOT X.TEMPLATE_STATUS IS NULL AND Y.TEMPLATE_STATUS IS NULL
        )
        OR (
          X.TEMPLATE_STATUS <> Y.TEMPLATE_STATUS
        )
      )
      OR (
        (
          X.USER_DEFINED_STATUS IS NULL AND NOT Y.USER_DEFINED_STATUS IS NULL
        )
        OR (
          NOT X.USER_DEFINED_STATUS IS NULL AND Y.USER_DEFINED_STATUS IS NULL
        )
        OR (
          X.USER_DEFINED_STATUS <> Y.USER_DEFINED_STATUS
        )
      )
      OR (
        (
          X.USER_REF_NO IS NULL AND NOT Y.USER_REF_NO IS NULL
        )
        OR (
          NOT X.USER_REF_NO IS NULL AND Y.USER_REF_NO IS NULL
        )
        OR (
          X.USER_REF_NO <> Y.USER_REF_NO
        )
      )
    )
    THEN 'UPD'
    WHEN X.CONTRACT_REF_NO IS NULL
    THEN 'DEL' /* - IF MULTIPLE PKs are there then concatenate them with AND */
  END AS OPT_FLAG
FROM DEVT_STG_FLX.CSTB_CONTRACT AS X
FULL JOIN DEVT_STG_FLX.CSTB_CONTRACT_BACKUP AS Y
  ON X.CONTRACT_REF_NO = Y.CONTRACT_REF_NO /* - IF MULTIPLE PKs are there then concatenate them with AND */
WHERE
  NOT OPT_FLAG IS NULL