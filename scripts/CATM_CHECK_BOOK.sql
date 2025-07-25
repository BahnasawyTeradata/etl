CREATE OR REPLACE VIEW DEVV_STG_FLX.CATM_CHECK_BOOK_get_delta AS LOCKING ROW FOR ACCESS
SELECT
  COALESCE(X.ACCOUNT, Y.ACCOUNT) AS ACCOUNT,
  COALESCE(X.FIRST_CHECK_NO, Y.FIRST_CHECK_NO) AS FIRST_CHECK_NO,
  X.AUTH_STAT,
  X.BM_AUTH_STAT,
  X.BM_RECORD_STAT,
  X.BM_REQUEST_STATUS,
  X.BRANCH,
  X.CHECK_LEAVES,
  X.CHECKER_DT_STAMP,
  X.CHECKER_ID,
  X.CHEQUE_BOOK_TYPE,
  X.CHQ_TYPE,
  X.CHQBOOK_DELIVERD,
  X.DELIVERY_ADD1,
  X.DELIVERY_ADD2,
  X.DELIVERY_ADD3,
  X.DELIVERY_ADD4,
  X.DELIVERY_DATE,
  X.DELIVERY_MODE,
  X.DELIVERY_REF_NO,
  X.EXTERNAL_REF_NO,
  X.INCL_FOR_CHKBOOK_PRINTING,
  X.ISSUE_DATE,
  X.LANGUAGE_CODE,
  X.MAKER_DT_STAMP,
  X.MAKER_ID,
  X.MOD_NO,
  X.ONCE_AUTH,
  X.ORDER_DATE,
  X.ORDER_DETAILS,
  X.PRINT_STATUS,
  X.RECORD_STAT,
  X.REQUEST_MODE,
  X.REQUEST_STATUS,
  X.SEQ_NO,
  X.SK_ACCOUNT,
  X.SK_ACCOUNT_FIRST_CHECK_NO,
  X.SK_CHECKER_ID,
  X.SK_FIRST_CHECK_NO,
  X.SK_MAKER_ID,
  X.TRN_REF_NO,
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
    WHEN Y.ACCOUNT IS NULL AND Y.FIRST_CHECK_NO IS NULL
    THEN 'INS' /* - IF MULTIPLE PKs are there then concatenate them with AND */
    WHEN X.ACCOUNT = Y.ACCOUNT
    AND X.FIRST_CHECK_NO = Y.FIRST_CHECK_NO /* - IF MULTIPLE PKs are there then concatenate them with AND */
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
          X.BM_REQUEST_STATUS IS NULL AND NOT Y.BM_REQUEST_STATUS IS NULL
        )
        OR (
          NOT X.BM_REQUEST_STATUS IS NULL AND Y.BM_REQUEST_STATUS IS NULL
        )
        OR (
          X.BM_REQUEST_STATUS <> Y.BM_REQUEST_STATUS
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
          X.CHECK_LEAVES IS NULL AND NOT Y.CHECK_LEAVES IS NULL
        )
        OR (
          NOT X.CHECK_LEAVES IS NULL AND Y.CHECK_LEAVES IS NULL
        )
        OR (
          X.CHECK_LEAVES <> Y.CHECK_LEAVES
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
          X.CHEQUE_BOOK_TYPE IS NULL AND NOT Y.CHEQUE_BOOK_TYPE IS NULL
        )
        OR (
          NOT X.CHEQUE_BOOK_TYPE IS NULL AND Y.CHEQUE_BOOK_TYPE IS NULL
        )
        OR (
          X.CHEQUE_BOOK_TYPE <> Y.CHEQUE_BOOK_TYPE
        )
      )
      OR (
        (
          X.CHQ_TYPE IS NULL AND NOT Y.CHQ_TYPE IS NULL
        )
        OR (
          NOT X.CHQ_TYPE IS NULL AND Y.CHQ_TYPE IS NULL
        )
        OR (
          X.CHQ_TYPE <> Y.CHQ_TYPE
        )
      )
      OR (
        (
          X.CHQBOOK_DELIVERD IS NULL AND NOT Y.CHQBOOK_DELIVERD IS NULL
        )
        OR (
          NOT X.CHQBOOK_DELIVERD IS NULL AND Y.CHQBOOK_DELIVERD IS NULL
        )
        OR (
          X.CHQBOOK_DELIVERD <> Y.CHQBOOK_DELIVERD
        )
      )
      OR (
        (
          X.DELIVERY_ADD1 IS NULL AND NOT Y.DELIVERY_ADD1 IS NULL
        )
        OR (
          NOT X.DELIVERY_ADD1 IS NULL AND Y.DELIVERY_ADD1 IS NULL
        )
        OR (
          X.DELIVERY_ADD1 <> Y.DELIVERY_ADD1
        )
      )
      OR (
        (
          X.DELIVERY_ADD2 IS NULL AND NOT Y.DELIVERY_ADD2 IS NULL
        )
        OR (
          NOT X.DELIVERY_ADD2 IS NULL AND Y.DELIVERY_ADD2 IS NULL
        )
        OR (
          X.DELIVERY_ADD2 <> Y.DELIVERY_ADD2
        )
      )
      OR (
        (
          X.DELIVERY_ADD3 IS NULL AND NOT Y.DELIVERY_ADD3 IS NULL
        )
        OR (
          NOT X.DELIVERY_ADD3 IS NULL AND Y.DELIVERY_ADD3 IS NULL
        )
        OR (
          X.DELIVERY_ADD3 <> Y.DELIVERY_ADD3
        )
      )
      OR (
        (
          X.DELIVERY_ADD4 IS NULL AND NOT Y.DELIVERY_ADD4 IS NULL
        )
        OR (
          NOT X.DELIVERY_ADD4 IS NULL AND Y.DELIVERY_ADD4 IS NULL
        )
        OR (
          X.DELIVERY_ADD4 <> Y.DELIVERY_ADD4
        )
      )
      OR (
        (
          X.DELIVERY_DATE IS NULL AND NOT Y.DELIVERY_DATE IS NULL
        )
        OR (
          NOT X.DELIVERY_DATE IS NULL AND Y.DELIVERY_DATE IS NULL
        )
        OR (
          X.DELIVERY_DATE <> Y.DELIVERY_DATE
        )
      )
      OR (
        (
          X.DELIVERY_MODE IS NULL AND NOT Y.DELIVERY_MODE IS NULL
        )
        OR (
          NOT X.DELIVERY_MODE IS NULL AND Y.DELIVERY_MODE IS NULL
        )
        OR (
          X.DELIVERY_MODE <> Y.DELIVERY_MODE
        )
      )
      OR (
        (
          X.DELIVERY_REF_NO IS NULL AND NOT Y.DELIVERY_REF_NO IS NULL
        )
        OR (
          NOT X.DELIVERY_REF_NO IS NULL AND Y.DELIVERY_REF_NO IS NULL
        )
        OR (
          X.DELIVERY_REF_NO <> Y.DELIVERY_REF_NO
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
          X.INCL_FOR_CHKBOOK_PRINTING IS NULL AND NOT Y.INCL_FOR_CHKBOOK_PRINTING IS NULL
        )
        OR (
          NOT X.INCL_FOR_CHKBOOK_PRINTING IS NULL AND Y.INCL_FOR_CHKBOOK_PRINTING IS NULL
        )
        OR (
          X.INCL_FOR_CHKBOOK_PRINTING <> Y.INCL_FOR_CHKBOOK_PRINTING
        )
      )
      OR (
        (
          X.ISSUE_DATE IS NULL AND NOT Y.ISSUE_DATE IS NULL
        )
        OR (
          NOT X.ISSUE_DATE IS NULL AND Y.ISSUE_DATE IS NULL
        )
        OR (
          X.ISSUE_DATE <> Y.ISSUE_DATE
        )
      )
      OR (
        (
          X.LANGUAGE_CODE IS NULL AND NOT Y.LANGUAGE_CODE IS NULL
        )
        OR (
          NOT X.LANGUAGE_CODE IS NULL AND Y.LANGUAGE_CODE IS NULL
        )
        OR (
          X.LANGUAGE_CODE <> Y.LANGUAGE_CODE
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
          X.ORDER_DATE IS NULL AND NOT Y.ORDER_DATE IS NULL
        )
        OR (
          NOT X.ORDER_DATE IS NULL AND Y.ORDER_DATE IS NULL
        )
        OR (
          X.ORDER_DATE <> Y.ORDER_DATE
        )
      )
      OR (
        (
          X.ORDER_DETAILS IS NULL AND NOT Y.ORDER_DETAILS IS NULL
        )
        OR (
          NOT X.ORDER_DETAILS IS NULL AND Y.ORDER_DETAILS IS NULL
        )
        OR (
          X.ORDER_DETAILS <> Y.ORDER_DETAILS
        )
      )
      OR (
        (
          X.PRINT_STATUS IS NULL AND NOT Y.PRINT_STATUS IS NULL
        )
        OR (
          NOT X.PRINT_STATUS IS NULL AND Y.PRINT_STATUS IS NULL
        )
        OR (
          X.PRINT_STATUS <> Y.PRINT_STATUS
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
          X.REQUEST_MODE IS NULL AND NOT Y.REQUEST_MODE IS NULL
        )
        OR (
          NOT X.REQUEST_MODE IS NULL AND Y.REQUEST_MODE IS NULL
        )
        OR (
          X.REQUEST_MODE <> Y.REQUEST_MODE
        )
      )
      OR (
        (
          X.REQUEST_STATUS IS NULL AND NOT Y.REQUEST_STATUS IS NULL
        )
        OR (
          NOT X.REQUEST_STATUS IS NULL AND Y.REQUEST_STATUS IS NULL
        )
        OR (
          X.REQUEST_STATUS <> Y.REQUEST_STATUS
        )
      )
      OR (
        (
          X.SEQ_NO IS NULL AND NOT Y.SEQ_NO IS NULL
        )
        OR (
          NOT X.SEQ_NO IS NULL AND Y.SEQ_NO IS NULL
        )
        OR (
          X.SEQ_NO <> Y.SEQ_NO
        )
      )
      OR (
        (
          X.SK_ACCOUNT IS NULL AND NOT Y.SK_ACCOUNT IS NULL
        )
        OR (
          NOT X.SK_ACCOUNT IS NULL AND Y.SK_ACCOUNT IS NULL
        )
        OR (
          X.SK_ACCOUNT <> Y.SK_ACCOUNT
        )
      )
      OR (
        (
          X.SK_ACCOUNT_FIRST_CHECK_NO IS NULL AND NOT Y.SK_ACCOUNT_FIRST_CHECK_NO IS NULL
        )
        OR (
          NOT X.SK_ACCOUNT_FIRST_CHECK_NO IS NULL AND Y.SK_ACCOUNT_FIRST_CHECK_NO IS NULL
        )
        OR (
          X.SK_ACCOUNT_FIRST_CHECK_NO <> Y.SK_ACCOUNT_FIRST_CHECK_NO
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
          X.SK_FIRST_CHECK_NO IS NULL AND NOT Y.SK_FIRST_CHECK_NO IS NULL
        )
        OR (
          NOT X.SK_FIRST_CHECK_NO IS NULL AND Y.SK_FIRST_CHECK_NO IS NULL
        )
        OR (
          X.SK_FIRST_CHECK_NO <> Y.SK_FIRST_CHECK_NO
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
      OR (
        (
          X.TRN_REF_NO IS NULL AND NOT Y.TRN_REF_NO IS NULL
        )
        OR (
          NOT X.TRN_REF_NO IS NULL AND Y.TRN_REF_NO IS NULL
        )
        OR (
          X.TRN_REF_NO <> Y.TRN_REF_NO
        )
      )
    )
    THEN 'UPD'
    WHEN X.ACCOUNT IS NULL AND X.FIRST_CHECK_NO IS NULL
    THEN 'DEL' /* - IF MULTIPLE PKs are there then concatenate them with AND */
  END AS OPT_FLAG
FROM DEVT_STG_FLX.CATM_CHECK_BOOK AS X
FULL JOIN DEVT_STG_FLX.CATM_CHECK_BOOK_BACKUP AS Y
  ON X.ACCOUNT = Y.ACCOUNT
  AND X.FIRST_CHECK_NO = Y.FIRST_CHECK_NO /* - IF MULTIPLE PKs are there then concatenate them with AND */
WHERE
  NOT OPT_FLAG IS NULL