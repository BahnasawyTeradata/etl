CREATE OR REPLACE VIEW DEVV_STG_FLX.CATM_CHECK_DETAILS_get_delta AS LOCKING ROW FOR ACCESS
SELECT
  COALESCE(X.ACCOUNT, Y.ACCOUNT) AS ACCOUNT,
  COALESCE(X.BRANCH, Y.BRANCH) AS BRANCH,
  COALESCE(X.CHECK_NO, Y.CHECK_NO) AS CHECK_NO,
  COALESCE(X.MOD_NO, Y.MOD_NO) AS MOD_NO,
  X.AMOUNT,
  X.AUTH_STAT,
  X.BENEFICIARY,
  X.BM_AUTH_STAT,
  X.BM_RECORD_STAT,
  X.BM_STATUS,
  X.CHECK_BOOK_NO,
  X.CHECK_DIGIT,
  X.CHECKER_DT_STAMP,
  X.CHECKER_ID,
  X.MAKER_DT_STAMP,
  X.MAKER_ID,
  X.ONCE_AUTH,
  X.PRESENTATION_DATE,
  X.RECORD_STAT,
  X.REJECT_CODE,
  X.REMARKS,
  X.SK_ACCOUNT,
  X.SK_ACCOUNT_CHECK_BOOK_NO,
  X.SK_ACCOUNT_CHECK_NO_MOD_NO,
  X.SK_ACCOUNT_CHECK_NO_STATUS,
  X.SK_BRANCH,
  X.SK_CHECK_NO,
  X.SK_BENEFICIARY,
  X.STATUS,
  X.UPDATE_MODE,
  X.VALUE_DATE,
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
    WHEN Y.ACCOUNT IS NULL AND Y.BRANCH IS NULL AND Y.CHECK_NO IS NULL AND Y.MOD_NO IS NULL
    THEN 'INS' /* - IF MULTIPLE PKs are there then concatenate them with AND */
    WHEN X.ACCOUNT = Y.ACCOUNT
    AND X.BRANCH = Y.BRANCH
    AND X.CHECK_NO = Y.CHECK_NO
    AND X.MOD_NO = Y.MOD_NO /* - IF MULTIPLE PKs are there then concatenate them with AND */
    AND (
      (
        (
          X.AMOUNT IS NULL AND NOT Y.AMOUNT IS NULL
        )
        OR (
          NOT X.AMOUNT IS NULL AND Y.AMOUNT IS NULL
        )
        OR (
          X.AMOUNT <> Y.AMOUNT
        )
      )
      OR (
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
          X.BENEFICIARY IS NULL AND NOT Y.BENEFICIARY IS NULL
        )
        OR (
          NOT X.BENEFICIARY IS NULL AND Y.BENEFICIARY IS NULL
        )
        OR (
          X.BENEFICIARY <> Y.BENEFICIARY
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
          X.BM_STATUS IS NULL AND NOT Y.BM_STATUS IS NULL
        )
        OR (
          NOT X.BM_STATUS IS NULL AND Y.BM_STATUS IS NULL
        )
        OR (
          X.BM_STATUS <> Y.BM_STATUS
        )
      )
      OR (
        (
          X.CHECK_BOOK_NO IS NULL AND NOT Y.CHECK_BOOK_NO IS NULL
        )
        OR (
          NOT X.CHECK_BOOK_NO IS NULL AND Y.CHECK_BOOK_NO IS NULL
        )
        OR (
          X.CHECK_BOOK_NO <> Y.CHECK_BOOK_NO
        )
      )
      OR (
        (
          X.CHECK_DIGIT IS NULL AND NOT Y.CHECK_DIGIT IS NULL
        )
        OR (
          NOT X.CHECK_DIGIT IS NULL AND Y.CHECK_DIGIT IS NULL
        )
        OR (
          X.CHECK_DIGIT <> Y.CHECK_DIGIT
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
          X.PRESENTATION_DATE IS NULL AND NOT Y.PRESENTATION_DATE IS NULL
        )
        OR (
          NOT X.PRESENTATION_DATE IS NULL AND Y.PRESENTATION_DATE IS NULL
        )
        OR (
          X.PRESENTATION_DATE <> Y.PRESENTATION_DATE
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
          X.REJECT_CODE IS NULL AND NOT Y.REJECT_CODE IS NULL
        )
        OR (
          NOT X.REJECT_CODE IS NULL AND Y.REJECT_CODE IS NULL
        )
        OR (
          X.REJECT_CODE <> Y.REJECT_CODE
        )
      )
      OR (
        (
          X.REMARKS IS NULL AND NOT Y.REMARKS IS NULL
        )
        OR (
          NOT X.REMARKS IS NULL AND Y.REMARKS IS NULL
        )
        OR (
          X.REMARKS <> Y.REMARKS
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
          X.SK_ACCOUNT_CHECK_BOOK_NO IS NULL AND NOT Y.SK_ACCOUNT_CHECK_BOOK_NO IS NULL
        )
        OR (
          NOT X.SK_ACCOUNT_CHECK_BOOK_NO IS NULL AND Y.SK_ACCOUNT_CHECK_BOOK_NO IS NULL
        )
        OR (
          X.SK_ACCOUNT_CHECK_BOOK_NO <> Y.SK_ACCOUNT_CHECK_BOOK_NO
        )
      )
      OR (
        (
          X.SK_ACCOUNT_CHECK_NO_MOD_NO IS NULL AND NOT Y.SK_ACCOUNT_CHECK_NO_MOD_NO IS NULL
        )
        OR (
          NOT X.SK_ACCOUNT_CHECK_NO_MOD_NO IS NULL AND Y.SK_ACCOUNT_CHECK_NO_MOD_NO IS NULL
        )
        OR (
          X.SK_ACCOUNT_CHECK_NO_MOD_NO <> Y.SK_ACCOUNT_CHECK_NO_MOD_NO
        )
      )
      OR (
        (
          X.SK_ACCOUNT_CHECK_NO_STATUS IS NULL AND NOT Y.SK_ACCOUNT_CHECK_NO_STATUS IS NULL
        )
        OR (
          NOT X.SK_ACCOUNT_CHECK_NO_STATUS IS NULL AND Y.SK_ACCOUNT_CHECK_NO_STATUS IS NULL
        )
        OR (
          X.SK_ACCOUNT_CHECK_NO_STATUS <> Y.SK_ACCOUNT_CHECK_NO_STATUS
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
          X.SK_CHECK_NO IS NULL AND NOT Y.SK_CHECK_NO IS NULL
        )
        OR (
          NOT X.SK_CHECK_NO IS NULL AND Y.SK_CHECK_NO IS NULL
        )
        OR (
          X.SK_CHECK_NO <> Y.SK_CHECK_NO
        )
      )
      OR (
        (
          X.SK_BENEFICIARY IS NULL AND NOT Y.SK_BENEFICIARY IS NULL
        )
        OR (
          NOT X.SK_BENEFICIARY IS NULL AND Y.SK_BENEFICIARY IS NULL
        )
        OR (
          X.SK_BENEFICIARY <> Y.SK_BENEFICIARY
        )
      )
      OR (
        (
          X.STATUS IS NULL AND NOT Y.STATUS IS NULL
        )
        OR (
          NOT X.STATUS IS NULL AND Y.STATUS IS NULL
        )
        OR (
          X.STATUS <> Y.STATUS
        )
      )
      OR (
        (
          X.UPDATE_MODE IS NULL AND NOT Y.UPDATE_MODE IS NULL
        )
        OR (
          NOT X.UPDATE_MODE IS NULL AND Y.UPDATE_MODE IS NULL
        )
        OR (
          X.UPDATE_MODE <> Y.UPDATE_MODE
        )
      )
      OR (
        (
          X.VALUE_DATE IS NULL AND NOT Y.VALUE_DATE IS NULL
        )
        OR (
          NOT X.VALUE_DATE IS NULL AND Y.VALUE_DATE IS NULL
        )
        OR (
          X.VALUE_DATE <> Y.VALUE_DATE
        )
      )
    )
    THEN 'UPD'
    WHEN X.ACCOUNT IS NULL AND X.BRANCH IS NULL AND X.CHECK_NO IS NULL AND X.MOD_NO IS NULL
    THEN 'DEL' /* - IF MULTIPLE PKs are there then concatenate them with AND */
  END AS OPT_FLAG
FROM DEVT_STG_FLX.CATM_CHECK_DETAILS AS X
FULL JOIN DEVT_STG_FLX.CATM_CHECK_DETAILS_BACKUP AS Y
  ON X.ACCOUNT = Y.ACCOUNT
  AND X.BRANCH = Y.BRANCH
  AND X.CHECK_NO = Y.CHECK_NO
  AND X.MOD_NO = Y.MOD_NO /* - IF MULTIPLE PKs are there then concatenate them with AND */
WHERE
  NOT OPT_FLAG IS NULL