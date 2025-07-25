CREATE OR REPLACE VIEW DEVV_STG_FLX.CATM_AMOUNT_BLOCKS_get_delta AS LOCKING ROW FOR ACCESS
SELECT
  COALESCE(X.AMOUNT_BLOCK_NO, Y.AMOUNT_BLOCK_NO) AS AMOUNT_BLOCK_NO,
  X.ACCOUNT,
  X.AMOUNT,
  X.AMOUNT_BLOCK_TYPE,
  X.AUTH_STAT,
  X.BENEFICIARY_EMAIL_ID,
  X.BENEFICIARY_FACEBOOK_ID,
  X.BENEFICIARY_TELEPHONE,
  X.BM_AMOUNT_BLOCK_TYPE,
  X.BM_AUTH_STAT,
  X.BM_HOLD_CODE,
  X.BRANCH,
  X.CHECKER_DT_STAMP,
  X.CHECKER_ID,
  X.EFFECTIVE_DATE,
  X.EVENT_SEQ_NO,
  X.EXPIRY_DATE,
  X.HOLD_CODE,
  X.MAKER_DT_STAMP,
  X.MAKER_ID,
  X.MOD_NO,
  X.ONCE_AUTH,
  X.RECORD_STAT,
  X.REFERENCE_NO,
  X.RELATED_REF_NO,
  X.REMARKS,
  X.SK_ACCOUNT,
  X.SK_AMOUNT_BLOCK_NO,
  X.SK_BRANCH,
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
    WHEN Y.AMOUNT_BLOCK_NO IS NULL
    THEN 'INS' /* - IF MULTIPLE PKs are there then concatenate them with AND */
    WHEN X.AMOUNT_BLOCK_NO = Y.AMOUNT_BLOCK_NO /* - IF MULTIPLE PKs are there then concatenate them with AND */
    AND (
      (
        (
          X.ACCOUNT IS NULL AND NOT Y.ACCOUNT IS NULL
        )
        OR (
          NOT X.ACCOUNT IS NULL AND Y.ACCOUNT IS NULL
        )
        OR (
          X.ACCOUNT <> Y.ACCOUNT
        )
      )
      OR (
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
          X.AMOUNT_BLOCK_TYPE IS NULL AND NOT Y.AMOUNT_BLOCK_TYPE IS NULL
        )
        OR (
          NOT X.AMOUNT_BLOCK_TYPE IS NULL AND Y.AMOUNT_BLOCK_TYPE IS NULL
        )
        OR (
          X.AMOUNT_BLOCK_TYPE <> Y.AMOUNT_BLOCK_TYPE
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
          X.BENEFICIARY_EMAIL_ID IS NULL AND NOT Y.BENEFICIARY_EMAIL_ID IS NULL
        )
        OR (
          NOT X.BENEFICIARY_EMAIL_ID IS NULL AND Y.BENEFICIARY_EMAIL_ID IS NULL
        )
        OR (
          X.BENEFICIARY_EMAIL_ID <> Y.BENEFICIARY_EMAIL_ID
        )
      )
      OR (
        (
          X.BENEFICIARY_FACEBOOK_ID IS NULL AND NOT Y.BENEFICIARY_FACEBOOK_ID IS NULL
        )
        OR (
          NOT X.BENEFICIARY_FACEBOOK_ID IS NULL AND Y.BENEFICIARY_FACEBOOK_ID IS NULL
        )
        OR (
          X.BENEFICIARY_FACEBOOK_ID <> Y.BENEFICIARY_FACEBOOK_ID
        )
      )
      OR (
        (
          X.BENEFICIARY_TELEPHONE IS NULL AND NOT Y.BENEFICIARY_TELEPHONE IS NULL
        )
        OR (
          NOT X.BENEFICIARY_TELEPHONE IS NULL AND Y.BENEFICIARY_TELEPHONE IS NULL
        )
        OR (
          X.BENEFICIARY_TELEPHONE <> Y.BENEFICIARY_TELEPHONE
        )
      )
      OR (
        (
          X.BM_AMOUNT_BLOCK_TYPE IS NULL AND NOT Y.BM_AMOUNT_BLOCK_TYPE IS NULL
        )
        OR (
          NOT X.BM_AMOUNT_BLOCK_TYPE IS NULL AND Y.BM_AMOUNT_BLOCK_TYPE IS NULL
        )
        OR (
          X.BM_AMOUNT_BLOCK_TYPE <> Y.BM_AMOUNT_BLOCK_TYPE
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
          X.BM_HOLD_CODE IS NULL AND NOT Y.BM_HOLD_CODE IS NULL
        )
        OR (
          NOT X.BM_HOLD_CODE IS NULL AND Y.BM_HOLD_CODE IS NULL
        )
        OR (
          X.BM_HOLD_CODE <> Y.BM_HOLD_CODE
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
          X.EFFECTIVE_DATE IS NULL AND NOT Y.EFFECTIVE_DATE IS NULL
        )
        OR (
          NOT X.EFFECTIVE_DATE IS NULL AND Y.EFFECTIVE_DATE IS NULL
        )
        OR (
          X.EFFECTIVE_DATE <> Y.EFFECTIVE_DATE
        )
      )
      OR (
        (
          X.EVENT_SEQ_NO IS NULL AND NOT Y.EVENT_SEQ_NO IS NULL
        )
        OR (
          NOT X.EVENT_SEQ_NO IS NULL AND Y.EVENT_SEQ_NO IS NULL
        )
        OR (
          X.EVENT_SEQ_NO <> Y.EVENT_SEQ_NO
        )
      )
      OR (
        (
          X.EXPIRY_DATE IS NULL AND NOT Y.EXPIRY_DATE IS NULL
        )
        OR (
          NOT X.EXPIRY_DATE IS NULL AND Y.EXPIRY_DATE IS NULL
        )
        OR (
          X.EXPIRY_DATE <> Y.EXPIRY_DATE
        )
      )
      OR (
        (
          X.HOLD_CODE IS NULL AND NOT Y.HOLD_CODE IS NULL
        )
        OR (
          NOT X.HOLD_CODE IS NULL AND Y.HOLD_CODE IS NULL
        )
        OR (
          X.HOLD_CODE <> Y.HOLD_CODE
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
          X.REFERENCE_NO IS NULL AND NOT Y.REFERENCE_NO IS NULL
        )
        OR (
          NOT X.REFERENCE_NO IS NULL AND Y.REFERENCE_NO IS NULL
        )
        OR (
          X.REFERENCE_NO <> Y.REFERENCE_NO
        )
      )
      OR (
        (
          X.RELATED_REF_NO IS NULL AND NOT Y.RELATED_REF_NO IS NULL
        )
        OR (
          NOT X.RELATED_REF_NO IS NULL AND Y.RELATED_REF_NO IS NULL
        )
        OR (
          X.RELATED_REF_NO <> Y.RELATED_REF_NO
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
          X.SK_AMOUNT_BLOCK_NO IS NULL AND NOT Y.SK_AMOUNT_BLOCK_NO IS NULL
        )
        OR (
          NOT X.SK_AMOUNT_BLOCK_NO IS NULL AND Y.SK_AMOUNT_BLOCK_NO IS NULL
        )
        OR (
          X.SK_AMOUNT_BLOCK_NO <> Y.SK_AMOUNT_BLOCK_NO
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
    WHEN X.AMOUNT_BLOCK_NO IS NULL
    THEN 'DEL' /* - IF MULTIPLE PKs are there then concatenate them with AND */
  END AS OPT_FLAG
FROM DEVT_STG_FLX.CATM_AMOUNT_BLOCKS AS X
FULL JOIN DEVT_STG_FLX.CATM_AMOUNT_BLOCKS_BACKUP AS Y
  ON X.AMOUNT_BLOCK_NO = Y.AMOUNT_BLOCK_NO /* - IF MULTIPLE PKs are there then concatenate them with AND */
WHERE
  NOT OPT_FLAG IS NULL