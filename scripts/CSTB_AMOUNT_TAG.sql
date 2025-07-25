CREATE OR REPLACE VIEW DEVV_STG_FLX.CSTB_AMOUNT_TAG_get_delta AS LOCKING ROW FOR ACCESS
SELECT
  COALESCE(X.AMOUNT_TAG, Y.AMOUNT_TAG) AS AMOUNT_TAG,
  COALESCE(X.LANGUAGE_CODE, Y.LANGUAGE_CODE) AS LANGUAGE_CODE,
  COALESCE(X.MODULE, Y.MODULE) AS MODULE,
  X.ACCT_ENTRY_DEFN_ALLOWED,
  X.AMOUNT_TAG_TYPE,
  X.BM_MODULE_AMOUNT_TAG,
  X.CHARGE_ALLOWED,
  X.COMMISSION_ALLOWED,
  X.DESCRIPTION,
  X.INTEREST_ALLOWED,
  X.ISSR_TAX_ALLOWED,
  X.LCY_AVG_EQL_REQD,
  X.OFFSET_AMOUNT_TAG,
  X.TAG_TO_BE_AVG_EQL_WITH,
  X.TAX_ALLOWED,
  X.TRACK_PAYABLE,
  X.TRACK_RECEIVABLE,
  X.TRACK_RECEIVABLES,
  X.TRAN_TAX_ALLOWED,
  X.UNREALISED,
  X.USER_DEFINED,
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
    WHEN Y.AMOUNT_TAG IS NULL AND Y.LANGUAGE_CODE IS NULL AND Y.MODULE IS NULL
    THEN 'INS' /* - IF MULTIPLE PKs are there then concatenate them with AND */
    WHEN X.AMOUNT_TAG = Y.AMOUNT_TAG
    AND X.LANGUAGE_CODE = Y.LANGUAGE_CODE
    AND X.MODULE = Y.MODULE /* - IF MULTIPLE PKs are there then concatenate them with AND */
    AND (
      (
        (
          X.ACCT_ENTRY_DEFN_ALLOWED IS NULL AND NOT Y.ACCT_ENTRY_DEFN_ALLOWED IS NULL
        )
        OR (
          NOT X.ACCT_ENTRY_DEFN_ALLOWED IS NULL AND Y.ACCT_ENTRY_DEFN_ALLOWED IS NULL
        )
        OR (
          X.ACCT_ENTRY_DEFN_ALLOWED <> Y.ACCT_ENTRY_DEFN_ALLOWED
        )
      )
      OR (
        (
          X.AMOUNT_TAG_TYPE IS NULL AND NOT Y.AMOUNT_TAG_TYPE IS NULL
        )
        OR (
          NOT X.AMOUNT_TAG_TYPE IS NULL AND Y.AMOUNT_TAG_TYPE IS NULL
        )
        OR (
          X.AMOUNT_TAG_TYPE <> Y.AMOUNT_TAG_TYPE
        )
      )
      OR (
        (
          X.BM_MODULE_AMOUNT_TAG IS NULL AND NOT Y.BM_MODULE_AMOUNT_TAG IS NULL
        )
        OR (
          NOT X.BM_MODULE_AMOUNT_TAG IS NULL AND Y.BM_MODULE_AMOUNT_TAG IS NULL
        )
        OR (
          X.BM_MODULE_AMOUNT_TAG <> Y.BM_MODULE_AMOUNT_TAG
        )
      )
      OR (
        (
          X.CHARGE_ALLOWED IS NULL AND NOT Y.CHARGE_ALLOWED IS NULL
        )
        OR (
          NOT X.CHARGE_ALLOWED IS NULL AND Y.CHARGE_ALLOWED IS NULL
        )
        OR (
          X.CHARGE_ALLOWED <> Y.CHARGE_ALLOWED
        )
      )
      OR (
        (
          X.COMMISSION_ALLOWED IS NULL AND NOT Y.COMMISSION_ALLOWED IS NULL
        )
        OR (
          NOT X.COMMISSION_ALLOWED IS NULL AND Y.COMMISSION_ALLOWED IS NULL
        )
        OR (
          X.COMMISSION_ALLOWED <> Y.COMMISSION_ALLOWED
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
          X.INTEREST_ALLOWED IS NULL AND NOT Y.INTEREST_ALLOWED IS NULL
        )
        OR (
          NOT X.INTEREST_ALLOWED IS NULL AND Y.INTEREST_ALLOWED IS NULL
        )
        OR (
          X.INTEREST_ALLOWED <> Y.INTEREST_ALLOWED
        )
      )
      OR (
        (
          X.ISSR_TAX_ALLOWED IS NULL AND NOT Y.ISSR_TAX_ALLOWED IS NULL
        )
        OR (
          NOT X.ISSR_TAX_ALLOWED IS NULL AND Y.ISSR_TAX_ALLOWED IS NULL
        )
        OR (
          X.ISSR_TAX_ALLOWED <> Y.ISSR_TAX_ALLOWED
        )
      )
      OR (
        (
          X.LCY_AVG_EQL_REQD IS NULL AND NOT Y.LCY_AVG_EQL_REQD IS NULL
        )
        OR (
          NOT X.LCY_AVG_EQL_REQD IS NULL AND Y.LCY_AVG_EQL_REQD IS NULL
        )
        OR (
          X.LCY_AVG_EQL_REQD <> Y.LCY_AVG_EQL_REQD
        )
      )
      OR (
        (
          X.OFFSET_AMOUNT_TAG IS NULL AND NOT Y.OFFSET_AMOUNT_TAG IS NULL
        )
        OR (
          NOT X.OFFSET_AMOUNT_TAG IS NULL AND Y.OFFSET_AMOUNT_TAG IS NULL
        )
        OR (
          X.OFFSET_AMOUNT_TAG <> Y.OFFSET_AMOUNT_TAG
        )
      )
      OR (
        (
          X.TAG_TO_BE_AVG_EQL_WITH IS NULL AND NOT Y.TAG_TO_BE_AVG_EQL_WITH IS NULL
        )
        OR (
          NOT X.TAG_TO_BE_AVG_EQL_WITH IS NULL AND Y.TAG_TO_BE_AVG_EQL_WITH IS NULL
        )
        OR (
          X.TAG_TO_BE_AVG_EQL_WITH <> Y.TAG_TO_BE_AVG_EQL_WITH
        )
      )
      OR (
        (
          X.TAX_ALLOWED IS NULL AND NOT Y.TAX_ALLOWED IS NULL
        )
        OR (
          NOT X.TAX_ALLOWED IS NULL AND Y.TAX_ALLOWED IS NULL
        )
        OR (
          X.TAX_ALLOWED <> Y.TAX_ALLOWED
        )
      )
      OR (
        (
          X.TRACK_PAYABLE IS NULL AND NOT Y.TRACK_PAYABLE IS NULL
        )
        OR (
          NOT X.TRACK_PAYABLE IS NULL AND Y.TRACK_PAYABLE IS NULL
        )
        OR (
          X.TRACK_PAYABLE <> Y.TRACK_PAYABLE
        )
      )
      OR (
        (
          X.TRACK_RECEIVABLE IS NULL AND NOT Y.TRACK_RECEIVABLE IS NULL
        )
        OR (
          NOT X.TRACK_RECEIVABLE IS NULL AND Y.TRACK_RECEIVABLE IS NULL
        )
        OR (
          X.TRACK_RECEIVABLE <> Y.TRACK_RECEIVABLE
        )
      )
      OR (
        (
          X.TRACK_RECEIVABLES IS NULL AND NOT Y.TRACK_RECEIVABLES IS NULL
        )
        OR (
          NOT X.TRACK_RECEIVABLES IS NULL AND Y.TRACK_RECEIVABLES IS NULL
        )
        OR (
          X.TRACK_RECEIVABLES <> Y.TRACK_RECEIVABLES
        )
      )
      OR (
        (
          X.TRAN_TAX_ALLOWED IS NULL AND NOT Y.TRAN_TAX_ALLOWED IS NULL
        )
        OR (
          NOT X.TRAN_TAX_ALLOWED IS NULL AND Y.TRAN_TAX_ALLOWED IS NULL
        )
        OR (
          X.TRAN_TAX_ALLOWED <> Y.TRAN_TAX_ALLOWED
        )
      )
      OR (
        (
          X.UNREALISED IS NULL AND NOT Y.UNREALISED IS NULL
        )
        OR (
          NOT X.UNREALISED IS NULL AND Y.UNREALISED IS NULL
        )
        OR (
          X.UNREALISED <> Y.UNREALISED
        )
      )
      OR (
        (
          X.USER_DEFINED IS NULL AND NOT Y.USER_DEFINED IS NULL
        )
        OR (
          NOT X.USER_DEFINED IS NULL AND Y.USER_DEFINED IS NULL
        )
        OR (
          X.USER_DEFINED <> Y.USER_DEFINED
        )
      )
    )
    THEN 'UPD'
    WHEN X.AMOUNT_TAG IS NULL AND X.LANGUAGE_CODE IS NULL AND X.MODULE IS NULL
    THEN 'DEL' /* - IF MULTIPLE PKs are there then concatenate them with AND */
  END AS OPT_FLAG
FROM DEVT_STG_FLX.CSTB_AMOUNT_TAG AS X
FULL JOIN DEVT_STG_FLX.CSTB_AMOUNT_TAG_BACKUP AS Y
  ON X.AMOUNT_TAG = Y.AMOUNT_TAG
  AND X.LANGUAGE_CODE = Y.LANGUAGE_CODE
  AND X.MODULE = Y.MODULE /* - IF MULTIPLE PKs are there then concatenate them with AND */
WHERE
  NOT OPT_FLAG IS NULL