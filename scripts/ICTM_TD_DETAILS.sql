CREATE OR REPLACE VIEW DEVV_STG_FLX.ICTM_TD_DETAILS_get_delta AS LOCKING ROW FOR ACCESS
SELECT
  COALESCE(X.ACC, Y.ACC) AS ACC,
  COALESCE(X.BRN, Y.BRN) AS BRN,
  COALESCE(X.CCY, Y.CCY) AS CCY,
  X.BM_DUPLICATE_ISSUE,
  X.CERTIFICATE_NO,
  X.DUPLICATE_ISSUE,
  X.LIQ_AMT,
  X.OFFSET_BRN,
  X.PAYIN_OPTION,
  X.PROJECTED_INT_TILL_MAT,
  X.REDEM_AMT,
  X.REFERENCE_NO,
  X.SK_ACC,
  X.SK_CERTIFICATE_NO,
  X.STOCK_CATLOG_CD,
  X.TD_AMOUNT,
  X.TD_OFFSET_ACC,
  X.TOPUP_AMT,
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
    WHEN Y.ACC IS NULL AND Y.BRN IS NULL AND Y.CCY IS NULL
    THEN 'INS' /* - IF MULTIPLE PKs are there then concatenate them with AND */
    WHEN X.ACC = Y.ACC
    AND X.BRN = Y.BRN
    AND X.CCY = Y.CCY /* - IF MULTIPLE PKs are there then concatenate them with AND */
    AND (
      (
        (
          X.BM_DUPLICATE_ISSUE IS NULL AND NOT Y.BM_DUPLICATE_ISSUE IS NULL
        )
        OR (
          NOT X.BM_DUPLICATE_ISSUE IS NULL AND Y.BM_DUPLICATE_ISSUE IS NULL
        )
        OR (
          X.BM_DUPLICATE_ISSUE <> Y.BM_DUPLICATE_ISSUE
        )
      )
      OR (
        (
          X.CERTIFICATE_NO IS NULL AND NOT Y.CERTIFICATE_NO IS NULL
        )
        OR (
          NOT X.CERTIFICATE_NO IS NULL AND Y.CERTIFICATE_NO IS NULL
        )
        OR (
          X.CERTIFICATE_NO <> Y.CERTIFICATE_NO
        )
      )
      OR (
        (
          X.DUPLICATE_ISSUE IS NULL AND NOT Y.DUPLICATE_ISSUE IS NULL
        )
        OR (
          NOT X.DUPLICATE_ISSUE IS NULL AND Y.DUPLICATE_ISSUE IS NULL
        )
        OR (
          X.DUPLICATE_ISSUE <> Y.DUPLICATE_ISSUE
        )
      )
      OR (
        (
          X.LIQ_AMT IS NULL AND NOT Y.LIQ_AMT IS NULL
        )
        OR (
          NOT X.LIQ_AMT IS NULL AND Y.LIQ_AMT IS NULL
        )
        OR (
          X.LIQ_AMT <> Y.LIQ_AMT
        )
      )
      OR (
        (
          X.OFFSET_BRN IS NULL AND NOT Y.OFFSET_BRN IS NULL
        )
        OR (
          NOT X.OFFSET_BRN IS NULL AND Y.OFFSET_BRN IS NULL
        )
        OR (
          X.OFFSET_BRN <> Y.OFFSET_BRN
        )
      )
      OR (
        (
          X.PAYIN_OPTION IS NULL AND NOT Y.PAYIN_OPTION IS NULL
        )
        OR (
          NOT X.PAYIN_OPTION IS NULL AND Y.PAYIN_OPTION IS NULL
        )
        OR (
          X.PAYIN_OPTION <> Y.PAYIN_OPTION
        )
      )
      OR (
        (
          X.PROJECTED_INT_TILL_MAT IS NULL AND NOT Y.PROJECTED_INT_TILL_MAT IS NULL
        )
        OR (
          NOT X.PROJECTED_INT_TILL_MAT IS NULL AND Y.PROJECTED_INT_TILL_MAT IS NULL
        )
        OR (
          X.PROJECTED_INT_TILL_MAT <> Y.PROJECTED_INT_TILL_MAT
        )
      )
      OR (
        (
          X.REDEM_AMT IS NULL AND NOT Y.REDEM_AMT IS NULL
        )
        OR (
          NOT X.REDEM_AMT IS NULL AND Y.REDEM_AMT IS NULL
        )
        OR (
          X.REDEM_AMT <> Y.REDEM_AMT
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
          X.SK_CERTIFICATE_NO IS NULL AND NOT Y.SK_CERTIFICATE_NO IS NULL
        )
        OR (
          NOT X.SK_CERTIFICATE_NO IS NULL AND Y.SK_CERTIFICATE_NO IS NULL
        )
        OR (
          X.SK_CERTIFICATE_NO <> Y.SK_CERTIFICATE_NO
        )
      )
      OR (
        (
          X.STOCK_CATLOG_CD IS NULL AND NOT Y.STOCK_CATLOG_CD IS NULL
        )
        OR (
          NOT X.STOCK_CATLOG_CD IS NULL AND Y.STOCK_CATLOG_CD IS NULL
        )
        OR (
          X.STOCK_CATLOG_CD <> Y.STOCK_CATLOG_CD
        )
      )
      OR (
        (
          X.TD_AMOUNT IS NULL AND NOT Y.TD_AMOUNT IS NULL
        )
        OR (
          NOT X.TD_AMOUNT IS NULL AND Y.TD_AMOUNT IS NULL
        )
        OR (
          X.TD_AMOUNT <> Y.TD_AMOUNT
        )
      )
      OR (
        (
          X.TD_OFFSET_ACC IS NULL AND NOT Y.TD_OFFSET_ACC IS NULL
        )
        OR (
          NOT X.TD_OFFSET_ACC IS NULL AND Y.TD_OFFSET_ACC IS NULL
        )
        OR (
          X.TD_OFFSET_ACC <> Y.TD_OFFSET_ACC
        )
      )
      OR (
        (
          X.TOPUP_AMT IS NULL AND NOT Y.TOPUP_AMT IS NULL
        )
        OR (
          NOT X.TOPUP_AMT IS NULL AND Y.TOPUP_AMT IS NULL
        )
        OR (
          X.TOPUP_AMT <> Y.TOPUP_AMT
        )
      )
    )
    THEN 'UPD'
    WHEN X.ACC IS NULL AND X.BRN IS NULL AND X.CCY IS NULL
    THEN 'DEL' /* - IF MULTIPLE PKs are there then concatenate them with AND */
  END AS OPT_FLAG
FROM DEVT_STG_FLX.ICTM_TD_DETAILS AS X
FULL JOIN DEVT_STG_FLX.ICTM_TD_DETAILS_BACKUP AS Y
  ON X.ACC = Y.ACC
  AND X.BRN = Y.BRN
  AND X.CCY = Y.CCY /* - IF MULTIPLE PKs are there then concatenate them with AND */
WHERE
  NOT OPT_FLAG IS NULL