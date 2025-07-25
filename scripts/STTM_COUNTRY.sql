CREATE OR REPLACE VIEW DEVV_STG_FLX.STTM_COUNTRY_get_delta AS LOCKING ROW FOR ACCESS
SELECT
  COALESCE(X.COUNTRY_CODE, Y.COUNTRY_CODE) AS COUNTRY_CODE,
  X.ALT_COUNTRY_CODE,
  X.AUTH_STAT,
  X.BLACKLISTED,
  X.BM_COUNTRY_CODE,
  X.BM_COUNTRY_CODE_NATIONALITY,
  X.BM_COUNTRY_CODE_NATIONALITY_CLS,
  X.CHECKER_DT_STAMP,
  X.CHECKER_ID,
  X.CLEARING_NETWORK,
  X.CLR_CODE_BIC,
  X.DESCRIPTION,
  X.GEN_MT205,
  X.IBAN_CHECK_REQD,
  X.INTRA_EUROPEAN,
  X.ISD_CODE,
  X.ISO_NUM_COUNTRY_CODE,
  X.LIMIT_CCY,
  X.MAKER_DT_STAMP,
  X.MAKER_ID,
  X.MOD_NO,
  X.ONCE_AUTH,
  X.OVERALL_LIMIT,
  X.RECORD_STAT,
  X.REGION_CODE,
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
    WHEN Y.COUNTRY_CODE IS NULL
    THEN 'INS' /* - IF MULTIPLE PKs are there then concatenate them with AND */
    WHEN X.COUNTRY_CODE = Y.COUNTRY_CODE /* - IF MULTIPLE PKs are there then concatenate them with AND */
    AND (
      (
        (
          X.ALT_COUNTRY_CODE IS NULL AND NOT Y.ALT_COUNTRY_CODE IS NULL
        )
        OR (
          NOT X.ALT_COUNTRY_CODE IS NULL AND Y.ALT_COUNTRY_CODE IS NULL
        )
        OR (
          X.ALT_COUNTRY_CODE <> Y.ALT_COUNTRY_CODE
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
          X.BLACKLISTED IS NULL AND NOT Y.BLACKLISTED IS NULL
        )
        OR (
          NOT X.BLACKLISTED IS NULL AND Y.BLACKLISTED IS NULL
        )
        OR (
          X.BLACKLISTED <> Y.BLACKLISTED
        )
      )
      OR (
        (
          X.BM_COUNTRY_CODE IS NULL AND NOT Y.BM_COUNTRY_CODE IS NULL
        )
        OR (
          NOT X.BM_COUNTRY_CODE IS NULL AND Y.BM_COUNTRY_CODE IS NULL
        )
        OR (
          X.BM_COUNTRY_CODE <> Y.BM_COUNTRY_CODE
        )
      )
      OR (
        (
          X.BM_COUNTRY_CODE_NATIONALITY IS NULL
          AND NOT Y.BM_COUNTRY_CODE_NATIONALITY IS NULL
        )
        OR (
          NOT X.BM_COUNTRY_CODE_NATIONALITY IS NULL
          AND Y.BM_COUNTRY_CODE_NATIONALITY IS NULL
        )
        OR (
          X.BM_COUNTRY_CODE_NATIONALITY <> Y.BM_COUNTRY_CODE_NATIONALITY
        )
      )
      OR (
        (
          X.BM_COUNTRY_CODE_NATIONALITY_CLS IS NULL
          AND NOT Y.BM_COUNTRY_CODE_NATIONALITY_CLS IS NULL
        )
        OR (
          NOT X.BM_COUNTRY_CODE_NATIONALITY_CLS IS NULL
          AND Y.BM_COUNTRY_CODE_NATIONALITY_CLS IS NULL
        )
        OR (
          X.BM_COUNTRY_CODE_NATIONALITY_CLS <> Y.BM_COUNTRY_CODE_NATIONALITY_CLS
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
          X.CLEARING_NETWORK IS NULL AND NOT Y.CLEARING_NETWORK IS NULL
        )
        OR (
          NOT X.CLEARING_NETWORK IS NULL AND Y.CLEARING_NETWORK IS NULL
        )
        OR (
          X.CLEARING_NETWORK <> Y.CLEARING_NETWORK
        )
      )
      OR (
        (
          X.CLR_CODE_BIC IS NULL AND NOT Y.CLR_CODE_BIC IS NULL
        )
        OR (
          NOT X.CLR_CODE_BIC IS NULL AND Y.CLR_CODE_BIC IS NULL
        )
        OR (
          X.CLR_CODE_BIC <> Y.CLR_CODE_BIC
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
          X.GEN_MT205 IS NULL AND NOT Y.GEN_MT205 IS NULL
        )
        OR (
          NOT X.GEN_MT205 IS NULL AND Y.GEN_MT205 IS NULL
        )
        OR (
          X.GEN_MT205 <> Y.GEN_MT205
        )
      )
      OR (
        (
          X.IBAN_CHECK_REQD IS NULL AND NOT Y.IBAN_CHECK_REQD IS NULL
        )
        OR (
          NOT X.IBAN_CHECK_REQD IS NULL AND Y.IBAN_CHECK_REQD IS NULL
        )
        OR (
          X.IBAN_CHECK_REQD <> Y.IBAN_CHECK_REQD
        )
      )
      OR (
        (
          X.INTRA_EUROPEAN IS NULL AND NOT Y.INTRA_EUROPEAN IS NULL
        )
        OR (
          NOT X.INTRA_EUROPEAN IS NULL AND Y.INTRA_EUROPEAN IS NULL
        )
        OR (
          X.INTRA_EUROPEAN <> Y.INTRA_EUROPEAN
        )
      )
      OR (
        (
          X.ISD_CODE IS NULL AND NOT Y.ISD_CODE IS NULL
        )
        OR (
          NOT X.ISD_CODE IS NULL AND Y.ISD_CODE IS NULL
        )
        OR (
          X.ISD_CODE <> Y.ISD_CODE
        )
      )
      OR (
        (
          X.ISO_NUM_COUNTRY_CODE IS NULL AND NOT Y.ISO_NUM_COUNTRY_CODE IS NULL
        )
        OR (
          NOT X.ISO_NUM_COUNTRY_CODE IS NULL AND Y.ISO_NUM_COUNTRY_CODE IS NULL
        )
        OR (
          X.ISO_NUM_COUNTRY_CODE <> Y.ISO_NUM_COUNTRY_CODE
        )
      )
      OR (
        (
          X.LIMIT_CCY IS NULL AND NOT Y.LIMIT_CCY IS NULL
        )
        OR (
          NOT X.LIMIT_CCY IS NULL AND Y.LIMIT_CCY IS NULL
        )
        OR (
          X.LIMIT_CCY <> Y.LIMIT_CCY
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
          X.OVERALL_LIMIT IS NULL AND NOT Y.OVERALL_LIMIT IS NULL
        )
        OR (
          NOT X.OVERALL_LIMIT IS NULL AND Y.OVERALL_LIMIT IS NULL
        )
        OR (
          X.OVERALL_LIMIT <> Y.OVERALL_LIMIT
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
          X.REGION_CODE IS NULL AND NOT Y.REGION_CODE IS NULL
        )
        OR (
          NOT X.REGION_CODE IS NULL AND Y.REGION_CODE IS NULL
        )
        OR (
          X.REGION_CODE <> Y.REGION_CODE
        )
      )
    )
    THEN 'UPD'
    WHEN X.COUNTRY_CODE IS NULL
    THEN 'DEL' /* - IF MULTIPLE PKs are there then concatenate them with AND */
  END AS OPT_FLAG
FROM DEVT_STG_FLX.STTM_COUNTRY AS X
FULL JOIN DEVT_STG_FLX.STTM_COUNTRY_BACKUP AS Y
  ON X.COUNTRY_CODE = Y.COUNTRY_CODE /* - IF MULTIPLE PKs are there then concatenate them with AND */
WHERE
  NOT OPT_FLAG IS NULL