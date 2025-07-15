CREATE OR REPLACE VIEW DEVV_STG_FLX.STTM_TRN_CODE_get_delta AS LOCKING ROW FOR ACCESS
SELECT
  COALESCE(X.TRN_CODE, Y.TRN_CODE) AS TRN_CODE,
  X.ACUMEN_TRN_CODE,
  X.AML_MONITORING,
  X.AUTH_STAT,
  X.AVAIL_BAL_REQD,
  X.AVL_DAYS,
  X.AVL_INFO,
  X.BAL_UPDATE_THRU_PPC,
  X.BM_TRN_CODE,
  X.BM_TRN_CODE_EV,
  X.BM_TRN_CODE_FUND,
  X.CHECKER_DT_STAMP,
  X.CHECKER_ID,
  X.CHEQUE_MANDATORY,
  X.COMPONENT_TYPE,
  X.CONSIDER_COVER_ACC,
  X.CONSIDER_FOR_ACTIVITY,
  X.ESCROW_PROCESSING,
  X.EXEMPT_ADV_INTEREST,
  X.IB_IN_LCY,
  X.IC_BAL_INCLUSION,
  X.IC_PENALTY,
  X.IC_TOVER_INCLUSION,
  X.IC_TXN_COUNT,
  X.IC_TXN_COUNT_NUMBER,
  X.IGNORE_LM_BVT_PROCESSING,
  X.INTRADAY_RELEASE,
  X.LEAVE_SALARY_PROCESSING,
  X.MAKER_DT_STAMP,
  X.MAKER_ID,
  X.MIS_HEAD,
  X.MOD_NO,
  X.NEW_VAL_DATE,
  X.ONCE_AUTH,
  X.PRODUCT_CAT,
  X.RECORD_STAT,
  X.SALARY_CREDIT,
  X.STMT_DT_BASIS,
  X.TRN_DESC,
  X.TRN_SWIFT_CODE,
  X.TRNOVER_LMT_INCLUSION,
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
    WHEN Y.TRN_CODE IS NULL
    THEN 'INS' /* - IF MULTIPLE PKs are there then concatenate them with AND */
    WHEN X.TRN_CODE = Y.TRN_CODE /* - IF MULTIPLE PKs are there then concatenate them with AND */
    AND (
      (
        (
          X.ACUMEN_TRN_CODE IS NULL AND NOT Y.ACUMEN_TRN_CODE IS NULL
        )
        OR (
          NOT X.ACUMEN_TRN_CODE IS NULL AND Y.ACUMEN_TRN_CODE IS NULL
        )
        OR (
          X.ACUMEN_TRN_CODE <> Y.ACUMEN_TRN_CODE
        )
      )
      OR (
        (
          X.AML_MONITORING IS NULL AND NOT Y.AML_MONITORING IS NULL
        )
        OR (
          NOT X.AML_MONITORING IS NULL AND Y.AML_MONITORING IS NULL
        )
        OR (
          X.AML_MONITORING <> Y.AML_MONITORING
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
          X.AVAIL_BAL_REQD IS NULL AND NOT Y.AVAIL_BAL_REQD IS NULL
        )
        OR (
          NOT X.AVAIL_BAL_REQD IS NULL AND Y.AVAIL_BAL_REQD IS NULL
        )
        OR (
          X.AVAIL_BAL_REQD <> Y.AVAIL_BAL_REQD
        )
      )
      OR (
        (
          X.AVL_DAYS IS NULL AND NOT Y.AVL_DAYS IS NULL
        )
        OR (
          NOT X.AVL_DAYS IS NULL AND Y.AVL_DAYS IS NULL
        )
        OR (
          X.AVL_DAYS <> Y.AVL_DAYS
        )
      )
      OR (
        (
          X.AVL_INFO IS NULL AND NOT Y.AVL_INFO IS NULL
        )
        OR (
          NOT X.AVL_INFO IS NULL AND Y.AVL_INFO IS NULL
        )
        OR (
          X.AVL_INFO <> Y.AVL_INFO
        )
      )
      OR (
        (
          X.BAL_UPDATE_THRU_PPC IS NULL AND NOT Y.BAL_UPDATE_THRU_PPC IS NULL
        )
        OR (
          NOT X.BAL_UPDATE_THRU_PPC IS NULL AND Y.BAL_UPDATE_THRU_PPC IS NULL
        )
        OR (
          X.BAL_UPDATE_THRU_PPC <> Y.BAL_UPDATE_THRU_PPC
        )
      )
      OR (
        (
          X.BM_TRN_CODE IS NULL AND NOT Y.BM_TRN_CODE IS NULL
        )
        OR (
          NOT X.BM_TRN_CODE IS NULL AND Y.BM_TRN_CODE IS NULL
        )
        OR (
          X.BM_TRN_CODE <> Y.BM_TRN_CODE
        )
      )
      OR (
        (
          X.BM_TRN_CODE_EV IS NULL AND NOT Y.BM_TRN_CODE_EV IS NULL
        )
        OR (
          NOT X.BM_TRN_CODE_EV IS NULL AND Y.BM_TRN_CODE_EV IS NULL
        )
        OR (
          X.BM_TRN_CODE_EV <> Y.BM_TRN_CODE_EV
        )
      )
      OR (
        (
          X.BM_TRN_CODE_FUND IS NULL AND NOT Y.BM_TRN_CODE_FUND IS NULL
        )
        OR (
          NOT X.BM_TRN_CODE_FUND IS NULL AND Y.BM_TRN_CODE_FUND IS NULL
        )
        OR (
          X.BM_TRN_CODE_FUND <> Y.BM_TRN_CODE_FUND
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
          X.CHEQUE_MANDATORY IS NULL AND NOT Y.CHEQUE_MANDATORY IS NULL
        )
        OR (
          NOT X.CHEQUE_MANDATORY IS NULL AND Y.CHEQUE_MANDATORY IS NULL
        )
        OR (
          X.CHEQUE_MANDATORY <> Y.CHEQUE_MANDATORY
        )
      )
      OR (
        (
          X.COMPONENT_TYPE IS NULL AND NOT Y.COMPONENT_TYPE IS NULL
        )
        OR (
          NOT X.COMPONENT_TYPE IS NULL AND Y.COMPONENT_TYPE IS NULL
        )
        OR (
          X.COMPONENT_TYPE <> Y.COMPONENT_TYPE
        )
      )
      OR (
        (
          X.CONSIDER_COVER_ACC IS NULL AND NOT Y.CONSIDER_COVER_ACC IS NULL
        )
        OR (
          NOT X.CONSIDER_COVER_ACC IS NULL AND Y.CONSIDER_COVER_ACC IS NULL
        )
        OR (
          X.CONSIDER_COVER_ACC <> Y.CONSIDER_COVER_ACC
        )
      )
      OR (
        (
          X.CONSIDER_FOR_ACTIVITY IS NULL AND NOT Y.CONSIDER_FOR_ACTIVITY IS NULL
        )
        OR (
          NOT X.CONSIDER_FOR_ACTIVITY IS NULL AND Y.CONSIDER_FOR_ACTIVITY IS NULL
        )
        OR (
          X.CONSIDER_FOR_ACTIVITY <> Y.CONSIDER_FOR_ACTIVITY
        )
      )
      OR (
        (
          X.ESCROW_PROCESSING IS NULL AND NOT Y.ESCROW_PROCESSING IS NULL
        )
        OR (
          NOT X.ESCROW_PROCESSING IS NULL AND Y.ESCROW_PROCESSING IS NULL
        )
        OR (
          X.ESCROW_PROCESSING <> Y.ESCROW_PROCESSING
        )
      )
      OR (
        (
          X.EXEMPT_ADV_INTEREST IS NULL AND NOT Y.EXEMPT_ADV_INTEREST IS NULL
        )
        OR (
          NOT X.EXEMPT_ADV_INTEREST IS NULL AND Y.EXEMPT_ADV_INTEREST IS NULL
        )
        OR (
          X.EXEMPT_ADV_INTEREST <> Y.EXEMPT_ADV_INTEREST
        )
      )
      OR (
        (
          X.IB_IN_LCY IS NULL AND NOT Y.IB_IN_LCY IS NULL
        )
        OR (
          NOT X.IB_IN_LCY IS NULL AND Y.IB_IN_LCY IS NULL
        )
        OR (
          X.IB_IN_LCY <> Y.IB_IN_LCY
        )
      )
      OR (
        (
          X.IC_BAL_INCLUSION IS NULL AND NOT Y.IC_BAL_INCLUSION IS NULL
        )
        OR (
          NOT X.IC_BAL_INCLUSION IS NULL AND Y.IC_BAL_INCLUSION IS NULL
        )
        OR (
          X.IC_BAL_INCLUSION <> Y.IC_BAL_INCLUSION
        )
      )
      OR (
        (
          X.IC_PENALTY IS NULL AND NOT Y.IC_PENALTY IS NULL
        )
        OR (
          NOT X.IC_PENALTY IS NULL AND Y.IC_PENALTY IS NULL
        )
        OR (
          X.IC_PENALTY <> Y.IC_PENALTY
        )
      )
      OR (
        (
          X.IC_TOVER_INCLUSION IS NULL AND NOT Y.IC_TOVER_INCLUSION IS NULL
        )
        OR (
          NOT X.IC_TOVER_INCLUSION IS NULL AND Y.IC_TOVER_INCLUSION IS NULL
        )
        OR (
          X.IC_TOVER_INCLUSION <> Y.IC_TOVER_INCLUSION
        )
      )
      OR (
        (
          X.IC_TXN_COUNT IS NULL AND NOT Y.IC_TXN_COUNT IS NULL
        )
        OR (
          NOT X.IC_TXN_COUNT IS NULL AND Y.IC_TXN_COUNT IS NULL
        )
        OR (
          X.IC_TXN_COUNT <> Y.IC_TXN_COUNT
        )
      )
      OR (
        (
          X.IC_TXN_COUNT_NUMBER IS NULL AND NOT Y.IC_TXN_COUNT_NUMBER IS NULL
        )
        OR (
          NOT X.IC_TXN_COUNT_NUMBER IS NULL AND Y.IC_TXN_COUNT_NUMBER IS NULL
        )
        OR (
          X.IC_TXN_COUNT_NUMBER <> Y.IC_TXN_COUNT_NUMBER
        )
      )
      OR (
        (
          X.IGNORE_LM_BVT_PROCESSING IS NULL AND NOT Y.IGNORE_LM_BVT_PROCESSING IS NULL
        )
        OR (
          NOT X.IGNORE_LM_BVT_PROCESSING IS NULL AND Y.IGNORE_LM_BVT_PROCESSING IS NULL
        )
        OR (
          X.IGNORE_LM_BVT_PROCESSING <> Y.IGNORE_LM_BVT_PROCESSING
        )
      )
      OR (
        (
          X.INTRADAY_RELEASE IS NULL AND NOT Y.INTRADAY_RELEASE IS NULL
        )
        OR (
          NOT X.INTRADAY_RELEASE IS NULL AND Y.INTRADAY_RELEASE IS NULL
        )
        OR (
          X.INTRADAY_RELEASE <> Y.INTRADAY_RELEASE
        )
      )
      OR (
        (
          X.LEAVE_SALARY_PROCESSING IS NULL AND NOT Y.LEAVE_SALARY_PROCESSING IS NULL
        )
        OR (
          NOT X.LEAVE_SALARY_PROCESSING IS NULL AND Y.LEAVE_SALARY_PROCESSING IS NULL
        )
        OR (
          X.LEAVE_SALARY_PROCESSING <> Y.LEAVE_SALARY_PROCESSING
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
          X.MIS_HEAD IS NULL AND NOT Y.MIS_HEAD IS NULL
        )
        OR (
          NOT X.MIS_HEAD IS NULL AND Y.MIS_HEAD IS NULL
        )
        OR (
          X.MIS_HEAD <> Y.MIS_HEAD
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
          X.NEW_VAL_DATE IS NULL AND NOT Y.NEW_VAL_DATE IS NULL
        )
        OR (
          NOT X.NEW_VAL_DATE IS NULL AND Y.NEW_VAL_DATE IS NULL
        )
        OR (
          X.NEW_VAL_DATE <> Y.NEW_VAL_DATE
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
          X.PRODUCT_CAT IS NULL AND NOT Y.PRODUCT_CAT IS NULL
        )
        OR (
          NOT X.PRODUCT_CAT IS NULL AND Y.PRODUCT_CAT IS NULL
        )
        OR (
          X.PRODUCT_CAT <> Y.PRODUCT_CAT
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
          X.SALARY_CREDIT IS NULL AND NOT Y.SALARY_CREDIT IS NULL
        )
        OR (
          NOT X.SALARY_CREDIT IS NULL AND Y.SALARY_CREDIT IS NULL
        )
        OR (
          X.SALARY_CREDIT <> Y.SALARY_CREDIT
        )
      )
      OR (
        (
          X.STMT_DT_BASIS IS NULL AND NOT Y.STMT_DT_BASIS IS NULL
        )
        OR (
          NOT X.STMT_DT_BASIS IS NULL AND Y.STMT_DT_BASIS IS NULL
        )
        OR (
          X.STMT_DT_BASIS <> Y.STMT_DT_BASIS
        )
      )
      OR (
        (
          X.TRN_DESC IS NULL AND NOT Y.TRN_DESC IS NULL
        )
        OR (
          NOT X.TRN_DESC IS NULL AND Y.TRN_DESC IS NULL
        )
        OR (
          X.TRN_DESC <> Y.TRN_DESC
        )
      )
      OR (
        (
          X.TRN_SWIFT_CODE IS NULL AND NOT Y.TRN_SWIFT_CODE IS NULL
        )
        OR (
          NOT X.TRN_SWIFT_CODE IS NULL AND Y.TRN_SWIFT_CODE IS NULL
        )
        OR (
          X.TRN_SWIFT_CODE <> Y.TRN_SWIFT_CODE
        )
      )
      OR (
        (
          X.TRNOVER_LMT_INCLUSION IS NULL AND NOT Y.TRNOVER_LMT_INCLUSION IS NULL
        )
        OR (
          NOT X.TRNOVER_LMT_INCLUSION IS NULL AND Y.TRNOVER_LMT_INCLUSION IS NULL
        )
        OR (
          X.TRNOVER_LMT_INCLUSION <> Y.TRNOVER_LMT_INCLUSION
        )
      )
    )
    THEN 'UPD'
    WHEN X.TRN_CODE IS NULL
    THEN 'DEL' /* - IF MULTIPLE PKs are there then concatenate them with AND */
  END AS OPT_FLAG
FROM DEVT_STG_FLX.STTM_TRN_CODE AS X
FULL JOIN DEVT_STG_FLX.STTM_TRN_CODE_BACKUP AS Y
  ON X.TRN_CODE = Y.TRN_CODE /* - IF MULTIPLE PKs are there then concatenate them with AND */
WHERE
  NOT OPT_FLAG IS NULL