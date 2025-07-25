CREATE OR REPLACE VIEW DEVV_STG_FLX.CSTB_EVENT_get_delta AS LOCKING ROW FOR ACCESS
SELECT
  COALESCE(X.EVENT_CODE, Y.EVENT_CODE) AS EVENT_CODE,
  COALESCE(X.MODULE, Y.MODULE) AS MODULE,
  X.ACCT_ENTRY_DEFN,
  X.ADVICES_DEFN,
  X.ALLOW_APPLICATION_OF_CHARGE,
  X.ALLOW_APPLICATION_OF_TRAN_TAX,
  X.ALLOW_ASSOCIATION_OF_CHARGE,
  X.ALLOW_ASSOCIATION_OF_INTEREST,
  X.ALLOW_ASSOCIATION_OF_ISSR_TAX,
  X.ALLOW_ASSOCIATION_OF_TRAN_TAX,
  X.ALLOW_LIQUIDATION_OF_CHARGE,
  X.ALLOW_LIQUIDATION_OF_TRAN_TAX,
  X.AUTH_STAT,
  X.BM_EVENT_CODE_LC_AVAILMENT,
  X.BM_EVENT_CODE_LC_BC,
  X.BM_MODULE_EVENT_CODE,
  X.CHARGE_ALLOWED,
  X.CHECKER_DT_STAMP,
  X.CHECKER_ID,
  X.COMMISSION_ALLOWED,
  X.EVENT_DESCR,
  X.INTEREST_ALLOWED,
  X.MAKER_DT_STAMP,
  X.MAKER_ID,
  X.MOD_NO,
  X.OCCURRENCE_ONCE_IN_A_LIFETIME,
  X.ONCE_AUTH,
  X.PROPAGATION_REQD,
  X.RECORD_STAT,
  X.SYS_INITIATED,
  X.TAX_ALLOWED,
  X.UDE_ADVICES_DEFN,
  X.USER_DEFINED,
  X.USER_INITIATED,
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
    WHEN Y.EVENT_CODE IS NULL AND Y.MODULE IS NULL
    THEN 'INS' /* - IF MULTIPLE PKs are there then concatenate them with AND */
    WHEN X.EVENT_CODE = Y.EVENT_CODE
    AND X.MODULE = Y.MODULE /* - IF MULTIPLE PKs are there then concatenate them with AND */
    AND (
      (
        (
          X.ACCT_ENTRY_DEFN IS NULL AND NOT Y.ACCT_ENTRY_DEFN IS NULL
        )
        OR (
          NOT X.ACCT_ENTRY_DEFN IS NULL AND Y.ACCT_ENTRY_DEFN IS NULL
        )
        OR (
          X.ACCT_ENTRY_DEFN <> Y.ACCT_ENTRY_DEFN
        )
      )
      OR (
        (
          X.ADVICES_DEFN IS NULL AND NOT Y.ADVICES_DEFN IS NULL
        )
        OR (
          NOT X.ADVICES_DEFN IS NULL AND Y.ADVICES_DEFN IS NULL
        )
        OR (
          X.ADVICES_DEFN <> Y.ADVICES_DEFN
        )
      )
      OR (
        (
          X.ALLOW_APPLICATION_OF_CHARGE IS NULL
          AND NOT Y.ALLOW_APPLICATION_OF_CHARGE IS NULL
        )
        OR (
          NOT X.ALLOW_APPLICATION_OF_CHARGE IS NULL
          AND Y.ALLOW_APPLICATION_OF_CHARGE IS NULL
        )
        OR (
          X.ALLOW_APPLICATION_OF_CHARGE <> Y.ALLOW_APPLICATION_OF_CHARGE
        )
      )
      OR (
        (
          X.ALLOW_APPLICATION_OF_TRAN_TAX IS NULL
          AND NOT Y.ALLOW_APPLICATION_OF_TRAN_TAX IS NULL
        )
        OR (
          NOT X.ALLOW_APPLICATION_OF_TRAN_TAX IS NULL
          AND Y.ALLOW_APPLICATION_OF_TRAN_TAX IS NULL
        )
        OR (
          X.ALLOW_APPLICATION_OF_TRAN_TAX <> Y.ALLOW_APPLICATION_OF_TRAN_TAX
        )
      )
      OR (
        (
          X.ALLOW_ASSOCIATION_OF_CHARGE IS NULL
          AND NOT Y.ALLOW_ASSOCIATION_OF_CHARGE IS NULL
        )
        OR (
          NOT X.ALLOW_ASSOCIATION_OF_CHARGE IS NULL
          AND Y.ALLOW_ASSOCIATION_OF_CHARGE IS NULL
        )
        OR (
          X.ALLOW_ASSOCIATION_OF_CHARGE <> Y.ALLOW_ASSOCIATION_OF_CHARGE
        )
      )
      OR (
        (
          X.ALLOW_ASSOCIATION_OF_INTEREST IS NULL
          AND NOT Y.ALLOW_ASSOCIATION_OF_INTEREST IS NULL
        )
        OR (
          NOT X.ALLOW_ASSOCIATION_OF_INTEREST IS NULL
          AND Y.ALLOW_ASSOCIATION_OF_INTEREST IS NULL
        )
        OR (
          X.ALLOW_ASSOCIATION_OF_INTEREST <> Y.ALLOW_ASSOCIATION_OF_INTEREST
        )
      )
      OR (
        (
          X.ALLOW_ASSOCIATION_OF_ISSR_TAX IS NULL
          AND NOT Y.ALLOW_ASSOCIATION_OF_ISSR_TAX IS NULL
        )
        OR (
          NOT X.ALLOW_ASSOCIATION_OF_ISSR_TAX IS NULL
          AND Y.ALLOW_ASSOCIATION_OF_ISSR_TAX IS NULL
        )
        OR (
          X.ALLOW_ASSOCIATION_OF_ISSR_TAX <> Y.ALLOW_ASSOCIATION_OF_ISSR_TAX
        )
      )
      OR (
        (
          X.ALLOW_ASSOCIATION_OF_TRAN_TAX IS NULL
          AND NOT Y.ALLOW_ASSOCIATION_OF_TRAN_TAX IS NULL
        )
        OR (
          NOT X.ALLOW_ASSOCIATION_OF_TRAN_TAX IS NULL
          AND Y.ALLOW_ASSOCIATION_OF_TRAN_TAX IS NULL
        )
        OR (
          X.ALLOW_ASSOCIATION_OF_TRAN_TAX <> Y.ALLOW_ASSOCIATION_OF_TRAN_TAX
        )
      )
      OR (
        (
          X.ALLOW_LIQUIDATION_OF_CHARGE IS NULL
          AND NOT Y.ALLOW_LIQUIDATION_OF_CHARGE IS NULL
        )
        OR (
          NOT X.ALLOW_LIQUIDATION_OF_CHARGE IS NULL
          AND Y.ALLOW_LIQUIDATION_OF_CHARGE IS NULL
        )
        OR (
          X.ALLOW_LIQUIDATION_OF_CHARGE <> Y.ALLOW_LIQUIDATION_OF_CHARGE
        )
      )
      OR (
        (
          X.ALLOW_LIQUIDATION_OF_TRAN_TAX IS NULL
          AND NOT Y.ALLOW_LIQUIDATION_OF_TRAN_TAX IS NULL
        )
        OR (
          NOT X.ALLOW_LIQUIDATION_OF_TRAN_TAX IS NULL
          AND Y.ALLOW_LIQUIDATION_OF_TRAN_TAX IS NULL
        )
        OR (
          X.ALLOW_LIQUIDATION_OF_TRAN_TAX <> Y.ALLOW_LIQUIDATION_OF_TRAN_TAX
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
          X.BM_EVENT_CODE_LC_AVAILMENT IS NULL AND NOT Y.BM_EVENT_CODE_LC_AVAILMENT IS NULL
        )
        OR (
          NOT X.BM_EVENT_CODE_LC_AVAILMENT IS NULL AND Y.BM_EVENT_CODE_LC_AVAILMENT IS NULL
        )
        OR (
          X.BM_EVENT_CODE_LC_AVAILMENT <> Y.BM_EVENT_CODE_LC_AVAILMENT
        )
      )
      OR (
        (
          X.BM_EVENT_CODE_LC_BC IS NULL AND NOT Y.BM_EVENT_CODE_LC_BC IS NULL
        )
        OR (
          NOT X.BM_EVENT_CODE_LC_BC IS NULL AND Y.BM_EVENT_CODE_LC_BC IS NULL
        )
        OR (
          X.BM_EVENT_CODE_LC_BC <> Y.BM_EVENT_CODE_LC_BC
        )
      )
      OR (
        (
          X.BM_MODULE_EVENT_CODE IS NULL AND NOT Y.BM_MODULE_EVENT_CODE IS NULL
        )
        OR (
          NOT X.BM_MODULE_EVENT_CODE IS NULL AND Y.BM_MODULE_EVENT_CODE IS NULL
        )
        OR (
          X.BM_MODULE_EVENT_CODE <> Y.BM_MODULE_EVENT_CODE
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
          X.EVENT_DESCR IS NULL AND NOT Y.EVENT_DESCR IS NULL
        )
        OR (
          NOT X.EVENT_DESCR IS NULL AND Y.EVENT_DESCR IS NULL
        )
        OR (
          X.EVENT_DESCR <> Y.EVENT_DESCR
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
          X.OCCURRENCE_ONCE_IN_A_LIFETIME IS NULL
          AND NOT Y.OCCURRENCE_ONCE_IN_A_LIFETIME IS NULL
        )
        OR (
          NOT X.OCCURRENCE_ONCE_IN_A_LIFETIME IS NULL
          AND Y.OCCURRENCE_ONCE_IN_A_LIFETIME IS NULL
        )
        OR (
          X.OCCURRENCE_ONCE_IN_A_LIFETIME <> Y.OCCURRENCE_ONCE_IN_A_LIFETIME
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
          X.PROPAGATION_REQD IS NULL AND NOT Y.PROPAGATION_REQD IS NULL
        )
        OR (
          NOT X.PROPAGATION_REQD IS NULL AND Y.PROPAGATION_REQD IS NULL
        )
        OR (
          X.PROPAGATION_REQD <> Y.PROPAGATION_REQD
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
          X.SYS_INITIATED IS NULL AND NOT Y.SYS_INITIATED IS NULL
        )
        OR (
          NOT X.SYS_INITIATED IS NULL AND Y.SYS_INITIATED IS NULL
        )
        OR (
          X.SYS_INITIATED <> Y.SYS_INITIATED
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
          X.UDE_ADVICES_DEFN IS NULL AND NOT Y.UDE_ADVICES_DEFN IS NULL
        )
        OR (
          NOT X.UDE_ADVICES_DEFN IS NULL AND Y.UDE_ADVICES_DEFN IS NULL
        )
        OR (
          X.UDE_ADVICES_DEFN <> Y.UDE_ADVICES_DEFN
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
      OR (
        (
          X.USER_INITIATED IS NULL AND NOT Y.USER_INITIATED IS NULL
        )
        OR (
          NOT X.USER_INITIATED IS NULL AND Y.USER_INITIATED IS NULL
        )
        OR (
          X.USER_INITIATED <> Y.USER_INITIATED
        )
      )
    )
    THEN 'UPD'
    WHEN X.EVENT_CODE IS NULL AND X.MODULE IS NULL
    THEN 'DEL' /* - IF MULTIPLE PKs are there then concatenate them with AND */
  END AS OPT_FLAG
FROM DEVT_STG_FLX.CSTB_EVENT AS X
FULL JOIN DEVT_STG_FLX.CSTB_EVENT_BACKUP AS Y
  ON X.EVENT_CODE = Y.EVENT_CODE
  AND X.MODULE = Y.MODULE /* - IF MULTIPLE PKs are there then concatenate them with AND */
WHERE
  NOT OPT_FLAG IS NULL