CREATE OR REPLACE VIEW DEVV_STG_FLX.STTM_CUST_ACCOUNT_get_delta AS LOCKING ROW FOR ACCESS
SELECT
  COALESCE(X.BRANCH_CODE, Y.BRANCH_CODE) AS BRANCH_CODE,
  COALESCE(X.CUST_AC_NO, Y.CUST_AC_NO) AS CUST_AC_NO,
  X.AC_DESC,
  X.AC_OPEN_DATE,
  X.AC_SET_CLOSE,
  X.AC_SET_CLOSE_DATE,
  X.AC_STAT_BLOCK,
  X.AC_STAT_DE_POST,
  X.AC_STAT_DORMANT,
  X.AC_STAT_FROZEN,
  X.AC_STAT_NO_CR,
  X.AC_STAT_NO_DR,
  X.AC_STAT_STOP_PAY,
  X.AC_STMT_CYCLE,
  X.AC_STMT_CYCLE2,
  X.AC_STMT_CYCLE3,
  X.AC_STMT_DAY,
  X.AC_STMT_TYPE,
  X.ACC_STATUS,
  X.ACC_STMT_DAY2,
  X.ACC_STMT_DAY3,
  X.ACC_STMT_TYPE2,
  X.ACC_STMT_TYPE3,
  X.ACC_TANKED_STAT,
  X.ACCOUNT_AUTO_CLOSED,
  X.ACCOUNT_CLASS,
  X.ACCOUNT_DERIVED_STATUS,
  X.ACCOUNT_TYPE,
  X.ACY_ACCRUED_CR_IC,
  X.ACY_ACCRUED_DR_IC,
  X.ACY_AVL_BAL,
  X.ACY_BLOCKED_AMOUNT,
  X.ACY_CURR_BALANCE,
  X.ACY_MTD_TOVER_CR,
  X.ACY_MTD_TOVER_DR,
  X.ACY_OPENING_BAL,
  X.ACY_SWEEP_INELIGIBLE,
  X.ACY_TANK_CR,
  X.ACY_TANK_DR,
  X.ACY_TANK_UNCOLLECTED,
  X.ACY_TODAY_TOVER_CR,
  X.ACY_TODAY_TOVER_DR,
  X.ACY_TOVER_CR,
  X.ACY_UNAUTH_CR,
  X.ACY_UNAUTH_DR,
  X.ACY_UNAUTH_TANK_CR,
  X.ACY_UNAUTH_TANK_DR,
  X.ACY_UNAUTH_TANK_UNCOLLECTED,
  X.ACY_UNAUTH_UNCOLLECTED,
  X.ACY_UNCOLLECTED,
  X.ADDRESS1,
  X.ADDRESS2,
  X.ADDRESS3,
  X.ADDRESS4,
  X.ALLOW_BACK_PERIOD_ENTRY,
  X.ALT_AC_NO,
  X.ATM_CUST_AC_NO,
  X.ATM_DLY_AMT_LIMIT,
  X.ATM_DLY_COUNT_LIMIT,
  X.ATM_FACILITY,
  X.AUTH_STAT,
  X.AUTO_CHQBK_REQUEST,
  X.AUTO_CREATE_POOL,
  X.AUTO_DC_REQUEST,
  X.AUTO_DEPOSIT,
  X.AUTO_DEPOSITS_BAL,
  X.AUTO_PROV_REQD,
  X.AUTO_REORDER_CHECK_LEAVES,
  X.AUTO_REORDER_CHECK_LEVEL,
  X.AUTO_REORDER_CHECK_REQUIRED,
  X.BALANCE_REPORT_SINCE,
  X.BALANCE_REPORT_TYPE,
  X.BM_AC_STAT_BLOCK,
  X.BM_AC_STAT_DE_POST,
  X.BM_AC_STAT_DORMANT,
  X.BM_AC_STAT_FROZEN,
  X.BM_AC_STAT_NO_CR,
  X.BM_AC_STAT_NO_DR,
  X.BM_AC_STAT_STOP_PAY,
  X.BM_AC_STMT_CYCLE,
  X.BM_ACCOUNT_TYPE,
  X.BM_ACCOUNT_TYPE_FINANCIAL,
  X.BM_AUTH_STAT,
  X.BM_CCY,
  X.BM_JOINT_AC_INDICATOR,
  X.BM_LIMIT_CCY,
  X.BM_MEDIA,
  X.BM_MOD_NO,
  X.BM_ONCE_AUTH,
  X.BM_RECORD_STAT,
  X.BM_SALARY_ACCOUNT,
  X.CAS_ACCOUNT,
  X.CAS_CUSTOMER,
  X.CCY,
  X.CHECKBOOK_NAME_1,
  X.CHECKBOOK_NAME_2,
  X.CHECKER_DT_STAMP,
  X.CHECKER_ID,
  X.CHEQUE_BOOK_FACILITY,
  X.CHG_DUE,
  X.CLEARING_AC_NO,
  X.CLEARING_BANK_CODE,
  X.CONSOL_CHG_ACC,
  X.CONSOL_CHG_BRN,
  X.CONSOLIDATION_REQD,
  X.CONTRIBUTE_TO_PDM,
  X.COUNTRY_CODE,
  X.CR_AUTO_EX_RATE_LMT,
  X.CR_CB_LINE,
  X.CR_GL,
  X.CR_HO_LINE,
  X.CR_LM_REV_DATE,
  X.CR_LM_START_DATE,
  X.CREDIT_TXN_LIMIT,
  X.CRS_STAT_REQD,
  X.CUST_NO,
  X.DATE_LAST_CR,
  X.DATE_LAST_CR_ACTIVITY,
  X.DATE_LAST_DR,
  X.DATE_LAST_DR_ACTIVITY,
  X.DAYLIGHT_LIMIT_AMOUNT,
  X.DEFAULT_WAIVER,
  X.DEFER_RECON,
  X.DISPLAY_IBAN_IN_ADVICES,
  X.DORMANCY_DATE,
  X.DORMANCY_DAYS,
  X.DORMANT_PARAM,
  X.DR_AUTO_EX_RATE_LMT,
  X.DR_CB_LINE,
  X.DR_GL,
  X.DR_HO_LINE,
  X.DR_INT_DUE,
  X.ESCROW_AC_NO,
  X.ESCROW_BRANCH_CODE,
  X.ESCROW_PERCENTAGE,
  X.ESCROW_TRANSFER,
  X.EXCL_SAMEDAY_RVRTRNS_FM_STMT,
  X.EXCLUDE_FROM_DISTRIBUTION,
  X.EXPOSURE_CATEGORY,
  X.FUND_ID,
  X.FUNDING,
  X.FUNDING_ACCOUNT,
  X.FUNDING_BRANCH,
  X.GEN_BALANCE_REPORT,
  X.GEN_INTERIM_STMT,
  X.GEN_INTERIM_STMT_ON_MVMT,
  X.GEN_STMT_ONLY_ON_MVMT,
  X.GEN_STMT_ONLY_ON_MVMT2,
  X.GEN_STMT_ONLY_ON_MVMT3,
  X.HAS_TOV,
  X.IBAN_AC_NO,
  X.INF_ACC_OPENING_AMT,
  X.INF_OFFSET_ACCOUNT,
  X.INF_OFFSET_BRANCH,
  X.INF_PAY_IN_OPTION,
  X.INF_WAIVE_ACC_OPEN_CHARGE,
  X.INHERIT_REPORTING,
  X.INTERIM_CREDIT_AMT,
  X.INTERIM_DEBIT_AMT,
  X.INTERIM_REPORT_SINCE,
  X.INTERIM_REPORT_TYPE,
  X.INTERIM_STMT_DAY_COUNT,
  X.INTERIM_STMT_YTD_COUNT,
  X.INTERMEDIARY_REQUIRED,
  X.JOINT_AC_INDICATOR,
  X.LAST_CCY_CONV_DATE,
  X.LAST_PURGE_DATE,
  X.LATEST_RUNBAL_SUBMMITED,
  X.LATEST_SRNO_SUBMITTED,
  X.LCY_CURR_BALANCE,
  X.LCY_MTD_TOVER_CR,
  X.LCY_MTD_TOVER_DR,
  X.LCY_OPENING_BAL,
  X.LCY_TANK_CR,
  X.LCY_TANK_DR,
  X.LCY_TODAY_TOVER_CR,
  X.LCY_TODAY_TOVER_DR,
  X.LCY_TOVER_CR,
  X.LIMIT_CCY,
  X.LINE_ID,
  X.LINKED_DEP_ACC,
  X.LINKED_DEP_BRANCH,
  X.LOCATION,
  X.LODGEMENT_BOOK_FACILITY,
  X.MAKER_DT_STAMP,
  X.MAKER_ID,
  X.MASTER_ACCOUNT_NO,
  X.MAX_NO_CHEQUE_REJECTIONS,
  X.MEDIA,
  X.MIN_REQD_BAL,
  X.MOD_NO,
  X.MOD9_VALIDATION_REQD,
  X.MODE_OF_OPERATION,
  X.MT110_RECON_REQD,
  X.MT210_REQD,
  X.MUDARABAH_ACCOUNTS,
  X.SK_CUST_NO,
  X.NETTING_REQUIRED,
  X.NO_CHEQUE_REJECTIONS,
  X.NO_OF_CHQ_REJ_RESET_ON,
  X.NOMINEE1,
  X.NOMINEE2,
  X.NSF_AUTO_UPDATE,
  X.NSF_BLACKLIST_STATUS,
  X.OFFLINE_LIMIT,
  X.ONCE_AUTH,
  X.ORG_FUNC_ID,
  X.OVERDRAFT_SINCE,
  X.OVERLINE_OD_SINCE,
  X.PASSBOOK_FACILITY,
  X.PASSBOOK_NUMBER,
  X.PINCODE,
  X.POSITIVE_PAY_AC,
  X.PREV_AC_SRNO_PRINTED_IN_PBK,
  X.PREV_LINE_NO,
  X.PREV_OVD_DATE,
  X.PREV_PAGE_NO,
  X.PREV_RUNBAL_PRINTED_IN_PBK,
  X.PREV_TOD_SINCE,
  X.PREVIOUS_STATEMENT_BALANCE,
  X.PREVIOUS_STATEMENT_BALANCE2,
  X.PREVIOUS_STATEMENT_BALANCE3,
  X.PREVIOUS_STATEMENT_DATE,
  X.PREVIOUS_STATEMENT_DATE2,
  X.PREVIOUS_STATEMENT_DATE3,
  X.PREVIOUS_STATEMENT_NO,
  X.PREVIOUS_STATEMENT_NO2,
  X.PREVIOUS_STATEMENT_NO3,
  X.PRODUCT_LIST,
  X.PROJECT_ACCOUNT,
  X.PROV_CCY_TYPE,
  X.PROVISION_AMOUNT,
  X.RECEIVABLE_AMOUNT,
  X.RECORD_STAT,
  X.REFERRAL_REQUIRED,
  X.REG_CC_AVAILABILITY,
  X.REG_CC_AVAILABLE_FUNDS,
  X.REG_D_APPLICABLE,
  X.REGD_END_DATE,
  X.REGD_PERIODICITY,
  X.REGD_START_DATE,
  X.REPL_CUST_SIG,
  X.RISK_FREE_EXP_AMOUNT,
  X.SALARY_ACCOUNT,
  X.SK_ACCOUNT_CLASS,
  X.SK_ADDRESS1_ADDRESS2_ADDRESS3_ADDRESS4,
  X.SK_ATM_FACILITY,
  X.SK_BRANCH_CODE,
  X.SK_CHEQUE_BOOK_FACILITY,
  X.SK_CLEARING_AC_NO,
  X.SK_CUST_AC_NO,
  X.SK_CHECKER_ID,
  X.SK_MAKER_ID,
  X.SK_LOCATION,
  X.SK_MIN_REQD_BAL,
  X.SK_SWEEP_OUT,
  X.SK_SWEEP_REQUIRED,
  X.SK_SWEEP_TYPE,
  X.SOD_NOTIFICATION_PERCENT,
  X.SPECIAL_CONDITION_PRODUCT,
  X.SPECIAL_CONDITION_TXNCODE,
  X.SPEND_ANALYSIS_REQD,
  X.STALE_DAYS,
  X.STATEMENT_ACCOUNT,
  X.STATUS_CHANGE_AUTOMATIC,
  X.STATUS_SINCE,
  X.SUBLIMIT,
  X.SWEEP_OUT,
  X.SWEEP_REQUIRED,
  X.SWEEP_TYPE,
  X.TD_CERT_PRINTED,
  X.TOD_END_DATE,
  X.TOD_LIMIT,
  X.TOD_LIMIT_END_DATE,
  X.TOD_LIMIT_START_DATE,
  X.TOD_SINCE,
  X.TOD_START_DATE,
  X.TRACK_RECEIVABLE,
  X.TRNOVER_LMT_CODE,
  X.TXN_CODE_LIST,
  X.TYPE_OF_CHQ,
  X.UNCOLL_FUNDS_LIMIT,
  X.VALIDATION_DIGIT,
  X.WITHDRAWABLE_UNCOLLED_FUND,
  X.ZAKAT_EXEMPTION,
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
    WHEN Y.BRANCH_CODE IS NULL AND Y.CUST_AC_NO IS NULL
    THEN 'INS' /* - IF MULTIPLE PKs are there then concatenate them with AND */
    WHEN X.BRANCH_CODE = Y.BRANCH_CODE
    AND X.CUST_AC_NO = Y.CUST_AC_NO /* - IF MULTIPLE PKs are there then concatenate them with AND */
    AND (
      (
        (
          X.AC_DESC IS NULL AND NOT Y.AC_DESC IS NULL
        )
        OR (
          NOT X.AC_DESC IS NULL AND Y.AC_DESC IS NULL
        )
        OR (
          X.AC_DESC <> Y.AC_DESC
        )
      )
      OR (
        (
          X.AC_OPEN_DATE IS NULL AND NOT Y.AC_OPEN_DATE IS NULL
        )
        OR (
          NOT X.AC_OPEN_DATE IS NULL AND Y.AC_OPEN_DATE IS NULL
        )
        OR (
          X.AC_OPEN_DATE <> Y.AC_OPEN_DATE
        )
      )
      OR (
        (
          X.AC_SET_CLOSE IS NULL AND NOT Y.AC_SET_CLOSE IS NULL
        )
        OR (
          NOT X.AC_SET_CLOSE IS NULL AND Y.AC_SET_CLOSE IS NULL
        )
        OR (
          X.AC_SET_CLOSE <> Y.AC_SET_CLOSE
        )
      )
      OR (
        (
          X.AC_SET_CLOSE_DATE IS NULL AND NOT Y.AC_SET_CLOSE_DATE IS NULL
        )
        OR (
          NOT X.AC_SET_CLOSE_DATE IS NULL AND Y.AC_SET_CLOSE_DATE IS NULL
        )
        OR (
          X.AC_SET_CLOSE_DATE <> Y.AC_SET_CLOSE_DATE
        )
      )
      OR (
        (
          X.AC_STAT_BLOCK IS NULL AND NOT Y.AC_STAT_BLOCK IS NULL
        )
        OR (
          NOT X.AC_STAT_BLOCK IS NULL AND Y.AC_STAT_BLOCK IS NULL
        )
        OR (
          X.AC_STAT_BLOCK <> Y.AC_STAT_BLOCK
        )
      )
      OR (
        (
          X.AC_STAT_DE_POST IS NULL AND NOT Y.AC_STAT_DE_POST IS NULL
        )
        OR (
          NOT X.AC_STAT_DE_POST IS NULL AND Y.AC_STAT_DE_POST IS NULL
        )
        OR (
          X.AC_STAT_DE_POST <> Y.AC_STAT_DE_POST
        )
      )
      OR (
        (
          X.AC_STAT_DORMANT IS NULL AND NOT Y.AC_STAT_DORMANT IS NULL
        )
        OR (
          NOT X.AC_STAT_DORMANT IS NULL AND Y.AC_STAT_DORMANT IS NULL
        )
        OR (
          X.AC_STAT_DORMANT <> Y.AC_STAT_DORMANT
        )
      )
      OR (
        (
          X.AC_STAT_FROZEN IS NULL AND NOT Y.AC_STAT_FROZEN IS NULL
        )
        OR (
          NOT X.AC_STAT_FROZEN IS NULL AND Y.AC_STAT_FROZEN IS NULL
        )
        OR (
          X.AC_STAT_FROZEN <> Y.AC_STAT_FROZEN
        )
      )
      OR (
        (
          X.AC_STAT_NO_CR IS NULL AND NOT Y.AC_STAT_NO_CR IS NULL
        )
        OR (
          NOT X.AC_STAT_NO_CR IS NULL AND Y.AC_STAT_NO_CR IS NULL
        )
        OR (
          X.AC_STAT_NO_CR <> Y.AC_STAT_NO_CR
        )
      )
      OR (
        (
          X.AC_STAT_NO_DR IS NULL AND NOT Y.AC_STAT_NO_DR IS NULL
        )
        OR (
          NOT X.AC_STAT_NO_DR IS NULL AND Y.AC_STAT_NO_DR IS NULL
        )
        OR (
          X.AC_STAT_NO_DR <> Y.AC_STAT_NO_DR
        )
      )
      OR (
        (
          X.AC_STAT_STOP_PAY IS NULL AND NOT Y.AC_STAT_STOP_PAY IS NULL
        )
        OR (
          NOT X.AC_STAT_STOP_PAY IS NULL AND Y.AC_STAT_STOP_PAY IS NULL
        )
        OR (
          X.AC_STAT_STOP_PAY <> Y.AC_STAT_STOP_PAY
        )
      )
      OR (
        (
          X.AC_STMT_CYCLE IS NULL AND NOT Y.AC_STMT_CYCLE IS NULL
        )
        OR (
          NOT X.AC_STMT_CYCLE IS NULL AND Y.AC_STMT_CYCLE IS NULL
        )
        OR (
          X.AC_STMT_CYCLE <> Y.AC_STMT_CYCLE
        )
      )
      OR (
        (
          X.AC_STMT_CYCLE2 IS NULL AND NOT Y.AC_STMT_CYCLE2 IS NULL
        )
        OR (
          NOT X.AC_STMT_CYCLE2 IS NULL AND Y.AC_STMT_CYCLE2 IS NULL
        )
        OR (
          X.AC_STMT_CYCLE2 <> Y.AC_STMT_CYCLE2
        )
      )
      OR (
        (
          X.AC_STMT_CYCLE3 IS NULL AND NOT Y.AC_STMT_CYCLE3 IS NULL
        )
        OR (
          NOT X.AC_STMT_CYCLE3 IS NULL AND Y.AC_STMT_CYCLE3 IS NULL
        )
        OR (
          X.AC_STMT_CYCLE3 <> Y.AC_STMT_CYCLE3
        )
      )
      OR (
        (
          X.AC_STMT_DAY IS NULL AND NOT Y.AC_STMT_DAY IS NULL
        )
        OR (
          NOT X.AC_STMT_DAY IS NULL AND Y.AC_STMT_DAY IS NULL
        )
        OR (
          X.AC_STMT_DAY <> Y.AC_STMT_DAY
        )
      )
      OR (
        (
          X.AC_STMT_TYPE IS NULL AND NOT Y.AC_STMT_TYPE IS NULL
        )
        OR (
          NOT X.AC_STMT_TYPE IS NULL AND Y.AC_STMT_TYPE IS NULL
        )
        OR (
          X.AC_STMT_TYPE <> Y.AC_STMT_TYPE
        )
      )
      OR (
        (
          X.ACC_STATUS IS NULL AND NOT Y.ACC_STATUS IS NULL
        )
        OR (
          NOT X.ACC_STATUS IS NULL AND Y.ACC_STATUS IS NULL
        )
        OR (
          X.ACC_STATUS <> Y.ACC_STATUS
        )
      )
      OR (
        (
          X.ACC_STMT_DAY2 IS NULL AND NOT Y.ACC_STMT_DAY2 IS NULL
        )
        OR (
          NOT X.ACC_STMT_DAY2 IS NULL AND Y.ACC_STMT_DAY2 IS NULL
        )
        OR (
          X.ACC_STMT_DAY2 <> Y.ACC_STMT_DAY2
        )
      )
      OR (
        (
          X.ACC_STMT_DAY3 IS NULL AND NOT Y.ACC_STMT_DAY3 IS NULL
        )
        OR (
          NOT X.ACC_STMT_DAY3 IS NULL AND Y.ACC_STMT_DAY3 IS NULL
        )
        OR (
          X.ACC_STMT_DAY3 <> Y.ACC_STMT_DAY3
        )
      )
      OR (
        (
          X.ACC_STMT_TYPE2 IS NULL AND NOT Y.ACC_STMT_TYPE2 IS NULL
        )
        OR (
          NOT X.ACC_STMT_TYPE2 IS NULL AND Y.ACC_STMT_TYPE2 IS NULL
        )
        OR (
          X.ACC_STMT_TYPE2 <> Y.ACC_STMT_TYPE2
        )
      )
      OR (
        (
          X.ACC_STMT_TYPE3 IS NULL AND NOT Y.ACC_STMT_TYPE3 IS NULL
        )
        OR (
          NOT X.ACC_STMT_TYPE3 IS NULL AND Y.ACC_STMT_TYPE3 IS NULL
        )
        OR (
          X.ACC_STMT_TYPE3 <> Y.ACC_STMT_TYPE3
        )
      )
      OR (
        (
          X.ACC_TANKED_STAT IS NULL AND NOT Y.ACC_TANKED_STAT IS NULL
        )
        OR (
          NOT X.ACC_TANKED_STAT IS NULL AND Y.ACC_TANKED_STAT IS NULL
        )
        OR (
          X.ACC_TANKED_STAT <> Y.ACC_TANKED_STAT
        )
      )
      OR (
        (
          X.ACCOUNT_AUTO_CLOSED IS NULL AND NOT Y.ACCOUNT_AUTO_CLOSED IS NULL
        )
        OR (
          NOT X.ACCOUNT_AUTO_CLOSED IS NULL AND Y.ACCOUNT_AUTO_CLOSED IS NULL
        )
        OR (
          X.ACCOUNT_AUTO_CLOSED <> Y.ACCOUNT_AUTO_CLOSED
        )
      )
      OR (
        (
          X.ACCOUNT_CLASS IS NULL AND NOT Y.ACCOUNT_CLASS IS NULL
        )
        OR (
          NOT X.ACCOUNT_CLASS IS NULL AND Y.ACCOUNT_CLASS IS NULL
        )
        OR (
          X.ACCOUNT_CLASS <> Y.ACCOUNT_CLASS
        )
      )
      OR (
        (
          X.ACCOUNT_DERIVED_STATUS IS NULL AND NOT Y.ACCOUNT_DERIVED_STATUS IS NULL
        )
        OR (
          NOT X.ACCOUNT_DERIVED_STATUS IS NULL AND Y.ACCOUNT_DERIVED_STATUS IS NULL
        )
        OR (
          X.ACCOUNT_DERIVED_STATUS <> Y.ACCOUNT_DERIVED_STATUS
        )
      )
      OR (
        (
          X.ACCOUNT_TYPE IS NULL AND NOT Y.ACCOUNT_TYPE IS NULL
        )
        OR (
          NOT X.ACCOUNT_TYPE IS NULL AND Y.ACCOUNT_TYPE IS NULL
        )
        OR (
          X.ACCOUNT_TYPE <> Y.ACCOUNT_TYPE
        )
      )
      OR (
        (
          X.ACY_ACCRUED_CR_IC IS NULL AND NOT Y.ACY_ACCRUED_CR_IC IS NULL
        )
        OR (
          NOT X.ACY_ACCRUED_CR_IC IS NULL AND Y.ACY_ACCRUED_CR_IC IS NULL
        )
        OR (
          X.ACY_ACCRUED_CR_IC <> Y.ACY_ACCRUED_CR_IC
        )
      )
      OR (
        (
          X.ACY_ACCRUED_DR_IC IS NULL AND NOT Y.ACY_ACCRUED_DR_IC IS NULL
        )
        OR (
          NOT X.ACY_ACCRUED_DR_IC IS NULL AND Y.ACY_ACCRUED_DR_IC IS NULL
        )
        OR (
          X.ACY_ACCRUED_DR_IC <> Y.ACY_ACCRUED_DR_IC
        )
      )
      OR (
        (
          X.ACY_AVL_BAL IS NULL AND NOT Y.ACY_AVL_BAL IS NULL
        )
        OR (
          NOT X.ACY_AVL_BAL IS NULL AND Y.ACY_AVL_BAL IS NULL
        )
        OR (
          X.ACY_AVL_BAL <> Y.ACY_AVL_BAL
        )
      )
      OR (
        (
          X.ACY_BLOCKED_AMOUNT IS NULL AND NOT Y.ACY_BLOCKED_AMOUNT IS NULL
        )
        OR (
          NOT X.ACY_BLOCKED_AMOUNT IS NULL AND Y.ACY_BLOCKED_AMOUNT IS NULL
        )
        OR (
          X.ACY_BLOCKED_AMOUNT <> Y.ACY_BLOCKED_AMOUNT
        )
      )
      OR (
        (
          X.ACY_CURR_BALANCE IS NULL AND NOT Y.ACY_CURR_BALANCE IS NULL
        )
        OR (
          NOT X.ACY_CURR_BALANCE IS NULL AND Y.ACY_CURR_BALANCE IS NULL
        )
        OR (
          X.ACY_CURR_BALANCE <> Y.ACY_CURR_BALANCE
        )
      )
      OR (
        (
          X.ACY_MTD_TOVER_CR IS NULL AND NOT Y.ACY_MTD_TOVER_CR IS NULL
        )
        OR (
          NOT X.ACY_MTD_TOVER_CR IS NULL AND Y.ACY_MTD_TOVER_CR IS NULL
        )
        OR (
          X.ACY_MTD_TOVER_CR <> Y.ACY_MTD_TOVER_CR
        )
      )
      OR (
        (
          X.ACY_MTD_TOVER_DR IS NULL AND NOT Y.ACY_MTD_TOVER_DR IS NULL
        )
        OR (
          NOT X.ACY_MTD_TOVER_DR IS NULL AND Y.ACY_MTD_TOVER_DR IS NULL
        )
        OR (
          X.ACY_MTD_TOVER_DR <> Y.ACY_MTD_TOVER_DR
        )
      )
      OR (
        (
          X.ACY_OPENING_BAL IS NULL AND NOT Y.ACY_OPENING_BAL IS NULL
        )
        OR (
          NOT X.ACY_OPENING_BAL IS NULL AND Y.ACY_OPENING_BAL IS NULL
        )
        OR (
          X.ACY_OPENING_BAL <> Y.ACY_OPENING_BAL
        )
      )
      OR (
        (
          X.ACY_SWEEP_INELIGIBLE IS NULL AND NOT Y.ACY_SWEEP_INELIGIBLE IS NULL
        )
        OR (
          NOT X.ACY_SWEEP_INELIGIBLE IS NULL AND Y.ACY_SWEEP_INELIGIBLE IS NULL
        )
        OR (
          X.ACY_SWEEP_INELIGIBLE <> Y.ACY_SWEEP_INELIGIBLE
        )
      )
      OR (
        (
          X.ACY_TANK_CR IS NULL AND NOT Y.ACY_TANK_CR IS NULL
        )
        OR (
          NOT X.ACY_TANK_CR IS NULL AND Y.ACY_TANK_CR IS NULL
        )
        OR (
          X.ACY_TANK_CR <> Y.ACY_TANK_CR
        )
      )
      OR (
        (
          X.ACY_TANK_DR IS NULL AND NOT Y.ACY_TANK_DR IS NULL
        )
        OR (
          NOT X.ACY_TANK_DR IS NULL AND Y.ACY_TANK_DR IS NULL
        )
        OR (
          X.ACY_TANK_DR <> Y.ACY_TANK_DR
        )
      )
      OR (
        (
          X.ACY_TANK_UNCOLLECTED IS NULL AND NOT Y.ACY_TANK_UNCOLLECTED IS NULL
        )
        OR (
          NOT X.ACY_TANK_UNCOLLECTED IS NULL AND Y.ACY_TANK_UNCOLLECTED IS NULL
        )
        OR (
          X.ACY_TANK_UNCOLLECTED <> Y.ACY_TANK_UNCOLLECTED
        )
      )
      OR (
        (
          X.ACY_TODAY_TOVER_CR IS NULL AND NOT Y.ACY_TODAY_TOVER_CR IS NULL
        )
        OR (
          NOT X.ACY_TODAY_TOVER_CR IS NULL AND Y.ACY_TODAY_TOVER_CR IS NULL
        )
        OR (
          X.ACY_TODAY_TOVER_CR <> Y.ACY_TODAY_TOVER_CR
        )
      )
      OR (
        (
          X.ACY_TODAY_TOVER_DR IS NULL AND NOT Y.ACY_TODAY_TOVER_DR IS NULL
        )
        OR (
          NOT X.ACY_TODAY_TOVER_DR IS NULL AND Y.ACY_TODAY_TOVER_DR IS NULL
        )
        OR (
          X.ACY_TODAY_TOVER_DR <> Y.ACY_TODAY_TOVER_DR
        )
      )
      OR (
        (
          X.ACY_TOVER_CR IS NULL AND NOT Y.ACY_TOVER_CR IS NULL
        )
        OR (
          NOT X.ACY_TOVER_CR IS NULL AND Y.ACY_TOVER_CR IS NULL
        )
        OR (
          X.ACY_TOVER_CR <> Y.ACY_TOVER_CR
        )
      )
      OR (
        (
          X.ACY_UNAUTH_CR IS NULL AND NOT Y.ACY_UNAUTH_CR IS NULL
        )
        OR (
          NOT X.ACY_UNAUTH_CR IS NULL AND Y.ACY_UNAUTH_CR IS NULL
        )
        OR (
          X.ACY_UNAUTH_CR <> Y.ACY_UNAUTH_CR
        )
      )
      OR (
        (
          X.ACY_UNAUTH_DR IS NULL AND NOT Y.ACY_UNAUTH_DR IS NULL
        )
        OR (
          NOT X.ACY_UNAUTH_DR IS NULL AND Y.ACY_UNAUTH_DR IS NULL
        )
        OR (
          X.ACY_UNAUTH_DR <> Y.ACY_UNAUTH_DR
        )
      )
      OR (
        (
          X.ACY_UNAUTH_TANK_CR IS NULL AND NOT Y.ACY_UNAUTH_TANK_CR IS NULL
        )
        OR (
          NOT X.ACY_UNAUTH_TANK_CR IS NULL AND Y.ACY_UNAUTH_TANK_CR IS NULL
        )
        OR (
          X.ACY_UNAUTH_TANK_CR <> Y.ACY_UNAUTH_TANK_CR
        )
      )
      OR (
        (
          X.ACY_UNAUTH_TANK_DR IS NULL AND NOT Y.ACY_UNAUTH_TANK_DR IS NULL
        )
        OR (
          NOT X.ACY_UNAUTH_TANK_DR IS NULL AND Y.ACY_UNAUTH_TANK_DR IS NULL
        )
        OR (
          X.ACY_UNAUTH_TANK_DR <> Y.ACY_UNAUTH_TANK_DR
        )
      )
      OR (
        (
          X.ACY_UNAUTH_TANK_UNCOLLECTED IS NULL
          AND NOT Y.ACY_UNAUTH_TANK_UNCOLLECTED IS NULL
        )
        OR (
          NOT X.ACY_UNAUTH_TANK_UNCOLLECTED IS NULL
          AND Y.ACY_UNAUTH_TANK_UNCOLLECTED IS NULL
        )
        OR (
          X.ACY_UNAUTH_TANK_UNCOLLECTED <> Y.ACY_UNAUTH_TANK_UNCOLLECTED
        )
      )
      OR (
        (
          X.ACY_UNAUTH_UNCOLLECTED IS NULL AND NOT Y.ACY_UNAUTH_UNCOLLECTED IS NULL
        )
        OR (
          NOT X.ACY_UNAUTH_UNCOLLECTED IS NULL AND Y.ACY_UNAUTH_UNCOLLECTED IS NULL
        )
        OR (
          X.ACY_UNAUTH_UNCOLLECTED <> Y.ACY_UNAUTH_UNCOLLECTED
        )
      )
      OR (
        (
          X.ACY_UNCOLLECTED IS NULL AND NOT Y.ACY_UNCOLLECTED IS NULL
        )
        OR (
          NOT X.ACY_UNCOLLECTED IS NULL AND Y.ACY_UNCOLLECTED IS NULL
        )
        OR (
          X.ACY_UNCOLLECTED <> Y.ACY_UNCOLLECTED
        )
      )
      OR (
        (
          X.ADDRESS1 IS NULL AND NOT Y.ADDRESS1 IS NULL
        )
        OR (
          NOT X.ADDRESS1 IS NULL AND Y.ADDRESS1 IS NULL
        )
        OR (
          X.ADDRESS1 <> Y.ADDRESS1
        )
      )
      OR (
        (
          X.ADDRESS2 IS NULL AND NOT Y.ADDRESS2 IS NULL
        )
        OR (
          NOT X.ADDRESS2 IS NULL AND Y.ADDRESS2 IS NULL
        )
        OR (
          X.ADDRESS2 <> Y.ADDRESS2
        )
      )
      OR (
        (
          X.ADDRESS3 IS NULL AND NOT Y.ADDRESS3 IS NULL
        )
        OR (
          NOT X.ADDRESS3 IS NULL AND Y.ADDRESS3 IS NULL
        )
        OR (
          X.ADDRESS3 <> Y.ADDRESS3
        )
      )
      OR (
        (
          X.ADDRESS4 IS NULL AND NOT Y.ADDRESS4 IS NULL
        )
        OR (
          NOT X.ADDRESS4 IS NULL AND Y.ADDRESS4 IS NULL
        )
        OR (
          X.ADDRESS4 <> Y.ADDRESS4
        )
      )
      OR (
        (
          X.ALLOW_BACK_PERIOD_ENTRY IS NULL AND NOT Y.ALLOW_BACK_PERIOD_ENTRY IS NULL
        )
        OR (
          NOT X.ALLOW_BACK_PERIOD_ENTRY IS NULL AND Y.ALLOW_BACK_PERIOD_ENTRY IS NULL
        )
        OR (
          X.ALLOW_BACK_PERIOD_ENTRY <> Y.ALLOW_BACK_PERIOD_ENTRY
        )
      )
      OR (
        (
          X.ALT_AC_NO IS NULL AND NOT Y.ALT_AC_NO IS NULL
        )
        OR (
          NOT X.ALT_AC_NO IS NULL AND Y.ALT_AC_NO IS NULL
        )
        OR (
          X.ALT_AC_NO <> Y.ALT_AC_NO
        )
      )
      OR (
        (
          X.ATM_CUST_AC_NO IS NULL AND NOT Y.ATM_CUST_AC_NO IS NULL
        )
        OR (
          NOT X.ATM_CUST_AC_NO IS NULL AND Y.ATM_CUST_AC_NO IS NULL
        )
        OR (
          X.ATM_CUST_AC_NO <> Y.ATM_CUST_AC_NO
        )
      )
      OR (
        (
          X.ATM_DLY_AMT_LIMIT IS NULL AND NOT Y.ATM_DLY_AMT_LIMIT IS NULL
        )
        OR (
          NOT X.ATM_DLY_AMT_LIMIT IS NULL AND Y.ATM_DLY_AMT_LIMIT IS NULL
        )
        OR (
          X.ATM_DLY_AMT_LIMIT <> Y.ATM_DLY_AMT_LIMIT
        )
      )
      OR (
        (
          X.ATM_DLY_COUNT_LIMIT IS NULL AND NOT Y.ATM_DLY_COUNT_LIMIT IS NULL
        )
        OR (
          NOT X.ATM_DLY_COUNT_LIMIT IS NULL AND Y.ATM_DLY_COUNT_LIMIT IS NULL
        )
        OR (
          X.ATM_DLY_COUNT_LIMIT <> Y.ATM_DLY_COUNT_LIMIT
        )
      )
      OR (
        (
          X.ATM_FACILITY IS NULL AND NOT Y.ATM_FACILITY IS NULL
        )
        OR (
          NOT X.ATM_FACILITY IS NULL AND Y.ATM_FACILITY IS NULL
        )
        OR (
          X.ATM_FACILITY <> Y.ATM_FACILITY
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
          X.AUTO_CHQBK_REQUEST IS NULL AND NOT Y.AUTO_CHQBK_REQUEST IS NULL
        )
        OR (
          NOT X.AUTO_CHQBK_REQUEST IS NULL AND Y.AUTO_CHQBK_REQUEST IS NULL
        )
        OR (
          X.AUTO_CHQBK_REQUEST <> Y.AUTO_CHQBK_REQUEST
        )
      )
      OR (
        (
          X.AUTO_CREATE_POOL IS NULL AND NOT Y.AUTO_CREATE_POOL IS NULL
        )
        OR (
          NOT X.AUTO_CREATE_POOL IS NULL AND Y.AUTO_CREATE_POOL IS NULL
        )
        OR (
          X.AUTO_CREATE_POOL <> Y.AUTO_CREATE_POOL
        )
      )
      OR (
        (
          X.AUTO_DC_REQUEST IS NULL AND NOT Y.AUTO_DC_REQUEST IS NULL
        )
        OR (
          NOT X.AUTO_DC_REQUEST IS NULL AND Y.AUTO_DC_REQUEST IS NULL
        )
        OR (
          X.AUTO_DC_REQUEST <> Y.AUTO_DC_REQUEST
        )
      )
      OR (
        (
          X.AUTO_DEPOSIT IS NULL AND NOT Y.AUTO_DEPOSIT IS NULL
        )
        OR (
          NOT X.AUTO_DEPOSIT IS NULL AND Y.AUTO_DEPOSIT IS NULL
        )
        OR (
          X.AUTO_DEPOSIT <> Y.AUTO_DEPOSIT
        )
      )
      OR (
        (
          X.AUTO_DEPOSITS_BAL IS NULL AND NOT Y.AUTO_DEPOSITS_BAL IS NULL
        )
        OR (
          NOT X.AUTO_DEPOSITS_BAL IS NULL AND Y.AUTO_DEPOSITS_BAL IS NULL
        )
        OR (
          X.AUTO_DEPOSITS_BAL <> Y.AUTO_DEPOSITS_BAL
        )
      )
      OR (
        (
          X.AUTO_PROV_REQD IS NULL AND NOT Y.AUTO_PROV_REQD IS NULL
        )
        OR (
          NOT X.AUTO_PROV_REQD IS NULL AND Y.AUTO_PROV_REQD IS NULL
        )
        OR (
          X.AUTO_PROV_REQD <> Y.AUTO_PROV_REQD
        )
      )
      OR (
        (
          X.AUTO_REORDER_CHECK_LEAVES IS NULL AND NOT Y.AUTO_REORDER_CHECK_LEAVES IS NULL
        )
        OR (
          NOT X.AUTO_REORDER_CHECK_LEAVES IS NULL AND Y.AUTO_REORDER_CHECK_LEAVES IS NULL
        )
        OR (
          X.AUTO_REORDER_CHECK_LEAVES <> Y.AUTO_REORDER_CHECK_LEAVES
        )
      )
      OR (
        (
          X.AUTO_REORDER_CHECK_LEVEL IS NULL AND NOT Y.AUTO_REORDER_CHECK_LEVEL IS NULL
        )
        OR (
          NOT X.AUTO_REORDER_CHECK_LEVEL IS NULL AND Y.AUTO_REORDER_CHECK_LEVEL IS NULL
        )
        OR (
          X.AUTO_REORDER_CHECK_LEVEL <> Y.AUTO_REORDER_CHECK_LEVEL
        )
      )
      OR (
        (
          X.AUTO_REORDER_CHECK_REQUIRED IS NULL
          AND NOT Y.AUTO_REORDER_CHECK_REQUIRED IS NULL
        )
        OR (
          NOT X.AUTO_REORDER_CHECK_REQUIRED IS NULL
          AND Y.AUTO_REORDER_CHECK_REQUIRED IS NULL
        )
        OR (
          X.AUTO_REORDER_CHECK_REQUIRED <> Y.AUTO_REORDER_CHECK_REQUIRED
        )
      )
      OR (
        (
          X.BALANCE_REPORT_SINCE IS NULL AND NOT Y.BALANCE_REPORT_SINCE IS NULL
        )
        OR (
          NOT X.BALANCE_REPORT_SINCE IS NULL AND Y.BALANCE_REPORT_SINCE IS NULL
        )
        OR (
          X.BALANCE_REPORT_SINCE <> Y.BALANCE_REPORT_SINCE
        )
      )
      OR (
        (
          X.BALANCE_REPORT_TYPE IS NULL AND NOT Y.BALANCE_REPORT_TYPE IS NULL
        )
        OR (
          NOT X.BALANCE_REPORT_TYPE IS NULL AND Y.BALANCE_REPORT_TYPE IS NULL
        )
        OR (
          X.BALANCE_REPORT_TYPE <> Y.BALANCE_REPORT_TYPE
        )
      )
      OR (
        (
          X.BM_AC_STAT_BLOCK IS NULL AND NOT Y.BM_AC_STAT_BLOCK IS NULL
        )
        OR (
          NOT X.BM_AC_STAT_BLOCK IS NULL AND Y.BM_AC_STAT_BLOCK IS NULL
        )
        OR (
          X.BM_AC_STAT_BLOCK <> Y.BM_AC_STAT_BLOCK
        )
      )
      OR (
        (
          X.BM_AC_STAT_DE_POST IS NULL AND NOT Y.BM_AC_STAT_DE_POST IS NULL
        )
        OR (
          NOT X.BM_AC_STAT_DE_POST IS NULL AND Y.BM_AC_STAT_DE_POST IS NULL
        )
        OR (
          X.BM_AC_STAT_DE_POST <> Y.BM_AC_STAT_DE_POST
        )
      )
      OR (
        (
          X.BM_AC_STAT_DORMANT IS NULL AND NOT Y.BM_AC_STAT_DORMANT IS NULL
        )
        OR (
          NOT X.BM_AC_STAT_DORMANT IS NULL AND Y.BM_AC_STAT_DORMANT IS NULL
        )
        OR (
          X.BM_AC_STAT_DORMANT <> Y.BM_AC_STAT_DORMANT
        )
      )
      OR (
        (
          X.BM_AC_STAT_FROZEN IS NULL AND NOT Y.BM_AC_STAT_FROZEN IS NULL
        )
        OR (
          NOT X.BM_AC_STAT_FROZEN IS NULL AND Y.BM_AC_STAT_FROZEN IS NULL
        )
        OR (
          X.BM_AC_STAT_FROZEN <> Y.BM_AC_STAT_FROZEN
        )
      )
      OR (
        (
          X.BM_AC_STAT_NO_CR IS NULL AND NOT Y.BM_AC_STAT_NO_CR IS NULL
        )
        OR (
          NOT X.BM_AC_STAT_NO_CR IS NULL AND Y.BM_AC_STAT_NO_CR IS NULL
        )
        OR (
          X.BM_AC_STAT_NO_CR <> Y.BM_AC_STAT_NO_CR
        )
      )
      OR (
        (
          X.BM_AC_STAT_NO_DR IS NULL AND NOT Y.BM_AC_STAT_NO_DR IS NULL
        )
        OR (
          NOT X.BM_AC_STAT_NO_DR IS NULL AND Y.BM_AC_STAT_NO_DR IS NULL
        )
        OR (
          X.BM_AC_STAT_NO_DR <> Y.BM_AC_STAT_NO_DR
        )
      )
      OR (
        (
          X.BM_AC_STAT_STOP_PAY IS NULL AND NOT Y.BM_AC_STAT_STOP_PAY IS NULL
        )
        OR (
          NOT X.BM_AC_STAT_STOP_PAY IS NULL AND Y.BM_AC_STAT_STOP_PAY IS NULL
        )
        OR (
          X.BM_AC_STAT_STOP_PAY <> Y.BM_AC_STAT_STOP_PAY
        )
      )
      OR (
        (
          X.BM_AC_STMT_CYCLE IS NULL AND NOT Y.BM_AC_STMT_CYCLE IS NULL
        )
        OR (
          NOT X.BM_AC_STMT_CYCLE IS NULL AND Y.BM_AC_STMT_CYCLE IS NULL
        )
        OR (
          X.BM_AC_STMT_CYCLE <> Y.BM_AC_STMT_CYCLE
        )
      )
      OR (
        (
          X.BM_ACCOUNT_TYPE IS NULL AND NOT Y.BM_ACCOUNT_TYPE IS NULL
        )
        OR (
          NOT X.BM_ACCOUNT_TYPE IS NULL AND Y.BM_ACCOUNT_TYPE IS NULL
        )
        OR (
          X.BM_ACCOUNT_TYPE <> Y.BM_ACCOUNT_TYPE
        )
      )
      OR (
        (
          X.BM_ACCOUNT_TYPE_FINANCIAL IS NULL AND NOT Y.BM_ACCOUNT_TYPE_FINANCIAL IS NULL
        )
        OR (
          NOT X.BM_ACCOUNT_TYPE_FINANCIAL IS NULL AND Y.BM_ACCOUNT_TYPE_FINANCIAL IS NULL
        )
        OR (
          X.BM_ACCOUNT_TYPE_FINANCIAL <> Y.BM_ACCOUNT_TYPE_FINANCIAL
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
          X.BM_CCY IS NULL AND NOT Y.BM_CCY IS NULL
        )
        OR (
          NOT X.BM_CCY IS NULL AND Y.BM_CCY IS NULL
        )
        OR (
          X.BM_CCY <> Y.BM_CCY
        )
      )
      OR (
        (
          X.BM_JOINT_AC_INDICATOR IS NULL AND NOT Y.BM_JOINT_AC_INDICATOR IS NULL
        )
        OR (
          NOT X.BM_JOINT_AC_INDICATOR IS NULL AND Y.BM_JOINT_AC_INDICATOR IS NULL
        )
        OR (
          X.BM_JOINT_AC_INDICATOR <> Y.BM_JOINT_AC_INDICATOR
        )
      )
      OR (
        (
          X.BM_LIMIT_CCY IS NULL AND NOT Y.BM_LIMIT_CCY IS NULL
        )
        OR (
          NOT X.BM_LIMIT_CCY IS NULL AND Y.BM_LIMIT_CCY IS NULL
        )
        OR (
          X.BM_LIMIT_CCY <> Y.BM_LIMIT_CCY
        )
      )
      OR (
        (
          X.BM_MEDIA IS NULL AND NOT Y.BM_MEDIA IS NULL
        )
        OR (
          NOT X.BM_MEDIA IS NULL AND Y.BM_MEDIA IS NULL
        )
        OR (
          X.BM_MEDIA <> Y.BM_MEDIA
        )
      )
      OR (
        (
          X.BM_MOD_NO IS NULL AND NOT Y.BM_MOD_NO IS NULL
        )
        OR (
          NOT X.BM_MOD_NO IS NULL AND Y.BM_MOD_NO IS NULL
        )
        OR (
          X.BM_MOD_NO <> Y.BM_MOD_NO
        )
      )
      OR (
        (
          X.BM_ONCE_AUTH IS NULL AND NOT Y.BM_ONCE_AUTH IS NULL
        )
        OR (
          NOT X.BM_ONCE_AUTH IS NULL AND Y.BM_ONCE_AUTH IS NULL
        )
        OR (
          X.BM_ONCE_AUTH <> Y.BM_ONCE_AUTH
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
          X.BM_SALARY_ACCOUNT IS NULL AND NOT Y.BM_SALARY_ACCOUNT IS NULL
        )
        OR (
          NOT X.BM_SALARY_ACCOUNT IS NULL AND Y.BM_SALARY_ACCOUNT IS NULL
        )
        OR (
          X.BM_SALARY_ACCOUNT <> Y.BM_SALARY_ACCOUNT
        )
      )
      OR (
        (
          X.CAS_ACCOUNT IS NULL AND NOT Y.CAS_ACCOUNT IS NULL
        )
        OR (
          NOT X.CAS_ACCOUNT IS NULL AND Y.CAS_ACCOUNT IS NULL
        )
        OR (
          X.CAS_ACCOUNT <> Y.CAS_ACCOUNT
        )
      )
      OR (
        (
          X.CAS_CUSTOMER IS NULL AND NOT Y.CAS_CUSTOMER IS NULL
        )
        OR (
          NOT X.CAS_CUSTOMER IS NULL AND Y.CAS_CUSTOMER IS NULL
        )
        OR (
          X.CAS_CUSTOMER <> Y.CAS_CUSTOMER
        )
      )
      OR (
        (
          X.CCY IS NULL AND NOT Y.CCY IS NULL
        )
        OR (
          NOT X.CCY IS NULL AND Y.CCY IS NULL
        )
        OR (
          X.CCY <> Y.CCY
        )
      )
      OR (
        (
          X.CHECKBOOK_NAME_1 IS NULL AND NOT Y.CHECKBOOK_NAME_1 IS NULL
        )
        OR (
          NOT X.CHECKBOOK_NAME_1 IS NULL AND Y.CHECKBOOK_NAME_1 IS NULL
        )
        OR (
          X.CHECKBOOK_NAME_1 <> Y.CHECKBOOK_NAME_1
        )
      )
      OR (
        (
          X.CHECKBOOK_NAME_2 IS NULL AND NOT Y.CHECKBOOK_NAME_2 IS NULL
        )
        OR (
          NOT X.CHECKBOOK_NAME_2 IS NULL AND Y.CHECKBOOK_NAME_2 IS NULL
        )
        OR (
          X.CHECKBOOK_NAME_2 <> Y.CHECKBOOK_NAME_2
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
          X.CHEQUE_BOOK_FACILITY IS NULL AND NOT Y.CHEQUE_BOOK_FACILITY IS NULL
        )
        OR (
          NOT X.CHEQUE_BOOK_FACILITY IS NULL AND Y.CHEQUE_BOOK_FACILITY IS NULL
        )
        OR (
          X.CHEQUE_BOOK_FACILITY <> Y.CHEQUE_BOOK_FACILITY
        )
      )
      OR (
        (
          X.CHG_DUE IS NULL AND NOT Y.CHG_DUE IS NULL
        )
        OR (
          NOT X.CHG_DUE IS NULL AND Y.CHG_DUE IS NULL
        )
        OR (
          X.CHG_DUE <> Y.CHG_DUE
        )
      )
      OR (
        (
          X.CLEARING_AC_NO IS NULL AND NOT Y.CLEARING_AC_NO IS NULL
        )
        OR (
          NOT X.CLEARING_AC_NO IS NULL AND Y.CLEARING_AC_NO IS NULL
        )
        OR (
          X.CLEARING_AC_NO <> Y.CLEARING_AC_NO
        )
      )
      OR (
        (
          X.CLEARING_BANK_CODE IS NULL AND NOT Y.CLEARING_BANK_CODE IS NULL
        )
        OR (
          NOT X.CLEARING_BANK_CODE IS NULL AND Y.CLEARING_BANK_CODE IS NULL
        )
        OR (
          X.CLEARING_BANK_CODE <> Y.CLEARING_BANK_CODE
        )
      )
      OR (
        (
          X.CONSOL_CHG_ACC IS NULL AND NOT Y.CONSOL_CHG_ACC IS NULL
        )
        OR (
          NOT X.CONSOL_CHG_ACC IS NULL AND Y.CONSOL_CHG_ACC IS NULL
        )
        OR (
          X.CONSOL_CHG_ACC <> Y.CONSOL_CHG_ACC
        )
      )
      OR (
        (
          X.CONSOL_CHG_BRN IS NULL AND NOT Y.CONSOL_CHG_BRN IS NULL
        )
        OR (
          NOT X.CONSOL_CHG_BRN IS NULL AND Y.CONSOL_CHG_BRN IS NULL
        )
        OR (
          X.CONSOL_CHG_BRN <> Y.CONSOL_CHG_BRN
        )
      )
      OR (
        (
          X.CONSOLIDATION_REQD IS NULL AND NOT Y.CONSOLIDATION_REQD IS NULL
        )
        OR (
          NOT X.CONSOLIDATION_REQD IS NULL AND Y.CONSOLIDATION_REQD IS NULL
        )
        OR (
          X.CONSOLIDATION_REQD <> Y.CONSOLIDATION_REQD
        )
      )
      OR (
        (
          X.CONTRIBUTE_TO_PDM IS NULL AND NOT Y.CONTRIBUTE_TO_PDM IS NULL
        )
        OR (
          NOT X.CONTRIBUTE_TO_PDM IS NULL AND Y.CONTRIBUTE_TO_PDM IS NULL
        )
        OR (
          X.CONTRIBUTE_TO_PDM <> Y.CONTRIBUTE_TO_PDM
        )
      )
      OR (
        (
          X.COUNTRY_CODE IS NULL AND NOT Y.COUNTRY_CODE IS NULL
        )
        OR (
          NOT X.COUNTRY_CODE IS NULL AND Y.COUNTRY_CODE IS NULL
        )
        OR (
          X.COUNTRY_CODE <> Y.COUNTRY_CODE
        )
      )
      OR (
        (
          X.CR_AUTO_EX_RATE_LMT IS NULL AND NOT Y.CR_AUTO_EX_RATE_LMT IS NULL
        )
        OR (
          NOT X.CR_AUTO_EX_RATE_LMT IS NULL AND Y.CR_AUTO_EX_RATE_LMT IS NULL
        )
        OR (
          X.CR_AUTO_EX_RATE_LMT <> Y.CR_AUTO_EX_RATE_LMT
        )
      )
      OR (
        (
          X.CR_CB_LINE IS NULL AND NOT Y.CR_CB_LINE IS NULL
        )
        OR (
          NOT X.CR_CB_LINE IS NULL AND Y.CR_CB_LINE IS NULL
        )
        OR (
          X.CR_CB_LINE <> Y.CR_CB_LINE
        )
      )
      OR (
        (
          X.CR_GL IS NULL AND NOT Y.CR_GL IS NULL
        )
        OR (
          NOT X.CR_GL IS NULL AND Y.CR_GL IS NULL
        )
        OR (
          X.CR_GL <> Y.CR_GL
        )
      )
      OR (
        (
          X.CR_HO_LINE IS NULL AND NOT Y.CR_HO_LINE IS NULL
        )
        OR (
          NOT X.CR_HO_LINE IS NULL AND Y.CR_HO_LINE IS NULL
        )
        OR (
          X.CR_HO_LINE <> Y.CR_HO_LINE
        )
      )
      OR (
        (
          X.CR_LM_REV_DATE IS NULL AND NOT Y.CR_LM_REV_DATE IS NULL
        )
        OR (
          NOT X.CR_LM_REV_DATE IS NULL AND Y.CR_LM_REV_DATE IS NULL
        )
        OR (
          X.CR_LM_REV_DATE <> Y.CR_LM_REV_DATE
        )
      )
      OR (
        (
          X.CR_LM_START_DATE IS NULL AND NOT Y.CR_LM_START_DATE IS NULL
        )
        OR (
          NOT X.CR_LM_START_DATE IS NULL AND Y.CR_LM_START_DATE IS NULL
        )
        OR (
          X.CR_LM_START_DATE <> Y.CR_LM_START_DATE
        )
      )
      OR (
        (
          X.CREDIT_TXN_LIMIT IS NULL AND NOT Y.CREDIT_TXN_LIMIT IS NULL
        )
        OR (
          NOT X.CREDIT_TXN_LIMIT IS NULL AND Y.CREDIT_TXN_LIMIT IS NULL
        )
        OR (
          X.CREDIT_TXN_LIMIT <> Y.CREDIT_TXN_LIMIT
        )
      )
      OR (
        (
          X.CRS_STAT_REQD IS NULL AND NOT Y.CRS_STAT_REQD IS NULL
        )
        OR (
          NOT X.CRS_STAT_REQD IS NULL AND Y.CRS_STAT_REQD IS NULL
        )
        OR (
          X.CRS_STAT_REQD <> Y.CRS_STAT_REQD
        )
      )
      OR (
        (
          X.CUST_NO IS NULL AND NOT Y.CUST_NO IS NULL
        )
        OR (
          NOT X.CUST_NO IS NULL AND Y.CUST_NO IS NULL
        )
        OR (
          X.CUST_NO <> Y.CUST_NO
        )
      )
      OR (
        (
          X.DATE_LAST_CR IS NULL AND NOT Y.DATE_LAST_CR IS NULL
        )
        OR (
          NOT X.DATE_LAST_CR IS NULL AND Y.DATE_LAST_CR IS NULL
        )
        OR (
          X.DATE_LAST_CR <> Y.DATE_LAST_CR
        )
      )
      OR (
        (
          X.DATE_LAST_CR_ACTIVITY IS NULL AND NOT Y.DATE_LAST_CR_ACTIVITY IS NULL
        )
        OR (
          NOT X.DATE_LAST_CR_ACTIVITY IS NULL AND Y.DATE_LAST_CR_ACTIVITY IS NULL
        )
        OR (
          X.DATE_LAST_CR_ACTIVITY <> Y.DATE_LAST_CR_ACTIVITY
        )
      )
      OR (
        (
          X.DATE_LAST_DR IS NULL AND NOT Y.DATE_LAST_DR IS NULL
        )
        OR (
          NOT X.DATE_LAST_DR IS NULL AND Y.DATE_LAST_DR IS NULL
        )
        OR (
          X.DATE_LAST_DR <> Y.DATE_LAST_DR
        )
      )
      OR (
        (
          X.DATE_LAST_DR_ACTIVITY IS NULL AND NOT Y.DATE_LAST_DR_ACTIVITY IS NULL
        )
        OR (
          NOT X.DATE_LAST_DR_ACTIVITY IS NULL AND Y.DATE_LAST_DR_ACTIVITY IS NULL
        )
        OR (
          X.DATE_LAST_DR_ACTIVITY <> Y.DATE_LAST_DR_ACTIVITY
        )
      )
      OR (
        (
          X.DAYLIGHT_LIMIT_AMOUNT IS NULL AND NOT Y.DAYLIGHT_LIMIT_AMOUNT IS NULL
        )
        OR (
          NOT X.DAYLIGHT_LIMIT_AMOUNT IS NULL AND Y.DAYLIGHT_LIMIT_AMOUNT IS NULL
        )
        OR (
          X.DAYLIGHT_LIMIT_AMOUNT <> Y.DAYLIGHT_LIMIT_AMOUNT
        )
      )
      OR (
        (
          X.DEFAULT_WAIVER IS NULL AND NOT Y.DEFAULT_WAIVER IS NULL
        )
        OR (
          NOT X.DEFAULT_WAIVER IS NULL AND Y.DEFAULT_WAIVER IS NULL
        )
        OR (
          X.DEFAULT_WAIVER <> Y.DEFAULT_WAIVER
        )
      )
      OR (
        (
          X.DEFER_RECON IS NULL AND NOT Y.DEFER_RECON IS NULL
        )
        OR (
          NOT X.DEFER_RECON IS NULL AND Y.DEFER_RECON IS NULL
        )
        OR (
          X.DEFER_RECON <> Y.DEFER_RECON
        )
      )
      OR (
        (
          X.DISPLAY_IBAN_IN_ADVICES IS NULL AND NOT Y.DISPLAY_IBAN_IN_ADVICES IS NULL
        )
        OR (
          NOT X.DISPLAY_IBAN_IN_ADVICES IS NULL AND Y.DISPLAY_IBAN_IN_ADVICES IS NULL
        )
        OR (
          X.DISPLAY_IBAN_IN_ADVICES <> Y.DISPLAY_IBAN_IN_ADVICES
        )
      )
      OR (
        (
          X.DORMANCY_DATE IS NULL AND NOT Y.DORMANCY_DATE IS NULL
        )
        OR (
          NOT X.DORMANCY_DATE IS NULL AND Y.DORMANCY_DATE IS NULL
        )
        OR (
          X.DORMANCY_DATE <> Y.DORMANCY_DATE
        )
      )
      OR (
        (
          X.DORMANCY_DAYS IS NULL AND NOT Y.DORMANCY_DAYS IS NULL
        )
        OR (
          NOT X.DORMANCY_DAYS IS NULL AND Y.DORMANCY_DAYS IS NULL
        )
        OR (
          X.DORMANCY_DAYS <> Y.DORMANCY_DAYS
        )
      )
      OR (
        (
          X.DORMANT_PARAM IS NULL AND NOT Y.DORMANT_PARAM IS NULL
        )
        OR (
          NOT X.DORMANT_PARAM IS NULL AND Y.DORMANT_PARAM IS NULL
        )
        OR (
          X.DORMANT_PARAM <> Y.DORMANT_PARAM
        )
      )
      OR (
        (
          X.DR_AUTO_EX_RATE_LMT IS NULL AND NOT Y.DR_AUTO_EX_RATE_LMT IS NULL
        )
        OR (
          NOT X.DR_AUTO_EX_RATE_LMT IS NULL AND Y.DR_AUTO_EX_RATE_LMT IS NULL
        )
        OR (
          X.DR_AUTO_EX_RATE_LMT <> Y.DR_AUTO_EX_RATE_LMT
        )
      )
      OR (
        (
          X.DR_CB_LINE IS NULL AND NOT Y.DR_CB_LINE IS NULL
        )
        OR (
          NOT X.DR_CB_LINE IS NULL AND Y.DR_CB_LINE IS NULL
        )
        OR (
          X.DR_CB_LINE <> Y.DR_CB_LINE
        )
      )
      OR (
        (
          X.DR_GL IS NULL AND NOT Y.DR_GL IS NULL
        )
        OR (
          NOT X.DR_GL IS NULL AND Y.DR_GL IS NULL
        )
        OR (
          X.DR_GL <> Y.DR_GL
        )
      )
      OR (
        (
          X.DR_HO_LINE IS NULL AND NOT Y.DR_HO_LINE IS NULL
        )
        OR (
          NOT X.DR_HO_LINE IS NULL AND Y.DR_HO_LINE IS NULL
        )
        OR (
          X.DR_HO_LINE <> Y.DR_HO_LINE
        )
      )
      OR (
        (
          X.DR_INT_DUE IS NULL AND NOT Y.DR_INT_DUE IS NULL
        )
        OR (
          NOT X.DR_INT_DUE IS NULL AND Y.DR_INT_DUE IS NULL
        )
        OR (
          X.DR_INT_DUE <> Y.DR_INT_DUE
        )
      )
      OR (
        (
          X.ESCROW_AC_NO IS NULL AND NOT Y.ESCROW_AC_NO IS NULL
        )
        OR (
          NOT X.ESCROW_AC_NO IS NULL AND Y.ESCROW_AC_NO IS NULL
        )
        OR (
          X.ESCROW_AC_NO <> Y.ESCROW_AC_NO
        )
      )
      OR (
        (
          X.ESCROW_BRANCH_CODE IS NULL AND NOT Y.ESCROW_BRANCH_CODE IS NULL
        )
        OR (
          NOT X.ESCROW_BRANCH_CODE IS NULL AND Y.ESCROW_BRANCH_CODE IS NULL
        )
        OR (
          X.ESCROW_BRANCH_CODE <> Y.ESCROW_BRANCH_CODE
        )
      )
      OR (
        (
          X.ESCROW_PERCENTAGE IS NULL AND NOT Y.ESCROW_PERCENTAGE IS NULL
        )
        OR (
          NOT X.ESCROW_PERCENTAGE IS NULL AND Y.ESCROW_PERCENTAGE IS NULL
        )
        OR (
          X.ESCROW_PERCENTAGE <> Y.ESCROW_PERCENTAGE
        )
      )
      OR (
        (
          X.ESCROW_TRANSFER IS NULL AND NOT Y.ESCROW_TRANSFER IS NULL
        )
        OR (
          NOT X.ESCROW_TRANSFER IS NULL AND Y.ESCROW_TRANSFER IS NULL
        )
        OR (
          X.ESCROW_TRANSFER <> Y.ESCROW_TRANSFER
        )
      )
      OR (
        (
          X.EXCL_SAMEDAY_RVRTRNS_FM_STMT IS NULL
          AND NOT Y.EXCL_SAMEDAY_RVRTRNS_FM_STMT IS NULL
        )
        OR (
          NOT X.EXCL_SAMEDAY_RVRTRNS_FM_STMT IS NULL
          AND Y.EXCL_SAMEDAY_RVRTRNS_FM_STMT IS NULL
        )
        OR (
          X.EXCL_SAMEDAY_RVRTRNS_FM_STMT <> Y.EXCL_SAMEDAY_RVRTRNS_FM_STMT
        )
      )
      OR (
        (
          X.EXCLUDE_FROM_DISTRIBUTION IS NULL AND NOT Y.EXCLUDE_FROM_DISTRIBUTION IS NULL
        )
        OR (
          NOT X.EXCLUDE_FROM_DISTRIBUTION IS NULL AND Y.EXCLUDE_FROM_DISTRIBUTION IS NULL
        )
        OR (
          X.EXCLUDE_FROM_DISTRIBUTION <> Y.EXCLUDE_FROM_DISTRIBUTION
        )
      )
      OR (
        (
          X.EXPOSURE_CATEGORY IS NULL AND NOT Y.EXPOSURE_CATEGORY IS NULL
        )
        OR (
          NOT X.EXPOSURE_CATEGORY IS NULL AND Y.EXPOSURE_CATEGORY IS NULL
        )
        OR (
          X.EXPOSURE_CATEGORY <> Y.EXPOSURE_CATEGORY
        )
      )
      OR (
        (
          X.FUND_ID IS NULL AND NOT Y.FUND_ID IS NULL
        )
        OR (
          NOT X.FUND_ID IS NULL AND Y.FUND_ID IS NULL
        )
        OR (
          X.FUND_ID <> Y.FUND_ID
        )
      )
      OR (
        (
          X.FUNDING IS NULL AND NOT Y.FUNDING IS NULL
        )
        OR (
          NOT X.FUNDING IS NULL AND Y.FUNDING IS NULL
        )
        OR (
          X.FUNDING <> Y.FUNDING
        )
      )
      OR (
        (
          X.FUNDING_ACCOUNT IS NULL AND NOT Y.FUNDING_ACCOUNT IS NULL
        )
        OR (
          NOT X.FUNDING_ACCOUNT IS NULL AND Y.FUNDING_ACCOUNT IS NULL
        )
        OR (
          X.FUNDING_ACCOUNT <> Y.FUNDING_ACCOUNT
        )
      )
      OR (
        (
          X.FUNDING_BRANCH IS NULL AND NOT Y.FUNDING_BRANCH IS NULL
        )
        OR (
          NOT X.FUNDING_BRANCH IS NULL AND Y.FUNDING_BRANCH IS NULL
        )
        OR (
          X.FUNDING_BRANCH <> Y.FUNDING_BRANCH
        )
      )
      OR (
        (
          X.GEN_BALANCE_REPORT IS NULL AND NOT Y.GEN_BALANCE_REPORT IS NULL
        )
        OR (
          NOT X.GEN_BALANCE_REPORT IS NULL AND Y.GEN_BALANCE_REPORT IS NULL
        )
        OR (
          X.GEN_BALANCE_REPORT <> Y.GEN_BALANCE_REPORT
        )
      )
      OR (
        (
          X.GEN_INTERIM_STMT IS NULL AND NOT Y.GEN_INTERIM_STMT IS NULL
        )
        OR (
          NOT X.GEN_INTERIM_STMT IS NULL AND Y.GEN_INTERIM_STMT IS NULL
        )
        OR (
          X.GEN_INTERIM_STMT <> Y.GEN_INTERIM_STMT
        )
      )
      OR (
        (
          X.GEN_INTERIM_STMT_ON_MVMT IS NULL AND NOT Y.GEN_INTERIM_STMT_ON_MVMT IS NULL
        )
        OR (
          NOT X.GEN_INTERIM_STMT_ON_MVMT IS NULL AND Y.GEN_INTERIM_STMT_ON_MVMT IS NULL
        )
        OR (
          X.GEN_INTERIM_STMT_ON_MVMT <> Y.GEN_INTERIM_STMT_ON_MVMT
        )
      )
      OR (
        (
          X.GEN_STMT_ONLY_ON_MVMT IS NULL AND NOT Y.GEN_STMT_ONLY_ON_MVMT IS NULL
        )
        OR (
          NOT X.GEN_STMT_ONLY_ON_MVMT IS NULL AND Y.GEN_STMT_ONLY_ON_MVMT IS NULL
        )
        OR (
          X.GEN_STMT_ONLY_ON_MVMT <> Y.GEN_STMT_ONLY_ON_MVMT
        )
      )
      OR (
        (
          X.GEN_STMT_ONLY_ON_MVMT2 IS NULL AND NOT Y.GEN_STMT_ONLY_ON_MVMT2 IS NULL
        )
        OR (
          NOT X.GEN_STMT_ONLY_ON_MVMT2 IS NULL AND Y.GEN_STMT_ONLY_ON_MVMT2 IS NULL
        )
        OR (
          X.GEN_STMT_ONLY_ON_MVMT2 <> Y.GEN_STMT_ONLY_ON_MVMT2
        )
      )
      OR (
        (
          X.GEN_STMT_ONLY_ON_MVMT3 IS NULL AND NOT Y.GEN_STMT_ONLY_ON_MVMT3 IS NULL
        )
        OR (
          NOT X.GEN_STMT_ONLY_ON_MVMT3 IS NULL AND Y.GEN_STMT_ONLY_ON_MVMT3 IS NULL
        )
        OR (
          X.GEN_STMT_ONLY_ON_MVMT3 <> Y.GEN_STMT_ONLY_ON_MVMT3
        )
      )
      OR (
        (
          X.HAS_TOV IS NULL AND NOT Y.HAS_TOV IS NULL
        )
        OR (
          NOT X.HAS_TOV IS NULL AND Y.HAS_TOV IS NULL
        )
        OR (
          X.HAS_TOV <> Y.HAS_TOV
        )
      )
      OR (
        (
          X.IBAN_AC_NO IS NULL AND NOT Y.IBAN_AC_NO IS NULL
        )
        OR (
          NOT X.IBAN_AC_NO IS NULL AND Y.IBAN_AC_NO IS NULL
        )
        OR (
          X.IBAN_AC_NO <> Y.IBAN_AC_NO
        )
      )
      OR (
        (
          X.INF_ACC_OPENING_AMT IS NULL AND NOT Y.INF_ACC_OPENING_AMT IS NULL
        )
        OR (
          NOT X.INF_ACC_OPENING_AMT IS NULL AND Y.INF_ACC_OPENING_AMT IS NULL
        )
        OR (
          X.INF_ACC_OPENING_AMT <> Y.INF_ACC_OPENING_AMT
        )
      )
      OR (
        (
          X.INF_OFFSET_ACCOUNT IS NULL AND NOT Y.INF_OFFSET_ACCOUNT IS NULL
        )
        OR (
          NOT X.INF_OFFSET_ACCOUNT IS NULL AND Y.INF_OFFSET_ACCOUNT IS NULL
        )
        OR (
          X.INF_OFFSET_ACCOUNT <> Y.INF_OFFSET_ACCOUNT
        )
      )
      OR (
        (
          X.INF_OFFSET_BRANCH IS NULL AND NOT Y.INF_OFFSET_BRANCH IS NULL
        )
        OR (
          NOT X.INF_OFFSET_BRANCH IS NULL AND Y.INF_OFFSET_BRANCH IS NULL
        )
        OR (
          X.INF_OFFSET_BRANCH <> Y.INF_OFFSET_BRANCH
        )
      )
      OR (
        (
          X.INF_PAY_IN_OPTION IS NULL AND NOT Y.INF_PAY_IN_OPTION IS NULL
        )
        OR (
          NOT X.INF_PAY_IN_OPTION IS NULL AND Y.INF_PAY_IN_OPTION IS NULL
        )
        OR (
          X.INF_PAY_IN_OPTION <> Y.INF_PAY_IN_OPTION
        )
      )
      OR (
        (
          X.INF_WAIVE_ACC_OPEN_CHARGE IS NULL AND NOT Y.INF_WAIVE_ACC_OPEN_CHARGE IS NULL
        )
        OR (
          NOT X.INF_WAIVE_ACC_OPEN_CHARGE IS NULL AND Y.INF_WAIVE_ACC_OPEN_CHARGE IS NULL
        )
        OR (
          X.INF_WAIVE_ACC_OPEN_CHARGE <> Y.INF_WAIVE_ACC_OPEN_CHARGE
        )
      )
      OR (
        (
          X.INHERIT_REPORTING IS NULL AND NOT Y.INHERIT_REPORTING IS NULL
        )
        OR (
          NOT X.INHERIT_REPORTING IS NULL AND Y.INHERIT_REPORTING IS NULL
        )
        OR (
          X.INHERIT_REPORTING <> Y.INHERIT_REPORTING
        )
      )
      OR (
        (
          X.INTERIM_CREDIT_AMT IS NULL AND NOT Y.INTERIM_CREDIT_AMT IS NULL
        )
        OR (
          NOT X.INTERIM_CREDIT_AMT IS NULL AND Y.INTERIM_CREDIT_AMT IS NULL
        )
        OR (
          X.INTERIM_CREDIT_AMT <> Y.INTERIM_CREDIT_AMT
        )
      )
      OR (
        (
          X.INTERIM_DEBIT_AMT IS NULL AND NOT Y.INTERIM_DEBIT_AMT IS NULL
        )
        OR (
          NOT X.INTERIM_DEBIT_AMT IS NULL AND Y.INTERIM_DEBIT_AMT IS NULL
        )
        OR (
          X.INTERIM_DEBIT_AMT <> Y.INTERIM_DEBIT_AMT
        )
      )
      OR (
        (
          X.INTERIM_REPORT_SINCE IS NULL AND NOT Y.INTERIM_REPORT_SINCE IS NULL
        )
        OR (
          NOT X.INTERIM_REPORT_SINCE IS NULL AND Y.INTERIM_REPORT_SINCE IS NULL
        )
        OR (
          X.INTERIM_REPORT_SINCE <> Y.INTERIM_REPORT_SINCE
        )
      )
      OR (
        (
          X.INTERIM_REPORT_TYPE IS NULL AND NOT Y.INTERIM_REPORT_TYPE IS NULL
        )
        OR (
          NOT X.INTERIM_REPORT_TYPE IS NULL AND Y.INTERIM_REPORT_TYPE IS NULL
        )
        OR (
          X.INTERIM_REPORT_TYPE <> Y.INTERIM_REPORT_TYPE
        )
      )
      OR (
        (
          X.INTERIM_STMT_DAY_COUNT IS NULL AND NOT Y.INTERIM_STMT_DAY_COUNT IS NULL
        )
        OR (
          NOT X.INTERIM_STMT_DAY_COUNT IS NULL AND Y.INTERIM_STMT_DAY_COUNT IS NULL
        )
        OR (
          X.INTERIM_STMT_DAY_COUNT <> Y.INTERIM_STMT_DAY_COUNT
        )
      )
      OR (
        (
          X.INTERIM_STMT_YTD_COUNT IS NULL AND NOT Y.INTERIM_STMT_YTD_COUNT IS NULL
        )
        OR (
          NOT X.INTERIM_STMT_YTD_COUNT IS NULL AND Y.INTERIM_STMT_YTD_COUNT IS NULL
        )
        OR (
          X.INTERIM_STMT_YTD_COUNT <> Y.INTERIM_STMT_YTD_COUNT
        )
      )
      OR (
        (
          X.INTERMEDIARY_REQUIRED IS NULL AND NOT Y.INTERMEDIARY_REQUIRED IS NULL
        )
        OR (
          NOT X.INTERMEDIARY_REQUIRED IS NULL AND Y.INTERMEDIARY_REQUIRED IS NULL
        )
        OR (
          X.INTERMEDIARY_REQUIRED <> Y.INTERMEDIARY_REQUIRED
        )
      )
      OR (
        (
          X.JOINT_AC_INDICATOR IS NULL AND NOT Y.JOINT_AC_INDICATOR IS NULL
        )
        OR (
          NOT X.JOINT_AC_INDICATOR IS NULL AND Y.JOINT_AC_INDICATOR IS NULL
        )
        OR (
          X.JOINT_AC_INDICATOR <> Y.JOINT_AC_INDICATOR
        )
      )
      OR (
        (
          X.LAST_CCY_CONV_DATE IS NULL AND NOT Y.LAST_CCY_CONV_DATE IS NULL
        )
        OR (
          NOT X.LAST_CCY_CONV_DATE IS NULL AND Y.LAST_CCY_CONV_DATE IS NULL
        )
        OR (
          X.LAST_CCY_CONV_DATE <> Y.LAST_CCY_CONV_DATE
        )
      )
      OR (
        (
          X.LAST_PURGE_DATE IS NULL AND NOT Y.LAST_PURGE_DATE IS NULL
        )
        OR (
          NOT X.LAST_PURGE_DATE IS NULL AND Y.LAST_PURGE_DATE IS NULL
        )
        OR (
          X.LAST_PURGE_DATE <> Y.LAST_PURGE_DATE
        )
      )
      OR (
        (
          X.LATEST_RUNBAL_SUBMMITED IS NULL AND NOT Y.LATEST_RUNBAL_SUBMMITED IS NULL
        )
        OR (
          NOT X.LATEST_RUNBAL_SUBMMITED IS NULL AND Y.LATEST_RUNBAL_SUBMMITED IS NULL
        )
        OR (
          X.LATEST_RUNBAL_SUBMMITED <> Y.LATEST_RUNBAL_SUBMMITED
        )
      )
      OR (
        (
          X.LATEST_SRNO_SUBMITTED IS NULL AND NOT Y.LATEST_SRNO_SUBMITTED IS NULL
        )
        OR (
          NOT X.LATEST_SRNO_SUBMITTED IS NULL AND Y.LATEST_SRNO_SUBMITTED IS NULL
        )
        OR (
          X.LATEST_SRNO_SUBMITTED <> Y.LATEST_SRNO_SUBMITTED
        )
      )
      OR (
        (
          X.LCY_CURR_BALANCE IS NULL AND NOT Y.LCY_CURR_BALANCE IS NULL
        )
        OR (
          NOT X.LCY_CURR_BALANCE IS NULL AND Y.LCY_CURR_BALANCE IS NULL
        )
        OR (
          X.LCY_CURR_BALANCE <> Y.LCY_CURR_BALANCE
        )
      )
      OR (
        (
          X.LCY_MTD_TOVER_CR IS NULL AND NOT Y.LCY_MTD_TOVER_CR IS NULL
        )
        OR (
          NOT X.LCY_MTD_TOVER_CR IS NULL AND Y.LCY_MTD_TOVER_CR IS NULL
        )
        OR (
          X.LCY_MTD_TOVER_CR <> Y.LCY_MTD_TOVER_CR
        )
      )
      OR (
        (
          X.LCY_MTD_TOVER_DR IS NULL AND NOT Y.LCY_MTD_TOVER_DR IS NULL
        )
        OR (
          NOT X.LCY_MTD_TOVER_DR IS NULL AND Y.LCY_MTD_TOVER_DR IS NULL
        )
        OR (
          X.LCY_MTD_TOVER_DR <> Y.LCY_MTD_TOVER_DR
        )
      )
      OR (
        (
          X.LCY_OPENING_BAL IS NULL AND NOT Y.LCY_OPENING_BAL IS NULL
        )
        OR (
          NOT X.LCY_OPENING_BAL IS NULL AND Y.LCY_OPENING_BAL IS NULL
        )
        OR (
          X.LCY_OPENING_BAL <> Y.LCY_OPENING_BAL
        )
      )
      OR (
        (
          X.LCY_TANK_CR IS NULL AND NOT Y.LCY_TANK_CR IS NULL
        )
        OR (
          NOT X.LCY_TANK_CR IS NULL AND Y.LCY_TANK_CR IS NULL
        )
        OR (
          X.LCY_TANK_CR <> Y.LCY_TANK_CR
        )
      )
      OR (
        (
          X.LCY_TANK_DR IS NULL AND NOT Y.LCY_TANK_DR IS NULL
        )
        OR (
          NOT X.LCY_TANK_DR IS NULL AND Y.LCY_TANK_DR IS NULL
        )
        OR (
          X.LCY_TANK_DR <> Y.LCY_TANK_DR
        )
      )
      OR (
        (
          X.LCY_TODAY_TOVER_CR IS NULL AND NOT Y.LCY_TODAY_TOVER_CR IS NULL
        )
        OR (
          NOT X.LCY_TODAY_TOVER_CR IS NULL AND Y.LCY_TODAY_TOVER_CR IS NULL
        )
        OR (
          X.LCY_TODAY_TOVER_CR <> Y.LCY_TODAY_TOVER_CR
        )
      )
      OR (
        (
          X.LCY_TODAY_TOVER_DR IS NULL AND NOT Y.LCY_TODAY_TOVER_DR IS NULL
        )
        OR (
          NOT X.LCY_TODAY_TOVER_DR IS NULL AND Y.LCY_TODAY_TOVER_DR IS NULL
        )
        OR (
          X.LCY_TODAY_TOVER_DR <> Y.LCY_TODAY_TOVER_DR
        )
      )
      OR (
        (
          X.LCY_TOVER_CR IS NULL AND NOT Y.LCY_TOVER_CR IS NULL
        )
        OR (
          NOT X.LCY_TOVER_CR IS NULL AND Y.LCY_TOVER_CR IS NULL
        )
        OR (
          X.LCY_TOVER_CR <> Y.LCY_TOVER_CR
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
          X.LINE_ID IS NULL AND NOT Y.LINE_ID IS NULL
        )
        OR (
          NOT X.LINE_ID IS NULL AND Y.LINE_ID IS NULL
        )
        OR (
          X.LINE_ID <> Y.LINE_ID
        )
      )
      OR (
        (
          X.LINKED_DEP_ACC IS NULL AND NOT Y.LINKED_DEP_ACC IS NULL
        )
        OR (
          NOT X.LINKED_DEP_ACC IS NULL AND Y.LINKED_DEP_ACC IS NULL
        )
        OR (
          X.LINKED_DEP_ACC <> Y.LINKED_DEP_ACC
        )
      )
      OR (
        (
          X.LINKED_DEP_BRANCH IS NULL AND NOT Y.LINKED_DEP_BRANCH IS NULL
        )
        OR (
          NOT X.LINKED_DEP_BRANCH IS NULL AND Y.LINKED_DEP_BRANCH IS NULL
        )
        OR (
          X.LINKED_DEP_BRANCH <> Y.LINKED_DEP_BRANCH
        )
      )
      OR (
        (
          X.LOCATION IS NULL AND NOT Y.LOCATION IS NULL
        )
        OR (
          NOT X.LOCATION IS NULL AND Y.LOCATION IS NULL
        )
        OR (
          X.LOCATION <> Y.LOCATION
        )
      )
      OR (
        (
          X.LODGEMENT_BOOK_FACILITY IS NULL AND NOT Y.LODGEMENT_BOOK_FACILITY IS NULL
        )
        OR (
          NOT X.LODGEMENT_BOOK_FACILITY IS NULL AND Y.LODGEMENT_BOOK_FACILITY IS NULL
        )
        OR (
          X.LODGEMENT_BOOK_FACILITY <> Y.LODGEMENT_BOOK_FACILITY
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
          X.MASTER_ACCOUNT_NO IS NULL AND NOT Y.MASTER_ACCOUNT_NO IS NULL
        )
        OR (
          NOT X.MASTER_ACCOUNT_NO IS NULL AND Y.MASTER_ACCOUNT_NO IS NULL
        )
        OR (
          X.MASTER_ACCOUNT_NO <> Y.MASTER_ACCOUNT_NO
        )
      )
      OR (
        (
          X.MAX_NO_CHEQUE_REJECTIONS IS NULL AND NOT Y.MAX_NO_CHEQUE_REJECTIONS IS NULL
        )
        OR (
          NOT X.MAX_NO_CHEQUE_REJECTIONS IS NULL AND Y.MAX_NO_CHEQUE_REJECTIONS IS NULL
        )
        OR (
          X.MAX_NO_CHEQUE_REJECTIONS <> Y.MAX_NO_CHEQUE_REJECTIONS
        )
      )
      OR (
        (
          X.MEDIA IS NULL AND NOT Y.MEDIA IS NULL
        )
        OR (
          NOT X.MEDIA IS NULL AND Y.MEDIA IS NULL
        )
        OR (
          X.MEDIA <> Y.MEDIA
        )
      )
      OR (
        (
          X.MIN_REQD_BAL IS NULL AND NOT Y.MIN_REQD_BAL IS NULL
        )
        OR (
          NOT X.MIN_REQD_BAL IS NULL AND Y.MIN_REQD_BAL IS NULL
        )
        OR (
          X.MIN_REQD_BAL <> Y.MIN_REQD_BAL
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
          X.MOD9_VALIDATION_REQD IS NULL AND NOT Y.MOD9_VALIDATION_REQD IS NULL
        )
        OR (
          NOT X.MOD9_VALIDATION_REQD IS NULL AND Y.MOD9_VALIDATION_REQD IS NULL
        )
        OR (
          X.MOD9_VALIDATION_REQD <> Y.MOD9_VALIDATION_REQD
        )
      )
      OR (
        (
          X.MODE_OF_OPERATION IS NULL AND NOT Y.MODE_OF_OPERATION IS NULL
        )
        OR (
          NOT X.MODE_OF_OPERATION IS NULL AND Y.MODE_OF_OPERATION IS NULL
        )
        OR (
          X.MODE_OF_OPERATION <> Y.MODE_OF_OPERATION
        )
      )
      OR (
        (
          X.MT110_RECON_REQD IS NULL AND NOT Y.MT110_RECON_REQD IS NULL
        )
        OR (
          NOT X.MT110_RECON_REQD IS NULL AND Y.MT110_RECON_REQD IS NULL
        )
        OR (
          X.MT110_RECON_REQD <> Y.MT110_RECON_REQD
        )
      )
      OR (
        (
          X.MT210_REQD IS NULL AND NOT Y.MT210_REQD IS NULL
        )
        OR (
          NOT X.MT210_REQD IS NULL AND Y.MT210_REQD IS NULL
        )
        OR (
          X.MT210_REQD <> Y.MT210_REQD
        )
      )
      OR (
        (
          X.MUDARABAH_ACCOUNTS IS NULL AND NOT Y.MUDARABAH_ACCOUNTS IS NULL
        )
        OR (
          NOT X.MUDARABAH_ACCOUNTS IS NULL AND Y.MUDARABAH_ACCOUNTS IS NULL
        )
        OR (
          X.MUDARABAH_ACCOUNTS <> Y.MUDARABAH_ACCOUNTS
        )
      )
      OR (
        (
          X.SK_CUST_NO IS NULL AND NOT Y.SK_CUST_NO IS NULL
        )
        OR (
          NOT X.SK_CUST_NO IS NULL AND Y.SK_CUST_NO IS NULL
        )
        OR (
          X.SK_CUST_NO <> Y.SK_CUST_NO
        )
      )
      OR (
        (
          X.NETTING_REQUIRED IS NULL AND NOT Y.NETTING_REQUIRED IS NULL
        )
        OR (
          NOT X.NETTING_REQUIRED IS NULL AND Y.NETTING_REQUIRED IS NULL
        )
        OR (
          X.NETTING_REQUIRED <> Y.NETTING_REQUIRED
        )
      )
      OR (
        (
          X.NO_CHEQUE_REJECTIONS IS NULL AND NOT Y.NO_CHEQUE_REJECTIONS IS NULL
        )
        OR (
          NOT X.NO_CHEQUE_REJECTIONS IS NULL AND Y.NO_CHEQUE_REJECTIONS IS NULL
        )
        OR (
          X.NO_CHEQUE_REJECTIONS <> Y.NO_CHEQUE_REJECTIONS
        )
      )
      OR (
        (
          X.NO_OF_CHQ_REJ_RESET_ON IS NULL AND NOT Y.NO_OF_CHQ_REJ_RESET_ON IS NULL
        )
        OR (
          NOT X.NO_OF_CHQ_REJ_RESET_ON IS NULL AND Y.NO_OF_CHQ_REJ_RESET_ON IS NULL
        )
        OR (
          X.NO_OF_CHQ_REJ_RESET_ON <> Y.NO_OF_CHQ_REJ_RESET_ON
        )
      )
      OR (
        (
          X.NOMINEE1 IS NULL AND NOT Y.NOMINEE1 IS NULL
        )
        OR (
          NOT X.NOMINEE1 IS NULL AND Y.NOMINEE1 IS NULL
        )
        OR (
          X.NOMINEE1 <> Y.NOMINEE1
        )
      )
      OR (
        (
          X.NOMINEE2 IS NULL AND NOT Y.NOMINEE2 IS NULL
        )
        OR (
          NOT X.NOMINEE2 IS NULL AND Y.NOMINEE2 IS NULL
        )
        OR (
          X.NOMINEE2 <> Y.NOMINEE2
        )
      )
      OR (
        (
          X.NSF_AUTO_UPDATE IS NULL AND NOT Y.NSF_AUTO_UPDATE IS NULL
        )
        OR (
          NOT X.NSF_AUTO_UPDATE IS NULL AND Y.NSF_AUTO_UPDATE IS NULL
        )
        OR (
          X.NSF_AUTO_UPDATE <> Y.NSF_AUTO_UPDATE
        )
      )
      OR (
        (
          X.NSF_BLACKLIST_STATUS IS NULL AND NOT Y.NSF_BLACKLIST_STATUS IS NULL
        )
        OR (
          NOT X.NSF_BLACKLIST_STATUS IS NULL AND Y.NSF_BLACKLIST_STATUS IS NULL
        )
        OR (
          X.NSF_BLACKLIST_STATUS <> Y.NSF_BLACKLIST_STATUS
        )
      )
      OR (
        (
          X.OFFLINE_LIMIT IS NULL AND NOT Y.OFFLINE_LIMIT IS NULL
        )
        OR (
          NOT X.OFFLINE_LIMIT IS NULL AND Y.OFFLINE_LIMIT IS NULL
        )
        OR (
          X.OFFLINE_LIMIT <> Y.OFFLINE_LIMIT
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
          X.ORG_FUNC_ID IS NULL AND NOT Y.ORG_FUNC_ID IS NULL
        )
        OR (
          NOT X.ORG_FUNC_ID IS NULL AND Y.ORG_FUNC_ID IS NULL
        )
        OR (
          X.ORG_FUNC_ID <> Y.ORG_FUNC_ID
        )
      )
      OR (
        (
          X.OVERDRAFT_SINCE IS NULL AND NOT Y.OVERDRAFT_SINCE IS NULL
        )
        OR (
          NOT X.OVERDRAFT_SINCE IS NULL AND Y.OVERDRAFT_SINCE IS NULL
        )
        OR (
          X.OVERDRAFT_SINCE <> Y.OVERDRAFT_SINCE
        )
      )
      OR (
        (
          X.OVERLINE_OD_SINCE IS NULL AND NOT Y.OVERLINE_OD_SINCE IS NULL
        )
        OR (
          NOT X.OVERLINE_OD_SINCE IS NULL AND Y.OVERLINE_OD_SINCE IS NULL
        )
        OR (
          X.OVERLINE_OD_SINCE <> Y.OVERLINE_OD_SINCE
        )
      )
      OR (
        (
          X.PASSBOOK_FACILITY IS NULL AND NOT Y.PASSBOOK_FACILITY IS NULL
        )
        OR (
          NOT X.PASSBOOK_FACILITY IS NULL AND Y.PASSBOOK_FACILITY IS NULL
        )
        OR (
          X.PASSBOOK_FACILITY <> Y.PASSBOOK_FACILITY
        )
      )
      OR (
        (
          X.PASSBOOK_NUMBER IS NULL AND NOT Y.PASSBOOK_NUMBER IS NULL
        )
        OR (
          NOT X.PASSBOOK_NUMBER IS NULL AND Y.PASSBOOK_NUMBER IS NULL
        )
        OR (
          X.PASSBOOK_NUMBER <> Y.PASSBOOK_NUMBER
        )
      )
      OR (
        (
          X.PINCODE IS NULL AND NOT Y.PINCODE IS NULL
        )
        OR (
          NOT X.PINCODE IS NULL AND Y.PINCODE IS NULL
        )
        OR (
          X.PINCODE <> Y.PINCODE
        )
      )
      OR (
        (
          X.POSITIVE_PAY_AC IS NULL AND NOT Y.POSITIVE_PAY_AC IS NULL
        )
        OR (
          NOT X.POSITIVE_PAY_AC IS NULL AND Y.POSITIVE_PAY_AC IS NULL
        )
        OR (
          X.POSITIVE_PAY_AC <> Y.POSITIVE_PAY_AC
        )
      )
      OR (
        (
          X.PREV_AC_SRNO_PRINTED_IN_PBK IS NULL
          AND NOT Y.PREV_AC_SRNO_PRINTED_IN_PBK IS NULL
        )
        OR (
          NOT X.PREV_AC_SRNO_PRINTED_IN_PBK IS NULL
          AND Y.PREV_AC_SRNO_PRINTED_IN_PBK IS NULL
        )
        OR (
          X.PREV_AC_SRNO_PRINTED_IN_PBK <> Y.PREV_AC_SRNO_PRINTED_IN_PBK
        )
      )
      OR (
        (
          X.PREV_LINE_NO IS NULL AND NOT Y.PREV_LINE_NO IS NULL
        )
        OR (
          NOT X.PREV_LINE_NO IS NULL AND Y.PREV_LINE_NO IS NULL
        )
        OR (
          X.PREV_LINE_NO <> Y.PREV_LINE_NO
        )
      )
      OR (
        (
          X.PREV_OVD_DATE IS NULL AND NOT Y.PREV_OVD_DATE IS NULL
        )
        OR (
          NOT X.PREV_OVD_DATE IS NULL AND Y.PREV_OVD_DATE IS NULL
        )
        OR (
          X.PREV_OVD_DATE <> Y.PREV_OVD_DATE
        )
      )
      OR (
        (
          X.PREV_PAGE_NO IS NULL AND NOT Y.PREV_PAGE_NO IS NULL
        )
        OR (
          NOT X.PREV_PAGE_NO IS NULL AND Y.PREV_PAGE_NO IS NULL
        )
        OR (
          X.PREV_PAGE_NO <> Y.PREV_PAGE_NO
        )
      )
      OR (
        (
          X.PREV_RUNBAL_PRINTED_IN_PBK IS NULL AND NOT Y.PREV_RUNBAL_PRINTED_IN_PBK IS NULL
        )
        OR (
          NOT X.PREV_RUNBAL_PRINTED_IN_PBK IS NULL AND Y.PREV_RUNBAL_PRINTED_IN_PBK IS NULL
        )
        OR (
          X.PREV_RUNBAL_PRINTED_IN_PBK <> Y.PREV_RUNBAL_PRINTED_IN_PBK
        )
      )
      OR (
        (
          X.PREV_TOD_SINCE IS NULL AND NOT Y.PREV_TOD_SINCE IS NULL
        )
        OR (
          NOT X.PREV_TOD_SINCE IS NULL AND Y.PREV_TOD_SINCE IS NULL
        )
        OR (
          X.PREV_TOD_SINCE <> Y.PREV_TOD_SINCE
        )
      )
      OR (
        (
          X.PREVIOUS_STATEMENT_BALANCE IS NULL AND NOT Y.PREVIOUS_STATEMENT_BALANCE IS NULL
        )
        OR (
          NOT X.PREVIOUS_STATEMENT_BALANCE IS NULL AND Y.PREVIOUS_STATEMENT_BALANCE IS NULL
        )
        OR (
          X.PREVIOUS_STATEMENT_BALANCE <> Y.PREVIOUS_STATEMENT_BALANCE
        )
      )
      OR (
        (
          X.PREVIOUS_STATEMENT_BALANCE2 IS NULL
          AND NOT Y.PREVIOUS_STATEMENT_BALANCE2 IS NULL
        )
        OR (
          NOT X.PREVIOUS_STATEMENT_BALANCE2 IS NULL
          AND Y.PREVIOUS_STATEMENT_BALANCE2 IS NULL
        )
        OR (
          X.PREVIOUS_STATEMENT_BALANCE2 <> Y.PREVIOUS_STATEMENT_BALANCE2
        )
      )
      OR (
        (
          X.PREVIOUS_STATEMENT_BALANCE3 IS NULL
          AND NOT Y.PREVIOUS_STATEMENT_BALANCE3 IS NULL
        )
        OR (
          NOT X.PREVIOUS_STATEMENT_BALANCE3 IS NULL
          AND Y.PREVIOUS_STATEMENT_BALANCE3 IS NULL
        )
        OR (
          X.PREVIOUS_STATEMENT_BALANCE3 <> Y.PREVIOUS_STATEMENT_BALANCE3
        )
      )
      OR (
        (
          X.PREVIOUS_STATEMENT_DATE IS NULL AND NOT Y.PREVIOUS_STATEMENT_DATE IS NULL
        )
        OR (
          NOT X.PREVIOUS_STATEMENT_DATE IS NULL AND Y.PREVIOUS_STATEMENT_DATE IS NULL
        )
        OR (
          X.PREVIOUS_STATEMENT_DATE <> Y.PREVIOUS_STATEMENT_DATE
        )
      )
      OR (
        (
          X.PREVIOUS_STATEMENT_DATE2 IS NULL AND NOT Y.PREVIOUS_STATEMENT_DATE2 IS NULL
        )
        OR (
          NOT X.PREVIOUS_STATEMENT_DATE2 IS NULL AND Y.PREVIOUS_STATEMENT_DATE2 IS NULL
        )
        OR (
          X.PREVIOUS_STATEMENT_DATE2 <> Y.PREVIOUS_STATEMENT_DATE2
        )
      )
      OR (
        (
          X.PREVIOUS_STATEMENT_DATE3 IS NULL AND NOT Y.PREVIOUS_STATEMENT_DATE3 IS NULL
        )
        OR (
          NOT X.PREVIOUS_STATEMENT_DATE3 IS NULL AND Y.PREVIOUS_STATEMENT_DATE3 IS NULL
        )
        OR (
          X.PREVIOUS_STATEMENT_DATE3 <> Y.PREVIOUS_STATEMENT_DATE3
        )
      )
      OR (
        (
          X.PREVIOUS_STATEMENT_NO IS NULL AND NOT Y.PREVIOUS_STATEMENT_NO IS NULL
        )
        OR (
          NOT X.PREVIOUS_STATEMENT_NO IS NULL AND Y.PREVIOUS_STATEMENT_NO IS NULL
        )
        OR (
          X.PREVIOUS_STATEMENT_NO <> Y.PREVIOUS_STATEMENT_NO
        )
      )
      OR (
        (
          X.PREVIOUS_STATEMENT_NO2 IS NULL AND NOT Y.PREVIOUS_STATEMENT_NO2 IS NULL
        )
        OR (
          NOT X.PREVIOUS_STATEMENT_NO2 IS NULL AND Y.PREVIOUS_STATEMENT_NO2 IS NULL
        )
        OR (
          X.PREVIOUS_STATEMENT_NO2 <> Y.PREVIOUS_STATEMENT_NO2
        )
      )
      OR (
        (
          X.PREVIOUS_STATEMENT_NO3 IS NULL AND NOT Y.PREVIOUS_STATEMENT_NO3 IS NULL
        )
        OR (
          NOT X.PREVIOUS_STATEMENT_NO3 IS NULL AND Y.PREVIOUS_STATEMENT_NO3 IS NULL
        )
        OR (
          X.PREVIOUS_STATEMENT_NO3 <> Y.PREVIOUS_STATEMENT_NO3
        )
      )
      OR (
        (
          X.PRODUCT_LIST IS NULL AND NOT Y.PRODUCT_LIST IS NULL
        )
        OR (
          NOT X.PRODUCT_LIST IS NULL AND Y.PRODUCT_LIST IS NULL
        )
        OR (
          X.PRODUCT_LIST <> Y.PRODUCT_LIST
        )
      )
      OR (
        (
          X.PROJECT_ACCOUNT IS NULL AND NOT Y.PROJECT_ACCOUNT IS NULL
        )
        OR (
          NOT X.PROJECT_ACCOUNT IS NULL AND Y.PROJECT_ACCOUNT IS NULL
        )
        OR (
          X.PROJECT_ACCOUNT <> Y.PROJECT_ACCOUNT
        )
      )
      OR (
        (
          X.PROV_CCY_TYPE IS NULL AND NOT Y.PROV_CCY_TYPE IS NULL
        )
        OR (
          NOT X.PROV_CCY_TYPE IS NULL AND Y.PROV_CCY_TYPE IS NULL
        )
        OR (
          X.PROV_CCY_TYPE <> Y.PROV_CCY_TYPE
        )
      )
      OR (
        (
          X.PROVISION_AMOUNT IS NULL AND NOT Y.PROVISION_AMOUNT IS NULL
        )
        OR (
          NOT X.PROVISION_AMOUNT IS NULL AND Y.PROVISION_AMOUNT IS NULL
        )
        OR (
          X.PROVISION_AMOUNT <> Y.PROVISION_AMOUNT
        )
      )
      OR (
        (
          X.RECEIVABLE_AMOUNT IS NULL AND NOT Y.RECEIVABLE_AMOUNT IS NULL
        )
        OR (
          NOT X.RECEIVABLE_AMOUNT IS NULL AND Y.RECEIVABLE_AMOUNT IS NULL
        )
        OR (
          X.RECEIVABLE_AMOUNT <> Y.RECEIVABLE_AMOUNT
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
          X.REFERRAL_REQUIRED IS NULL AND NOT Y.REFERRAL_REQUIRED IS NULL
        )
        OR (
          NOT X.REFERRAL_REQUIRED IS NULL AND Y.REFERRAL_REQUIRED IS NULL
        )
        OR (
          X.REFERRAL_REQUIRED <> Y.REFERRAL_REQUIRED
        )
      )
      OR (
        (
          X.REG_CC_AVAILABILITY IS NULL AND NOT Y.REG_CC_AVAILABILITY IS NULL
        )
        OR (
          NOT X.REG_CC_AVAILABILITY IS NULL AND Y.REG_CC_AVAILABILITY IS NULL
        )
        OR (
          X.REG_CC_AVAILABILITY <> Y.REG_CC_AVAILABILITY
        )
      )
      OR (
        (
          X.REG_CC_AVAILABLE_FUNDS IS NULL AND NOT Y.REG_CC_AVAILABLE_FUNDS IS NULL
        )
        OR (
          NOT X.REG_CC_AVAILABLE_FUNDS IS NULL AND Y.REG_CC_AVAILABLE_FUNDS IS NULL
        )
        OR (
          X.REG_CC_AVAILABLE_FUNDS <> Y.REG_CC_AVAILABLE_FUNDS
        )
      )
      OR (
        (
          X.REG_D_APPLICABLE IS NULL AND NOT Y.REG_D_APPLICABLE IS NULL
        )
        OR (
          NOT X.REG_D_APPLICABLE IS NULL AND Y.REG_D_APPLICABLE IS NULL
        )
        OR (
          X.REG_D_APPLICABLE <> Y.REG_D_APPLICABLE
        )
      )
      OR (
        (
          X.REGD_END_DATE IS NULL AND NOT Y.REGD_END_DATE IS NULL
        )
        OR (
          NOT X.REGD_END_DATE IS NULL AND Y.REGD_END_DATE IS NULL
        )
        OR (
          X.REGD_END_DATE <> Y.REGD_END_DATE
        )
      )
      OR (
        (
          X.REGD_PERIODICITY IS NULL AND NOT Y.REGD_PERIODICITY IS NULL
        )
        OR (
          NOT X.REGD_PERIODICITY IS NULL AND Y.REGD_PERIODICITY IS NULL
        )
        OR (
          X.REGD_PERIODICITY <> Y.REGD_PERIODICITY
        )
      )
      OR (
        (
          X.REGD_START_DATE IS NULL AND NOT Y.REGD_START_DATE IS NULL
        )
        OR (
          NOT X.REGD_START_DATE IS NULL AND Y.REGD_START_DATE IS NULL
        )
        OR (
          X.REGD_START_DATE <> Y.REGD_START_DATE
        )
      )
      OR (
        (
          X.REPL_CUST_SIG IS NULL AND NOT Y.REPL_CUST_SIG IS NULL
        )
        OR (
          NOT X.REPL_CUST_SIG IS NULL AND Y.REPL_CUST_SIG IS NULL
        )
        OR (
          X.REPL_CUST_SIG <> Y.REPL_CUST_SIG
        )
      )
      OR (
        (
          X.RISK_FREE_EXP_AMOUNT IS NULL AND NOT Y.RISK_FREE_EXP_AMOUNT IS NULL
        )
        OR (
          NOT X.RISK_FREE_EXP_AMOUNT IS NULL AND Y.RISK_FREE_EXP_AMOUNT IS NULL
        )
        OR (
          X.RISK_FREE_EXP_AMOUNT <> Y.RISK_FREE_EXP_AMOUNT
        )
      )
      OR (
        (
          X.SALARY_ACCOUNT IS NULL AND NOT Y.SALARY_ACCOUNT IS NULL
        )
        OR (
          NOT X.SALARY_ACCOUNT IS NULL AND Y.SALARY_ACCOUNT IS NULL
        )
        OR (
          X.SALARY_ACCOUNT <> Y.SALARY_ACCOUNT
        )
      )
      OR (
        (
          X.SK_ACCOUNT_CLASS IS NULL AND NOT Y.SK_ACCOUNT_CLASS IS NULL
        )
        OR (
          NOT X.SK_ACCOUNT_CLASS IS NULL AND Y.SK_ACCOUNT_CLASS IS NULL
        )
        OR (
          X.SK_ACCOUNT_CLASS <> Y.SK_ACCOUNT_CLASS
        )
      )
      OR (
        (
          X.SK_ADDRESS1_ADDRESS2_ADDRESS3_ADDRESS4 IS NULL
          AND NOT Y.SK_ADDRESS1_ADDRESS2_ADDRESS3_ADDRESS4 IS NULL
        )
        OR (
          NOT X.SK_ADDRESS1_ADDRESS2_ADDRESS3_ADDRESS4 IS NULL
          AND Y.SK_ADDRESS1_ADDRESS2_ADDRESS3_ADDRESS4 IS NULL
        )
        OR (
          X.SK_ADDRESS1_ADDRESS2_ADDRESS3_ADDRESS4 <> Y.SK_ADDRESS1_ADDRESS2_ADDRESS3_ADDRESS4
        )
      )
      OR (
        (
          X.SK_ATM_FACILITY IS NULL AND NOT Y.SK_ATM_FACILITY IS NULL
        )
        OR (
          NOT X.SK_ATM_FACILITY IS NULL AND Y.SK_ATM_FACILITY IS NULL
        )
        OR (
          X.SK_ATM_FACILITY <> Y.SK_ATM_FACILITY
        )
      )
      OR (
        (
          X.SK_BRANCH_CODE IS NULL AND NOT Y.SK_BRANCH_CODE IS NULL
        )
        OR (
          NOT X.SK_BRANCH_CODE IS NULL AND Y.SK_BRANCH_CODE IS NULL
        )
        OR (
          X.SK_BRANCH_CODE <> Y.SK_BRANCH_CODE
        )
      )
      OR (
        (
          X.SK_CHEQUE_BOOK_FACILITY IS NULL AND NOT Y.SK_CHEQUE_BOOK_FACILITY IS NULL
        )
        OR (
          NOT X.SK_CHEQUE_BOOK_FACILITY IS NULL AND Y.SK_CHEQUE_BOOK_FACILITY IS NULL
        )
        OR (
          X.SK_CHEQUE_BOOK_FACILITY <> Y.SK_CHEQUE_BOOK_FACILITY
        )
      )
      OR (
        (
          X.SK_CLEARING_AC_NO IS NULL AND NOT Y.SK_CLEARING_AC_NO IS NULL
        )
        OR (
          NOT X.SK_CLEARING_AC_NO IS NULL AND Y.SK_CLEARING_AC_NO IS NULL
        )
        OR (
          X.SK_CLEARING_AC_NO <> Y.SK_CLEARING_AC_NO
        )
      )
      OR (
        (
          X.SK_CUST_AC_NO IS NULL AND NOT Y.SK_CUST_AC_NO IS NULL
        )
        OR (
          NOT X.SK_CUST_AC_NO IS NULL AND Y.SK_CUST_AC_NO IS NULL
        )
        OR (
          X.SK_CUST_AC_NO <> Y.SK_CUST_AC_NO
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
      OR (
        (
          X.SK_LOCATION IS NULL AND NOT Y.SK_LOCATION IS NULL
        )
        OR (
          NOT X.SK_LOCATION IS NULL AND Y.SK_LOCATION IS NULL
        )
        OR (
          X.SK_LOCATION <> Y.SK_LOCATION
        )
      )
      OR (
        (
          X.SK_MIN_REQD_BAL IS NULL AND NOT Y.SK_MIN_REQD_BAL IS NULL
        )
        OR (
          NOT X.SK_MIN_REQD_BAL IS NULL AND Y.SK_MIN_REQD_BAL IS NULL
        )
        OR (
          X.SK_MIN_REQD_BAL <> Y.SK_MIN_REQD_BAL
        )
      )
      OR (
        (
          X.SK_SWEEP_OUT IS NULL AND NOT Y.SK_SWEEP_OUT IS NULL
        )
        OR (
          NOT X.SK_SWEEP_OUT IS NULL AND Y.SK_SWEEP_OUT IS NULL
        )
        OR (
          X.SK_SWEEP_OUT <> Y.SK_SWEEP_OUT
        )
      )
      OR (
        (
          X.SK_SWEEP_REQUIRED IS NULL AND NOT Y.SK_SWEEP_REQUIRED IS NULL
        )
        OR (
          NOT X.SK_SWEEP_REQUIRED IS NULL AND Y.SK_SWEEP_REQUIRED IS NULL
        )
        OR (
          X.SK_SWEEP_REQUIRED <> Y.SK_SWEEP_REQUIRED
        )
      )
      OR (
        (
          X.SK_SWEEP_TYPE IS NULL AND NOT Y.SK_SWEEP_TYPE IS NULL
        )
        OR (
          NOT X.SK_SWEEP_TYPE IS NULL AND Y.SK_SWEEP_TYPE IS NULL
        )
        OR (
          X.SK_SWEEP_TYPE <> Y.SK_SWEEP_TYPE
        )
      )
      OR (
        (
          X.SOD_NOTIFICATION_PERCENT IS NULL AND NOT Y.SOD_NOTIFICATION_PERCENT IS NULL
        )
        OR (
          NOT X.SOD_NOTIFICATION_PERCENT IS NULL AND Y.SOD_NOTIFICATION_PERCENT IS NULL
        )
        OR (
          X.SOD_NOTIFICATION_PERCENT <> Y.SOD_NOTIFICATION_PERCENT
        )
      )
      OR (
        (
          X.SPECIAL_CONDITION_PRODUCT IS NULL AND NOT Y.SPECIAL_CONDITION_PRODUCT IS NULL
        )
        OR (
          NOT X.SPECIAL_CONDITION_PRODUCT IS NULL AND Y.SPECIAL_CONDITION_PRODUCT IS NULL
        )
        OR (
          X.SPECIAL_CONDITION_PRODUCT <> Y.SPECIAL_CONDITION_PRODUCT
        )
      )
      OR (
        (
          X.SPECIAL_CONDITION_TXNCODE IS NULL AND NOT Y.SPECIAL_CONDITION_TXNCODE IS NULL
        )
        OR (
          NOT X.SPECIAL_CONDITION_TXNCODE IS NULL AND Y.SPECIAL_CONDITION_TXNCODE IS NULL
        )
        OR (
          X.SPECIAL_CONDITION_TXNCODE <> Y.SPECIAL_CONDITION_TXNCODE
        )
      )
      OR (
        (
          X.SPEND_ANALYSIS_REQD IS NULL AND NOT Y.SPEND_ANALYSIS_REQD IS NULL
        )
        OR (
          NOT X.SPEND_ANALYSIS_REQD IS NULL AND Y.SPEND_ANALYSIS_REQD IS NULL
        )
        OR (
          X.SPEND_ANALYSIS_REQD <> Y.SPEND_ANALYSIS_REQD
        )
      )
      OR (
        (
          X.STALE_DAYS IS NULL AND NOT Y.STALE_DAYS IS NULL
        )
        OR (
          NOT X.STALE_DAYS IS NULL AND Y.STALE_DAYS IS NULL
        )
        OR (
          X.STALE_DAYS <> Y.STALE_DAYS
        )
      )
      OR (
        (
          X.STATEMENT_ACCOUNT IS NULL AND NOT Y.STATEMENT_ACCOUNT IS NULL
        )
        OR (
          NOT X.STATEMENT_ACCOUNT IS NULL AND Y.STATEMENT_ACCOUNT IS NULL
        )
        OR (
          X.STATEMENT_ACCOUNT <> Y.STATEMENT_ACCOUNT
        )
      )
      OR (
        (
          X.STATUS_CHANGE_AUTOMATIC IS NULL AND NOT Y.STATUS_CHANGE_AUTOMATIC IS NULL
        )
        OR (
          NOT X.STATUS_CHANGE_AUTOMATIC IS NULL AND Y.STATUS_CHANGE_AUTOMATIC IS NULL
        )
        OR (
          X.STATUS_CHANGE_AUTOMATIC <> Y.STATUS_CHANGE_AUTOMATIC
        )
      )
      OR (
        (
          X.STATUS_SINCE IS NULL AND NOT Y.STATUS_SINCE IS NULL
        )
        OR (
          NOT X.STATUS_SINCE IS NULL AND Y.STATUS_SINCE IS NULL
        )
        OR (
          X.STATUS_SINCE <> Y.STATUS_SINCE
        )
      )
      OR (
        (
          X.SUBLIMIT IS NULL AND NOT Y.SUBLIMIT IS NULL
        )
        OR (
          NOT X.SUBLIMIT IS NULL AND Y.SUBLIMIT IS NULL
        )
        OR (
          X.SUBLIMIT <> Y.SUBLIMIT
        )
      )
      OR (
        (
          X.SWEEP_OUT IS NULL AND NOT Y.SWEEP_OUT IS NULL
        )
        OR (
          NOT X.SWEEP_OUT IS NULL AND Y.SWEEP_OUT IS NULL
        )
        OR (
          X.SWEEP_OUT <> Y.SWEEP_OUT
        )
      )
      OR (
        (
          X.SWEEP_REQUIRED IS NULL AND NOT Y.SWEEP_REQUIRED IS NULL
        )
        OR (
          NOT X.SWEEP_REQUIRED IS NULL AND Y.SWEEP_REQUIRED IS NULL
        )
        OR (
          X.SWEEP_REQUIRED <> Y.SWEEP_REQUIRED
        )
      )
      OR (
        (
          X.SWEEP_TYPE IS NULL AND NOT Y.SWEEP_TYPE IS NULL
        )
        OR (
          NOT X.SWEEP_TYPE IS NULL AND Y.SWEEP_TYPE IS NULL
        )
        OR (
          X.SWEEP_TYPE <> Y.SWEEP_TYPE
        )
      )
      OR (
        (
          X.TD_CERT_PRINTED IS NULL AND NOT Y.TD_CERT_PRINTED IS NULL
        )
        OR (
          NOT X.TD_CERT_PRINTED IS NULL AND Y.TD_CERT_PRINTED IS NULL
        )
        OR (
          X.TD_CERT_PRINTED <> Y.TD_CERT_PRINTED
        )
      )
      OR (
        (
          X.TOD_END_DATE IS NULL AND NOT Y.TOD_END_DATE IS NULL
        )
        OR (
          NOT X.TOD_END_DATE IS NULL AND Y.TOD_END_DATE IS NULL
        )
        OR (
          X.TOD_END_DATE <> Y.TOD_END_DATE
        )
      )
      OR (
        (
          X.TOD_LIMIT IS NULL AND NOT Y.TOD_LIMIT IS NULL
        )
        OR (
          NOT X.TOD_LIMIT IS NULL AND Y.TOD_LIMIT IS NULL
        )
        OR (
          X.TOD_LIMIT <> Y.TOD_LIMIT
        )
      )
      OR (
        (
          X.TOD_LIMIT_END_DATE IS NULL AND NOT Y.TOD_LIMIT_END_DATE IS NULL
        )
        OR (
          NOT X.TOD_LIMIT_END_DATE IS NULL AND Y.TOD_LIMIT_END_DATE IS NULL
        )
        OR (
          X.TOD_LIMIT_END_DATE <> Y.TOD_LIMIT_END_DATE
        )
      )
      OR (
        (
          X.TOD_LIMIT_START_DATE IS NULL AND NOT Y.TOD_LIMIT_START_DATE IS NULL
        )
        OR (
          NOT X.TOD_LIMIT_START_DATE IS NULL AND Y.TOD_LIMIT_START_DATE IS NULL
        )
        OR (
          X.TOD_LIMIT_START_DATE <> Y.TOD_LIMIT_START_DATE
        )
      )
      OR (
        (
          X.TOD_SINCE IS NULL AND NOT Y.TOD_SINCE IS NULL
        )
        OR (
          NOT X.TOD_SINCE IS NULL AND Y.TOD_SINCE IS NULL
        )
        OR (
          X.TOD_SINCE <> Y.TOD_SINCE
        )
      )
      OR (
        (
          X.TOD_START_DATE IS NULL AND NOT Y.TOD_START_DATE IS NULL
        )
        OR (
          NOT X.TOD_START_DATE IS NULL AND Y.TOD_START_DATE IS NULL
        )
        OR (
          X.TOD_START_DATE <> Y.TOD_START_DATE
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
          X.TRNOVER_LMT_CODE IS NULL AND NOT Y.TRNOVER_LMT_CODE IS NULL
        )
        OR (
          NOT X.TRNOVER_LMT_CODE IS NULL AND Y.TRNOVER_LMT_CODE IS NULL
        )
        OR (
          X.TRNOVER_LMT_CODE <> Y.TRNOVER_LMT_CODE
        )
      )
      OR (
        (
          X.TXN_CODE_LIST IS NULL AND NOT Y.TXN_CODE_LIST IS NULL
        )
        OR (
          NOT X.TXN_CODE_LIST IS NULL AND Y.TXN_CODE_LIST IS NULL
        )
        OR (
          X.TXN_CODE_LIST <> Y.TXN_CODE_LIST
        )
      )
      OR (
        (
          X.TYPE_OF_CHQ IS NULL AND NOT Y.TYPE_OF_CHQ IS NULL
        )
        OR (
          NOT X.TYPE_OF_CHQ IS NULL AND Y.TYPE_OF_CHQ IS NULL
        )
        OR (
          X.TYPE_OF_CHQ <> Y.TYPE_OF_CHQ
        )
      )
      OR (
        (
          X.UNCOLL_FUNDS_LIMIT IS NULL AND NOT Y.UNCOLL_FUNDS_LIMIT IS NULL
        )
        OR (
          NOT X.UNCOLL_FUNDS_LIMIT IS NULL AND Y.UNCOLL_FUNDS_LIMIT IS NULL
        )
        OR (
          X.UNCOLL_FUNDS_LIMIT <> Y.UNCOLL_FUNDS_LIMIT
        )
      )
      OR (
        (
          X.VALIDATION_DIGIT IS NULL AND NOT Y.VALIDATION_DIGIT IS NULL
        )
        OR (
          NOT X.VALIDATION_DIGIT IS NULL AND Y.VALIDATION_DIGIT IS NULL
        )
        OR (
          X.VALIDATION_DIGIT <> Y.VALIDATION_DIGIT
        )
      )
      OR (
        (
          X.WITHDRAWABLE_UNCOLLED_FUND IS NULL AND NOT Y.WITHDRAWABLE_UNCOLLED_FUND IS NULL
        )
        OR (
          NOT X.WITHDRAWABLE_UNCOLLED_FUND IS NULL AND Y.WITHDRAWABLE_UNCOLLED_FUND IS NULL
        )
        OR (
          X.WITHDRAWABLE_UNCOLLED_FUND <> Y.WITHDRAWABLE_UNCOLLED_FUND
        )
      )
      OR (
        (
          X.ZAKAT_EXEMPTION IS NULL AND NOT Y.ZAKAT_EXEMPTION IS NULL
        )
        OR (
          NOT X.ZAKAT_EXEMPTION IS NULL AND Y.ZAKAT_EXEMPTION IS NULL
        )
        OR (
          X.ZAKAT_EXEMPTION <> Y.ZAKAT_EXEMPTION
        )
      )
    )
    THEN 'UPD'
    WHEN X.BRANCH_CODE IS NULL AND X.CUST_AC_NO IS NULL
    THEN 'DEL' /* - IF MULTIPLE PKs are there then concatenate them with AND */
  END AS OPT_FLAG
FROM DEVT_STG_FLX.STTM_CUST_ACCOUNT AS X
FULL JOIN DEVT_STG_FLX.STTM_CUST_ACCOUNT_BACKUP AS Y
  ON X.BRANCH_CODE = Y.BRANCH_CODE
  AND X.CUST_AC_NO = Y.CUST_AC_NO /* - IF MULTIPLE PKs are there then concatenate them with AND */
WHERE
  NOT OPT_FLAG IS NULL