CREATE OR REPLACE VIEW DEVV_STG_FLX.STTM_ACCOUNT_CLASS_get_delta AS LOCKING ROW FOR ACCESS
SELECT
  COALESCE(X.ACCOUNT_CLASS, Y.ACCOUNT_CLASS) AS ACCOUNT_CLASS,
  X.AC_CLASS_TYPE,
  X.AC_STAT_DE_POST,
  X.ACC_STATISTICS,
  X.ACC_STMT_CYCLE,
  X.ACC_STMT_CYCLE2,
  X.ACC_STMT_CYCLE3,
  X.ACC_STMT_TYPE,
  X.ACC_STMT_TYPE2,
  X.ACC_STMT_TYPE3,
  X.ACCCLS_AUTODEP_LIST,
  X.ACCOUNT_CODE,
  X.ACST_FORMAT,
  X.ADHOC_HOL,
  X.ADVICE_DAYS,
  X.ALLOW_BACK_PERIOD_ENTRY,
  X.ALLOW_PART_LIQ_WITH_AMT_BLK,
  X.ALLOW_PREPAYMENT,
  X.ALLOW_TOPUP,
  X.APPLICABLE_DEP_TENOR,
  X.ATM_FACILITY,
  X.AUTH_STAT,
  X.AUTO_DEP_AC_CLASS,
  X.AUTO_DEP_BREAK_METHOD,
  X.AUTO_DEP_CCY,
  X.AUTO_DEP_DEF_RATE_CODE,
  X.AUTO_DEP_DEF_RATE_TYPE,
  X.AUTO_DEP_TRN_CODE,
  X.AUTO_EXTENSION,
  X.AUTO_PROV_REQD,
  X.AUTO_REORDER_CHECK_LEAVES,
  X.AUTO_REORDER_CHECK_LEVEL,
  X.AUTO_REORDER_CHECK_REQUIRED,
  X.AUTO_ROLLOVER,
  X.AVAIL_BAL_REQD,
  X.BALANCE_REPORT_SINCE,
  X.BALANCE_REPORT_TYPE,
  X.BLK_MAT_DAYS,
  X.BLK_MAT_MONTHS,
  X.BLK_OPEN_DAYS,
  X.BLK_OPEN_MONTHS,
  X.BM_AC_CLASS_TYPE,
  X.BM_ACCOUNT_CODE,
  X.BM_CLUSTER_ID,
  X.BM_INTRATE_CUMAMT_REQD,
  X.BM_RECORD_STAT,
  X.BRANCH_LIST,
  X.BREAK_DEPOSITS_FIRST,
  X.CASH_RESERVE_RATIO,
  X.CCY_LIST,
  X.CCY_OPTION_PROD,
  X.CHECKER_DT_STAMP,
  X.CHECKER_ID,
  X.CHEQUE_BOOK_FACILITY,
  X.CHG_START_ADV,
  X.CHK_MIN_BAL_FOR_SWEEP,
  X.CLOSE_ON_MATURITY,
  X.CLUSTER_ID,
  X.COMP_WISE_TRACKING_REQD,
  X.CONSOLIDATION_REQD,
  X.CR_CB_LINE,
  X.CR_GL_LINE,
  X.CR_HO_LINE,
  X.CUSCAT_LIST,
  X.DAYLIGHT_LIMIT,
  X.DAYS,
  X.DAYS_AUTH_NOACTIVITY,
  X.DAYS_UNAUTH_NOACTIVITY,
  X.DEFAULT_TENOR_DAYS,
  X.DEFAULT_TENOR_MONTHS,
  X.DEFAULT_TENOR_YEARS,
  X.DEFERRED_BAL_UPDATE,
  X.DENM_DEPOSIT,
  X.DEP_MULTIPLE_OF,
  X.DEPOSIT_PC_BRIDGE_GL,
  X.DEPOSIT_PC_CATEGORY,
  X.DEPOSIT_PC_TRN_CODE,
  X.DESCRIPTION,
  X.DISPLAY_IBAN_IN_ADVICES,
  X.DORMANCY,
  X.DORMANT_PARAM,
  X.DR_CB_LINE,
  X.DR_GL_LINE,
  X.DR_HO_LINE,
  X.DR_INT_LIQD_DAYS,
  X.DR_INT_LIQD_MODE,
  X.DR_INT_NOTICE,
  X.DR_INT_USING_RECV,
  X.DUAL_CCY_DEPOSIT,
  X.ENABLE_REV_SWEEP_IN,
  X.ENABLE_SWEEP_IN,
  X.END_DATE,
  X.ESCROW_PROCESS,
  X.EVENT_CLASS_CODE,
  X.EXCL_SAMEDAY_RVRTRNS_FM_STMT,
  X.EXCLUDE_FROM_DISTRIBUTION,
  X.EXPOSURE_CATEGORY,
  X.FIXING_DAYS,
  X.FREE_BANKING_DAYS,
  X.FREQUENCY,
  X.GEN_BALANCE_REPORT,
  X.GEN_INTERIM_STMT,
  X.GEN_INTERIM_STMT_ON_MVMT,
  X.GRACE_PERIOD,
  X.HAS_DRCR_ADV,
  X.HAS_IS,
  X.HOLIDAY_CALENDAR,
  X.HOLIDAY_MOVEMENT,
  X.IC_INCLUSION,
  X.ILM_APPLICABLE,
  X.INT_RATE_OPTION_REDAMT,
  X.INT_RATE_OPTION_REMAMT,
  X.INT_RATE_OPTION_TOPUP,
  X.INTERIM_REPORT_SINCE,
  X.INTERIM_REPORT_TYPE,
  X.INTERPAY,
  X.INTRATE_CUMAMT_REQD,
  X.LIMIT_CHECK_REQUIRED,
  X.LINK_CCY,
  X.LODGEMENT_BOOK,
  X.LRGDRBALFLG,
  X.MAKER_DT_STAMP,
  X.MAKER_ID,
  X.MAT_DT_MOV_ACROS_MONTH,
  X.MAX_AMOUNT,
  X.MAX_NO_CHEQUE_REJECTIONS,
  X.MAX_TENOR_DAYS,
  X.MAX_TENOR_MONTHS,
  X.MAX_TENOR_YEARS,
  X.MIN_AMOUNT,
  X.MIN_BAL_REQD,
  X.MIN_TENOR_DAYS,
  X.MIN_TENOR_MONTHS,
  X.MIN_TENOR_YEARS,
  X.MINOR_MAJOR,
  X.MOD_NO,
  X.MONTH_END_DEPOSIT,
  X.MOVE_INT_TO_UNCLAIMED,
  X.MOVE_PRIC_TO_UNCLAIMED,
  X.MUDARABAH_ACC_CLASS,
  X.MUDARABAH_FUND_ID,
  X.NATURAL_GL_SIGN,
  X.OFFLINE_LIMIT,
  X.ONCE_AUTH,
  X.OVERDRAFT_FACILITY,
  X.PARTIAL_LIQUIDATION,
  X.PASSBOOK_FACILITY,
  X.PASSBOOK_RT_PRODUCT,
  X.POOL_CODE,
  X.PRE_REDEMPTION,
  X.PRODUCT_LIST,
  X.PROFIT_CALC_BAL_TYPE,
  X.PROFIT_SHARING,
  X.PROJECT_ACCOUNT,
  X.PROV_CCY_TYPE,
  X.PROVIDE_INTEREST_ON_BROKEN_DEP,
  X.PROVISIONING_FREQUENCY,
  X.RATE_CHART_TENOR,
  X.RD_FLAG,
  X.RD_MAX_SCHEDULE_DAYS,
  X.RD_MIN_INSTALLMENT_AMT,
  X.RD_MIN_SCHEDULE_DAYS,
  X.RD_MOVE_FUNDS_ON_OVD,
  X.RD_MOVE_MAT_TO_UNCLAIMED,
  X.RD_SCHEDULE_DAYS,
  X.RD_SCHEDULE_MONTHS,
  X.RD_SCHEDULE_YEARS,
  X.RECOMPUTE_MAT_DATE,
  X.RECORD_STAT,
  X.REDEMPTION,
  X.REFERENCE_DATE,
  X.REFERRAL_REQUIRED,
  X.REG_D_APPLICABLE,
  X.REGD_PERIODICITY,
  X.REPL_CUST_SIG,
  X.SAL_BLK_DAYS,
  X.SEND_NOTIFICATION,
  X.SK_ACC_STMT_CYCLE,
  X.SK_ACCOUNT_CLASS,
  X.SK_AUTO_ROLLOVER,
  X.SK_CLUSTER_ID,
  X.SK_DEFAULT_TENOR_DAYS,
  X.SK_DEFAULT_TENOR_MONTHS,
  X.SK_DEFAULT_TENOR_YEARS,
  X.SK_CHECKER_ID,
  X.SK_MAKER_ID,
  X.SK_LIMIT_CHECK_REQUIRED,
  X.SK_MAX_TENOR_YEARS,
  X.SK_OVERDRAFT_FACILITY,
  X.SPECIAL_RATE_CODE,
  X.SPEND_ANALYSIS_REQD,
  X.STACCCLS_BRANCH_LIST,
  X.STACCCLS_CCY_LIST,
  X.STACCCLS_CUSCAT_LIST,
  X.START_DATE,
  X.STATEMENT_DAY,
  X.STATEMENT_DAY2,
  X.STATEMENT_DAY3,
  X.STATUS_CHANGE_AUTOMATIC,
  X.SWEEP_MODE,
  X.TD_RATECHART_ALLOW,
  X.TRACK_ACCRUED_IC,
  X.TRACK_RECEIVABLE,
  X.TRN_CODE,
  X.TRN_REV_CODE,
  X.TRNOVER_LMT_CODE,
  X.TRUSTAC_CASH_LIMIT,
  X.TURNOVER_EVENT_CLASS_CODE,
  X.TURNOVER_RTOH_CODE,
  X.TXN_CODE_LIST,
  X.VERIFY_FUNDS_FOR_DRINT,
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
    WHEN Y.ACCOUNT_CLASS IS NULL
    THEN 'INS' /* - IF MULTIPLE PKs are there then concatenate them with AND */
    WHEN X.ACCOUNT_CLASS = Y.ACCOUNT_CLASS /* - IF MULTIPLE PKs are there then concatenate them with AND */
    AND (
      (
        (
          X.AC_CLASS_TYPE IS NULL AND NOT Y.AC_CLASS_TYPE IS NULL
        )
        OR (
          NOT X.AC_CLASS_TYPE IS NULL AND Y.AC_CLASS_TYPE IS NULL
        )
        OR (
          X.AC_CLASS_TYPE <> Y.AC_CLASS_TYPE
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
          X.ACC_STATISTICS IS NULL AND NOT Y.ACC_STATISTICS IS NULL
        )
        OR (
          NOT X.ACC_STATISTICS IS NULL AND Y.ACC_STATISTICS IS NULL
        )
        OR (
          X.ACC_STATISTICS <> Y.ACC_STATISTICS
        )
      )
      OR (
        (
          X.ACC_STMT_CYCLE IS NULL AND NOT Y.ACC_STMT_CYCLE IS NULL
        )
        OR (
          NOT X.ACC_STMT_CYCLE IS NULL AND Y.ACC_STMT_CYCLE IS NULL
        )
        OR (
          X.ACC_STMT_CYCLE <> Y.ACC_STMT_CYCLE
        )
      )
      OR (
        (
          X.ACC_STMT_CYCLE2 IS NULL AND NOT Y.ACC_STMT_CYCLE2 IS NULL
        )
        OR (
          NOT X.ACC_STMT_CYCLE2 IS NULL AND Y.ACC_STMT_CYCLE2 IS NULL
        )
        OR (
          X.ACC_STMT_CYCLE2 <> Y.ACC_STMT_CYCLE2
        )
      )
      OR (
        (
          X.ACC_STMT_CYCLE3 IS NULL AND NOT Y.ACC_STMT_CYCLE3 IS NULL
        )
        OR (
          NOT X.ACC_STMT_CYCLE3 IS NULL AND Y.ACC_STMT_CYCLE3 IS NULL
        )
        OR (
          X.ACC_STMT_CYCLE3 <> Y.ACC_STMT_CYCLE3
        )
      )
      OR (
        (
          X.ACC_STMT_TYPE IS NULL AND NOT Y.ACC_STMT_TYPE IS NULL
        )
        OR (
          NOT X.ACC_STMT_TYPE IS NULL AND Y.ACC_STMT_TYPE IS NULL
        )
        OR (
          X.ACC_STMT_TYPE <> Y.ACC_STMT_TYPE
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
          X.ACCCLS_AUTODEP_LIST IS NULL AND NOT Y.ACCCLS_AUTODEP_LIST IS NULL
        )
        OR (
          NOT X.ACCCLS_AUTODEP_LIST IS NULL AND Y.ACCCLS_AUTODEP_LIST IS NULL
        )
        OR (
          X.ACCCLS_AUTODEP_LIST <> Y.ACCCLS_AUTODEP_LIST
        )
      )
      OR (
        (
          X.ACCOUNT_CODE IS NULL AND NOT Y.ACCOUNT_CODE IS NULL
        )
        OR (
          NOT X.ACCOUNT_CODE IS NULL AND Y.ACCOUNT_CODE IS NULL
        )
        OR (
          X.ACCOUNT_CODE <> Y.ACCOUNT_CODE
        )
      )
      OR (
        (
          X.ACST_FORMAT IS NULL AND NOT Y.ACST_FORMAT IS NULL
        )
        OR (
          NOT X.ACST_FORMAT IS NULL AND Y.ACST_FORMAT IS NULL
        )
        OR (
          X.ACST_FORMAT <> Y.ACST_FORMAT
        )
      )
      OR (
        (
          X.ADHOC_HOL IS NULL AND NOT Y.ADHOC_HOL IS NULL
        )
        OR (
          NOT X.ADHOC_HOL IS NULL AND Y.ADHOC_HOL IS NULL
        )
        OR (
          X.ADHOC_HOL <> Y.ADHOC_HOL
        )
      )
      OR (
        (
          X.ADVICE_DAYS IS NULL AND NOT Y.ADVICE_DAYS IS NULL
        )
        OR (
          NOT X.ADVICE_DAYS IS NULL AND Y.ADVICE_DAYS IS NULL
        )
        OR (
          X.ADVICE_DAYS <> Y.ADVICE_DAYS
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
          X.ALLOW_PART_LIQ_WITH_AMT_BLK IS NULL
          AND NOT Y.ALLOW_PART_LIQ_WITH_AMT_BLK IS NULL
        )
        OR (
          NOT X.ALLOW_PART_LIQ_WITH_AMT_BLK IS NULL
          AND Y.ALLOW_PART_LIQ_WITH_AMT_BLK IS NULL
        )
        OR (
          X.ALLOW_PART_LIQ_WITH_AMT_BLK <> Y.ALLOW_PART_LIQ_WITH_AMT_BLK
        )
      )
      OR (
        (
          X.ALLOW_PREPAYMENT IS NULL AND NOT Y.ALLOW_PREPAYMENT IS NULL
        )
        OR (
          NOT X.ALLOW_PREPAYMENT IS NULL AND Y.ALLOW_PREPAYMENT IS NULL
        )
        OR (
          X.ALLOW_PREPAYMENT <> Y.ALLOW_PREPAYMENT
        )
      )
      OR (
        (
          X.ALLOW_TOPUP IS NULL AND NOT Y.ALLOW_TOPUP IS NULL
        )
        OR (
          NOT X.ALLOW_TOPUP IS NULL AND Y.ALLOW_TOPUP IS NULL
        )
        OR (
          X.ALLOW_TOPUP <> Y.ALLOW_TOPUP
        )
      )
      OR (
        (
          X.APPLICABLE_DEP_TENOR IS NULL AND NOT Y.APPLICABLE_DEP_TENOR IS NULL
        )
        OR (
          NOT X.APPLICABLE_DEP_TENOR IS NULL AND Y.APPLICABLE_DEP_TENOR IS NULL
        )
        OR (
          X.APPLICABLE_DEP_TENOR <> Y.APPLICABLE_DEP_TENOR
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
          X.AUTO_DEP_AC_CLASS IS NULL AND NOT Y.AUTO_DEP_AC_CLASS IS NULL
        )
        OR (
          NOT X.AUTO_DEP_AC_CLASS IS NULL AND Y.AUTO_DEP_AC_CLASS IS NULL
        )
        OR (
          X.AUTO_DEP_AC_CLASS <> Y.AUTO_DEP_AC_CLASS
        )
      )
      OR (
        (
          X.AUTO_DEP_BREAK_METHOD IS NULL AND NOT Y.AUTO_DEP_BREAK_METHOD IS NULL
        )
        OR (
          NOT X.AUTO_DEP_BREAK_METHOD IS NULL AND Y.AUTO_DEP_BREAK_METHOD IS NULL
        )
        OR (
          X.AUTO_DEP_BREAK_METHOD <> Y.AUTO_DEP_BREAK_METHOD
        )
      )
      OR (
        (
          X.AUTO_DEP_CCY IS NULL AND NOT Y.AUTO_DEP_CCY IS NULL
        )
        OR (
          NOT X.AUTO_DEP_CCY IS NULL AND Y.AUTO_DEP_CCY IS NULL
        )
        OR (
          X.AUTO_DEP_CCY <> Y.AUTO_DEP_CCY
        )
      )
      OR (
        (
          X.AUTO_DEP_DEF_RATE_CODE IS NULL AND NOT Y.AUTO_DEP_DEF_RATE_CODE IS NULL
        )
        OR (
          NOT X.AUTO_DEP_DEF_RATE_CODE IS NULL AND Y.AUTO_DEP_DEF_RATE_CODE IS NULL
        )
        OR (
          X.AUTO_DEP_DEF_RATE_CODE <> Y.AUTO_DEP_DEF_RATE_CODE
        )
      )
      OR (
        (
          X.AUTO_DEP_DEF_RATE_TYPE IS NULL AND NOT Y.AUTO_DEP_DEF_RATE_TYPE IS NULL
        )
        OR (
          NOT X.AUTO_DEP_DEF_RATE_TYPE IS NULL AND Y.AUTO_DEP_DEF_RATE_TYPE IS NULL
        )
        OR (
          X.AUTO_DEP_DEF_RATE_TYPE <> Y.AUTO_DEP_DEF_RATE_TYPE
        )
      )
      OR (
        (
          X.AUTO_DEP_TRN_CODE IS NULL AND NOT Y.AUTO_DEP_TRN_CODE IS NULL
        )
        OR (
          NOT X.AUTO_DEP_TRN_CODE IS NULL AND Y.AUTO_DEP_TRN_CODE IS NULL
        )
        OR (
          X.AUTO_DEP_TRN_CODE <> Y.AUTO_DEP_TRN_CODE
        )
      )
      OR (
        (
          X.AUTO_EXTENSION IS NULL AND NOT Y.AUTO_EXTENSION IS NULL
        )
        OR (
          NOT X.AUTO_EXTENSION IS NULL AND Y.AUTO_EXTENSION IS NULL
        )
        OR (
          X.AUTO_EXTENSION <> Y.AUTO_EXTENSION
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
          X.AUTO_ROLLOVER IS NULL AND NOT Y.AUTO_ROLLOVER IS NULL
        )
        OR (
          NOT X.AUTO_ROLLOVER IS NULL AND Y.AUTO_ROLLOVER IS NULL
        )
        OR (
          X.AUTO_ROLLOVER <> Y.AUTO_ROLLOVER
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
          X.BLK_MAT_DAYS IS NULL AND NOT Y.BLK_MAT_DAYS IS NULL
        )
        OR (
          NOT X.BLK_MAT_DAYS IS NULL AND Y.BLK_MAT_DAYS IS NULL
        )
        OR (
          X.BLK_MAT_DAYS <> Y.BLK_MAT_DAYS
        )
      )
      OR (
        (
          X.BLK_MAT_MONTHS IS NULL AND NOT Y.BLK_MAT_MONTHS IS NULL
        )
        OR (
          NOT X.BLK_MAT_MONTHS IS NULL AND Y.BLK_MAT_MONTHS IS NULL
        )
        OR (
          X.BLK_MAT_MONTHS <> Y.BLK_MAT_MONTHS
        )
      )
      OR (
        (
          X.BLK_OPEN_DAYS IS NULL AND NOT Y.BLK_OPEN_DAYS IS NULL
        )
        OR (
          NOT X.BLK_OPEN_DAYS IS NULL AND Y.BLK_OPEN_DAYS IS NULL
        )
        OR (
          X.BLK_OPEN_DAYS <> Y.BLK_OPEN_DAYS
        )
      )
      OR (
        (
          X.BLK_OPEN_MONTHS IS NULL AND NOT Y.BLK_OPEN_MONTHS IS NULL
        )
        OR (
          NOT X.BLK_OPEN_MONTHS IS NULL AND Y.BLK_OPEN_MONTHS IS NULL
        )
        OR (
          X.BLK_OPEN_MONTHS <> Y.BLK_OPEN_MONTHS
        )
      )
      OR (
        (
          X.BM_AC_CLASS_TYPE IS NULL AND NOT Y.BM_AC_CLASS_TYPE IS NULL
        )
        OR (
          NOT X.BM_AC_CLASS_TYPE IS NULL AND Y.BM_AC_CLASS_TYPE IS NULL
        )
        OR (
          X.BM_AC_CLASS_TYPE <> Y.BM_AC_CLASS_TYPE
        )
      )
      OR (
        (
          X.BM_ACCOUNT_CODE IS NULL AND NOT Y.BM_ACCOUNT_CODE IS NULL
        )
        OR (
          NOT X.BM_ACCOUNT_CODE IS NULL AND Y.BM_ACCOUNT_CODE IS NULL
        )
        OR (
          X.BM_ACCOUNT_CODE <> Y.BM_ACCOUNT_CODE
        )
      )
      OR (
        (
          X.BM_CLUSTER_ID IS NULL AND NOT Y.BM_CLUSTER_ID IS NULL
        )
        OR (
          NOT X.BM_CLUSTER_ID IS NULL AND Y.BM_CLUSTER_ID IS NULL
        )
        OR (
          X.BM_CLUSTER_ID <> Y.BM_CLUSTER_ID
        )
      )
      OR (
        (
          X.BM_INTRATE_CUMAMT_REQD IS NULL AND NOT Y.BM_INTRATE_CUMAMT_REQD IS NULL
        )
        OR (
          NOT X.BM_INTRATE_CUMAMT_REQD IS NULL AND Y.BM_INTRATE_CUMAMT_REQD IS NULL
        )
        OR (
          X.BM_INTRATE_CUMAMT_REQD <> Y.BM_INTRATE_CUMAMT_REQD
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
          X.BRANCH_LIST IS NULL AND NOT Y.BRANCH_LIST IS NULL
        )
        OR (
          NOT X.BRANCH_LIST IS NULL AND Y.BRANCH_LIST IS NULL
        )
        OR (
          X.BRANCH_LIST <> Y.BRANCH_LIST
        )
      )
      OR (
        (
          X.BREAK_DEPOSITS_FIRST IS NULL AND NOT Y.BREAK_DEPOSITS_FIRST IS NULL
        )
        OR (
          NOT X.BREAK_DEPOSITS_FIRST IS NULL AND Y.BREAK_DEPOSITS_FIRST IS NULL
        )
        OR (
          X.BREAK_DEPOSITS_FIRST <> Y.BREAK_DEPOSITS_FIRST
        )
      )
      OR (
        (
          X.CASH_RESERVE_RATIO IS NULL AND NOT Y.CASH_RESERVE_RATIO IS NULL
        )
        OR (
          NOT X.CASH_RESERVE_RATIO IS NULL AND Y.CASH_RESERVE_RATIO IS NULL
        )
        OR (
          X.CASH_RESERVE_RATIO <> Y.CASH_RESERVE_RATIO
        )
      )
      OR (
        (
          X.CCY_LIST IS NULL AND NOT Y.CCY_LIST IS NULL
        )
        OR (
          NOT X.CCY_LIST IS NULL AND Y.CCY_LIST IS NULL
        )
        OR (
          X.CCY_LIST <> Y.CCY_LIST
        )
      )
      OR (
        (
          X.CCY_OPTION_PROD IS NULL AND NOT Y.CCY_OPTION_PROD IS NULL
        )
        OR (
          NOT X.CCY_OPTION_PROD IS NULL AND Y.CCY_OPTION_PROD IS NULL
        )
        OR (
          X.CCY_OPTION_PROD <> Y.CCY_OPTION_PROD
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
          X.CHG_START_ADV IS NULL AND NOT Y.CHG_START_ADV IS NULL
        )
        OR (
          NOT X.CHG_START_ADV IS NULL AND Y.CHG_START_ADV IS NULL
        )
        OR (
          X.CHG_START_ADV <> Y.CHG_START_ADV
        )
      )
      OR (
        (
          X.CHK_MIN_BAL_FOR_SWEEP IS NULL AND NOT Y.CHK_MIN_BAL_FOR_SWEEP IS NULL
        )
        OR (
          NOT X.CHK_MIN_BAL_FOR_SWEEP IS NULL AND Y.CHK_MIN_BAL_FOR_SWEEP IS NULL
        )
        OR (
          X.CHK_MIN_BAL_FOR_SWEEP <> Y.CHK_MIN_BAL_FOR_SWEEP
        )
      )
      OR (
        (
          X.CLOSE_ON_MATURITY IS NULL AND NOT Y.CLOSE_ON_MATURITY IS NULL
        )
        OR (
          NOT X.CLOSE_ON_MATURITY IS NULL AND Y.CLOSE_ON_MATURITY IS NULL
        )
        OR (
          X.CLOSE_ON_MATURITY <> Y.CLOSE_ON_MATURITY
        )
      )
      OR (
        (
          X.CLUSTER_ID IS NULL AND NOT Y.CLUSTER_ID IS NULL
        )
        OR (
          NOT X.CLUSTER_ID IS NULL AND Y.CLUSTER_ID IS NULL
        )
        OR (
          X.CLUSTER_ID <> Y.CLUSTER_ID
        )
      )
      OR (
        (
          X.COMP_WISE_TRACKING_REQD IS NULL AND NOT Y.COMP_WISE_TRACKING_REQD IS NULL
        )
        OR (
          NOT X.COMP_WISE_TRACKING_REQD IS NULL AND Y.COMP_WISE_TRACKING_REQD IS NULL
        )
        OR (
          X.COMP_WISE_TRACKING_REQD <> Y.COMP_WISE_TRACKING_REQD
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
          X.CR_GL_LINE IS NULL AND NOT Y.CR_GL_LINE IS NULL
        )
        OR (
          NOT X.CR_GL_LINE IS NULL AND Y.CR_GL_LINE IS NULL
        )
        OR (
          X.CR_GL_LINE <> Y.CR_GL_LINE
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
          X.CUSCAT_LIST IS NULL AND NOT Y.CUSCAT_LIST IS NULL
        )
        OR (
          NOT X.CUSCAT_LIST IS NULL AND Y.CUSCAT_LIST IS NULL
        )
        OR (
          X.CUSCAT_LIST <> Y.CUSCAT_LIST
        )
      )
      OR (
        (
          X.DAYLIGHT_LIMIT IS NULL AND NOT Y.DAYLIGHT_LIMIT IS NULL
        )
        OR (
          NOT X.DAYLIGHT_LIMIT IS NULL AND Y.DAYLIGHT_LIMIT IS NULL
        )
        OR (
          X.DAYLIGHT_LIMIT <> Y.DAYLIGHT_LIMIT
        )
      )
      OR (
        (
          X.DAYS IS NULL AND NOT Y.DAYS IS NULL
        )
        OR (
          NOT X.DAYS IS NULL AND Y.DAYS IS NULL
        )
        OR (
          X.DAYS <> Y.DAYS
        )
      )
      OR (
        (
          X.DAYS_AUTH_NOACTIVITY IS NULL AND NOT Y.DAYS_AUTH_NOACTIVITY IS NULL
        )
        OR (
          NOT X.DAYS_AUTH_NOACTIVITY IS NULL AND Y.DAYS_AUTH_NOACTIVITY IS NULL
        )
        OR (
          X.DAYS_AUTH_NOACTIVITY <> Y.DAYS_AUTH_NOACTIVITY
        )
      )
      OR (
        (
          X.DAYS_UNAUTH_NOACTIVITY IS NULL AND NOT Y.DAYS_UNAUTH_NOACTIVITY IS NULL
        )
        OR (
          NOT X.DAYS_UNAUTH_NOACTIVITY IS NULL AND Y.DAYS_UNAUTH_NOACTIVITY IS NULL
        )
        OR (
          X.DAYS_UNAUTH_NOACTIVITY <> Y.DAYS_UNAUTH_NOACTIVITY
        )
      )
      OR (
        (
          X.DEFAULT_TENOR_DAYS IS NULL AND NOT Y.DEFAULT_TENOR_DAYS IS NULL
        )
        OR (
          NOT X.DEFAULT_TENOR_DAYS IS NULL AND Y.DEFAULT_TENOR_DAYS IS NULL
        )
        OR (
          X.DEFAULT_TENOR_DAYS <> Y.DEFAULT_TENOR_DAYS
        )
      )
      OR (
        (
          X.DEFAULT_TENOR_MONTHS IS NULL AND NOT Y.DEFAULT_TENOR_MONTHS IS NULL
        )
        OR (
          NOT X.DEFAULT_TENOR_MONTHS IS NULL AND Y.DEFAULT_TENOR_MONTHS IS NULL
        )
        OR (
          X.DEFAULT_TENOR_MONTHS <> Y.DEFAULT_TENOR_MONTHS
        )
      )
      OR (
        (
          X.DEFAULT_TENOR_YEARS IS NULL AND NOT Y.DEFAULT_TENOR_YEARS IS NULL
        )
        OR (
          NOT X.DEFAULT_TENOR_YEARS IS NULL AND Y.DEFAULT_TENOR_YEARS IS NULL
        )
        OR (
          X.DEFAULT_TENOR_YEARS <> Y.DEFAULT_TENOR_YEARS
        )
      )
      OR (
        (
          X.DEFERRED_BAL_UPDATE IS NULL AND NOT Y.DEFERRED_BAL_UPDATE IS NULL
        )
        OR (
          NOT X.DEFERRED_BAL_UPDATE IS NULL AND Y.DEFERRED_BAL_UPDATE IS NULL
        )
        OR (
          X.DEFERRED_BAL_UPDATE <> Y.DEFERRED_BAL_UPDATE
        )
      )
      OR (
        (
          X.DENM_DEPOSIT IS NULL AND NOT Y.DENM_DEPOSIT IS NULL
        )
        OR (
          NOT X.DENM_DEPOSIT IS NULL AND Y.DENM_DEPOSIT IS NULL
        )
        OR (
          X.DENM_DEPOSIT <> Y.DENM_DEPOSIT
        )
      )
      OR (
        (
          X.DEP_MULTIPLE_OF IS NULL AND NOT Y.DEP_MULTIPLE_OF IS NULL
        )
        OR (
          NOT X.DEP_MULTIPLE_OF IS NULL AND Y.DEP_MULTIPLE_OF IS NULL
        )
        OR (
          X.DEP_MULTIPLE_OF <> Y.DEP_MULTIPLE_OF
        )
      )
      OR (
        (
          X.DEPOSIT_PC_BRIDGE_GL IS NULL AND NOT Y.DEPOSIT_PC_BRIDGE_GL IS NULL
        )
        OR (
          NOT X.DEPOSIT_PC_BRIDGE_GL IS NULL AND Y.DEPOSIT_PC_BRIDGE_GL IS NULL
        )
        OR (
          X.DEPOSIT_PC_BRIDGE_GL <> Y.DEPOSIT_PC_BRIDGE_GL
        )
      )
      OR (
        (
          X.DEPOSIT_PC_CATEGORY IS NULL AND NOT Y.DEPOSIT_PC_CATEGORY IS NULL
        )
        OR (
          NOT X.DEPOSIT_PC_CATEGORY IS NULL AND Y.DEPOSIT_PC_CATEGORY IS NULL
        )
        OR (
          X.DEPOSIT_PC_CATEGORY <> Y.DEPOSIT_PC_CATEGORY
        )
      )
      OR (
        (
          X.DEPOSIT_PC_TRN_CODE IS NULL AND NOT Y.DEPOSIT_PC_TRN_CODE IS NULL
        )
        OR (
          NOT X.DEPOSIT_PC_TRN_CODE IS NULL AND Y.DEPOSIT_PC_TRN_CODE IS NULL
        )
        OR (
          X.DEPOSIT_PC_TRN_CODE <> Y.DEPOSIT_PC_TRN_CODE
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
          X.DORMANCY IS NULL AND NOT Y.DORMANCY IS NULL
        )
        OR (
          NOT X.DORMANCY IS NULL AND Y.DORMANCY IS NULL
        )
        OR (
          X.DORMANCY <> Y.DORMANCY
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
          X.DR_GL_LINE IS NULL AND NOT Y.DR_GL_LINE IS NULL
        )
        OR (
          NOT X.DR_GL_LINE IS NULL AND Y.DR_GL_LINE IS NULL
        )
        OR (
          X.DR_GL_LINE <> Y.DR_GL_LINE
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
          X.DR_INT_LIQD_DAYS IS NULL AND NOT Y.DR_INT_LIQD_DAYS IS NULL
        )
        OR (
          NOT X.DR_INT_LIQD_DAYS IS NULL AND Y.DR_INT_LIQD_DAYS IS NULL
        )
        OR (
          X.DR_INT_LIQD_DAYS <> Y.DR_INT_LIQD_DAYS
        )
      )
      OR (
        (
          X.DR_INT_LIQD_MODE IS NULL AND NOT Y.DR_INT_LIQD_MODE IS NULL
        )
        OR (
          NOT X.DR_INT_LIQD_MODE IS NULL AND Y.DR_INT_LIQD_MODE IS NULL
        )
        OR (
          X.DR_INT_LIQD_MODE <> Y.DR_INT_LIQD_MODE
        )
      )
      OR (
        (
          X.DR_INT_NOTICE IS NULL AND NOT Y.DR_INT_NOTICE IS NULL
        )
        OR (
          NOT X.DR_INT_NOTICE IS NULL AND Y.DR_INT_NOTICE IS NULL
        )
        OR (
          X.DR_INT_NOTICE <> Y.DR_INT_NOTICE
        )
      )
      OR (
        (
          X.DR_INT_USING_RECV IS NULL AND NOT Y.DR_INT_USING_RECV IS NULL
        )
        OR (
          NOT X.DR_INT_USING_RECV IS NULL AND Y.DR_INT_USING_RECV IS NULL
        )
        OR (
          X.DR_INT_USING_RECV <> Y.DR_INT_USING_RECV
        )
      )
      OR (
        (
          X.DUAL_CCY_DEPOSIT IS NULL AND NOT Y.DUAL_CCY_DEPOSIT IS NULL
        )
        OR (
          NOT X.DUAL_CCY_DEPOSIT IS NULL AND Y.DUAL_CCY_DEPOSIT IS NULL
        )
        OR (
          X.DUAL_CCY_DEPOSIT <> Y.DUAL_CCY_DEPOSIT
        )
      )
      OR (
        (
          X.ENABLE_REV_SWEEP_IN IS NULL AND NOT Y.ENABLE_REV_SWEEP_IN IS NULL
        )
        OR (
          NOT X.ENABLE_REV_SWEEP_IN IS NULL AND Y.ENABLE_REV_SWEEP_IN IS NULL
        )
        OR (
          X.ENABLE_REV_SWEEP_IN <> Y.ENABLE_REV_SWEEP_IN
        )
      )
      OR (
        (
          X.ENABLE_SWEEP_IN IS NULL AND NOT Y.ENABLE_SWEEP_IN IS NULL
        )
        OR (
          NOT X.ENABLE_SWEEP_IN IS NULL AND Y.ENABLE_SWEEP_IN IS NULL
        )
        OR (
          X.ENABLE_SWEEP_IN <> Y.ENABLE_SWEEP_IN
        )
      )
      OR (
        (
          X.END_DATE IS NULL AND NOT Y.END_DATE IS NULL
        )
        OR (
          NOT X.END_DATE IS NULL AND Y.END_DATE IS NULL
        )
        OR (
          X.END_DATE <> Y.END_DATE
        )
      )
      OR (
        (
          X.ESCROW_PROCESS IS NULL AND NOT Y.ESCROW_PROCESS IS NULL
        )
        OR (
          NOT X.ESCROW_PROCESS IS NULL AND Y.ESCROW_PROCESS IS NULL
        )
        OR (
          X.ESCROW_PROCESS <> Y.ESCROW_PROCESS
        )
      )
      OR (
        (
          X.EVENT_CLASS_CODE IS NULL AND NOT Y.EVENT_CLASS_CODE IS NULL
        )
        OR (
          NOT X.EVENT_CLASS_CODE IS NULL AND Y.EVENT_CLASS_CODE IS NULL
        )
        OR (
          X.EVENT_CLASS_CODE <> Y.EVENT_CLASS_CODE
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
          X.FIXING_DAYS IS NULL AND NOT Y.FIXING_DAYS IS NULL
        )
        OR (
          NOT X.FIXING_DAYS IS NULL AND Y.FIXING_DAYS IS NULL
        )
        OR (
          X.FIXING_DAYS <> Y.FIXING_DAYS
        )
      )
      OR (
        (
          X.FREE_BANKING_DAYS IS NULL AND NOT Y.FREE_BANKING_DAYS IS NULL
        )
        OR (
          NOT X.FREE_BANKING_DAYS IS NULL AND Y.FREE_BANKING_DAYS IS NULL
        )
        OR (
          X.FREE_BANKING_DAYS <> Y.FREE_BANKING_DAYS
        )
      )
      OR (
        (
          X.FREQUENCY IS NULL AND NOT Y.FREQUENCY IS NULL
        )
        OR (
          NOT X.FREQUENCY IS NULL AND Y.FREQUENCY IS NULL
        )
        OR (
          X.FREQUENCY <> Y.FREQUENCY
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
          X.GRACE_PERIOD IS NULL AND NOT Y.GRACE_PERIOD IS NULL
        )
        OR (
          NOT X.GRACE_PERIOD IS NULL AND Y.GRACE_PERIOD IS NULL
        )
        OR (
          X.GRACE_PERIOD <> Y.GRACE_PERIOD
        )
      )
      OR (
        (
          X.HAS_DRCR_ADV IS NULL AND NOT Y.HAS_DRCR_ADV IS NULL
        )
        OR (
          NOT X.HAS_DRCR_ADV IS NULL AND Y.HAS_DRCR_ADV IS NULL
        )
        OR (
          X.HAS_DRCR_ADV <> Y.HAS_DRCR_ADV
        )
      )
      OR (
        (
          X.HAS_IS IS NULL AND NOT Y.HAS_IS IS NULL
        )
        OR (
          NOT X.HAS_IS IS NULL AND Y.HAS_IS IS NULL
        )
        OR (
          X.HAS_IS <> Y.HAS_IS
        )
      )
      OR (
        (
          X.HOLIDAY_CALENDAR IS NULL AND NOT Y.HOLIDAY_CALENDAR IS NULL
        )
        OR (
          NOT X.HOLIDAY_CALENDAR IS NULL AND Y.HOLIDAY_CALENDAR IS NULL
        )
        OR (
          X.HOLIDAY_CALENDAR <> Y.HOLIDAY_CALENDAR
        )
      )
      OR (
        (
          X.HOLIDAY_MOVEMENT IS NULL AND NOT Y.HOLIDAY_MOVEMENT IS NULL
        )
        OR (
          NOT X.HOLIDAY_MOVEMENT IS NULL AND Y.HOLIDAY_MOVEMENT IS NULL
        )
        OR (
          X.HOLIDAY_MOVEMENT <> Y.HOLIDAY_MOVEMENT
        )
      )
      OR (
        (
          X.IC_INCLUSION IS NULL AND NOT Y.IC_INCLUSION IS NULL
        )
        OR (
          NOT X.IC_INCLUSION IS NULL AND Y.IC_INCLUSION IS NULL
        )
        OR (
          X.IC_INCLUSION <> Y.IC_INCLUSION
        )
      )
      OR (
        (
          X.ILM_APPLICABLE IS NULL AND NOT Y.ILM_APPLICABLE IS NULL
        )
        OR (
          NOT X.ILM_APPLICABLE IS NULL AND Y.ILM_APPLICABLE IS NULL
        )
        OR (
          X.ILM_APPLICABLE <> Y.ILM_APPLICABLE
        )
      )
      OR (
        (
          X.INT_RATE_OPTION_REDAMT IS NULL AND NOT Y.INT_RATE_OPTION_REDAMT IS NULL
        )
        OR (
          NOT X.INT_RATE_OPTION_REDAMT IS NULL AND Y.INT_RATE_OPTION_REDAMT IS NULL
        )
        OR (
          X.INT_RATE_OPTION_REDAMT <> Y.INT_RATE_OPTION_REDAMT
        )
      )
      OR (
        (
          X.INT_RATE_OPTION_REMAMT IS NULL AND NOT Y.INT_RATE_OPTION_REMAMT IS NULL
        )
        OR (
          NOT X.INT_RATE_OPTION_REMAMT IS NULL AND Y.INT_RATE_OPTION_REMAMT IS NULL
        )
        OR (
          X.INT_RATE_OPTION_REMAMT <> Y.INT_RATE_OPTION_REMAMT
        )
      )
      OR (
        (
          X.INT_RATE_OPTION_TOPUP IS NULL AND NOT Y.INT_RATE_OPTION_TOPUP IS NULL
        )
        OR (
          NOT X.INT_RATE_OPTION_TOPUP IS NULL AND Y.INT_RATE_OPTION_TOPUP IS NULL
        )
        OR (
          X.INT_RATE_OPTION_TOPUP <> Y.INT_RATE_OPTION_TOPUP
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
          X.INTERPAY IS NULL AND NOT Y.INTERPAY IS NULL
        )
        OR (
          NOT X.INTERPAY IS NULL AND Y.INTERPAY IS NULL
        )
        OR (
          X.INTERPAY <> Y.INTERPAY
        )
      )
      OR (
        (
          X.INTRATE_CUMAMT_REQD IS NULL AND NOT Y.INTRATE_CUMAMT_REQD IS NULL
        )
        OR (
          NOT X.INTRATE_CUMAMT_REQD IS NULL AND Y.INTRATE_CUMAMT_REQD IS NULL
        )
        OR (
          X.INTRATE_CUMAMT_REQD <> Y.INTRATE_CUMAMT_REQD
        )
      )
      OR (
        (
          X.LIMIT_CHECK_REQUIRED IS NULL AND NOT Y.LIMIT_CHECK_REQUIRED IS NULL
        )
        OR (
          NOT X.LIMIT_CHECK_REQUIRED IS NULL AND Y.LIMIT_CHECK_REQUIRED IS NULL
        )
        OR (
          X.LIMIT_CHECK_REQUIRED <> Y.LIMIT_CHECK_REQUIRED
        )
      )
      OR (
        (
          X.LINK_CCY IS NULL AND NOT Y.LINK_CCY IS NULL
        )
        OR (
          NOT X.LINK_CCY IS NULL AND Y.LINK_CCY IS NULL
        )
        OR (
          X.LINK_CCY <> Y.LINK_CCY
        )
      )
      OR (
        (
          X.LODGEMENT_BOOK IS NULL AND NOT Y.LODGEMENT_BOOK IS NULL
        )
        OR (
          NOT X.LODGEMENT_BOOK IS NULL AND Y.LODGEMENT_BOOK IS NULL
        )
        OR (
          X.LODGEMENT_BOOK <> Y.LODGEMENT_BOOK
        )
      )
      OR (
        (
          X.LRGDRBALFLG IS NULL AND NOT Y.LRGDRBALFLG IS NULL
        )
        OR (
          NOT X.LRGDRBALFLG IS NULL AND Y.LRGDRBALFLG IS NULL
        )
        OR (
          X.LRGDRBALFLG <> Y.LRGDRBALFLG
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
          X.MAT_DT_MOV_ACROS_MONTH IS NULL AND NOT Y.MAT_DT_MOV_ACROS_MONTH IS NULL
        )
        OR (
          NOT X.MAT_DT_MOV_ACROS_MONTH IS NULL AND Y.MAT_DT_MOV_ACROS_MONTH IS NULL
        )
        OR (
          X.MAT_DT_MOV_ACROS_MONTH <> Y.MAT_DT_MOV_ACROS_MONTH
        )
      )
      OR (
        (
          X.MAX_AMOUNT IS NULL AND NOT Y.MAX_AMOUNT IS NULL
        )
        OR (
          NOT X.MAX_AMOUNT IS NULL AND Y.MAX_AMOUNT IS NULL
        )
        OR (
          X.MAX_AMOUNT <> Y.MAX_AMOUNT
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
          X.MAX_TENOR_DAYS IS NULL AND NOT Y.MAX_TENOR_DAYS IS NULL
        )
        OR (
          NOT X.MAX_TENOR_DAYS IS NULL AND Y.MAX_TENOR_DAYS IS NULL
        )
        OR (
          X.MAX_TENOR_DAYS <> Y.MAX_TENOR_DAYS
        )
      )
      OR (
        (
          X.MAX_TENOR_MONTHS IS NULL AND NOT Y.MAX_TENOR_MONTHS IS NULL
        )
        OR (
          NOT X.MAX_TENOR_MONTHS IS NULL AND Y.MAX_TENOR_MONTHS IS NULL
        )
        OR (
          X.MAX_TENOR_MONTHS <> Y.MAX_TENOR_MONTHS
        )
      )
      OR (
        (
          X.MAX_TENOR_YEARS IS NULL AND NOT Y.MAX_TENOR_YEARS IS NULL
        )
        OR (
          NOT X.MAX_TENOR_YEARS IS NULL AND Y.MAX_TENOR_YEARS IS NULL
        )
        OR (
          X.MAX_TENOR_YEARS <> Y.MAX_TENOR_YEARS
        )
      )
      OR (
        (
          X.MIN_AMOUNT IS NULL AND NOT Y.MIN_AMOUNT IS NULL
        )
        OR (
          NOT X.MIN_AMOUNT IS NULL AND Y.MIN_AMOUNT IS NULL
        )
        OR (
          X.MIN_AMOUNT <> Y.MIN_AMOUNT
        )
      )
      OR (
        (
          X.MIN_BAL_REQD IS NULL AND NOT Y.MIN_BAL_REQD IS NULL
        )
        OR (
          NOT X.MIN_BAL_REQD IS NULL AND Y.MIN_BAL_REQD IS NULL
        )
        OR (
          X.MIN_BAL_REQD <> Y.MIN_BAL_REQD
        )
      )
      OR (
        (
          X.MIN_TENOR_DAYS IS NULL AND NOT Y.MIN_TENOR_DAYS IS NULL
        )
        OR (
          NOT X.MIN_TENOR_DAYS IS NULL AND Y.MIN_TENOR_DAYS IS NULL
        )
        OR (
          X.MIN_TENOR_DAYS <> Y.MIN_TENOR_DAYS
        )
      )
      OR (
        (
          X.MIN_TENOR_MONTHS IS NULL AND NOT Y.MIN_TENOR_MONTHS IS NULL
        )
        OR (
          NOT X.MIN_TENOR_MONTHS IS NULL AND Y.MIN_TENOR_MONTHS IS NULL
        )
        OR (
          X.MIN_TENOR_MONTHS <> Y.MIN_TENOR_MONTHS
        )
      )
      OR (
        (
          X.MIN_TENOR_YEARS IS NULL AND NOT Y.MIN_TENOR_YEARS IS NULL
        )
        OR (
          NOT X.MIN_TENOR_YEARS IS NULL AND Y.MIN_TENOR_YEARS IS NULL
        )
        OR (
          X.MIN_TENOR_YEARS <> Y.MIN_TENOR_YEARS
        )
      )
      OR (
        (
          X.MINOR_MAJOR IS NULL AND NOT Y.MINOR_MAJOR IS NULL
        )
        OR (
          NOT X.MINOR_MAJOR IS NULL AND Y.MINOR_MAJOR IS NULL
        )
        OR (
          X.MINOR_MAJOR <> Y.MINOR_MAJOR
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
          X.MONTH_END_DEPOSIT IS NULL AND NOT Y.MONTH_END_DEPOSIT IS NULL
        )
        OR (
          NOT X.MONTH_END_DEPOSIT IS NULL AND Y.MONTH_END_DEPOSIT IS NULL
        )
        OR (
          X.MONTH_END_DEPOSIT <> Y.MONTH_END_DEPOSIT
        )
      )
      OR (
        (
          X.MOVE_INT_TO_UNCLAIMED IS NULL AND NOT Y.MOVE_INT_TO_UNCLAIMED IS NULL
        )
        OR (
          NOT X.MOVE_INT_TO_UNCLAIMED IS NULL AND Y.MOVE_INT_TO_UNCLAIMED IS NULL
        )
        OR (
          X.MOVE_INT_TO_UNCLAIMED <> Y.MOVE_INT_TO_UNCLAIMED
        )
      )
      OR (
        (
          X.MOVE_PRIC_TO_UNCLAIMED IS NULL AND NOT Y.MOVE_PRIC_TO_UNCLAIMED IS NULL
        )
        OR (
          NOT X.MOVE_PRIC_TO_UNCLAIMED IS NULL AND Y.MOVE_PRIC_TO_UNCLAIMED IS NULL
        )
        OR (
          X.MOVE_PRIC_TO_UNCLAIMED <> Y.MOVE_PRIC_TO_UNCLAIMED
        )
      )
      OR (
        (
          X.MUDARABAH_ACC_CLASS IS NULL AND NOT Y.MUDARABAH_ACC_CLASS IS NULL
        )
        OR (
          NOT X.MUDARABAH_ACC_CLASS IS NULL AND Y.MUDARABAH_ACC_CLASS IS NULL
        )
        OR (
          X.MUDARABAH_ACC_CLASS <> Y.MUDARABAH_ACC_CLASS
        )
      )
      OR (
        (
          X.MUDARABAH_FUND_ID IS NULL AND NOT Y.MUDARABAH_FUND_ID IS NULL
        )
        OR (
          NOT X.MUDARABAH_FUND_ID IS NULL AND Y.MUDARABAH_FUND_ID IS NULL
        )
        OR (
          X.MUDARABAH_FUND_ID <> Y.MUDARABAH_FUND_ID
        )
      )
      OR (
        (
          X.NATURAL_GL_SIGN IS NULL AND NOT Y.NATURAL_GL_SIGN IS NULL
        )
        OR (
          NOT X.NATURAL_GL_SIGN IS NULL AND Y.NATURAL_GL_SIGN IS NULL
        )
        OR (
          X.NATURAL_GL_SIGN <> Y.NATURAL_GL_SIGN
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
          X.OVERDRAFT_FACILITY IS NULL AND NOT Y.OVERDRAFT_FACILITY IS NULL
        )
        OR (
          NOT X.OVERDRAFT_FACILITY IS NULL AND Y.OVERDRAFT_FACILITY IS NULL
        )
        OR (
          X.OVERDRAFT_FACILITY <> Y.OVERDRAFT_FACILITY
        )
      )
      OR (
        (
          X.PARTIAL_LIQUIDATION IS NULL AND NOT Y.PARTIAL_LIQUIDATION IS NULL
        )
        OR (
          NOT X.PARTIAL_LIQUIDATION IS NULL AND Y.PARTIAL_LIQUIDATION IS NULL
        )
        OR (
          X.PARTIAL_LIQUIDATION <> Y.PARTIAL_LIQUIDATION
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
          X.PASSBOOK_RT_PRODUCT IS NULL AND NOT Y.PASSBOOK_RT_PRODUCT IS NULL
        )
        OR (
          NOT X.PASSBOOK_RT_PRODUCT IS NULL AND Y.PASSBOOK_RT_PRODUCT IS NULL
        )
        OR (
          X.PASSBOOK_RT_PRODUCT <> Y.PASSBOOK_RT_PRODUCT
        )
      )
      OR (
        (
          X.POOL_CODE IS NULL AND NOT Y.POOL_CODE IS NULL
        )
        OR (
          NOT X.POOL_CODE IS NULL AND Y.POOL_CODE IS NULL
        )
        OR (
          X.POOL_CODE <> Y.POOL_CODE
        )
      )
      OR (
        (
          X.PRE_REDEMPTION IS NULL AND NOT Y.PRE_REDEMPTION IS NULL
        )
        OR (
          NOT X.PRE_REDEMPTION IS NULL AND Y.PRE_REDEMPTION IS NULL
        )
        OR (
          X.PRE_REDEMPTION <> Y.PRE_REDEMPTION
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
          X.PROFIT_CALC_BAL_TYPE IS NULL AND NOT Y.PROFIT_CALC_BAL_TYPE IS NULL
        )
        OR (
          NOT X.PROFIT_CALC_BAL_TYPE IS NULL AND Y.PROFIT_CALC_BAL_TYPE IS NULL
        )
        OR (
          X.PROFIT_CALC_BAL_TYPE <> Y.PROFIT_CALC_BAL_TYPE
        )
      )
      OR (
        (
          X.PROFIT_SHARING IS NULL AND NOT Y.PROFIT_SHARING IS NULL
        )
        OR (
          NOT X.PROFIT_SHARING IS NULL AND Y.PROFIT_SHARING IS NULL
        )
        OR (
          X.PROFIT_SHARING <> Y.PROFIT_SHARING
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
          X.PROVIDE_INTEREST_ON_BROKEN_DEP IS NULL
          AND NOT Y.PROVIDE_INTEREST_ON_BROKEN_DEP IS NULL
        )
        OR (
          NOT X.PROVIDE_INTEREST_ON_BROKEN_DEP IS NULL
          AND Y.PROVIDE_INTEREST_ON_BROKEN_DEP IS NULL
        )
        OR (
          X.PROVIDE_INTEREST_ON_BROKEN_DEP <> Y.PROVIDE_INTEREST_ON_BROKEN_DEP
        )
      )
      OR (
        (
          X.PROVISIONING_FREQUENCY IS NULL AND NOT Y.PROVISIONING_FREQUENCY IS NULL
        )
        OR (
          NOT X.PROVISIONING_FREQUENCY IS NULL AND Y.PROVISIONING_FREQUENCY IS NULL
        )
        OR (
          X.PROVISIONING_FREQUENCY <> Y.PROVISIONING_FREQUENCY
        )
      )
      OR (
        (
          X.RATE_CHART_TENOR IS NULL AND NOT Y.RATE_CHART_TENOR IS NULL
        )
        OR (
          NOT X.RATE_CHART_TENOR IS NULL AND Y.RATE_CHART_TENOR IS NULL
        )
        OR (
          X.RATE_CHART_TENOR <> Y.RATE_CHART_TENOR
        )
      )
      OR (
        (
          X.RD_FLAG IS NULL AND NOT Y.RD_FLAG IS NULL
        )
        OR (
          NOT X.RD_FLAG IS NULL AND Y.RD_FLAG IS NULL
        )
        OR (
          X.RD_FLAG <> Y.RD_FLAG
        )
      )
      OR (
        (
          X.RD_MAX_SCHEDULE_DAYS IS NULL AND NOT Y.RD_MAX_SCHEDULE_DAYS IS NULL
        )
        OR (
          NOT X.RD_MAX_SCHEDULE_DAYS IS NULL AND Y.RD_MAX_SCHEDULE_DAYS IS NULL
        )
        OR (
          X.RD_MAX_SCHEDULE_DAYS <> Y.RD_MAX_SCHEDULE_DAYS
        )
      )
      OR (
        (
          X.RD_MIN_INSTALLMENT_AMT IS NULL AND NOT Y.RD_MIN_INSTALLMENT_AMT IS NULL
        )
        OR (
          NOT X.RD_MIN_INSTALLMENT_AMT IS NULL AND Y.RD_MIN_INSTALLMENT_AMT IS NULL
        )
        OR (
          X.RD_MIN_INSTALLMENT_AMT <> Y.RD_MIN_INSTALLMENT_AMT
        )
      )
      OR (
        (
          X.RD_MIN_SCHEDULE_DAYS IS NULL AND NOT Y.RD_MIN_SCHEDULE_DAYS IS NULL
        )
        OR (
          NOT X.RD_MIN_SCHEDULE_DAYS IS NULL AND Y.RD_MIN_SCHEDULE_DAYS IS NULL
        )
        OR (
          X.RD_MIN_SCHEDULE_DAYS <> Y.RD_MIN_SCHEDULE_DAYS
        )
      )
      OR (
        (
          X.RD_MOVE_FUNDS_ON_OVD IS NULL AND NOT Y.RD_MOVE_FUNDS_ON_OVD IS NULL
        )
        OR (
          NOT X.RD_MOVE_FUNDS_ON_OVD IS NULL AND Y.RD_MOVE_FUNDS_ON_OVD IS NULL
        )
        OR (
          X.RD_MOVE_FUNDS_ON_OVD <> Y.RD_MOVE_FUNDS_ON_OVD
        )
      )
      OR (
        (
          X.RD_MOVE_MAT_TO_UNCLAIMED IS NULL AND NOT Y.RD_MOVE_MAT_TO_UNCLAIMED IS NULL
        )
        OR (
          NOT X.RD_MOVE_MAT_TO_UNCLAIMED IS NULL AND Y.RD_MOVE_MAT_TO_UNCLAIMED IS NULL
        )
        OR (
          X.RD_MOVE_MAT_TO_UNCLAIMED <> Y.RD_MOVE_MAT_TO_UNCLAIMED
        )
      )
      OR (
        (
          X.RD_SCHEDULE_DAYS IS NULL AND NOT Y.RD_SCHEDULE_DAYS IS NULL
        )
        OR (
          NOT X.RD_SCHEDULE_DAYS IS NULL AND Y.RD_SCHEDULE_DAYS IS NULL
        )
        OR (
          X.RD_SCHEDULE_DAYS <> Y.RD_SCHEDULE_DAYS
        )
      )
      OR (
        (
          X.RD_SCHEDULE_MONTHS IS NULL AND NOT Y.RD_SCHEDULE_MONTHS IS NULL
        )
        OR (
          NOT X.RD_SCHEDULE_MONTHS IS NULL AND Y.RD_SCHEDULE_MONTHS IS NULL
        )
        OR (
          X.RD_SCHEDULE_MONTHS <> Y.RD_SCHEDULE_MONTHS
        )
      )
      OR (
        (
          X.RD_SCHEDULE_YEARS IS NULL AND NOT Y.RD_SCHEDULE_YEARS IS NULL
        )
        OR (
          NOT X.RD_SCHEDULE_YEARS IS NULL AND Y.RD_SCHEDULE_YEARS IS NULL
        )
        OR (
          X.RD_SCHEDULE_YEARS <> Y.RD_SCHEDULE_YEARS
        )
      )
      OR (
        (
          X.RECOMPUTE_MAT_DATE IS NULL AND NOT Y.RECOMPUTE_MAT_DATE IS NULL
        )
        OR (
          NOT X.RECOMPUTE_MAT_DATE IS NULL AND Y.RECOMPUTE_MAT_DATE IS NULL
        )
        OR (
          X.RECOMPUTE_MAT_DATE <> Y.RECOMPUTE_MAT_DATE
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
          X.REDEMPTION IS NULL AND NOT Y.REDEMPTION IS NULL
        )
        OR (
          NOT X.REDEMPTION IS NULL AND Y.REDEMPTION IS NULL
        )
        OR (
          X.REDEMPTION <> Y.REDEMPTION
        )
      )
      OR (
        (
          X.REFERENCE_DATE IS NULL AND NOT Y.REFERENCE_DATE IS NULL
        )
        OR (
          NOT X.REFERENCE_DATE IS NULL AND Y.REFERENCE_DATE IS NULL
        )
        OR (
          X.REFERENCE_DATE <> Y.REFERENCE_DATE
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
          X.SAL_BLK_DAYS IS NULL AND NOT Y.SAL_BLK_DAYS IS NULL
        )
        OR (
          NOT X.SAL_BLK_DAYS IS NULL AND Y.SAL_BLK_DAYS IS NULL
        )
        OR (
          X.SAL_BLK_DAYS <> Y.SAL_BLK_DAYS
        )
      )
      OR (
        (
          X.SEND_NOTIFICATION IS NULL AND NOT Y.SEND_NOTIFICATION IS NULL
        )
        OR (
          NOT X.SEND_NOTIFICATION IS NULL AND Y.SEND_NOTIFICATION IS NULL
        )
        OR (
          X.SEND_NOTIFICATION <> Y.SEND_NOTIFICATION
        )
      )
      OR (
        (
          X.SK_ACC_STMT_CYCLE IS NULL AND NOT Y.SK_ACC_STMT_CYCLE IS NULL
        )
        OR (
          NOT X.SK_ACC_STMT_CYCLE IS NULL AND Y.SK_ACC_STMT_CYCLE IS NULL
        )
        OR (
          X.SK_ACC_STMT_CYCLE <> Y.SK_ACC_STMT_CYCLE
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
          X.SK_AUTO_ROLLOVER IS NULL AND NOT Y.SK_AUTO_ROLLOVER IS NULL
        )
        OR (
          NOT X.SK_AUTO_ROLLOVER IS NULL AND Y.SK_AUTO_ROLLOVER IS NULL
        )
        OR (
          X.SK_AUTO_ROLLOVER <> Y.SK_AUTO_ROLLOVER
        )
      )
      OR (
        (
          X.SK_CLUSTER_ID IS NULL AND NOT Y.SK_CLUSTER_ID IS NULL
        )
        OR (
          NOT X.SK_CLUSTER_ID IS NULL AND Y.SK_CLUSTER_ID IS NULL
        )
        OR (
          X.SK_CLUSTER_ID <> Y.SK_CLUSTER_ID
        )
      )
      OR (
        (
          X.SK_DEFAULT_TENOR_DAYS IS NULL AND NOT Y.SK_DEFAULT_TENOR_DAYS IS NULL
        )
        OR (
          NOT X.SK_DEFAULT_TENOR_DAYS IS NULL AND Y.SK_DEFAULT_TENOR_DAYS IS NULL
        )
        OR (
          X.SK_DEFAULT_TENOR_DAYS <> Y.SK_DEFAULT_TENOR_DAYS
        )
      )
      OR (
        (
          X.SK_DEFAULT_TENOR_MONTHS IS NULL AND NOT Y.SK_DEFAULT_TENOR_MONTHS IS NULL
        )
        OR (
          NOT X.SK_DEFAULT_TENOR_MONTHS IS NULL AND Y.SK_DEFAULT_TENOR_MONTHS IS NULL
        )
        OR (
          X.SK_DEFAULT_TENOR_MONTHS <> Y.SK_DEFAULT_TENOR_MONTHS
        )
      )
      OR (
        (
          X.SK_DEFAULT_TENOR_YEARS IS NULL AND NOT Y.SK_DEFAULT_TENOR_YEARS IS NULL
        )
        OR (
          NOT X.SK_DEFAULT_TENOR_YEARS IS NULL AND Y.SK_DEFAULT_TENOR_YEARS IS NULL
        )
        OR (
          X.SK_DEFAULT_TENOR_YEARS <> Y.SK_DEFAULT_TENOR_YEARS
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
          X.SK_LIMIT_CHECK_REQUIRED IS NULL AND NOT Y.SK_LIMIT_CHECK_REQUIRED IS NULL
        )
        OR (
          NOT X.SK_LIMIT_CHECK_REQUIRED IS NULL AND Y.SK_LIMIT_CHECK_REQUIRED IS NULL
        )
        OR (
          X.SK_LIMIT_CHECK_REQUIRED <> Y.SK_LIMIT_CHECK_REQUIRED
        )
      )
      OR (
        (
          X.SK_MAX_TENOR_YEARS IS NULL AND NOT Y.SK_MAX_TENOR_YEARS IS NULL
        )
        OR (
          NOT X.SK_MAX_TENOR_YEARS IS NULL AND Y.SK_MAX_TENOR_YEARS IS NULL
        )
        OR (
          X.SK_MAX_TENOR_YEARS <> Y.SK_MAX_TENOR_YEARS
        )
      )
      OR (
        (
          X.SK_OVERDRAFT_FACILITY IS NULL AND NOT Y.SK_OVERDRAFT_FACILITY IS NULL
        )
        OR (
          NOT X.SK_OVERDRAFT_FACILITY IS NULL AND Y.SK_OVERDRAFT_FACILITY IS NULL
        )
        OR (
          X.SK_OVERDRAFT_FACILITY <> Y.SK_OVERDRAFT_FACILITY
        )
      )
      OR (
        (
          X.SPECIAL_RATE_CODE IS NULL AND NOT Y.SPECIAL_RATE_CODE IS NULL
        )
        OR (
          NOT X.SPECIAL_RATE_CODE IS NULL AND Y.SPECIAL_RATE_CODE IS NULL
        )
        OR (
          X.SPECIAL_RATE_CODE <> Y.SPECIAL_RATE_CODE
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
          X.STACCCLS_BRANCH_LIST IS NULL AND NOT Y.STACCCLS_BRANCH_LIST IS NULL
        )
        OR (
          NOT X.STACCCLS_BRANCH_LIST IS NULL AND Y.STACCCLS_BRANCH_LIST IS NULL
        )
        OR (
          X.STACCCLS_BRANCH_LIST <> Y.STACCCLS_BRANCH_LIST
        )
      )
      OR (
        (
          X.STACCCLS_CCY_LIST IS NULL AND NOT Y.STACCCLS_CCY_LIST IS NULL
        )
        OR (
          NOT X.STACCCLS_CCY_LIST IS NULL AND Y.STACCCLS_CCY_LIST IS NULL
        )
        OR (
          X.STACCCLS_CCY_LIST <> Y.STACCCLS_CCY_LIST
        )
      )
      OR (
        (
          X.STACCCLS_CUSCAT_LIST IS NULL AND NOT Y.STACCCLS_CUSCAT_LIST IS NULL
        )
        OR (
          NOT X.STACCCLS_CUSCAT_LIST IS NULL AND Y.STACCCLS_CUSCAT_LIST IS NULL
        )
        OR (
          X.STACCCLS_CUSCAT_LIST <> Y.STACCCLS_CUSCAT_LIST
        )
      )
      OR (
        (
          X.START_DATE IS NULL AND NOT Y.START_DATE IS NULL
        )
        OR (
          NOT X.START_DATE IS NULL AND Y.START_DATE IS NULL
        )
        OR (
          X.START_DATE <> Y.START_DATE
        )
      )
      OR (
        (
          X.STATEMENT_DAY IS NULL AND NOT Y.STATEMENT_DAY IS NULL
        )
        OR (
          NOT X.STATEMENT_DAY IS NULL AND Y.STATEMENT_DAY IS NULL
        )
        OR (
          X.STATEMENT_DAY <> Y.STATEMENT_DAY
        )
      )
      OR (
        (
          X.STATEMENT_DAY2 IS NULL AND NOT Y.STATEMENT_DAY2 IS NULL
        )
        OR (
          NOT X.STATEMENT_DAY2 IS NULL AND Y.STATEMENT_DAY2 IS NULL
        )
        OR (
          X.STATEMENT_DAY2 <> Y.STATEMENT_DAY2
        )
      )
      OR (
        (
          X.STATEMENT_DAY3 IS NULL AND NOT Y.STATEMENT_DAY3 IS NULL
        )
        OR (
          NOT X.STATEMENT_DAY3 IS NULL AND Y.STATEMENT_DAY3 IS NULL
        )
        OR (
          X.STATEMENT_DAY3 <> Y.STATEMENT_DAY3
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
          X.SWEEP_MODE IS NULL AND NOT Y.SWEEP_MODE IS NULL
        )
        OR (
          NOT X.SWEEP_MODE IS NULL AND Y.SWEEP_MODE IS NULL
        )
        OR (
          X.SWEEP_MODE <> Y.SWEEP_MODE
        )
      )
      OR (
        (
          X.TD_RATECHART_ALLOW IS NULL AND NOT Y.TD_RATECHART_ALLOW IS NULL
        )
        OR (
          NOT X.TD_RATECHART_ALLOW IS NULL AND Y.TD_RATECHART_ALLOW IS NULL
        )
        OR (
          X.TD_RATECHART_ALLOW <> Y.TD_RATECHART_ALLOW
        )
      )
      OR (
        (
          X.TRACK_ACCRUED_IC IS NULL AND NOT Y.TRACK_ACCRUED_IC IS NULL
        )
        OR (
          NOT X.TRACK_ACCRUED_IC IS NULL AND Y.TRACK_ACCRUED_IC IS NULL
        )
        OR (
          X.TRACK_ACCRUED_IC <> Y.TRACK_ACCRUED_IC
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
          X.TRN_CODE IS NULL AND NOT Y.TRN_CODE IS NULL
        )
        OR (
          NOT X.TRN_CODE IS NULL AND Y.TRN_CODE IS NULL
        )
        OR (
          X.TRN_CODE <> Y.TRN_CODE
        )
      )
      OR (
        (
          X.TRN_REV_CODE IS NULL AND NOT Y.TRN_REV_CODE IS NULL
        )
        OR (
          NOT X.TRN_REV_CODE IS NULL AND Y.TRN_REV_CODE IS NULL
        )
        OR (
          X.TRN_REV_CODE <> Y.TRN_REV_CODE
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
          X.TRUSTAC_CASH_LIMIT IS NULL AND NOT Y.TRUSTAC_CASH_LIMIT IS NULL
        )
        OR (
          NOT X.TRUSTAC_CASH_LIMIT IS NULL AND Y.TRUSTAC_CASH_LIMIT IS NULL
        )
        OR (
          X.TRUSTAC_CASH_LIMIT <> Y.TRUSTAC_CASH_LIMIT
        )
      )
      OR (
        (
          X.TURNOVER_EVENT_CLASS_CODE IS NULL AND NOT Y.TURNOVER_EVENT_CLASS_CODE IS NULL
        )
        OR (
          NOT X.TURNOVER_EVENT_CLASS_CODE IS NULL AND Y.TURNOVER_EVENT_CLASS_CODE IS NULL
        )
        OR (
          X.TURNOVER_EVENT_CLASS_CODE <> Y.TURNOVER_EVENT_CLASS_CODE
        )
      )
      OR (
        (
          X.TURNOVER_RTOH_CODE IS NULL AND NOT Y.TURNOVER_RTOH_CODE IS NULL
        )
        OR (
          NOT X.TURNOVER_RTOH_CODE IS NULL AND Y.TURNOVER_RTOH_CODE IS NULL
        )
        OR (
          X.TURNOVER_RTOH_CODE <> Y.TURNOVER_RTOH_CODE
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
          X.VERIFY_FUNDS_FOR_DRINT IS NULL AND NOT Y.VERIFY_FUNDS_FOR_DRINT IS NULL
        )
        OR (
          NOT X.VERIFY_FUNDS_FOR_DRINT IS NULL AND Y.VERIFY_FUNDS_FOR_DRINT IS NULL
        )
        OR (
          X.VERIFY_FUNDS_FOR_DRINT <> Y.VERIFY_FUNDS_FOR_DRINT
        )
      )
    )
    THEN 'UPD'
    WHEN X.ACCOUNT_CLASS IS NULL
    THEN 'DEL' /* - IF MULTIPLE PKs are there then concatenate them with AND */
  END AS OPT_FLAG
FROM DEVT_STG_FLX.STTM_ACCOUNT_CLASS AS X
FULL JOIN DEVT_STG_FLX.STTM_ACCOUNT_CLASS_BACKUP AS Y
  ON X.ACCOUNT_CLASS = Y.ACCOUNT_CLASS /* - IF MULTIPLE PKs are there then concatenate them with AND */
WHERE
  NOT OPT_FLAG IS NULL