CREATE OR REPLACE VIEW DEVV_STG_FLX.ICTM_ACC_get_delta AS LOCKING ROW FOR ACCESS
SELECT
  COALESCE(X.ACC, Y.ACC) AS ACC,
  COALESCE(X.BRN, Y.BRN) AS BRN,
  X.ACC_TYPE,
  X.ALLOW_PREPAYMENT,
  X.AUTO_EXTENSION,
  X.AUTO_ROLLOVER,
  X.BM_BOOK_CCY,
  X.BOOK_ACC,
  X.BOOK_BRN,
  X.BOOK_CCY,
  X.BVT_DATE,
  X.CALC_ACC,
  X.CHARGE_BOOK_ACC,
  X.CHARGE_BOOK_BRN,
  X.CHARGE_BOOK_CCY,
  X.CHG_START_DATE,
  X.CLOSE_ON_MATURITY,
  X.COMBVT_DATE,
  X.DEP_TENOR_PREF,
  X.HAS_DRCR_ADV,
  X.HAS_IS,
  X.INT_START_DATE,
  X.INTEREST_RATE,
  X.INTRATE_CUMAMT_REQD,
  X.INTRATE_CUMAMT_ROLL_REQD,
  X.LAST_IS_DATE,
  X.LAST_ROLLOVER_DATE,
  X.LIQUIDATION_AMT,
  X.MATURITY_AMOUNT,
  X.MATURITY_DATE,
  X.MOVE_INT_TO_UNCLAIMED,
  X.MOVE_PRIC_TO_UNCLAIMED,
  X.NEXT_MATURITY_DATE,
  X.ORIG_TENOR_DAYS,
  X.ORIG_TENOR_MONTHS,
  X.ORIG_TENOR_YEARS,
  X.OVERRIDE_AMT_BLOCK,
  X.PRINC_LIQ_AC,
  X.PRINC_LIQ_BRANCH,
  X.RD_AUTO_PMNT_TAKEDOWN,
  X.RD_FLAG,
  X.RD_INSTALLMENT_AMT,
  X.RD_MOVE_FUNDS_ON_OVD,
  X.RD_MOVE_MAT_TO_UNCLAIMED,
  X.RD_PAY_SCHDT,
  X.RD_PAYMENT_ACC,
  X.RD_PAYMENT_BRN,
  X.RD_PAYMENT_CCY,
  X.RD_TAKEDOWN_DAYS,
  X.RD_TAKEDOWN_MONTHS,
  X.RD_TAKEDOWN_YEARS,
  X.REDM_INT_PAYOUT,
  X.ROLL_TENOR_DAYS,
  X.ROLL_TENOR_MONTHS,
  X.ROLL_TENOR_PREF,
  X.ROLL_TENOR_YEARS,
  X.ROLLOVER_AMT,
  X.ROLLOVER_TYPE,
  X.SK_ACC,
  X.SK_AUTO_ROLLOVER,
  X.SK_BOOK_ACC,
  X.SK_BRN,
  X.SK_CLOSE_ON_MATURITY,
  X.SK_INTEREST_RATE,
  X.SK_ORIG_TENOR_DAYS,
  X.SK_ORIG_TENOR_MONTHS,
  X.SK_ORIG_TENOR_YEARS,
  X.TENOR_CODE,
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
    WHEN Y.ACC IS NULL AND Y.BRN IS NULL
    THEN 'INS' /* - IF MULTIPLE PKs are there then concatenate them with AND */
    WHEN X.ACC = Y.ACC
    AND X.BRN = Y.BRN /* - IF MULTIPLE PKs are there then concatenate them with AND */
    AND (
      (
        (
          X.ACC_TYPE IS NULL AND NOT Y.ACC_TYPE IS NULL
        )
        OR (
          NOT X.ACC_TYPE IS NULL AND Y.ACC_TYPE IS NULL
        )
        OR (
          X.ACC_TYPE <> Y.ACC_TYPE
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
          X.BM_BOOK_CCY IS NULL AND NOT Y.BM_BOOK_CCY IS NULL
        )
        OR (
          NOT X.BM_BOOK_CCY IS NULL AND Y.BM_BOOK_CCY IS NULL
        )
        OR (
          X.BM_BOOK_CCY <> Y.BM_BOOK_CCY
        )
      )
      OR (
        (
          X.BOOK_ACC IS NULL AND NOT Y.BOOK_ACC IS NULL
        )
        OR (
          NOT X.BOOK_ACC IS NULL AND Y.BOOK_ACC IS NULL
        )
        OR (
          X.BOOK_ACC <> Y.BOOK_ACC
        )
      )
      OR (
        (
          X.BOOK_BRN IS NULL AND NOT Y.BOOK_BRN IS NULL
        )
        OR (
          NOT X.BOOK_BRN IS NULL AND Y.BOOK_BRN IS NULL
        )
        OR (
          X.BOOK_BRN <> Y.BOOK_BRN
        )
      )
      OR (
        (
          X.BOOK_CCY IS NULL AND NOT Y.BOOK_CCY IS NULL
        )
        OR (
          NOT X.BOOK_CCY IS NULL AND Y.BOOK_CCY IS NULL
        )
        OR (
          X.BOOK_CCY <> Y.BOOK_CCY
        )
      )
      OR (
        (
          X.BVT_DATE IS NULL AND NOT Y.BVT_DATE IS NULL
        )
        OR (
          NOT X.BVT_DATE IS NULL AND Y.BVT_DATE IS NULL
        )
        OR (
          X.BVT_DATE <> Y.BVT_DATE
        )
      )
      OR (
        (
          X.CALC_ACC IS NULL AND NOT Y.CALC_ACC IS NULL
        )
        OR (
          NOT X.CALC_ACC IS NULL AND Y.CALC_ACC IS NULL
        )
        OR (
          X.CALC_ACC <> Y.CALC_ACC
        )
      )
      OR (
        (
          X.CHARGE_BOOK_ACC IS NULL AND NOT Y.CHARGE_BOOK_ACC IS NULL
        )
        OR (
          NOT X.CHARGE_BOOK_ACC IS NULL AND Y.CHARGE_BOOK_ACC IS NULL
        )
        OR (
          X.CHARGE_BOOK_ACC <> Y.CHARGE_BOOK_ACC
        )
      )
      OR (
        (
          X.CHARGE_BOOK_BRN IS NULL AND NOT Y.CHARGE_BOOK_BRN IS NULL
        )
        OR (
          NOT X.CHARGE_BOOK_BRN IS NULL AND Y.CHARGE_BOOK_BRN IS NULL
        )
        OR (
          X.CHARGE_BOOK_BRN <> Y.CHARGE_BOOK_BRN
        )
      )
      OR (
        (
          X.CHARGE_BOOK_CCY IS NULL AND NOT Y.CHARGE_BOOK_CCY IS NULL
        )
        OR (
          NOT X.CHARGE_BOOK_CCY IS NULL AND Y.CHARGE_BOOK_CCY IS NULL
        )
        OR (
          X.CHARGE_BOOK_CCY <> Y.CHARGE_BOOK_CCY
        )
      )
      OR (
        (
          X.CHG_START_DATE IS NULL AND NOT Y.CHG_START_DATE IS NULL
        )
        OR (
          NOT X.CHG_START_DATE IS NULL AND Y.CHG_START_DATE IS NULL
        )
        OR (
          X.CHG_START_DATE <> Y.CHG_START_DATE
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
          X.COMBVT_DATE IS NULL AND NOT Y.COMBVT_DATE IS NULL
        )
        OR (
          NOT X.COMBVT_DATE IS NULL AND Y.COMBVT_DATE IS NULL
        )
        OR (
          X.COMBVT_DATE <> Y.COMBVT_DATE
        )
      )
      OR (
        (
          X.DEP_TENOR_PREF IS NULL AND NOT Y.DEP_TENOR_PREF IS NULL
        )
        OR (
          NOT X.DEP_TENOR_PREF IS NULL AND Y.DEP_TENOR_PREF IS NULL
        )
        OR (
          X.DEP_TENOR_PREF <> Y.DEP_TENOR_PREF
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
          X.INT_START_DATE IS NULL AND NOT Y.INT_START_DATE IS NULL
        )
        OR (
          NOT X.INT_START_DATE IS NULL AND Y.INT_START_DATE IS NULL
        )
        OR (
          X.INT_START_DATE <> Y.INT_START_DATE
        )
      )
      OR (
        (
          X.INTEREST_RATE IS NULL AND NOT Y.INTEREST_RATE IS NULL
        )
        OR (
          NOT X.INTEREST_RATE IS NULL AND Y.INTEREST_RATE IS NULL
        )
        OR (
          X.INTEREST_RATE <> Y.INTEREST_RATE
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
          X.INTRATE_CUMAMT_ROLL_REQD IS NULL AND NOT Y.INTRATE_CUMAMT_ROLL_REQD IS NULL
        )
        OR (
          NOT X.INTRATE_CUMAMT_ROLL_REQD IS NULL AND Y.INTRATE_CUMAMT_ROLL_REQD IS NULL
        )
        OR (
          X.INTRATE_CUMAMT_ROLL_REQD <> Y.INTRATE_CUMAMT_ROLL_REQD
        )
      )
      OR (
        (
          X.LAST_IS_DATE IS NULL AND NOT Y.LAST_IS_DATE IS NULL
        )
        OR (
          NOT X.LAST_IS_DATE IS NULL AND Y.LAST_IS_DATE IS NULL
        )
        OR (
          X.LAST_IS_DATE <> Y.LAST_IS_DATE
        )
      )
      OR (
        (
          X.LAST_ROLLOVER_DATE IS NULL AND NOT Y.LAST_ROLLOVER_DATE IS NULL
        )
        OR (
          NOT X.LAST_ROLLOVER_DATE IS NULL AND Y.LAST_ROLLOVER_DATE IS NULL
        )
        OR (
          X.LAST_ROLLOVER_DATE <> Y.LAST_ROLLOVER_DATE
        )
      )
      OR (
        (
          X.LIQUIDATION_AMT IS NULL AND NOT Y.LIQUIDATION_AMT IS NULL
        )
        OR (
          NOT X.LIQUIDATION_AMT IS NULL AND Y.LIQUIDATION_AMT IS NULL
        )
        OR (
          X.LIQUIDATION_AMT <> Y.LIQUIDATION_AMT
        )
      )
      OR (
        (
          X.MATURITY_AMOUNT IS NULL AND NOT Y.MATURITY_AMOUNT IS NULL
        )
        OR (
          NOT X.MATURITY_AMOUNT IS NULL AND Y.MATURITY_AMOUNT IS NULL
        )
        OR (
          X.MATURITY_AMOUNT <> Y.MATURITY_AMOUNT
        )
      )
      OR (
        (
          X.MATURITY_DATE IS NULL AND NOT Y.MATURITY_DATE IS NULL
        )
        OR (
          NOT X.MATURITY_DATE IS NULL AND Y.MATURITY_DATE IS NULL
        )
        OR (
          X.MATURITY_DATE <> Y.MATURITY_DATE
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
          X.NEXT_MATURITY_DATE IS NULL AND NOT Y.NEXT_MATURITY_DATE IS NULL
        )
        OR (
          NOT X.NEXT_MATURITY_DATE IS NULL AND Y.NEXT_MATURITY_DATE IS NULL
        )
        OR (
          X.NEXT_MATURITY_DATE <> Y.NEXT_MATURITY_DATE
        )
      )
      OR (
        (
          X.ORIG_TENOR_DAYS IS NULL AND NOT Y.ORIG_TENOR_DAYS IS NULL
        )
        OR (
          NOT X.ORIG_TENOR_DAYS IS NULL AND Y.ORIG_TENOR_DAYS IS NULL
        )
        OR (
          X.ORIG_TENOR_DAYS <> Y.ORIG_TENOR_DAYS
        )
      )
      OR (
        (
          X.ORIG_TENOR_MONTHS IS NULL AND NOT Y.ORIG_TENOR_MONTHS IS NULL
        )
        OR (
          NOT X.ORIG_TENOR_MONTHS IS NULL AND Y.ORIG_TENOR_MONTHS IS NULL
        )
        OR (
          X.ORIG_TENOR_MONTHS <> Y.ORIG_TENOR_MONTHS
        )
      )
      OR (
        (
          X.ORIG_TENOR_YEARS IS NULL AND NOT Y.ORIG_TENOR_YEARS IS NULL
        )
        OR (
          NOT X.ORIG_TENOR_YEARS IS NULL AND Y.ORIG_TENOR_YEARS IS NULL
        )
        OR (
          X.ORIG_TENOR_YEARS <> Y.ORIG_TENOR_YEARS
        )
      )
      OR (
        (
          X.OVERRIDE_AMT_BLOCK IS NULL AND NOT Y.OVERRIDE_AMT_BLOCK IS NULL
        )
        OR (
          NOT X.OVERRIDE_AMT_BLOCK IS NULL AND Y.OVERRIDE_AMT_BLOCK IS NULL
        )
        OR (
          X.OVERRIDE_AMT_BLOCK <> Y.OVERRIDE_AMT_BLOCK
        )
      )
      OR (
        (
          X.PRINC_LIQ_AC IS NULL AND NOT Y.PRINC_LIQ_AC IS NULL
        )
        OR (
          NOT X.PRINC_LIQ_AC IS NULL AND Y.PRINC_LIQ_AC IS NULL
        )
        OR (
          X.PRINC_LIQ_AC <> Y.PRINC_LIQ_AC
        )
      )
      OR (
        (
          X.PRINC_LIQ_BRANCH IS NULL AND NOT Y.PRINC_LIQ_BRANCH IS NULL
        )
        OR (
          NOT X.PRINC_LIQ_BRANCH IS NULL AND Y.PRINC_LIQ_BRANCH IS NULL
        )
        OR (
          X.PRINC_LIQ_BRANCH <> Y.PRINC_LIQ_BRANCH
        )
      )
      OR (
        (
          X.RD_AUTO_PMNT_TAKEDOWN IS NULL AND NOT Y.RD_AUTO_PMNT_TAKEDOWN IS NULL
        )
        OR (
          NOT X.RD_AUTO_PMNT_TAKEDOWN IS NULL AND Y.RD_AUTO_PMNT_TAKEDOWN IS NULL
        )
        OR (
          X.RD_AUTO_PMNT_TAKEDOWN <> Y.RD_AUTO_PMNT_TAKEDOWN
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
          X.RD_INSTALLMENT_AMT IS NULL AND NOT Y.RD_INSTALLMENT_AMT IS NULL
        )
        OR (
          NOT X.RD_INSTALLMENT_AMT IS NULL AND Y.RD_INSTALLMENT_AMT IS NULL
        )
        OR (
          X.RD_INSTALLMENT_AMT <> Y.RD_INSTALLMENT_AMT
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
          X.RD_PAY_SCHDT IS NULL AND NOT Y.RD_PAY_SCHDT IS NULL
        )
        OR (
          NOT X.RD_PAY_SCHDT IS NULL AND Y.RD_PAY_SCHDT IS NULL
        )
        OR (
          X.RD_PAY_SCHDT <> Y.RD_PAY_SCHDT
        )
      )
      OR (
        (
          X.RD_PAYMENT_ACC IS NULL AND NOT Y.RD_PAYMENT_ACC IS NULL
        )
        OR (
          NOT X.RD_PAYMENT_ACC IS NULL AND Y.RD_PAYMENT_ACC IS NULL
        )
        OR (
          X.RD_PAYMENT_ACC <> Y.RD_PAYMENT_ACC
        )
      )
      OR (
        (
          X.RD_PAYMENT_BRN IS NULL AND NOT Y.RD_PAYMENT_BRN IS NULL
        )
        OR (
          NOT X.RD_PAYMENT_BRN IS NULL AND Y.RD_PAYMENT_BRN IS NULL
        )
        OR (
          X.RD_PAYMENT_BRN <> Y.RD_PAYMENT_BRN
        )
      )
      OR (
        (
          X.RD_PAYMENT_CCY IS NULL AND NOT Y.RD_PAYMENT_CCY IS NULL
        )
        OR (
          NOT X.RD_PAYMENT_CCY IS NULL AND Y.RD_PAYMENT_CCY IS NULL
        )
        OR (
          X.RD_PAYMENT_CCY <> Y.RD_PAYMENT_CCY
        )
      )
      OR (
        (
          X.RD_TAKEDOWN_DAYS IS NULL AND NOT Y.RD_TAKEDOWN_DAYS IS NULL
        )
        OR (
          NOT X.RD_TAKEDOWN_DAYS IS NULL AND Y.RD_TAKEDOWN_DAYS IS NULL
        )
        OR (
          X.RD_TAKEDOWN_DAYS <> Y.RD_TAKEDOWN_DAYS
        )
      )
      OR (
        (
          X.RD_TAKEDOWN_MONTHS IS NULL AND NOT Y.RD_TAKEDOWN_MONTHS IS NULL
        )
        OR (
          NOT X.RD_TAKEDOWN_MONTHS IS NULL AND Y.RD_TAKEDOWN_MONTHS IS NULL
        )
        OR (
          X.RD_TAKEDOWN_MONTHS <> Y.RD_TAKEDOWN_MONTHS
        )
      )
      OR (
        (
          X.RD_TAKEDOWN_YEARS IS NULL AND NOT Y.RD_TAKEDOWN_YEARS IS NULL
        )
        OR (
          NOT X.RD_TAKEDOWN_YEARS IS NULL AND Y.RD_TAKEDOWN_YEARS IS NULL
        )
        OR (
          X.RD_TAKEDOWN_YEARS <> Y.RD_TAKEDOWN_YEARS
        )
      )
      OR (
        (
          X.REDM_INT_PAYOUT IS NULL AND NOT Y.REDM_INT_PAYOUT IS NULL
        )
        OR (
          NOT X.REDM_INT_PAYOUT IS NULL AND Y.REDM_INT_PAYOUT IS NULL
        )
        OR (
          X.REDM_INT_PAYOUT <> Y.REDM_INT_PAYOUT
        )
      )
      OR (
        (
          X.ROLL_TENOR_DAYS IS NULL AND NOT Y.ROLL_TENOR_DAYS IS NULL
        )
        OR (
          NOT X.ROLL_TENOR_DAYS IS NULL AND Y.ROLL_TENOR_DAYS IS NULL
        )
        OR (
          X.ROLL_TENOR_DAYS <> Y.ROLL_TENOR_DAYS
        )
      )
      OR (
        (
          X.ROLL_TENOR_MONTHS IS NULL AND NOT Y.ROLL_TENOR_MONTHS IS NULL
        )
        OR (
          NOT X.ROLL_TENOR_MONTHS IS NULL AND Y.ROLL_TENOR_MONTHS IS NULL
        )
        OR (
          X.ROLL_TENOR_MONTHS <> Y.ROLL_TENOR_MONTHS
        )
      )
      OR (
        (
          X.ROLL_TENOR_PREF IS NULL AND NOT Y.ROLL_TENOR_PREF IS NULL
        )
        OR (
          NOT X.ROLL_TENOR_PREF IS NULL AND Y.ROLL_TENOR_PREF IS NULL
        )
        OR (
          X.ROLL_TENOR_PREF <> Y.ROLL_TENOR_PREF
        )
      )
      OR (
        (
          X.ROLL_TENOR_YEARS IS NULL AND NOT Y.ROLL_TENOR_YEARS IS NULL
        )
        OR (
          NOT X.ROLL_TENOR_YEARS IS NULL AND Y.ROLL_TENOR_YEARS IS NULL
        )
        OR (
          X.ROLL_TENOR_YEARS <> Y.ROLL_TENOR_YEARS
        )
      )
      OR (
        (
          X.ROLLOVER_AMT IS NULL AND NOT Y.ROLLOVER_AMT IS NULL
        )
        OR (
          NOT X.ROLLOVER_AMT IS NULL AND Y.ROLLOVER_AMT IS NULL
        )
        OR (
          X.ROLLOVER_AMT <> Y.ROLLOVER_AMT
        )
      )
      OR (
        (
          X.ROLLOVER_TYPE IS NULL AND NOT Y.ROLLOVER_TYPE IS NULL
        )
        OR (
          NOT X.ROLLOVER_TYPE IS NULL AND Y.ROLLOVER_TYPE IS NULL
        )
        OR (
          X.ROLLOVER_TYPE <> Y.ROLLOVER_TYPE
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
          X.SK_BOOK_ACC IS NULL AND NOT Y.SK_BOOK_ACC IS NULL
        )
        OR (
          NOT X.SK_BOOK_ACC IS NULL AND Y.SK_BOOK_ACC IS NULL
        )
        OR (
          X.SK_BOOK_ACC <> Y.SK_BOOK_ACC
        )
      )
      OR (
        (
          X.SK_BRN IS NULL AND NOT Y.SK_BRN IS NULL
        )
        OR (
          NOT X.SK_BRN IS NULL AND Y.SK_BRN IS NULL
        )
        OR (
          X.SK_BRN <> Y.SK_BRN
        )
      )
      OR (
        (
          X.SK_CLOSE_ON_MATURITY IS NULL AND NOT Y.SK_CLOSE_ON_MATURITY IS NULL
        )
        OR (
          NOT X.SK_CLOSE_ON_MATURITY IS NULL AND Y.SK_CLOSE_ON_MATURITY IS NULL
        )
        OR (
          X.SK_CLOSE_ON_MATURITY <> Y.SK_CLOSE_ON_MATURITY
        )
      )
      OR (
        (
          X.SK_INTEREST_RATE IS NULL AND NOT Y.SK_INTEREST_RATE IS NULL
        )
        OR (
          NOT X.SK_INTEREST_RATE IS NULL AND Y.SK_INTEREST_RATE IS NULL
        )
        OR (
          X.SK_INTEREST_RATE <> Y.SK_INTEREST_RATE
        )
      )
      OR (
        (
          X.SK_ORIG_TENOR_DAYS IS NULL AND NOT Y.SK_ORIG_TENOR_DAYS IS NULL
        )
        OR (
          NOT X.SK_ORIG_TENOR_DAYS IS NULL AND Y.SK_ORIG_TENOR_DAYS IS NULL
        )
        OR (
          X.SK_ORIG_TENOR_DAYS <> Y.SK_ORIG_TENOR_DAYS
        )
      )
      OR (
        (
          X.SK_ORIG_TENOR_MONTHS IS NULL AND NOT Y.SK_ORIG_TENOR_MONTHS IS NULL
        )
        OR (
          NOT X.SK_ORIG_TENOR_MONTHS IS NULL AND Y.SK_ORIG_TENOR_MONTHS IS NULL
        )
        OR (
          X.SK_ORIG_TENOR_MONTHS <> Y.SK_ORIG_TENOR_MONTHS
        )
      )
      OR (
        (
          X.SK_ORIG_TENOR_YEARS IS NULL AND NOT Y.SK_ORIG_TENOR_YEARS IS NULL
        )
        OR (
          NOT X.SK_ORIG_TENOR_YEARS IS NULL AND Y.SK_ORIG_TENOR_YEARS IS NULL
        )
        OR (
          X.SK_ORIG_TENOR_YEARS <> Y.SK_ORIG_TENOR_YEARS
        )
      )
      OR (
        (
          X.TENOR_CODE IS NULL AND NOT Y.TENOR_CODE IS NULL
        )
        OR (
          NOT X.TENOR_CODE IS NULL AND Y.TENOR_CODE IS NULL
        )
        OR (
          X.TENOR_CODE <> Y.TENOR_CODE
        )
      )
    )
    THEN 'UPD'
    WHEN X.ACC IS NULL AND X.BRN IS NULL
    THEN 'DEL' /* - IF MULTIPLE PKs are there then concatenate them with AND */
  END AS OPT_FLAG
FROM DEVT_STG_FLX.ICTM_ACC AS X
FULL JOIN DEVT_STG_FLX.ICTM_ACC_BACKUP AS Y
  ON X.ACC = Y.ACC
  AND X.BRN = Y.BRN /* - IF MULTIPLE PKs are there then concatenate them with AND */
WHERE
  NOT OPT_FLAG IS NULL