CREATE OR REPLACE VIEW DEVV_STG_FLX.DETB_RTL_TELLER_get_delta AS LOCKING ROW FOR ACCESS
SELECT
  COALESCE(X.TRN_REF_NO, Y.TRN_REF_NO) AS TRN_REF_NO,
  X.ACY_LCY_RATE,
  X.ACY_LCY_RATE_1,
  X.ACY_LCY_RATE_2,
  X.ACY_LCY_RATE_3,
  X.ACY_LCY_RATE_4,
  X.AUTH_STAT,
  X.BENEF_ADDR1,
  X.BENEF_ADDR2,
  X.BENEF_ADDR3,
  X.BENEF_ADDR4,
  X.BENEF_NAME,
  X.BILL_ISSUE_DATE,
  X.BILL_NUMBER,
  X.BM_AUTH_STAT,
  X.BM_CHG_CCY,
  X.BM_DR_ACC,
  X.BM_MODULE_EVENT_CODE,
  X.BM_OFS_CCY,
  X.BM_OFS_TRN_CODE,
  X.BM_OFS_TRN_CODE_FUND,
  X.BM_SCODE,
  X.BM_SCODE,
  X.BM_TXN_CCY,
  X.BM_TXN_TANKED,
  X.BM_TXN_TRN_CODE,
  X.BM_TXN_TRN_CODE_EV,
  X.BRANCH_CODE,
  X.CASHBACK_AMOUNT,
  X.CHARGE_ACCOUNT,
  X.CHECKER_DT_STAMP,
  X.CHECKER_ID,
  X.CHEQUE_ISSUE_DATE,
  X.CHG_AMT,
  X.CHG_AMT_1,
  X.CHG_AMT_2,
  X.CHG_AMT_3,
  X.CHG_AMT_4,
  X.CHG_CCY,
  X.CHG_CCY_1,
  X.CHG_CCY_2,
  X.CHG_CCY_3,
  X.CHG_CCY_4,
  X.CHG_CCY_ACY_RATE,
  X.CHG_CCY_ACY_RATE_1,
  X.CHG_CCY_ACY_RATE_2,
  X.CHG_CCY_ACY_RATE_3,
  X.CHG_CCY_ACY_RATE_4,
  X.CHG_CONTRA_LEG,
  X.CHG_DESC,
  X.CHG_DESC1,
  X.CHG_DESC2,
  X.CHG_DESC3,
  X.CHG_DESC4,
  X.CHG_DR_LEG,
  X.CHG_DR_LEG_1,
  X.CHG_DR_LEG_2,
  X.CHG_DR_LEG_3,
  X.CHG_DR_LEG_4,
  X.CHG_GL,
  X.CHG_GL_1,
  X.CHG_GL_2,
  X.CHG_GL_3,
  X.CHG_GL_4,
  X.CHG_IN_ACY,
  X.CHG_IN_ACY_1,
  X.CHG_IN_ACY_2,
  X.CHG_IN_ACY_3,
  X.CHG_IN_ACY_4,
  X.CHG_IN_LCY,
  X.CHG_IN_LCY_1,
  X.CHG_IN_LCY_2,
  X.CHG_IN_LCY_3,
  X.CHG_IN_LCY_4,
  X.CHG_TYPE,
  X.CHG_TYPE1,
  X.CHG_TYPE2,
  X.CHG_TYPE3,
  X.CHG_TYPE4,
  X.CONSUMER_NO,
  X.CR_INSTRUMENT_CODE,
  X.DEPOSIT_SLIP_NO,
  X.DR_ACC,
  X.DR_INSTRUMENT_CODE,
  X.END_POINT,
  X.ESN,
  X.EVENT_CODE,
  X.EXCH_RATE,
  X.FT_PROBLEM,
  X.FUND_ID,
  X.FUND_REF_NO,
  X.INSTR_DATE,
  X.LCY_AMOUNT,
  X.LCY_CHG_EXCH_RATE,
  X.LCY_CHG_EXCH_RATE1,
  X.LCY_CHG_EXCH_RATE2,
  X.LCY_CHG_EXCH_RATE3,
  X.LCY_CHG_EXCH_RATE4,
  X.LCY_EXCH_RATE,
  X.LIQN_DT,
  X.MAINT_CHARGE_APPLIED,
  X.MAKER_DT_STAMP,
  X.MAKER_ID,
  X.MIS_HEAD,
  X.MIS_HEAD_1,
  X.MIS_HEAD_2,
  X.MIS_HEAD_3,
  X.MIS_HEAD_4,
  X.MIS_HEAD_5,
  X.MOD_NO,
  X.MODULE,
  X.NARRATIVE,
  X.NEGOTIATED_RATE,
  X.NEGOTIATION_REF_NO,
  X.NETTING_IND,
  X.NETTING_IND_1,
  X.NETTING_IND_2,
  X.NETTING_IND_3,
  X.NETTING_IND_4,
  X.NETTING_REQD,
  X.OFS_ACC,
  X.OFS_AMOUNT,
  X.OFS_BRANCH,
  X.OFS_CCY,
  X.OFS_TRN_CODE,
  X.PASSPORTIC_NO,
  X.PRODUCT_CODE,
  X.PROJECT_NAME,
  X.RECORD_STAT,
  X.REJECT_CODE,
  X.REL_CUSTOMER,
  X.REM_ACC,
  X.REM_BANK,
  X.REM_BRANCH,
  X.REPAIR_REASON,
  X.REVR_DT,
  X.ROUTE_CODE,
  X.ROUTING_NO,
  X.SCODE,
  X.SDB_REF_NO,
  X.SERIAL_NO,
  X.SERVICE_PROVIDER,
  X.SK_BENEF_ADDR1_BENEF_ADDR2_BENEF_ADDR3_BENEF_ADDR4,
  X.SK_REL_CUSTOMER,
  X.SK_BRANCH_CODE,
  X.SK_OFS_BRANCH,
  X.SK_TXN_BRANCH,
  X.SK_CHECKER_ID,
  X.SK_MAKER_ID,
  X.SK_BENEF_NAME,
  X.SK_OFS_ACC,
  X.SK_PRODUCT_CODE,
  X.SK_SCODE,
  X.SK_TRN_REF_NO,
  X.SK_TXN_ACC,
  X.THEIR_ACC,
  X.THEIR_ACC1,
  X.THEIR_ACC2,
  X.THEIR_ACC3,
  X.THEIR_ACC4,
  X.THEIR_CHGS,
  X.THEIR_CHGS1,
  X.THEIR_CHGS2,
  X.THEIR_CHGS3,
  X.THEIR_CHGS4,
  X.TIME_RECEIVED,
  X.TOKEN_NO,
  X.TOT_CHG_IN_TCY,
  X.TRACK_RECEIVABLE,
  X.TRN_DT,
  X.TXN_ACC,
  X.TXN_AMOUNT,
  X.TXN_BRANCH,
  X.TXN_CCY,
  X.TXN_CODE,
  X.TXN_CODE_1,
  X.TXN_CODE_2,
  X.TXN_CODE_3,
  X.TXN_CODE_4,
  X.TXN_STATUS,
  X.TXN_TANKED,
  X.TXN_TRN_CODE,
  X.UNIT_ID,
  X.UNIT_PAYMENT,
  X.VALUE_DT,
  X.WAIVER,
  X.WAIVER1,
  X.WAIVER2,
  X.WAIVER3,
  X.WAIVER4,
  X.XREF,
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
    WHEN Y.TRN_REF_NO IS NULL
    THEN 'INS' /* - IF MULTIPLE PKs are there then concatenate them with AND */
    WHEN X.TRN_REF_NO = Y.TRN_REF_NO /* - IF MULTIPLE PKs are there then concatenate them with AND */
    AND (
      (
        (
          X.ACY_LCY_RATE IS NULL AND NOT Y.ACY_LCY_RATE IS NULL
        )
        OR (
          NOT X.ACY_LCY_RATE IS NULL AND Y.ACY_LCY_RATE IS NULL
        )
        OR (
          X.ACY_LCY_RATE <> Y.ACY_LCY_RATE
        )
      )
      OR (
        (
          X.ACY_LCY_RATE_1 IS NULL AND NOT Y.ACY_LCY_RATE_1 IS NULL
        )
        OR (
          NOT X.ACY_LCY_RATE_1 IS NULL AND Y.ACY_LCY_RATE_1 IS NULL
        )
        OR (
          X.ACY_LCY_RATE_1 <> Y.ACY_LCY_RATE_1
        )
      )
      OR (
        (
          X.ACY_LCY_RATE_2 IS NULL AND NOT Y.ACY_LCY_RATE_2 IS NULL
        )
        OR (
          NOT X.ACY_LCY_RATE_2 IS NULL AND Y.ACY_LCY_RATE_2 IS NULL
        )
        OR (
          X.ACY_LCY_RATE_2 <> Y.ACY_LCY_RATE_2
        )
      )
      OR (
        (
          X.ACY_LCY_RATE_3 IS NULL AND NOT Y.ACY_LCY_RATE_3 IS NULL
        )
        OR (
          NOT X.ACY_LCY_RATE_3 IS NULL AND Y.ACY_LCY_RATE_3 IS NULL
        )
        OR (
          X.ACY_LCY_RATE_3 <> Y.ACY_LCY_RATE_3
        )
      )
      OR (
        (
          X.ACY_LCY_RATE_4 IS NULL AND NOT Y.ACY_LCY_RATE_4 IS NULL
        )
        OR (
          NOT X.ACY_LCY_RATE_4 IS NULL AND Y.ACY_LCY_RATE_4 IS NULL
        )
        OR (
          X.ACY_LCY_RATE_4 <> Y.ACY_LCY_RATE_4
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
          X.BENEF_ADDR1 IS NULL AND NOT Y.BENEF_ADDR1 IS NULL
        )
        OR (
          NOT X.BENEF_ADDR1 IS NULL AND Y.BENEF_ADDR1 IS NULL
        )
        OR (
          X.BENEF_ADDR1 <> Y.BENEF_ADDR1
        )
      )
      OR (
        (
          X.BENEF_ADDR2 IS NULL AND NOT Y.BENEF_ADDR2 IS NULL
        )
        OR (
          NOT X.BENEF_ADDR2 IS NULL AND Y.BENEF_ADDR2 IS NULL
        )
        OR (
          X.BENEF_ADDR2 <> Y.BENEF_ADDR2
        )
      )
      OR (
        (
          X.BENEF_ADDR3 IS NULL AND NOT Y.BENEF_ADDR3 IS NULL
        )
        OR (
          NOT X.BENEF_ADDR3 IS NULL AND Y.BENEF_ADDR3 IS NULL
        )
        OR (
          X.BENEF_ADDR3 <> Y.BENEF_ADDR3
        )
      )
      OR (
        (
          X.BENEF_ADDR4 IS NULL AND NOT Y.BENEF_ADDR4 IS NULL
        )
        OR (
          NOT X.BENEF_ADDR4 IS NULL AND Y.BENEF_ADDR4 IS NULL
        )
        OR (
          X.BENEF_ADDR4 <> Y.BENEF_ADDR4
        )
      )
      OR (
        (
          X.BENEF_NAME IS NULL AND NOT Y.BENEF_NAME IS NULL
        )
        OR (
          NOT X.BENEF_NAME IS NULL AND Y.BENEF_NAME IS NULL
        )
        OR (
          X.BENEF_NAME <> Y.BENEF_NAME
        )
      )
      OR (
        (
          X.BILL_ISSUE_DATE IS NULL AND NOT Y.BILL_ISSUE_DATE IS NULL
        )
        OR (
          NOT X.BILL_ISSUE_DATE IS NULL AND Y.BILL_ISSUE_DATE IS NULL
        )
        OR (
          X.BILL_ISSUE_DATE <> Y.BILL_ISSUE_DATE
        )
      )
      OR (
        (
          X.BILL_NUMBER IS NULL AND NOT Y.BILL_NUMBER IS NULL
        )
        OR (
          NOT X.BILL_NUMBER IS NULL AND Y.BILL_NUMBER IS NULL
        )
        OR (
          X.BILL_NUMBER <> Y.BILL_NUMBER
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
          X.BM_CHG_CCY IS NULL AND NOT Y.BM_CHG_CCY IS NULL
        )
        OR (
          NOT X.BM_CHG_CCY IS NULL AND Y.BM_CHG_CCY IS NULL
        )
        OR (
          X.BM_CHG_CCY <> Y.BM_CHG_CCY
        )
      )
      OR (
        (
          X.BM_DR_ACC IS NULL AND NOT Y.BM_DR_ACC IS NULL
        )
        OR (
          NOT X.BM_DR_ACC IS NULL AND Y.BM_DR_ACC IS NULL
        )
        OR (
          X.BM_DR_ACC <> Y.BM_DR_ACC
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
          X.BM_OFS_CCY IS NULL AND NOT Y.BM_OFS_CCY IS NULL
        )
        OR (
          NOT X.BM_OFS_CCY IS NULL AND Y.BM_OFS_CCY IS NULL
        )
        OR (
          X.BM_OFS_CCY <> Y.BM_OFS_CCY
        )
      )
      OR (
        (
          X.BM_OFS_TRN_CODE IS NULL AND NOT Y.BM_OFS_TRN_CODE IS NULL
        )
        OR (
          NOT X.BM_OFS_TRN_CODE IS NULL AND Y.BM_OFS_TRN_CODE IS NULL
        )
        OR (
          X.BM_OFS_TRN_CODE <> Y.BM_OFS_TRN_CODE
        )
      )
      OR (
        (
          X.BM_OFS_TRN_CODE_FUND IS NULL AND NOT Y.BM_OFS_TRN_CODE_FUND IS NULL
        )
        OR (
          NOT X.BM_OFS_TRN_CODE_FUND IS NULL AND Y.BM_OFS_TRN_CODE_FUND IS NULL
        )
        OR (
          X.BM_OFS_TRN_CODE_FUND <> Y.BM_OFS_TRN_CODE_FUND
        )
      )
      OR (
        (
          X.BM_SCODE IS NULL AND NOT Y.BM_SCODE IS NULL
        )
        OR (
          NOT X.BM_SCODE IS NULL AND Y.BM_SCODE IS NULL
        )
        OR (
          X.BM_SCODE <> Y.BM_SCODE
        )
      )
      OR (
        (
          X.BM_SCODE IS NULL AND NOT Y.BM_SCODE IS NULL
        )
        OR (
          NOT X.BM_SCODE IS NULL AND Y.BM_SCODE IS NULL
        )
        OR (
          X.BM_SCODE <> Y.BM_SCODE
        )
      )
      OR (
        (
          X.BM_TXN_CCY IS NULL AND NOT Y.BM_TXN_CCY IS NULL
        )
        OR (
          NOT X.BM_TXN_CCY IS NULL AND Y.BM_TXN_CCY IS NULL
        )
        OR (
          X.BM_TXN_CCY <> Y.BM_TXN_CCY
        )
      )
      OR (
        (
          X.BM_TXN_TANKED IS NULL AND NOT Y.BM_TXN_TANKED IS NULL
        )
        OR (
          NOT X.BM_TXN_TANKED IS NULL AND Y.BM_TXN_TANKED IS NULL
        )
        OR (
          X.BM_TXN_TANKED <> Y.BM_TXN_TANKED
        )
      )
      OR (
        (
          X.BM_TXN_TRN_CODE IS NULL AND NOT Y.BM_TXN_TRN_CODE IS NULL
        )
        OR (
          NOT X.BM_TXN_TRN_CODE IS NULL AND Y.BM_TXN_TRN_CODE IS NULL
        )
        OR (
          X.BM_TXN_TRN_CODE <> Y.BM_TXN_TRN_CODE
        )
      )
      OR (
        (
          X.BM_TXN_TRN_CODE_EV IS NULL AND NOT Y.BM_TXN_TRN_CODE_EV IS NULL
        )
        OR (
          NOT X.BM_TXN_TRN_CODE_EV IS NULL AND Y.BM_TXN_TRN_CODE_EV IS NULL
        )
        OR (
          X.BM_TXN_TRN_CODE_EV <> Y.BM_TXN_TRN_CODE_EV
        )
      )
      OR (
        (
          X.BRANCH_CODE IS NULL AND NOT Y.BRANCH_CODE IS NULL
        )
        OR (
          NOT X.BRANCH_CODE IS NULL AND Y.BRANCH_CODE IS NULL
        )
        OR (
          X.BRANCH_CODE <> Y.BRANCH_CODE
        )
      )
      OR (
        (
          X.CASHBACK_AMOUNT IS NULL AND NOT Y.CASHBACK_AMOUNT IS NULL
        )
        OR (
          NOT X.CASHBACK_AMOUNT IS NULL AND Y.CASHBACK_AMOUNT IS NULL
        )
        OR (
          X.CASHBACK_AMOUNT <> Y.CASHBACK_AMOUNT
        )
      )
      OR (
        (
          X.CHARGE_ACCOUNT IS NULL AND NOT Y.CHARGE_ACCOUNT IS NULL
        )
        OR (
          NOT X.CHARGE_ACCOUNT IS NULL AND Y.CHARGE_ACCOUNT IS NULL
        )
        OR (
          X.CHARGE_ACCOUNT <> Y.CHARGE_ACCOUNT
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
          X.CHEQUE_ISSUE_DATE IS NULL AND NOT Y.CHEQUE_ISSUE_DATE IS NULL
        )
        OR (
          NOT X.CHEQUE_ISSUE_DATE IS NULL AND Y.CHEQUE_ISSUE_DATE IS NULL
        )
        OR (
          X.CHEQUE_ISSUE_DATE <> Y.CHEQUE_ISSUE_DATE
        )
      )
      OR (
        (
          X.CHG_AMT IS NULL AND NOT Y.CHG_AMT IS NULL
        )
        OR (
          NOT X.CHG_AMT IS NULL AND Y.CHG_AMT IS NULL
        )
        OR (
          X.CHG_AMT <> Y.CHG_AMT
        )
      )
      OR (
        (
          X.CHG_AMT_1 IS NULL AND NOT Y.CHG_AMT_1 IS NULL
        )
        OR (
          NOT X.CHG_AMT_1 IS NULL AND Y.CHG_AMT_1 IS NULL
        )
        OR (
          X.CHG_AMT_1 <> Y.CHG_AMT_1
        )
      )
      OR (
        (
          X.CHG_AMT_2 IS NULL AND NOT Y.CHG_AMT_2 IS NULL
        )
        OR (
          NOT X.CHG_AMT_2 IS NULL AND Y.CHG_AMT_2 IS NULL
        )
        OR (
          X.CHG_AMT_2 <> Y.CHG_AMT_2
        )
      )
      OR (
        (
          X.CHG_AMT_3 IS NULL AND NOT Y.CHG_AMT_3 IS NULL
        )
        OR (
          NOT X.CHG_AMT_3 IS NULL AND Y.CHG_AMT_3 IS NULL
        )
        OR (
          X.CHG_AMT_3 <> Y.CHG_AMT_3
        )
      )
      OR (
        (
          X.CHG_AMT_4 IS NULL AND NOT Y.CHG_AMT_4 IS NULL
        )
        OR (
          NOT X.CHG_AMT_4 IS NULL AND Y.CHG_AMT_4 IS NULL
        )
        OR (
          X.CHG_AMT_4 <> Y.CHG_AMT_4
        )
      )
      OR (
        (
          X.CHG_CCY IS NULL AND NOT Y.CHG_CCY IS NULL
        )
        OR (
          NOT X.CHG_CCY IS NULL AND Y.CHG_CCY IS NULL
        )
        OR (
          X.CHG_CCY <> Y.CHG_CCY
        )
      )
      OR (
        (
          X.CHG_CCY_1 IS NULL AND NOT Y.CHG_CCY_1 IS NULL
        )
        OR (
          NOT X.CHG_CCY_1 IS NULL AND Y.CHG_CCY_1 IS NULL
        )
        OR (
          X.CHG_CCY_1 <> Y.CHG_CCY_1
        )
      )
      OR (
        (
          X.CHG_CCY_2 IS NULL AND NOT Y.CHG_CCY_2 IS NULL
        )
        OR (
          NOT X.CHG_CCY_2 IS NULL AND Y.CHG_CCY_2 IS NULL
        )
        OR (
          X.CHG_CCY_2 <> Y.CHG_CCY_2
        )
      )
      OR (
        (
          X.CHG_CCY_3 IS NULL AND NOT Y.CHG_CCY_3 IS NULL
        )
        OR (
          NOT X.CHG_CCY_3 IS NULL AND Y.CHG_CCY_3 IS NULL
        )
        OR (
          X.CHG_CCY_3 <> Y.CHG_CCY_3
        )
      )
      OR (
        (
          X.CHG_CCY_4 IS NULL AND NOT Y.CHG_CCY_4 IS NULL
        )
        OR (
          NOT X.CHG_CCY_4 IS NULL AND Y.CHG_CCY_4 IS NULL
        )
        OR (
          X.CHG_CCY_4 <> Y.CHG_CCY_4
        )
      )
      OR (
        (
          X.CHG_CCY_ACY_RATE IS NULL AND NOT Y.CHG_CCY_ACY_RATE IS NULL
        )
        OR (
          NOT X.CHG_CCY_ACY_RATE IS NULL AND Y.CHG_CCY_ACY_RATE IS NULL
        )
        OR (
          X.CHG_CCY_ACY_RATE <> Y.CHG_CCY_ACY_RATE
        )
      )
      OR (
        (
          X.CHG_CCY_ACY_RATE_1 IS NULL AND NOT Y.CHG_CCY_ACY_RATE_1 IS NULL
        )
        OR (
          NOT X.CHG_CCY_ACY_RATE_1 IS NULL AND Y.CHG_CCY_ACY_RATE_1 IS NULL
        )
        OR (
          X.CHG_CCY_ACY_RATE_1 <> Y.CHG_CCY_ACY_RATE_1
        )
      )
      OR (
        (
          X.CHG_CCY_ACY_RATE_2 IS NULL AND NOT Y.CHG_CCY_ACY_RATE_2 IS NULL
        )
        OR (
          NOT X.CHG_CCY_ACY_RATE_2 IS NULL AND Y.CHG_CCY_ACY_RATE_2 IS NULL
        )
        OR (
          X.CHG_CCY_ACY_RATE_2 <> Y.CHG_CCY_ACY_RATE_2
        )
      )
      OR (
        (
          X.CHG_CCY_ACY_RATE_3 IS NULL AND NOT Y.CHG_CCY_ACY_RATE_3 IS NULL
        )
        OR (
          NOT X.CHG_CCY_ACY_RATE_3 IS NULL AND Y.CHG_CCY_ACY_RATE_3 IS NULL
        )
        OR (
          X.CHG_CCY_ACY_RATE_3 <> Y.CHG_CCY_ACY_RATE_3
        )
      )
      OR (
        (
          X.CHG_CCY_ACY_RATE_4 IS NULL AND NOT Y.CHG_CCY_ACY_RATE_4 IS NULL
        )
        OR (
          NOT X.CHG_CCY_ACY_RATE_4 IS NULL AND Y.CHG_CCY_ACY_RATE_4 IS NULL
        )
        OR (
          X.CHG_CCY_ACY_RATE_4 <> Y.CHG_CCY_ACY_RATE_4
        )
      )
      OR (
        (
          X.CHG_CONTRA_LEG IS NULL AND NOT Y.CHG_CONTRA_LEG IS NULL
        )
        OR (
          NOT X.CHG_CONTRA_LEG IS NULL AND Y.CHG_CONTRA_LEG IS NULL
        )
        OR (
          X.CHG_CONTRA_LEG <> Y.CHG_CONTRA_LEG
        )
      )
      OR (
        (
          X.CHG_DESC IS NULL AND NOT Y.CHG_DESC IS NULL
        )
        OR (
          NOT X.CHG_DESC IS NULL AND Y.CHG_DESC IS NULL
        )
        OR (
          X.CHG_DESC <> Y.CHG_DESC
        )
      )
      OR (
        (
          X.CHG_DESC1 IS NULL AND NOT Y.CHG_DESC1 IS NULL
        )
        OR (
          NOT X.CHG_DESC1 IS NULL AND Y.CHG_DESC1 IS NULL
        )
        OR (
          X.CHG_DESC1 <> Y.CHG_DESC1
        )
      )
      OR (
        (
          X.CHG_DESC2 IS NULL AND NOT Y.CHG_DESC2 IS NULL
        )
        OR (
          NOT X.CHG_DESC2 IS NULL AND Y.CHG_DESC2 IS NULL
        )
        OR (
          X.CHG_DESC2 <> Y.CHG_DESC2
        )
      )
      OR (
        (
          X.CHG_DESC3 IS NULL AND NOT Y.CHG_DESC3 IS NULL
        )
        OR (
          NOT X.CHG_DESC3 IS NULL AND Y.CHG_DESC3 IS NULL
        )
        OR (
          X.CHG_DESC3 <> Y.CHG_DESC3
        )
      )
      OR (
        (
          X.CHG_DESC4 IS NULL AND NOT Y.CHG_DESC4 IS NULL
        )
        OR (
          NOT X.CHG_DESC4 IS NULL AND Y.CHG_DESC4 IS NULL
        )
        OR (
          X.CHG_DESC4 <> Y.CHG_DESC4
        )
      )
      OR (
        (
          X.CHG_DR_LEG IS NULL AND NOT Y.CHG_DR_LEG IS NULL
        )
        OR (
          NOT X.CHG_DR_LEG IS NULL AND Y.CHG_DR_LEG IS NULL
        )
        OR (
          X.CHG_DR_LEG <> Y.CHG_DR_LEG
        )
      )
      OR (
        (
          X.CHG_DR_LEG_1 IS NULL AND NOT Y.CHG_DR_LEG_1 IS NULL
        )
        OR (
          NOT X.CHG_DR_LEG_1 IS NULL AND Y.CHG_DR_LEG_1 IS NULL
        )
        OR (
          X.CHG_DR_LEG_1 <> Y.CHG_DR_LEG_1
        )
      )
      OR (
        (
          X.CHG_DR_LEG_2 IS NULL AND NOT Y.CHG_DR_LEG_2 IS NULL
        )
        OR (
          NOT X.CHG_DR_LEG_2 IS NULL AND Y.CHG_DR_LEG_2 IS NULL
        )
        OR (
          X.CHG_DR_LEG_2 <> Y.CHG_DR_LEG_2
        )
      )
      OR (
        (
          X.CHG_DR_LEG_3 IS NULL AND NOT Y.CHG_DR_LEG_3 IS NULL
        )
        OR (
          NOT X.CHG_DR_LEG_3 IS NULL AND Y.CHG_DR_LEG_3 IS NULL
        )
        OR (
          X.CHG_DR_LEG_3 <> Y.CHG_DR_LEG_3
        )
      )
      OR (
        (
          X.CHG_DR_LEG_4 IS NULL AND NOT Y.CHG_DR_LEG_4 IS NULL
        )
        OR (
          NOT X.CHG_DR_LEG_4 IS NULL AND Y.CHG_DR_LEG_4 IS NULL
        )
        OR (
          X.CHG_DR_LEG_4 <> Y.CHG_DR_LEG_4
        )
      )
      OR (
        (
          X.CHG_GL IS NULL AND NOT Y.CHG_GL IS NULL
        )
        OR (
          NOT X.CHG_GL IS NULL AND Y.CHG_GL IS NULL
        )
        OR (
          X.CHG_GL <> Y.CHG_GL
        )
      )
      OR (
        (
          X.CHG_GL_1 IS NULL AND NOT Y.CHG_GL_1 IS NULL
        )
        OR (
          NOT X.CHG_GL_1 IS NULL AND Y.CHG_GL_1 IS NULL
        )
        OR (
          X.CHG_GL_1 <> Y.CHG_GL_1
        )
      )
      OR (
        (
          X.CHG_GL_2 IS NULL AND NOT Y.CHG_GL_2 IS NULL
        )
        OR (
          NOT X.CHG_GL_2 IS NULL AND Y.CHG_GL_2 IS NULL
        )
        OR (
          X.CHG_GL_2 <> Y.CHG_GL_2
        )
      )
      OR (
        (
          X.CHG_GL_3 IS NULL AND NOT Y.CHG_GL_3 IS NULL
        )
        OR (
          NOT X.CHG_GL_3 IS NULL AND Y.CHG_GL_3 IS NULL
        )
        OR (
          X.CHG_GL_3 <> Y.CHG_GL_3
        )
      )
      OR (
        (
          X.CHG_GL_4 IS NULL AND NOT Y.CHG_GL_4 IS NULL
        )
        OR (
          NOT X.CHG_GL_4 IS NULL AND Y.CHG_GL_4 IS NULL
        )
        OR (
          X.CHG_GL_4 <> Y.CHG_GL_4
        )
      )
      OR (
        (
          X.CHG_IN_ACY IS NULL AND NOT Y.CHG_IN_ACY IS NULL
        )
        OR (
          NOT X.CHG_IN_ACY IS NULL AND Y.CHG_IN_ACY IS NULL
        )
        OR (
          X.CHG_IN_ACY <> Y.CHG_IN_ACY
        )
      )
      OR (
        (
          X.CHG_IN_ACY_1 IS NULL AND NOT Y.CHG_IN_ACY_1 IS NULL
        )
        OR (
          NOT X.CHG_IN_ACY_1 IS NULL AND Y.CHG_IN_ACY_1 IS NULL
        )
        OR (
          X.CHG_IN_ACY_1 <> Y.CHG_IN_ACY_1
        )
      )
      OR (
        (
          X.CHG_IN_ACY_2 IS NULL AND NOT Y.CHG_IN_ACY_2 IS NULL
        )
        OR (
          NOT X.CHG_IN_ACY_2 IS NULL AND Y.CHG_IN_ACY_2 IS NULL
        )
        OR (
          X.CHG_IN_ACY_2 <> Y.CHG_IN_ACY_2
        )
      )
      OR (
        (
          X.CHG_IN_ACY_3 IS NULL AND NOT Y.CHG_IN_ACY_3 IS NULL
        )
        OR (
          NOT X.CHG_IN_ACY_3 IS NULL AND Y.CHG_IN_ACY_3 IS NULL
        )
        OR (
          X.CHG_IN_ACY_3 <> Y.CHG_IN_ACY_3
        )
      )
      OR (
        (
          X.CHG_IN_ACY_4 IS NULL AND NOT Y.CHG_IN_ACY_4 IS NULL
        )
        OR (
          NOT X.CHG_IN_ACY_4 IS NULL AND Y.CHG_IN_ACY_4 IS NULL
        )
        OR (
          X.CHG_IN_ACY_4 <> Y.CHG_IN_ACY_4
        )
      )
      OR (
        (
          X.CHG_IN_LCY IS NULL AND NOT Y.CHG_IN_LCY IS NULL
        )
        OR (
          NOT X.CHG_IN_LCY IS NULL AND Y.CHG_IN_LCY IS NULL
        )
        OR (
          X.CHG_IN_LCY <> Y.CHG_IN_LCY
        )
      )
      OR (
        (
          X.CHG_IN_LCY_1 IS NULL AND NOT Y.CHG_IN_LCY_1 IS NULL
        )
        OR (
          NOT X.CHG_IN_LCY_1 IS NULL AND Y.CHG_IN_LCY_1 IS NULL
        )
        OR (
          X.CHG_IN_LCY_1 <> Y.CHG_IN_LCY_1
        )
      )
      OR (
        (
          X.CHG_IN_LCY_2 IS NULL AND NOT Y.CHG_IN_LCY_2 IS NULL
        )
        OR (
          NOT X.CHG_IN_LCY_2 IS NULL AND Y.CHG_IN_LCY_2 IS NULL
        )
        OR (
          X.CHG_IN_LCY_2 <> Y.CHG_IN_LCY_2
        )
      )
      OR (
        (
          X.CHG_IN_LCY_3 IS NULL AND NOT Y.CHG_IN_LCY_3 IS NULL
        )
        OR (
          NOT X.CHG_IN_LCY_3 IS NULL AND Y.CHG_IN_LCY_3 IS NULL
        )
        OR (
          X.CHG_IN_LCY_3 <> Y.CHG_IN_LCY_3
        )
      )
      OR (
        (
          X.CHG_IN_LCY_4 IS NULL AND NOT Y.CHG_IN_LCY_4 IS NULL
        )
        OR (
          NOT X.CHG_IN_LCY_4 IS NULL AND Y.CHG_IN_LCY_4 IS NULL
        )
        OR (
          X.CHG_IN_LCY_4 <> Y.CHG_IN_LCY_4
        )
      )
      OR (
        (
          X.CHG_TYPE IS NULL AND NOT Y.CHG_TYPE IS NULL
        )
        OR (
          NOT X.CHG_TYPE IS NULL AND Y.CHG_TYPE IS NULL
        )
        OR (
          X.CHG_TYPE <> Y.CHG_TYPE
        )
      )
      OR (
        (
          X.CHG_TYPE1 IS NULL AND NOT Y.CHG_TYPE1 IS NULL
        )
        OR (
          NOT X.CHG_TYPE1 IS NULL AND Y.CHG_TYPE1 IS NULL
        )
        OR (
          X.CHG_TYPE1 <> Y.CHG_TYPE1
        )
      )
      OR (
        (
          X.CHG_TYPE2 IS NULL AND NOT Y.CHG_TYPE2 IS NULL
        )
        OR (
          NOT X.CHG_TYPE2 IS NULL AND Y.CHG_TYPE2 IS NULL
        )
        OR (
          X.CHG_TYPE2 <> Y.CHG_TYPE2
        )
      )
      OR (
        (
          X.CHG_TYPE3 IS NULL AND NOT Y.CHG_TYPE3 IS NULL
        )
        OR (
          NOT X.CHG_TYPE3 IS NULL AND Y.CHG_TYPE3 IS NULL
        )
        OR (
          X.CHG_TYPE3 <> Y.CHG_TYPE3
        )
      )
      OR (
        (
          X.CHG_TYPE4 IS NULL AND NOT Y.CHG_TYPE4 IS NULL
        )
        OR (
          NOT X.CHG_TYPE4 IS NULL AND Y.CHG_TYPE4 IS NULL
        )
        OR (
          X.CHG_TYPE4 <> Y.CHG_TYPE4
        )
      )
      OR (
        (
          X.CONSUMER_NO IS NULL AND NOT Y.CONSUMER_NO IS NULL
        )
        OR (
          NOT X.CONSUMER_NO IS NULL AND Y.CONSUMER_NO IS NULL
        )
        OR (
          X.CONSUMER_NO <> Y.CONSUMER_NO
        )
      )
      OR (
        (
          X.CR_INSTRUMENT_CODE IS NULL AND NOT Y.CR_INSTRUMENT_CODE IS NULL
        )
        OR (
          NOT X.CR_INSTRUMENT_CODE IS NULL AND Y.CR_INSTRUMENT_CODE IS NULL
        )
        OR (
          X.CR_INSTRUMENT_CODE <> Y.CR_INSTRUMENT_CODE
        )
      )
      OR (
        (
          X.DEPOSIT_SLIP_NO IS NULL AND NOT Y.DEPOSIT_SLIP_NO IS NULL
        )
        OR (
          NOT X.DEPOSIT_SLIP_NO IS NULL AND Y.DEPOSIT_SLIP_NO IS NULL
        )
        OR (
          X.DEPOSIT_SLIP_NO <> Y.DEPOSIT_SLIP_NO
        )
      )
      OR (
        (
          X.DR_ACC IS NULL AND NOT Y.DR_ACC IS NULL
        )
        OR (
          NOT X.DR_ACC IS NULL AND Y.DR_ACC IS NULL
        )
        OR (
          X.DR_ACC <> Y.DR_ACC
        )
      )
      OR (
        (
          X.DR_INSTRUMENT_CODE IS NULL AND NOT Y.DR_INSTRUMENT_CODE IS NULL
        )
        OR (
          NOT X.DR_INSTRUMENT_CODE IS NULL AND Y.DR_INSTRUMENT_CODE IS NULL
        )
        OR (
          X.DR_INSTRUMENT_CODE <> Y.DR_INSTRUMENT_CODE
        )
      )
      OR (
        (
          X.END_POINT IS NULL AND NOT Y.END_POINT IS NULL
        )
        OR (
          NOT X.END_POINT IS NULL AND Y.END_POINT IS NULL
        )
        OR (
          X.END_POINT <> Y.END_POINT
        )
      )
      OR (
        (
          X.ESN IS NULL AND NOT Y.ESN IS NULL
        )
        OR (
          NOT X.ESN IS NULL AND Y.ESN IS NULL
        )
        OR (
          X.ESN <> Y.ESN
        )
      )
      OR (
        (
          X.EVENT_CODE IS NULL AND NOT Y.EVENT_CODE IS NULL
        )
        OR (
          NOT X.EVENT_CODE IS NULL AND Y.EVENT_CODE IS NULL
        )
        OR (
          X.EVENT_CODE <> Y.EVENT_CODE
        )
      )
      OR (
        (
          X.EXCH_RATE IS NULL AND NOT Y.EXCH_RATE IS NULL
        )
        OR (
          NOT X.EXCH_RATE IS NULL AND Y.EXCH_RATE IS NULL
        )
        OR (
          X.EXCH_RATE <> Y.EXCH_RATE
        )
      )
      OR (
        (
          X.FT_PROBLEM IS NULL AND NOT Y.FT_PROBLEM IS NULL
        )
        OR (
          NOT X.FT_PROBLEM IS NULL AND Y.FT_PROBLEM IS NULL
        )
        OR (
          X.FT_PROBLEM <> Y.FT_PROBLEM
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
          X.FUND_REF_NO IS NULL AND NOT Y.FUND_REF_NO IS NULL
        )
        OR (
          NOT X.FUND_REF_NO IS NULL AND Y.FUND_REF_NO IS NULL
        )
        OR (
          X.FUND_REF_NO <> Y.FUND_REF_NO
        )
      )
      OR (
        (
          X.INSTR_DATE IS NULL AND NOT Y.INSTR_DATE IS NULL
        )
        OR (
          NOT X.INSTR_DATE IS NULL AND Y.INSTR_DATE IS NULL
        )
        OR (
          X.INSTR_DATE <> Y.INSTR_DATE
        )
      )
      OR (
        (
          X.LCY_AMOUNT IS NULL AND NOT Y.LCY_AMOUNT IS NULL
        )
        OR (
          NOT X.LCY_AMOUNT IS NULL AND Y.LCY_AMOUNT IS NULL
        )
        OR (
          X.LCY_AMOUNT <> Y.LCY_AMOUNT
        )
      )
      OR (
        (
          X.LCY_CHG_EXCH_RATE IS NULL AND NOT Y.LCY_CHG_EXCH_RATE IS NULL
        )
        OR (
          NOT X.LCY_CHG_EXCH_RATE IS NULL AND Y.LCY_CHG_EXCH_RATE IS NULL
        )
        OR (
          X.LCY_CHG_EXCH_RATE <> Y.LCY_CHG_EXCH_RATE
        )
      )
      OR (
        (
          X.LCY_CHG_EXCH_RATE1 IS NULL AND NOT Y.LCY_CHG_EXCH_RATE1 IS NULL
        )
        OR (
          NOT X.LCY_CHG_EXCH_RATE1 IS NULL AND Y.LCY_CHG_EXCH_RATE1 IS NULL
        )
        OR (
          X.LCY_CHG_EXCH_RATE1 <> Y.LCY_CHG_EXCH_RATE1
        )
      )
      OR (
        (
          X.LCY_CHG_EXCH_RATE2 IS NULL AND NOT Y.LCY_CHG_EXCH_RATE2 IS NULL
        )
        OR (
          NOT X.LCY_CHG_EXCH_RATE2 IS NULL AND Y.LCY_CHG_EXCH_RATE2 IS NULL
        )
        OR (
          X.LCY_CHG_EXCH_RATE2 <> Y.LCY_CHG_EXCH_RATE2
        )
      )
      OR (
        (
          X.LCY_CHG_EXCH_RATE3 IS NULL AND NOT Y.LCY_CHG_EXCH_RATE3 IS NULL
        )
        OR (
          NOT X.LCY_CHG_EXCH_RATE3 IS NULL AND Y.LCY_CHG_EXCH_RATE3 IS NULL
        )
        OR (
          X.LCY_CHG_EXCH_RATE3 <> Y.LCY_CHG_EXCH_RATE3
        )
      )
      OR (
        (
          X.LCY_CHG_EXCH_RATE4 IS NULL AND NOT Y.LCY_CHG_EXCH_RATE4 IS NULL
        )
        OR (
          NOT X.LCY_CHG_EXCH_RATE4 IS NULL AND Y.LCY_CHG_EXCH_RATE4 IS NULL
        )
        OR (
          X.LCY_CHG_EXCH_RATE4 <> Y.LCY_CHG_EXCH_RATE4
        )
      )
      OR (
        (
          X.LCY_EXCH_RATE IS NULL AND NOT Y.LCY_EXCH_RATE IS NULL
        )
        OR (
          NOT X.LCY_EXCH_RATE IS NULL AND Y.LCY_EXCH_RATE IS NULL
        )
        OR (
          X.LCY_EXCH_RATE <> Y.LCY_EXCH_RATE
        )
      )
      OR (
        (
          X.LIQN_DT IS NULL AND NOT Y.LIQN_DT IS NULL
        )
        OR (
          NOT X.LIQN_DT IS NULL AND Y.LIQN_DT IS NULL
        )
        OR (
          X.LIQN_DT <> Y.LIQN_DT
        )
      )
      OR (
        (
          X.MAINT_CHARGE_APPLIED IS NULL AND NOT Y.MAINT_CHARGE_APPLIED IS NULL
        )
        OR (
          NOT X.MAINT_CHARGE_APPLIED IS NULL AND Y.MAINT_CHARGE_APPLIED IS NULL
        )
        OR (
          X.MAINT_CHARGE_APPLIED <> Y.MAINT_CHARGE_APPLIED
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
          X.MIS_HEAD_1 IS NULL AND NOT Y.MIS_HEAD_1 IS NULL
        )
        OR (
          NOT X.MIS_HEAD_1 IS NULL AND Y.MIS_HEAD_1 IS NULL
        )
        OR (
          X.MIS_HEAD_1 <> Y.MIS_HEAD_1
        )
      )
      OR (
        (
          X.MIS_HEAD_2 IS NULL AND NOT Y.MIS_HEAD_2 IS NULL
        )
        OR (
          NOT X.MIS_HEAD_2 IS NULL AND Y.MIS_HEAD_2 IS NULL
        )
        OR (
          X.MIS_HEAD_2 <> Y.MIS_HEAD_2
        )
      )
      OR (
        (
          X.MIS_HEAD_3 IS NULL AND NOT Y.MIS_HEAD_3 IS NULL
        )
        OR (
          NOT X.MIS_HEAD_3 IS NULL AND Y.MIS_HEAD_3 IS NULL
        )
        OR (
          X.MIS_HEAD_3 <> Y.MIS_HEAD_3
        )
      )
      OR (
        (
          X.MIS_HEAD_4 IS NULL AND NOT Y.MIS_HEAD_4 IS NULL
        )
        OR (
          NOT X.MIS_HEAD_4 IS NULL AND Y.MIS_HEAD_4 IS NULL
        )
        OR (
          X.MIS_HEAD_4 <> Y.MIS_HEAD_4
        )
      )
      OR (
        (
          X.MIS_HEAD_5 IS NULL AND NOT Y.MIS_HEAD_5 IS NULL
        )
        OR (
          NOT X.MIS_HEAD_5 IS NULL AND Y.MIS_HEAD_5 IS NULL
        )
        OR (
          X.MIS_HEAD_5 <> Y.MIS_HEAD_5
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
          X.MODULE IS NULL AND NOT Y.MODULE IS NULL
        )
        OR (
          NOT X.MODULE IS NULL AND Y.MODULE IS NULL
        )
        OR (
          X.MODULE <> Y.MODULE
        )
      )
      OR (
        (
          X.NARRATIVE IS NULL AND NOT Y.NARRATIVE IS NULL
        )
        OR (
          NOT X.NARRATIVE IS NULL AND Y.NARRATIVE IS NULL
        )
        OR (
          X.NARRATIVE <> Y.NARRATIVE
        )
      )
      OR (
        (
          X.NEGOTIATED_RATE IS NULL AND NOT Y.NEGOTIATED_RATE IS NULL
        )
        OR (
          NOT X.NEGOTIATED_RATE IS NULL AND Y.NEGOTIATED_RATE IS NULL
        )
        OR (
          X.NEGOTIATED_RATE <> Y.NEGOTIATED_RATE
        )
      )
      OR (
        (
          X.NEGOTIATION_REF_NO IS NULL AND NOT Y.NEGOTIATION_REF_NO IS NULL
        )
        OR (
          NOT X.NEGOTIATION_REF_NO IS NULL AND Y.NEGOTIATION_REF_NO IS NULL
        )
        OR (
          X.NEGOTIATION_REF_NO <> Y.NEGOTIATION_REF_NO
        )
      )
      OR (
        (
          X.NETTING_IND IS NULL AND NOT Y.NETTING_IND IS NULL
        )
        OR (
          NOT X.NETTING_IND IS NULL AND Y.NETTING_IND IS NULL
        )
        OR (
          X.NETTING_IND <> Y.NETTING_IND
        )
      )
      OR (
        (
          X.NETTING_IND_1 IS NULL AND NOT Y.NETTING_IND_1 IS NULL
        )
        OR (
          NOT X.NETTING_IND_1 IS NULL AND Y.NETTING_IND_1 IS NULL
        )
        OR (
          X.NETTING_IND_1 <> Y.NETTING_IND_1
        )
      )
      OR (
        (
          X.NETTING_IND_2 IS NULL AND NOT Y.NETTING_IND_2 IS NULL
        )
        OR (
          NOT X.NETTING_IND_2 IS NULL AND Y.NETTING_IND_2 IS NULL
        )
        OR (
          X.NETTING_IND_2 <> Y.NETTING_IND_2
        )
      )
      OR (
        (
          X.NETTING_IND_3 IS NULL AND NOT Y.NETTING_IND_3 IS NULL
        )
        OR (
          NOT X.NETTING_IND_3 IS NULL AND Y.NETTING_IND_3 IS NULL
        )
        OR (
          X.NETTING_IND_3 <> Y.NETTING_IND_3
        )
      )
      OR (
        (
          X.NETTING_IND_4 IS NULL AND NOT Y.NETTING_IND_4 IS NULL
        )
        OR (
          NOT X.NETTING_IND_4 IS NULL AND Y.NETTING_IND_4 IS NULL
        )
        OR (
          X.NETTING_IND_4 <> Y.NETTING_IND_4
        )
      )
      OR (
        (
          X.NETTING_REQD IS NULL AND NOT Y.NETTING_REQD IS NULL
        )
        OR (
          NOT X.NETTING_REQD IS NULL AND Y.NETTING_REQD IS NULL
        )
        OR (
          X.NETTING_REQD <> Y.NETTING_REQD
        )
      )
      OR (
        (
          X.OFS_ACC IS NULL AND NOT Y.OFS_ACC IS NULL
        )
        OR (
          NOT X.OFS_ACC IS NULL AND Y.OFS_ACC IS NULL
        )
        OR (
          X.OFS_ACC <> Y.OFS_ACC
        )
      )
      OR (
        (
          X.OFS_AMOUNT IS NULL AND NOT Y.OFS_AMOUNT IS NULL
        )
        OR (
          NOT X.OFS_AMOUNT IS NULL AND Y.OFS_AMOUNT IS NULL
        )
        OR (
          X.OFS_AMOUNT <> Y.OFS_AMOUNT
        )
      )
      OR (
        (
          X.OFS_BRANCH IS NULL AND NOT Y.OFS_BRANCH IS NULL
        )
        OR (
          NOT X.OFS_BRANCH IS NULL AND Y.OFS_BRANCH IS NULL
        )
        OR (
          X.OFS_BRANCH <> Y.OFS_BRANCH
        )
      )
      OR (
        (
          X.OFS_CCY IS NULL AND NOT Y.OFS_CCY IS NULL
        )
        OR (
          NOT X.OFS_CCY IS NULL AND Y.OFS_CCY IS NULL
        )
        OR (
          X.OFS_CCY <> Y.OFS_CCY
        )
      )
      OR (
        (
          X.OFS_TRN_CODE IS NULL AND NOT Y.OFS_TRN_CODE IS NULL
        )
        OR (
          NOT X.OFS_TRN_CODE IS NULL AND Y.OFS_TRN_CODE IS NULL
        )
        OR (
          X.OFS_TRN_CODE <> Y.OFS_TRN_CODE
        )
      )
      OR (
        (
          X.PASSPORTIC_NO IS NULL AND NOT Y.PASSPORTIC_NO IS NULL
        )
        OR (
          NOT X.PASSPORTIC_NO IS NULL AND Y.PASSPORTIC_NO IS NULL
        )
        OR (
          X.PASSPORTIC_NO <> Y.PASSPORTIC_NO
        )
      )
      OR (
        (
          X.PRODUCT_CODE IS NULL AND NOT Y.PRODUCT_CODE IS NULL
        )
        OR (
          NOT X.PRODUCT_CODE IS NULL AND Y.PRODUCT_CODE IS NULL
        )
        OR (
          X.PRODUCT_CODE <> Y.PRODUCT_CODE
        )
      )
      OR (
        (
          X.PROJECT_NAME IS NULL AND NOT Y.PROJECT_NAME IS NULL
        )
        OR (
          NOT X.PROJECT_NAME IS NULL AND Y.PROJECT_NAME IS NULL
        )
        OR (
          X.PROJECT_NAME <> Y.PROJECT_NAME
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
          X.REL_CUSTOMER IS NULL AND NOT Y.REL_CUSTOMER IS NULL
        )
        OR (
          NOT X.REL_CUSTOMER IS NULL AND Y.REL_CUSTOMER IS NULL
        )
        OR (
          X.REL_CUSTOMER <> Y.REL_CUSTOMER
        )
      )
      OR (
        (
          X.REM_ACC IS NULL AND NOT Y.REM_ACC IS NULL
        )
        OR (
          NOT X.REM_ACC IS NULL AND Y.REM_ACC IS NULL
        )
        OR (
          X.REM_ACC <> Y.REM_ACC
        )
      )
      OR (
        (
          X.REM_BANK IS NULL AND NOT Y.REM_BANK IS NULL
        )
        OR (
          NOT X.REM_BANK IS NULL AND Y.REM_BANK IS NULL
        )
        OR (
          X.REM_BANK <> Y.REM_BANK
        )
      )
      OR (
        (
          X.REM_BRANCH IS NULL AND NOT Y.REM_BRANCH IS NULL
        )
        OR (
          NOT X.REM_BRANCH IS NULL AND Y.REM_BRANCH IS NULL
        )
        OR (
          X.REM_BRANCH <> Y.REM_BRANCH
        )
      )
      OR (
        (
          X.REPAIR_REASON IS NULL AND NOT Y.REPAIR_REASON IS NULL
        )
        OR (
          NOT X.REPAIR_REASON IS NULL AND Y.REPAIR_REASON IS NULL
        )
        OR (
          X.REPAIR_REASON <> Y.REPAIR_REASON
        )
      )
      OR (
        (
          X.REVR_DT IS NULL AND NOT Y.REVR_DT IS NULL
        )
        OR (
          NOT X.REVR_DT IS NULL AND Y.REVR_DT IS NULL
        )
        OR (
          X.REVR_DT <> Y.REVR_DT
        )
      )
      OR (
        (
          X.ROUTE_CODE IS NULL AND NOT Y.ROUTE_CODE IS NULL
        )
        OR (
          NOT X.ROUTE_CODE IS NULL AND Y.ROUTE_CODE IS NULL
        )
        OR (
          X.ROUTE_CODE <> Y.ROUTE_CODE
        )
      )
      OR (
        (
          X.ROUTING_NO IS NULL AND NOT Y.ROUTING_NO IS NULL
        )
        OR (
          NOT X.ROUTING_NO IS NULL AND Y.ROUTING_NO IS NULL
        )
        OR (
          X.ROUTING_NO <> Y.ROUTING_NO
        )
      )
      OR (
        (
          X.SCODE IS NULL AND NOT Y.SCODE IS NULL
        )
        OR (
          NOT X.SCODE IS NULL AND Y.SCODE IS NULL
        )
        OR (
          X.SCODE <> Y.SCODE
        )
      )
      OR (
        (
          X.SDB_REF_NO IS NULL AND NOT Y.SDB_REF_NO IS NULL
        )
        OR (
          NOT X.SDB_REF_NO IS NULL AND Y.SDB_REF_NO IS NULL
        )
        OR (
          X.SDB_REF_NO <> Y.SDB_REF_NO
        )
      )
      OR (
        (
          X.SERIAL_NO IS NULL AND NOT Y.SERIAL_NO IS NULL
        )
        OR (
          NOT X.SERIAL_NO IS NULL AND Y.SERIAL_NO IS NULL
        )
        OR (
          X.SERIAL_NO <> Y.SERIAL_NO
        )
      )
      OR (
        (
          X.SERVICE_PROVIDER IS NULL AND NOT Y.SERVICE_PROVIDER IS NULL
        )
        OR (
          NOT X.SERVICE_PROVIDER IS NULL AND Y.SERVICE_PROVIDER IS NULL
        )
        OR (
          X.SERVICE_PROVIDER <> Y.SERVICE_PROVIDER
        )
      )
      OR (
        (
          X.SK_BENEF_ADDR1_BENEF_ADDR2_BENEF_ADDR3_BENEF_ADDR4 IS NULL
          AND NOT Y.SK_BENEF_ADDR1_BENEF_ADDR2_BENEF_ADDR3_BENEF_ADDR4 IS NULL
        )
        OR (
          NOT X.SK_BENEF_ADDR1_BENEF_ADDR2_BENEF_ADDR3_BENEF_ADDR4 IS NULL
          AND Y.SK_BENEF_ADDR1_BENEF_ADDR2_BENEF_ADDR3_BENEF_ADDR4 IS NULL
        )
        OR (
          X.SK_BENEF_ADDR1_BENEF_ADDR2_BENEF_ADDR3_BENEF_ADDR4 <> Y.SK_BENEF_ADDR1_BENEF_ADDR2_BENEF_ADDR3_BENEF_ADDR4
        )
      )
      OR (
        (
          X.SK_REL_CUSTOMER IS NULL AND NOT Y.SK_REL_CUSTOMER IS NULL
        )
        OR (
          NOT X.SK_REL_CUSTOMER IS NULL AND Y.SK_REL_CUSTOMER IS NULL
        )
        OR (
          X.SK_REL_CUSTOMER <> Y.SK_REL_CUSTOMER
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
          X.SK_OFS_BRANCH IS NULL AND NOT Y.SK_OFS_BRANCH IS NULL
        )
        OR (
          NOT X.SK_OFS_BRANCH IS NULL AND Y.SK_OFS_BRANCH IS NULL
        )
        OR (
          X.SK_OFS_BRANCH <> Y.SK_OFS_BRANCH
        )
      )
      OR (
        (
          X.SK_TXN_BRANCH IS NULL AND NOT Y.SK_TXN_BRANCH IS NULL
        )
        OR (
          NOT X.SK_TXN_BRANCH IS NULL AND Y.SK_TXN_BRANCH IS NULL
        )
        OR (
          X.SK_TXN_BRANCH <> Y.SK_TXN_BRANCH
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
          X.SK_BENEF_NAME IS NULL AND NOT Y.SK_BENEF_NAME IS NULL
        )
        OR (
          NOT X.SK_BENEF_NAME IS NULL AND Y.SK_BENEF_NAME IS NULL
        )
        OR (
          X.SK_BENEF_NAME <> Y.SK_BENEF_NAME
        )
      )
      OR (
        (
          X.SK_OFS_ACC IS NULL AND NOT Y.SK_OFS_ACC IS NULL
        )
        OR (
          NOT X.SK_OFS_ACC IS NULL AND Y.SK_OFS_ACC IS NULL
        )
        OR (
          X.SK_OFS_ACC <> Y.SK_OFS_ACC
        )
      )
      OR (
        (
          X.SK_PRODUCT_CODE IS NULL AND NOT Y.SK_PRODUCT_CODE IS NULL
        )
        OR (
          NOT X.SK_PRODUCT_CODE IS NULL AND Y.SK_PRODUCT_CODE IS NULL
        )
        OR (
          X.SK_PRODUCT_CODE <> Y.SK_PRODUCT_CODE
        )
      )
      OR (
        (
          X.SK_SCODE IS NULL AND NOT Y.SK_SCODE IS NULL
        )
        OR (
          NOT X.SK_SCODE IS NULL AND Y.SK_SCODE IS NULL
        )
        OR (
          X.SK_SCODE <> Y.SK_SCODE
        )
      )
      OR (
        (
          X.SK_TRN_REF_NO IS NULL AND NOT Y.SK_TRN_REF_NO IS NULL
        )
        OR (
          NOT X.SK_TRN_REF_NO IS NULL AND Y.SK_TRN_REF_NO IS NULL
        )
        OR (
          X.SK_TRN_REF_NO <> Y.SK_TRN_REF_NO
        )
      )
      OR (
        (
          X.SK_TXN_ACC IS NULL AND NOT Y.SK_TXN_ACC IS NULL
        )
        OR (
          NOT X.SK_TXN_ACC IS NULL AND Y.SK_TXN_ACC IS NULL
        )
        OR (
          X.SK_TXN_ACC <> Y.SK_TXN_ACC
        )
      )
      OR (
        (
          X.THEIR_ACC IS NULL AND NOT Y.THEIR_ACC IS NULL
        )
        OR (
          NOT X.THEIR_ACC IS NULL AND Y.THEIR_ACC IS NULL
        )
        OR (
          X.THEIR_ACC <> Y.THEIR_ACC
        )
      )
      OR (
        (
          X.THEIR_ACC1 IS NULL AND NOT Y.THEIR_ACC1 IS NULL
        )
        OR (
          NOT X.THEIR_ACC1 IS NULL AND Y.THEIR_ACC1 IS NULL
        )
        OR (
          X.THEIR_ACC1 <> Y.THEIR_ACC1
        )
      )
      OR (
        (
          X.THEIR_ACC2 IS NULL AND NOT Y.THEIR_ACC2 IS NULL
        )
        OR (
          NOT X.THEIR_ACC2 IS NULL AND Y.THEIR_ACC2 IS NULL
        )
        OR (
          X.THEIR_ACC2 <> Y.THEIR_ACC2
        )
      )
      OR (
        (
          X.THEIR_ACC3 IS NULL AND NOT Y.THEIR_ACC3 IS NULL
        )
        OR (
          NOT X.THEIR_ACC3 IS NULL AND Y.THEIR_ACC3 IS NULL
        )
        OR (
          X.THEIR_ACC3 <> Y.THEIR_ACC3
        )
      )
      OR (
        (
          X.THEIR_ACC4 IS NULL AND NOT Y.THEIR_ACC4 IS NULL
        )
        OR (
          NOT X.THEIR_ACC4 IS NULL AND Y.THEIR_ACC4 IS NULL
        )
        OR (
          X.THEIR_ACC4 <> Y.THEIR_ACC4
        )
      )
      OR (
        (
          X.THEIR_CHGS IS NULL AND NOT Y.THEIR_CHGS IS NULL
        )
        OR (
          NOT X.THEIR_CHGS IS NULL AND Y.THEIR_CHGS IS NULL
        )
        OR (
          X.THEIR_CHGS <> Y.THEIR_CHGS
        )
      )
      OR (
        (
          X.THEIR_CHGS1 IS NULL AND NOT Y.THEIR_CHGS1 IS NULL
        )
        OR (
          NOT X.THEIR_CHGS1 IS NULL AND Y.THEIR_CHGS1 IS NULL
        )
        OR (
          X.THEIR_CHGS1 <> Y.THEIR_CHGS1
        )
      )
      OR (
        (
          X.THEIR_CHGS2 IS NULL AND NOT Y.THEIR_CHGS2 IS NULL
        )
        OR (
          NOT X.THEIR_CHGS2 IS NULL AND Y.THEIR_CHGS2 IS NULL
        )
        OR (
          X.THEIR_CHGS2 <> Y.THEIR_CHGS2
        )
      )
      OR (
        (
          X.THEIR_CHGS3 IS NULL AND NOT Y.THEIR_CHGS3 IS NULL
        )
        OR (
          NOT X.THEIR_CHGS3 IS NULL AND Y.THEIR_CHGS3 IS NULL
        )
        OR (
          X.THEIR_CHGS3 <> Y.THEIR_CHGS3
        )
      )
      OR (
        (
          X.THEIR_CHGS4 IS NULL AND NOT Y.THEIR_CHGS4 IS NULL
        )
        OR (
          NOT X.THEIR_CHGS4 IS NULL AND Y.THEIR_CHGS4 IS NULL
        )
        OR (
          X.THEIR_CHGS4 <> Y.THEIR_CHGS4
        )
      )
      OR (
        (
          X.TIME_RECEIVED IS NULL AND NOT Y.TIME_RECEIVED IS NULL
        )
        OR (
          NOT X.TIME_RECEIVED IS NULL AND Y.TIME_RECEIVED IS NULL
        )
        OR (
          X.TIME_RECEIVED <> Y.TIME_RECEIVED
        )
      )
      OR (
        (
          X.TOKEN_NO IS NULL AND NOT Y.TOKEN_NO IS NULL
        )
        OR (
          NOT X.TOKEN_NO IS NULL AND Y.TOKEN_NO IS NULL
        )
        OR (
          X.TOKEN_NO <> Y.TOKEN_NO
        )
      )
      OR (
        (
          X.TOT_CHG_IN_TCY IS NULL AND NOT Y.TOT_CHG_IN_TCY IS NULL
        )
        OR (
          NOT X.TOT_CHG_IN_TCY IS NULL AND Y.TOT_CHG_IN_TCY IS NULL
        )
        OR (
          X.TOT_CHG_IN_TCY <> Y.TOT_CHG_IN_TCY
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
          X.TRN_DT IS NULL AND NOT Y.TRN_DT IS NULL
        )
        OR (
          NOT X.TRN_DT IS NULL AND Y.TRN_DT IS NULL
        )
        OR (
          X.TRN_DT <> Y.TRN_DT
        )
      )
      OR (
        (
          X.TXN_ACC IS NULL AND NOT Y.TXN_ACC IS NULL
        )
        OR (
          NOT X.TXN_ACC IS NULL AND Y.TXN_ACC IS NULL
        )
        OR (
          X.TXN_ACC <> Y.TXN_ACC
        )
      )
      OR (
        (
          X.TXN_AMOUNT IS NULL AND NOT Y.TXN_AMOUNT IS NULL
        )
        OR (
          NOT X.TXN_AMOUNT IS NULL AND Y.TXN_AMOUNT IS NULL
        )
        OR (
          X.TXN_AMOUNT <> Y.TXN_AMOUNT
        )
      )
      OR (
        (
          X.TXN_BRANCH IS NULL AND NOT Y.TXN_BRANCH IS NULL
        )
        OR (
          NOT X.TXN_BRANCH IS NULL AND Y.TXN_BRANCH IS NULL
        )
        OR (
          X.TXN_BRANCH <> Y.TXN_BRANCH
        )
      )
      OR (
        (
          X.TXN_CCY IS NULL AND NOT Y.TXN_CCY IS NULL
        )
        OR (
          NOT X.TXN_CCY IS NULL AND Y.TXN_CCY IS NULL
        )
        OR (
          X.TXN_CCY <> Y.TXN_CCY
        )
      )
      OR (
        (
          X.TXN_CODE IS NULL AND NOT Y.TXN_CODE IS NULL
        )
        OR (
          NOT X.TXN_CODE IS NULL AND Y.TXN_CODE IS NULL
        )
        OR (
          X.TXN_CODE <> Y.TXN_CODE
        )
      )
      OR (
        (
          X.TXN_CODE_1 IS NULL AND NOT Y.TXN_CODE_1 IS NULL
        )
        OR (
          NOT X.TXN_CODE_1 IS NULL AND Y.TXN_CODE_1 IS NULL
        )
        OR (
          X.TXN_CODE_1 <> Y.TXN_CODE_1
        )
      )
      OR (
        (
          X.TXN_CODE_2 IS NULL AND NOT Y.TXN_CODE_2 IS NULL
        )
        OR (
          NOT X.TXN_CODE_2 IS NULL AND Y.TXN_CODE_2 IS NULL
        )
        OR (
          X.TXN_CODE_2 <> Y.TXN_CODE_2
        )
      )
      OR (
        (
          X.TXN_CODE_3 IS NULL AND NOT Y.TXN_CODE_3 IS NULL
        )
        OR (
          NOT X.TXN_CODE_3 IS NULL AND Y.TXN_CODE_3 IS NULL
        )
        OR (
          X.TXN_CODE_3 <> Y.TXN_CODE_3
        )
      )
      OR (
        (
          X.TXN_CODE_4 IS NULL AND NOT Y.TXN_CODE_4 IS NULL
        )
        OR (
          NOT X.TXN_CODE_4 IS NULL AND Y.TXN_CODE_4 IS NULL
        )
        OR (
          X.TXN_CODE_4 <> Y.TXN_CODE_4
        )
      )
      OR (
        (
          X.TXN_STATUS IS NULL AND NOT Y.TXN_STATUS IS NULL
        )
        OR (
          NOT X.TXN_STATUS IS NULL AND Y.TXN_STATUS IS NULL
        )
        OR (
          X.TXN_STATUS <> Y.TXN_STATUS
        )
      )
      OR (
        (
          X.TXN_TANKED IS NULL AND NOT Y.TXN_TANKED IS NULL
        )
        OR (
          NOT X.TXN_TANKED IS NULL AND Y.TXN_TANKED IS NULL
        )
        OR (
          X.TXN_TANKED <> Y.TXN_TANKED
        )
      )
      OR (
        (
          X.TXN_TRN_CODE IS NULL AND NOT Y.TXN_TRN_CODE IS NULL
        )
        OR (
          NOT X.TXN_TRN_CODE IS NULL AND Y.TXN_TRN_CODE IS NULL
        )
        OR (
          X.TXN_TRN_CODE <> Y.TXN_TRN_CODE
        )
      )
      OR (
        (
          X.UNIT_ID IS NULL AND NOT Y.UNIT_ID IS NULL
        )
        OR (
          NOT X.UNIT_ID IS NULL AND Y.UNIT_ID IS NULL
        )
        OR (
          X.UNIT_ID <> Y.UNIT_ID
        )
      )
      OR (
        (
          X.UNIT_PAYMENT IS NULL AND NOT Y.UNIT_PAYMENT IS NULL
        )
        OR (
          NOT X.UNIT_PAYMENT IS NULL AND Y.UNIT_PAYMENT IS NULL
        )
        OR (
          X.UNIT_PAYMENT <> Y.UNIT_PAYMENT
        )
      )
      OR (
        (
          X.VALUE_DT IS NULL AND NOT Y.VALUE_DT IS NULL
        )
        OR (
          NOT X.VALUE_DT IS NULL AND Y.VALUE_DT IS NULL
        )
        OR (
          X.VALUE_DT <> Y.VALUE_DT
        )
      )
      OR (
        (
          X.WAIVER IS NULL AND NOT Y.WAIVER IS NULL
        )
        OR (
          NOT X.WAIVER IS NULL AND Y.WAIVER IS NULL
        )
        OR (
          X.WAIVER <> Y.WAIVER
        )
      )
      OR (
        (
          X.WAIVER1 IS NULL AND NOT Y.WAIVER1 IS NULL
        )
        OR (
          NOT X.WAIVER1 IS NULL AND Y.WAIVER1 IS NULL
        )
        OR (
          X.WAIVER1 <> Y.WAIVER1
        )
      )
      OR (
        (
          X.WAIVER2 IS NULL AND NOT Y.WAIVER2 IS NULL
        )
        OR (
          NOT X.WAIVER2 IS NULL AND Y.WAIVER2 IS NULL
        )
        OR (
          X.WAIVER2 <> Y.WAIVER2
        )
      )
      OR (
        (
          X.WAIVER3 IS NULL AND NOT Y.WAIVER3 IS NULL
        )
        OR (
          NOT X.WAIVER3 IS NULL AND Y.WAIVER3 IS NULL
        )
        OR (
          X.WAIVER3 <> Y.WAIVER3
        )
      )
      OR (
        (
          X.WAIVER4 IS NULL AND NOT Y.WAIVER4 IS NULL
        )
        OR (
          NOT X.WAIVER4 IS NULL AND Y.WAIVER4 IS NULL
        )
        OR (
          X.WAIVER4 <> Y.WAIVER4
        )
      )
      OR (
        (
          X.XREF IS NULL AND NOT Y.XREF IS NULL
        )
        OR (
          NOT X.XREF IS NULL AND Y.XREF IS NULL
        )
        OR (
          X.XREF <> Y.XREF
        )
      )
    )
    THEN 'UPD'
    WHEN X.TRN_REF_NO IS NULL
    THEN 'DEL' /* - IF MULTIPLE PKs are there then concatenate them with AND */
  END AS OPT_FLAG
FROM DEVT_STG_FLX.DETB_RTL_TELLER AS X
FULL JOIN DEVT_STG_FLX.DETB_RTL_TELLER_BACKUP AS Y
  ON X.TRN_REF_NO = Y.TRN_REF_NO /* - IF MULTIPLE PKs are there then concatenate them with AND */
WHERE
  NOT OPT_FLAG IS NULL