CREATE OR REPLACE VIEW DEVV_STG_FLX.BCTB_CONTRACT_MASTER_get_delta AS LOCKING ROW FOR ACCESS
SELECT
  COALESCE(X.BCREFNO, Y.BCREFNO) AS BCREFNO,
  COALESCE(X.EVENT_SEQ_NO, Y.EVENT_SEQ_NO) AS EVENT_SEQ_NO,
  X.ACKN_DATE,
  X.ACKN_RECVD,
  X.ACP_FROM_DATE,
  X.ACP_ICCF_COL_TYPE,
  X.ACP_INT_COMP,
  X.ACP_TO_ADV_FLAG,
  X.ACP_TO_DATE,
  X.ACP_TO_DIS_FLAG,
  X.ACP_TO_FORFAIT_FLAG,
  X.ADVANCE_BY_LOAN,
  X.ALLOW_BROKERAGE,
  X.ALLOW_PREPAY,
  X.ALLOW_ROLLOVER,
  X.ANCILLARY_MESG,
  X.ANCILLARY_MESG_FUNCTION,
  X.AUTO_ACP_TO_ADV_FLAG,
  X.AUTO_LIQ_FLAG,
  X.AUTO_LIQD_RETRY_COUNT,
  X.AVAILABLE_COLLATERAL,
  X.BASE_DATE,
  X.BASE_DATE_CODE,
  X.BASE_DATE_DESC,
  X.BILL_AMT,
  X.BILL_AMT_LCY,
  X.BILL_AMT_LIQD,
  X.BILL_CCY,
  X.BILL_DUE_AMT,
  X.BM_BILL_CCY,
  X.BM_COLLATERAL_CCY,
  X.BM_DOC_ORG_RECV_FLAG,
  X.BM_EVENT_CODE,
  X.BM_LIMITS_MONITORING_FLAG,
  X.BM_PRODUCT_TYPE,
  X.BM_STAGE,
  X.BROK_PAID_BY_US,
  X.BROKER,
  X.CHECK_NUMBER,
  X.CHG_CLM_SWIFT,
  X.CHGCLM_TEMPLATE_ID,
  X.COLL_CONTRIBUTION,
  X.COLL_TO_DIS_FLAG,
  X.COLL_TO_PUR_FLAG,
  X.COLL_TO_TRF_FLAG,
  X.COLLATERAL_AMT,
  X.COLLATERAL_AMT_LCY,
  X.COLLATERAL_CCY,
  X.COLLATERAL_DESC,
  X.COLLECT_LC_ADV_CHG_FROM,
  X.COLLECTION_REF,
  X.CONFMD_LC_AMT,
  X.CONTRACT_DERIVED_STATUS,
  X.COVERING_LETTER_DATE,
  X.CR_VALUE_DATE,
  X.CUST_MARGIN,
  X.CUSTOMER_ID,
  X.CUSTOMER_PARTY_TYPE,
  X.DIS_TO_COLL_FLAG,
  X.DIS_TO_FORFAIT_FLAG,
  X.DISCOUNTING_METHOD,
  X.DOC_DUP_RECV_FLAG,
  X.DOC_FLAG,
  X.DOC_ORG_RECV_FLAG,
  X.DR_VALUE_DATE,
  X.EFFC_LCY_COL_EX_RATE,
  X.EFFC_LCY_PUR_XRATE,
  X.EVENT_CODE,
  X.EXCH_RATE,
  X.FORFAITING_DOC_DATE,
  X.FORFAITING_REQ_DATE,
  X.FROM_CALC_DATE,
  X.FUND_ID,
  X.FURTHER_IDENTIFICATION,
  X.GRACE_DAYS,
  X.ICCF_PICKUP_CCY,
  X.INT_LIQD_MODE,
  X.INTERNAL_REMARKS,
  X.IS_CHARGE_CHANGED,
  X.IS_ICCF_CHANGED,
  X.IS_SETTLEMENT_CHANGED,
  X.IS_TAX_CHANGED,
  X.LC_AMT,
  X.LC_FLAG,
  X.LC_ISSUE_DATE,
  X.LC_LIAB_AMT,
  X.LC_NOT_ADVISED,
  X.LIMITS_MONITORING_FLAG,
  X.LINKED_TO_LOAN,
  X.LIQD_USING_COLLAT,
  X.LIQUIDATION_DATE,
  X.LOAN_REF_NO,
  X.MARGIN_AMT,
  X.MARGIN_AMT_LIQD,
  X.MARGIN_DUE_AMT,
  X.MATURITY_DATE,
  X.MSG_PROC_FLAG,
  X.OPERATION,
  X.OUR_CHARGES_REFUSED,
  X.OUR_LC_REF,
  X.OURLC_APPLICANT,
  X.OVD_OVERDRAFT_FLAG,
  X.PASS_INTEREST_TO_THEM,
  X.PASS_OUR_CHG_TO_THEM,
  X.PRODUCT_TYPE,
  X.PUR_TO_COLL_FLAG,
  X.PURCHASE_AMT,
  X.REBATE_AMT,
  X.REDISCOUNT_FLAG,
  X.REFUND_INT_CODE,
  X.REFUND_INT_RATE,
  X.REFUND_INTEREST,
  X.REIMBURSEMENT_DAYS,
  X.ROLLOVER_FLAG,
  X.ROLLOVER_REF_NO,
  X.SETTLE_AVAILABLE_AMT,
  X.SK_CUSTOMER_ID,
  X.SK_BCREFNO,
  X.SK_OUR_LC_REF,
  X.SK_TENOR_CODE,
  X.STAGE,
  X.STAGE_ASOF_DATE,
  X.STATUS_ASOF_DATE,
  X.STATUS_CHK_REQD,
  X.STATUS_CONTROL_FLAG,
  X.SUBSYSTEM_STAT,
  X.TENOR_CODE,
  X.TENOR_DAYS,
  X.TFR_COLLATERAL_FROM_LC,
  X.THEIR_CHARGES_REFUSED,
  X.THEIR_CHG_AMT,
  X.THEIR_CHG_CCY,
  X.THEIR_LC_REF,
  X.TO_CALC_DATE,
  X.TRANSFERRED_COLLATERAL_AMT,
  X.TRANSIT_DAYS,
  X.TXN_DATE,
  X.ULTI_ROLL_PARENT,
  X.UNLINKED_FX_RATE,
  X.USE_LCREF_IN_MSG,
  X.USER_DEFINED_STATUS,
  X.VALUE_DATE,
  X.VERIFY_FUNDS,
  X.VERSION_NO,
  X.WAIVE_END_DATE,
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
    WHEN Y.BCREFNO IS NULL AND Y.EVENT_SEQ_NO IS NULL
    THEN 'INS' /* - IF MULTIPLE PKs are there then concatenate them with AND */
    WHEN X.BCREFNO = Y.BCREFNO
    AND X.EVENT_SEQ_NO = Y.EVENT_SEQ_NO /* - IF MULTIPLE PKs are there then concatenate them with AND */
    AND (
      (
        (
          X.ACKN_DATE IS NULL AND NOT Y.ACKN_DATE IS NULL
        )
        OR (
          NOT X.ACKN_DATE IS NULL AND Y.ACKN_DATE IS NULL
        )
        OR (
          X.ACKN_DATE <> Y.ACKN_DATE
        )
      )
      OR (
        (
          X.ACKN_RECVD IS NULL AND NOT Y.ACKN_RECVD IS NULL
        )
        OR (
          NOT X.ACKN_RECVD IS NULL AND Y.ACKN_RECVD IS NULL
        )
        OR (
          X.ACKN_RECVD <> Y.ACKN_RECVD
        )
      )
      OR (
        (
          X.ACP_FROM_DATE IS NULL AND NOT Y.ACP_FROM_DATE IS NULL
        )
        OR (
          NOT X.ACP_FROM_DATE IS NULL AND Y.ACP_FROM_DATE IS NULL
        )
        OR (
          X.ACP_FROM_DATE <> Y.ACP_FROM_DATE
        )
      )
      OR (
        (
          X.ACP_ICCF_COL_TYPE IS NULL AND NOT Y.ACP_ICCF_COL_TYPE IS NULL
        )
        OR (
          NOT X.ACP_ICCF_COL_TYPE IS NULL AND Y.ACP_ICCF_COL_TYPE IS NULL
        )
        OR (
          X.ACP_ICCF_COL_TYPE <> Y.ACP_ICCF_COL_TYPE
        )
      )
      OR (
        (
          X.ACP_INT_COMP IS NULL AND NOT Y.ACP_INT_COMP IS NULL
        )
        OR (
          NOT X.ACP_INT_COMP IS NULL AND Y.ACP_INT_COMP IS NULL
        )
        OR (
          X.ACP_INT_COMP <> Y.ACP_INT_COMP
        )
      )
      OR (
        (
          X.ACP_TO_ADV_FLAG IS NULL AND NOT Y.ACP_TO_ADV_FLAG IS NULL
        )
        OR (
          NOT X.ACP_TO_ADV_FLAG IS NULL AND Y.ACP_TO_ADV_FLAG IS NULL
        )
        OR (
          X.ACP_TO_ADV_FLAG <> Y.ACP_TO_ADV_FLAG
        )
      )
      OR (
        (
          X.ACP_TO_DATE IS NULL AND NOT Y.ACP_TO_DATE IS NULL
        )
        OR (
          NOT X.ACP_TO_DATE IS NULL AND Y.ACP_TO_DATE IS NULL
        )
        OR (
          X.ACP_TO_DATE <> Y.ACP_TO_DATE
        )
      )
      OR (
        (
          X.ACP_TO_DIS_FLAG IS NULL AND NOT Y.ACP_TO_DIS_FLAG IS NULL
        )
        OR (
          NOT X.ACP_TO_DIS_FLAG IS NULL AND Y.ACP_TO_DIS_FLAG IS NULL
        )
        OR (
          X.ACP_TO_DIS_FLAG <> Y.ACP_TO_DIS_FLAG
        )
      )
      OR (
        (
          X.ACP_TO_FORFAIT_FLAG IS NULL AND NOT Y.ACP_TO_FORFAIT_FLAG IS NULL
        )
        OR (
          NOT X.ACP_TO_FORFAIT_FLAG IS NULL AND Y.ACP_TO_FORFAIT_FLAG IS NULL
        )
        OR (
          X.ACP_TO_FORFAIT_FLAG <> Y.ACP_TO_FORFAIT_FLAG
        )
      )
      OR (
        (
          X.ADVANCE_BY_LOAN IS NULL AND NOT Y.ADVANCE_BY_LOAN IS NULL
        )
        OR (
          NOT X.ADVANCE_BY_LOAN IS NULL AND Y.ADVANCE_BY_LOAN IS NULL
        )
        OR (
          X.ADVANCE_BY_LOAN <> Y.ADVANCE_BY_LOAN
        )
      )
      OR (
        (
          X.ALLOW_BROKERAGE IS NULL AND NOT Y.ALLOW_BROKERAGE IS NULL
        )
        OR (
          NOT X.ALLOW_BROKERAGE IS NULL AND Y.ALLOW_BROKERAGE IS NULL
        )
        OR (
          X.ALLOW_BROKERAGE <> Y.ALLOW_BROKERAGE
        )
      )
      OR (
        (
          X.ALLOW_PREPAY IS NULL AND NOT Y.ALLOW_PREPAY IS NULL
        )
        OR (
          NOT X.ALLOW_PREPAY IS NULL AND Y.ALLOW_PREPAY IS NULL
        )
        OR (
          X.ALLOW_PREPAY <> Y.ALLOW_PREPAY
        )
      )
      OR (
        (
          X.ALLOW_ROLLOVER IS NULL AND NOT Y.ALLOW_ROLLOVER IS NULL
        )
        OR (
          NOT X.ALLOW_ROLLOVER IS NULL AND Y.ALLOW_ROLLOVER IS NULL
        )
        OR (
          X.ALLOW_ROLLOVER <> Y.ALLOW_ROLLOVER
        )
      )
      OR (
        (
          X.ANCILLARY_MESG IS NULL AND NOT Y.ANCILLARY_MESG IS NULL
        )
        OR (
          NOT X.ANCILLARY_MESG IS NULL AND Y.ANCILLARY_MESG IS NULL
        )
        OR (
          X.ANCILLARY_MESG <> Y.ANCILLARY_MESG
        )
      )
      OR (
        (
          X.ANCILLARY_MESG_FUNCTION IS NULL AND NOT Y.ANCILLARY_MESG_FUNCTION IS NULL
        )
        OR (
          NOT X.ANCILLARY_MESG_FUNCTION IS NULL AND Y.ANCILLARY_MESG_FUNCTION IS NULL
        )
        OR (
          X.ANCILLARY_MESG_FUNCTION <> Y.ANCILLARY_MESG_FUNCTION
        )
      )
      OR (
        (
          X.AUTO_ACP_TO_ADV_FLAG IS NULL AND NOT Y.AUTO_ACP_TO_ADV_FLAG IS NULL
        )
        OR (
          NOT X.AUTO_ACP_TO_ADV_FLAG IS NULL AND Y.AUTO_ACP_TO_ADV_FLAG IS NULL
        )
        OR (
          X.AUTO_ACP_TO_ADV_FLAG <> Y.AUTO_ACP_TO_ADV_FLAG
        )
      )
      OR (
        (
          X.AUTO_LIQ_FLAG IS NULL AND NOT Y.AUTO_LIQ_FLAG IS NULL
        )
        OR (
          NOT X.AUTO_LIQ_FLAG IS NULL AND Y.AUTO_LIQ_FLAG IS NULL
        )
        OR (
          X.AUTO_LIQ_FLAG <> Y.AUTO_LIQ_FLAG
        )
      )
      OR (
        (
          X.AUTO_LIQD_RETRY_COUNT IS NULL AND NOT Y.AUTO_LIQD_RETRY_COUNT IS NULL
        )
        OR (
          NOT X.AUTO_LIQD_RETRY_COUNT IS NULL AND Y.AUTO_LIQD_RETRY_COUNT IS NULL
        )
        OR (
          X.AUTO_LIQD_RETRY_COUNT <> Y.AUTO_LIQD_RETRY_COUNT
        )
      )
      OR (
        (
          X.AVAILABLE_COLLATERAL IS NULL AND NOT Y.AVAILABLE_COLLATERAL IS NULL
        )
        OR (
          NOT X.AVAILABLE_COLLATERAL IS NULL AND Y.AVAILABLE_COLLATERAL IS NULL
        )
        OR (
          X.AVAILABLE_COLLATERAL <> Y.AVAILABLE_COLLATERAL
        )
      )
      OR (
        (
          X.BASE_DATE IS NULL AND NOT Y.BASE_DATE IS NULL
        )
        OR (
          NOT X.BASE_DATE IS NULL AND Y.BASE_DATE IS NULL
        )
        OR (
          X.BASE_DATE <> Y.BASE_DATE
        )
      )
      OR (
        (
          X.BASE_DATE_CODE IS NULL AND NOT Y.BASE_DATE_CODE IS NULL
        )
        OR (
          NOT X.BASE_DATE_CODE IS NULL AND Y.BASE_DATE_CODE IS NULL
        )
        OR (
          X.BASE_DATE_CODE <> Y.BASE_DATE_CODE
        )
      )
      OR (
        (
          X.BASE_DATE_DESC IS NULL AND NOT Y.BASE_DATE_DESC IS NULL
        )
        OR (
          NOT X.BASE_DATE_DESC IS NULL AND Y.BASE_DATE_DESC IS NULL
        )
        OR (
          X.BASE_DATE_DESC <> Y.BASE_DATE_DESC
        )
      )
      OR (
        (
          X.BILL_AMT IS NULL AND NOT Y.BILL_AMT IS NULL
        )
        OR (
          NOT X.BILL_AMT IS NULL AND Y.BILL_AMT IS NULL
        )
        OR (
          X.BILL_AMT <> Y.BILL_AMT
        )
      )
      OR (
        (
          X.BILL_AMT_LCY IS NULL AND NOT Y.BILL_AMT_LCY IS NULL
        )
        OR (
          NOT X.BILL_AMT_LCY IS NULL AND Y.BILL_AMT_LCY IS NULL
        )
        OR (
          X.BILL_AMT_LCY <> Y.BILL_AMT_LCY
        )
      )
      OR (
        (
          X.BILL_AMT_LIQD IS NULL AND NOT Y.BILL_AMT_LIQD IS NULL
        )
        OR (
          NOT X.BILL_AMT_LIQD IS NULL AND Y.BILL_AMT_LIQD IS NULL
        )
        OR (
          X.BILL_AMT_LIQD <> Y.BILL_AMT_LIQD
        )
      )
      OR (
        (
          X.BILL_CCY IS NULL AND NOT Y.BILL_CCY IS NULL
        )
        OR (
          NOT X.BILL_CCY IS NULL AND Y.BILL_CCY IS NULL
        )
        OR (
          X.BILL_CCY <> Y.BILL_CCY
        )
      )
      OR (
        (
          X.BILL_DUE_AMT IS NULL AND NOT Y.BILL_DUE_AMT IS NULL
        )
        OR (
          NOT X.BILL_DUE_AMT IS NULL AND Y.BILL_DUE_AMT IS NULL
        )
        OR (
          X.BILL_DUE_AMT <> Y.BILL_DUE_AMT
        )
      )
      OR (
        (
          X.BM_BILL_CCY IS NULL AND NOT Y.BM_BILL_CCY IS NULL
        )
        OR (
          NOT X.BM_BILL_CCY IS NULL AND Y.BM_BILL_CCY IS NULL
        )
        OR (
          X.BM_BILL_CCY <> Y.BM_BILL_CCY
        )
      )
      OR (
        (
          X.BM_COLLATERAL_CCY IS NULL AND NOT Y.BM_COLLATERAL_CCY IS NULL
        )
        OR (
          NOT X.BM_COLLATERAL_CCY IS NULL AND Y.BM_COLLATERAL_CCY IS NULL
        )
        OR (
          X.BM_COLLATERAL_CCY <> Y.BM_COLLATERAL_CCY
        )
      )
      OR (
        (
          X.BM_DOC_ORG_RECV_FLAG IS NULL AND NOT Y.BM_DOC_ORG_RECV_FLAG IS NULL
        )
        OR (
          NOT X.BM_DOC_ORG_RECV_FLAG IS NULL AND Y.BM_DOC_ORG_RECV_FLAG IS NULL
        )
        OR (
          X.BM_DOC_ORG_RECV_FLAG <> Y.BM_DOC_ORG_RECV_FLAG
        )
      )
      OR (
        (
          X.BM_EVENT_CODE IS NULL AND NOT Y.BM_EVENT_CODE IS NULL
        )
        OR (
          NOT X.BM_EVENT_CODE IS NULL AND Y.BM_EVENT_CODE IS NULL
        )
        OR (
          X.BM_EVENT_CODE <> Y.BM_EVENT_CODE
        )
      )
      OR (
        (
          X.BM_LIMITS_MONITORING_FLAG IS NULL AND NOT Y.BM_LIMITS_MONITORING_FLAG IS NULL
        )
        OR (
          NOT X.BM_LIMITS_MONITORING_FLAG IS NULL AND Y.BM_LIMITS_MONITORING_FLAG IS NULL
        )
        OR (
          X.BM_LIMITS_MONITORING_FLAG <> Y.BM_LIMITS_MONITORING_FLAG
        )
      )
      OR (
        (
          X.BM_PRODUCT_TYPE IS NULL AND NOT Y.BM_PRODUCT_TYPE IS NULL
        )
        OR (
          NOT X.BM_PRODUCT_TYPE IS NULL AND Y.BM_PRODUCT_TYPE IS NULL
        )
        OR (
          X.BM_PRODUCT_TYPE <> Y.BM_PRODUCT_TYPE
        )
      )
      OR (
        (
          X.BM_STAGE IS NULL AND NOT Y.BM_STAGE IS NULL
        )
        OR (
          NOT X.BM_STAGE IS NULL AND Y.BM_STAGE IS NULL
        )
        OR (
          X.BM_STAGE <> Y.BM_STAGE
        )
      )
      OR (
        (
          X.BROK_PAID_BY_US IS NULL AND NOT Y.BROK_PAID_BY_US IS NULL
        )
        OR (
          NOT X.BROK_PAID_BY_US IS NULL AND Y.BROK_PAID_BY_US IS NULL
        )
        OR (
          X.BROK_PAID_BY_US <> Y.BROK_PAID_BY_US
        )
      )
      OR (
        (
          X.BROKER IS NULL AND NOT Y.BROKER IS NULL
        )
        OR (
          NOT X.BROKER IS NULL AND Y.BROKER IS NULL
        )
        OR (
          X.BROKER <> Y.BROKER
        )
      )
      OR (
        (
          X.CHECK_NUMBER IS NULL AND NOT Y.CHECK_NUMBER IS NULL
        )
        OR (
          NOT X.CHECK_NUMBER IS NULL AND Y.CHECK_NUMBER IS NULL
        )
        OR (
          X.CHECK_NUMBER <> Y.CHECK_NUMBER
        )
      )
      OR (
        (
          X.CHG_CLM_SWIFT IS NULL AND NOT Y.CHG_CLM_SWIFT IS NULL
        )
        OR (
          NOT X.CHG_CLM_SWIFT IS NULL AND Y.CHG_CLM_SWIFT IS NULL
        )
        OR (
          X.CHG_CLM_SWIFT <> Y.CHG_CLM_SWIFT
        )
      )
      OR (
        (
          X.CHGCLM_TEMPLATE_ID IS NULL AND NOT Y.CHGCLM_TEMPLATE_ID IS NULL
        )
        OR (
          NOT X.CHGCLM_TEMPLATE_ID IS NULL AND Y.CHGCLM_TEMPLATE_ID IS NULL
        )
        OR (
          X.CHGCLM_TEMPLATE_ID <> Y.CHGCLM_TEMPLATE_ID
        )
      )
      OR (
        (
          X.COLL_CONTRIBUTION IS NULL AND NOT Y.COLL_CONTRIBUTION IS NULL
        )
        OR (
          NOT X.COLL_CONTRIBUTION IS NULL AND Y.COLL_CONTRIBUTION IS NULL
        )
        OR (
          X.COLL_CONTRIBUTION <> Y.COLL_CONTRIBUTION
        )
      )
      OR (
        (
          X.COLL_TO_DIS_FLAG IS NULL AND NOT Y.COLL_TO_DIS_FLAG IS NULL
        )
        OR (
          NOT X.COLL_TO_DIS_FLAG IS NULL AND Y.COLL_TO_DIS_FLAG IS NULL
        )
        OR (
          X.COLL_TO_DIS_FLAG <> Y.COLL_TO_DIS_FLAG
        )
      )
      OR (
        (
          X.COLL_TO_PUR_FLAG IS NULL AND NOT Y.COLL_TO_PUR_FLAG IS NULL
        )
        OR (
          NOT X.COLL_TO_PUR_FLAG IS NULL AND Y.COLL_TO_PUR_FLAG IS NULL
        )
        OR (
          X.COLL_TO_PUR_FLAG <> Y.COLL_TO_PUR_FLAG
        )
      )
      OR (
        (
          X.COLL_TO_TRF_FLAG IS NULL AND NOT Y.COLL_TO_TRF_FLAG IS NULL
        )
        OR (
          NOT X.COLL_TO_TRF_FLAG IS NULL AND Y.COLL_TO_TRF_FLAG IS NULL
        )
        OR (
          X.COLL_TO_TRF_FLAG <> Y.COLL_TO_TRF_FLAG
        )
      )
      OR (
        (
          X.COLLATERAL_AMT IS NULL AND NOT Y.COLLATERAL_AMT IS NULL
        )
        OR (
          NOT X.COLLATERAL_AMT IS NULL AND Y.COLLATERAL_AMT IS NULL
        )
        OR (
          X.COLLATERAL_AMT <> Y.COLLATERAL_AMT
        )
      )
      OR (
        (
          X.COLLATERAL_AMT_LCY IS NULL AND NOT Y.COLLATERAL_AMT_LCY IS NULL
        )
        OR (
          NOT X.COLLATERAL_AMT_LCY IS NULL AND Y.COLLATERAL_AMT_LCY IS NULL
        )
        OR (
          X.COLLATERAL_AMT_LCY <> Y.COLLATERAL_AMT_LCY
        )
      )
      OR (
        (
          X.COLLATERAL_CCY IS NULL AND NOT Y.COLLATERAL_CCY IS NULL
        )
        OR (
          NOT X.COLLATERAL_CCY IS NULL AND Y.COLLATERAL_CCY IS NULL
        )
        OR (
          X.COLLATERAL_CCY <> Y.COLLATERAL_CCY
        )
      )
      OR (
        (
          X.COLLATERAL_DESC IS NULL AND NOT Y.COLLATERAL_DESC IS NULL
        )
        OR (
          NOT X.COLLATERAL_DESC IS NULL AND Y.COLLATERAL_DESC IS NULL
        )
        OR (
          X.COLLATERAL_DESC <> Y.COLLATERAL_DESC
        )
      )
      OR (
        (
          X.COLLECT_LC_ADV_CHG_FROM IS NULL AND NOT Y.COLLECT_LC_ADV_CHG_FROM IS NULL
        )
        OR (
          NOT X.COLLECT_LC_ADV_CHG_FROM IS NULL AND Y.COLLECT_LC_ADV_CHG_FROM IS NULL
        )
        OR (
          X.COLLECT_LC_ADV_CHG_FROM <> Y.COLLECT_LC_ADV_CHG_FROM
        )
      )
      OR (
        (
          X.COLLECTION_REF IS NULL AND NOT Y.COLLECTION_REF IS NULL
        )
        OR (
          NOT X.COLLECTION_REF IS NULL AND Y.COLLECTION_REF IS NULL
        )
        OR (
          X.COLLECTION_REF <> Y.COLLECTION_REF
        )
      )
      OR (
        (
          X.CONFMD_LC_AMT IS NULL AND NOT Y.CONFMD_LC_AMT IS NULL
        )
        OR (
          NOT X.CONFMD_LC_AMT IS NULL AND Y.CONFMD_LC_AMT IS NULL
        )
        OR (
          X.CONFMD_LC_AMT <> Y.CONFMD_LC_AMT
        )
      )
      OR (
        (
          X.CONTRACT_DERIVED_STATUS IS NULL AND NOT Y.CONTRACT_DERIVED_STATUS IS NULL
        )
        OR (
          NOT X.CONTRACT_DERIVED_STATUS IS NULL AND Y.CONTRACT_DERIVED_STATUS IS NULL
        )
        OR (
          X.CONTRACT_DERIVED_STATUS <> Y.CONTRACT_DERIVED_STATUS
        )
      )
      OR (
        (
          X.COVERING_LETTER_DATE IS NULL AND NOT Y.COVERING_LETTER_DATE IS NULL
        )
        OR (
          NOT X.COVERING_LETTER_DATE IS NULL AND Y.COVERING_LETTER_DATE IS NULL
        )
        OR (
          X.COVERING_LETTER_DATE <> Y.COVERING_LETTER_DATE
        )
      )
      OR (
        (
          X.CR_VALUE_DATE IS NULL AND NOT Y.CR_VALUE_DATE IS NULL
        )
        OR (
          NOT X.CR_VALUE_DATE IS NULL AND Y.CR_VALUE_DATE IS NULL
        )
        OR (
          X.CR_VALUE_DATE <> Y.CR_VALUE_DATE
        )
      )
      OR (
        (
          X.CUST_MARGIN IS NULL AND NOT Y.CUST_MARGIN IS NULL
        )
        OR (
          NOT X.CUST_MARGIN IS NULL AND Y.CUST_MARGIN IS NULL
        )
        OR (
          X.CUST_MARGIN <> Y.CUST_MARGIN
        )
      )
      OR (
        (
          X.CUSTOMER_ID IS NULL AND NOT Y.CUSTOMER_ID IS NULL
        )
        OR (
          NOT X.CUSTOMER_ID IS NULL AND Y.CUSTOMER_ID IS NULL
        )
        OR (
          X.CUSTOMER_ID <> Y.CUSTOMER_ID
        )
      )
      OR (
        (
          X.CUSTOMER_PARTY_TYPE IS NULL AND NOT Y.CUSTOMER_PARTY_TYPE IS NULL
        )
        OR (
          NOT X.CUSTOMER_PARTY_TYPE IS NULL AND Y.CUSTOMER_PARTY_TYPE IS NULL
        )
        OR (
          X.CUSTOMER_PARTY_TYPE <> Y.CUSTOMER_PARTY_TYPE
        )
      )
      OR (
        (
          X.DIS_TO_COLL_FLAG IS NULL AND NOT Y.DIS_TO_COLL_FLAG IS NULL
        )
        OR (
          NOT X.DIS_TO_COLL_FLAG IS NULL AND Y.DIS_TO_COLL_FLAG IS NULL
        )
        OR (
          X.DIS_TO_COLL_FLAG <> Y.DIS_TO_COLL_FLAG
        )
      )
      OR (
        (
          X.DIS_TO_FORFAIT_FLAG IS NULL AND NOT Y.DIS_TO_FORFAIT_FLAG IS NULL
        )
        OR (
          NOT X.DIS_TO_FORFAIT_FLAG IS NULL AND Y.DIS_TO_FORFAIT_FLAG IS NULL
        )
        OR (
          X.DIS_TO_FORFAIT_FLAG <> Y.DIS_TO_FORFAIT_FLAG
        )
      )
      OR (
        (
          X.DISCOUNTING_METHOD IS NULL AND NOT Y.DISCOUNTING_METHOD IS NULL
        )
        OR (
          NOT X.DISCOUNTING_METHOD IS NULL AND Y.DISCOUNTING_METHOD IS NULL
        )
        OR (
          X.DISCOUNTING_METHOD <> Y.DISCOUNTING_METHOD
        )
      )
      OR (
        (
          X.DOC_DUP_RECV_FLAG IS NULL AND NOT Y.DOC_DUP_RECV_FLAG IS NULL
        )
        OR (
          NOT X.DOC_DUP_RECV_FLAG IS NULL AND Y.DOC_DUP_RECV_FLAG IS NULL
        )
        OR (
          X.DOC_DUP_RECV_FLAG <> Y.DOC_DUP_RECV_FLAG
        )
      )
      OR (
        (
          X.DOC_FLAG IS NULL AND NOT Y.DOC_FLAG IS NULL
        )
        OR (
          NOT X.DOC_FLAG IS NULL AND Y.DOC_FLAG IS NULL
        )
        OR (
          X.DOC_FLAG <> Y.DOC_FLAG
        )
      )
      OR (
        (
          X.DOC_ORG_RECV_FLAG IS NULL AND NOT Y.DOC_ORG_RECV_FLAG IS NULL
        )
        OR (
          NOT X.DOC_ORG_RECV_FLAG IS NULL AND Y.DOC_ORG_RECV_FLAG IS NULL
        )
        OR (
          X.DOC_ORG_RECV_FLAG <> Y.DOC_ORG_RECV_FLAG
        )
      )
      OR (
        (
          X.DR_VALUE_DATE IS NULL AND NOT Y.DR_VALUE_DATE IS NULL
        )
        OR (
          NOT X.DR_VALUE_DATE IS NULL AND Y.DR_VALUE_DATE IS NULL
        )
        OR (
          X.DR_VALUE_DATE <> Y.DR_VALUE_DATE
        )
      )
      OR (
        (
          X.EFFC_LCY_COL_EX_RATE IS NULL AND NOT Y.EFFC_LCY_COL_EX_RATE IS NULL
        )
        OR (
          NOT X.EFFC_LCY_COL_EX_RATE IS NULL AND Y.EFFC_LCY_COL_EX_RATE IS NULL
        )
        OR (
          X.EFFC_LCY_COL_EX_RATE <> Y.EFFC_LCY_COL_EX_RATE
        )
      )
      OR (
        (
          X.EFFC_LCY_PUR_XRATE IS NULL AND NOT Y.EFFC_LCY_PUR_XRATE IS NULL
        )
        OR (
          NOT X.EFFC_LCY_PUR_XRATE IS NULL AND Y.EFFC_LCY_PUR_XRATE IS NULL
        )
        OR (
          X.EFFC_LCY_PUR_XRATE <> Y.EFFC_LCY_PUR_XRATE
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
          X.FORFAITING_DOC_DATE IS NULL AND NOT Y.FORFAITING_DOC_DATE IS NULL
        )
        OR (
          NOT X.FORFAITING_DOC_DATE IS NULL AND Y.FORFAITING_DOC_DATE IS NULL
        )
        OR (
          X.FORFAITING_DOC_DATE <> Y.FORFAITING_DOC_DATE
        )
      )
      OR (
        (
          X.FORFAITING_REQ_DATE IS NULL AND NOT Y.FORFAITING_REQ_DATE IS NULL
        )
        OR (
          NOT X.FORFAITING_REQ_DATE IS NULL AND Y.FORFAITING_REQ_DATE IS NULL
        )
        OR (
          X.FORFAITING_REQ_DATE <> Y.FORFAITING_REQ_DATE
        )
      )
      OR (
        (
          X.FROM_CALC_DATE IS NULL AND NOT Y.FROM_CALC_DATE IS NULL
        )
        OR (
          NOT X.FROM_CALC_DATE IS NULL AND Y.FROM_CALC_DATE IS NULL
        )
        OR (
          X.FROM_CALC_DATE <> Y.FROM_CALC_DATE
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
          X.FURTHER_IDENTIFICATION IS NULL AND NOT Y.FURTHER_IDENTIFICATION IS NULL
        )
        OR (
          NOT X.FURTHER_IDENTIFICATION IS NULL AND Y.FURTHER_IDENTIFICATION IS NULL
        )
        OR (
          X.FURTHER_IDENTIFICATION <> Y.FURTHER_IDENTIFICATION
        )
      )
      OR (
        (
          X.GRACE_DAYS IS NULL AND NOT Y.GRACE_DAYS IS NULL
        )
        OR (
          NOT X.GRACE_DAYS IS NULL AND Y.GRACE_DAYS IS NULL
        )
        OR (
          X.GRACE_DAYS <> Y.GRACE_DAYS
        )
      )
      OR (
        (
          X.ICCF_PICKUP_CCY IS NULL AND NOT Y.ICCF_PICKUP_CCY IS NULL
        )
        OR (
          NOT X.ICCF_PICKUP_CCY IS NULL AND Y.ICCF_PICKUP_CCY IS NULL
        )
        OR (
          X.ICCF_PICKUP_CCY <> Y.ICCF_PICKUP_CCY
        )
      )
      OR (
        (
          X.INT_LIQD_MODE IS NULL AND NOT Y.INT_LIQD_MODE IS NULL
        )
        OR (
          NOT X.INT_LIQD_MODE IS NULL AND Y.INT_LIQD_MODE IS NULL
        )
        OR (
          X.INT_LIQD_MODE <> Y.INT_LIQD_MODE
        )
      )
      OR (
        (
          X.INTERNAL_REMARKS IS NULL AND NOT Y.INTERNAL_REMARKS IS NULL
        )
        OR (
          NOT X.INTERNAL_REMARKS IS NULL AND Y.INTERNAL_REMARKS IS NULL
        )
        OR (
          X.INTERNAL_REMARKS <> Y.INTERNAL_REMARKS
        )
      )
      OR (
        (
          X.IS_CHARGE_CHANGED IS NULL AND NOT Y.IS_CHARGE_CHANGED IS NULL
        )
        OR (
          NOT X.IS_CHARGE_CHANGED IS NULL AND Y.IS_CHARGE_CHANGED IS NULL
        )
        OR (
          X.IS_CHARGE_CHANGED <> Y.IS_CHARGE_CHANGED
        )
      )
      OR (
        (
          X.IS_ICCF_CHANGED IS NULL AND NOT Y.IS_ICCF_CHANGED IS NULL
        )
        OR (
          NOT X.IS_ICCF_CHANGED IS NULL AND Y.IS_ICCF_CHANGED IS NULL
        )
        OR (
          X.IS_ICCF_CHANGED <> Y.IS_ICCF_CHANGED
        )
      )
      OR (
        (
          X.IS_SETTLEMENT_CHANGED IS NULL AND NOT Y.IS_SETTLEMENT_CHANGED IS NULL
        )
        OR (
          NOT X.IS_SETTLEMENT_CHANGED IS NULL AND Y.IS_SETTLEMENT_CHANGED IS NULL
        )
        OR (
          X.IS_SETTLEMENT_CHANGED <> Y.IS_SETTLEMENT_CHANGED
        )
      )
      OR (
        (
          X.IS_TAX_CHANGED IS NULL AND NOT Y.IS_TAX_CHANGED IS NULL
        )
        OR (
          NOT X.IS_TAX_CHANGED IS NULL AND Y.IS_TAX_CHANGED IS NULL
        )
        OR (
          X.IS_TAX_CHANGED <> Y.IS_TAX_CHANGED
        )
      )
      OR (
        (
          X.LC_AMT IS NULL AND NOT Y.LC_AMT IS NULL
        )
        OR (
          NOT X.LC_AMT IS NULL AND Y.LC_AMT IS NULL
        )
        OR (
          X.LC_AMT <> Y.LC_AMT
        )
      )
      OR (
        (
          X.LC_FLAG IS NULL AND NOT Y.LC_FLAG IS NULL
        )
        OR (
          NOT X.LC_FLAG IS NULL AND Y.LC_FLAG IS NULL
        )
        OR (
          X.LC_FLAG <> Y.LC_FLAG
        )
      )
      OR (
        (
          X.LC_ISSUE_DATE IS NULL AND NOT Y.LC_ISSUE_DATE IS NULL
        )
        OR (
          NOT X.LC_ISSUE_DATE IS NULL AND Y.LC_ISSUE_DATE IS NULL
        )
        OR (
          X.LC_ISSUE_DATE <> Y.LC_ISSUE_DATE
        )
      )
      OR (
        (
          X.LC_LIAB_AMT IS NULL AND NOT Y.LC_LIAB_AMT IS NULL
        )
        OR (
          NOT X.LC_LIAB_AMT IS NULL AND Y.LC_LIAB_AMT IS NULL
        )
        OR (
          X.LC_LIAB_AMT <> Y.LC_LIAB_AMT
        )
      )
      OR (
        (
          X.LC_NOT_ADVISED IS NULL AND NOT Y.LC_NOT_ADVISED IS NULL
        )
        OR (
          NOT X.LC_NOT_ADVISED IS NULL AND Y.LC_NOT_ADVISED IS NULL
        )
        OR (
          X.LC_NOT_ADVISED <> Y.LC_NOT_ADVISED
        )
      )
      OR (
        (
          X.LIMITS_MONITORING_FLAG IS NULL AND NOT Y.LIMITS_MONITORING_FLAG IS NULL
        )
        OR (
          NOT X.LIMITS_MONITORING_FLAG IS NULL AND Y.LIMITS_MONITORING_FLAG IS NULL
        )
        OR (
          X.LIMITS_MONITORING_FLAG <> Y.LIMITS_MONITORING_FLAG
        )
      )
      OR (
        (
          X.LINKED_TO_LOAN IS NULL AND NOT Y.LINKED_TO_LOAN IS NULL
        )
        OR (
          NOT X.LINKED_TO_LOAN IS NULL AND Y.LINKED_TO_LOAN IS NULL
        )
        OR (
          X.LINKED_TO_LOAN <> Y.LINKED_TO_LOAN
        )
      )
      OR (
        (
          X.LIQD_USING_COLLAT IS NULL AND NOT Y.LIQD_USING_COLLAT IS NULL
        )
        OR (
          NOT X.LIQD_USING_COLLAT IS NULL AND Y.LIQD_USING_COLLAT IS NULL
        )
        OR (
          X.LIQD_USING_COLLAT <> Y.LIQD_USING_COLLAT
        )
      )
      OR (
        (
          X.LIQUIDATION_DATE IS NULL AND NOT Y.LIQUIDATION_DATE IS NULL
        )
        OR (
          NOT X.LIQUIDATION_DATE IS NULL AND Y.LIQUIDATION_DATE IS NULL
        )
        OR (
          X.LIQUIDATION_DATE <> Y.LIQUIDATION_DATE
        )
      )
      OR (
        (
          X.LOAN_REF_NO IS NULL AND NOT Y.LOAN_REF_NO IS NULL
        )
        OR (
          NOT X.LOAN_REF_NO IS NULL AND Y.LOAN_REF_NO IS NULL
        )
        OR (
          X.LOAN_REF_NO <> Y.LOAN_REF_NO
        )
      )
      OR (
        (
          X.MARGIN_AMT IS NULL AND NOT Y.MARGIN_AMT IS NULL
        )
        OR (
          NOT X.MARGIN_AMT IS NULL AND Y.MARGIN_AMT IS NULL
        )
        OR (
          X.MARGIN_AMT <> Y.MARGIN_AMT
        )
      )
      OR (
        (
          X.MARGIN_AMT_LIQD IS NULL AND NOT Y.MARGIN_AMT_LIQD IS NULL
        )
        OR (
          NOT X.MARGIN_AMT_LIQD IS NULL AND Y.MARGIN_AMT_LIQD IS NULL
        )
        OR (
          X.MARGIN_AMT_LIQD <> Y.MARGIN_AMT_LIQD
        )
      )
      OR (
        (
          X.MARGIN_DUE_AMT IS NULL AND NOT Y.MARGIN_DUE_AMT IS NULL
        )
        OR (
          NOT X.MARGIN_DUE_AMT IS NULL AND Y.MARGIN_DUE_AMT IS NULL
        )
        OR (
          X.MARGIN_DUE_AMT <> Y.MARGIN_DUE_AMT
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
          X.MSG_PROC_FLAG IS NULL AND NOT Y.MSG_PROC_FLAG IS NULL
        )
        OR (
          NOT X.MSG_PROC_FLAG IS NULL AND Y.MSG_PROC_FLAG IS NULL
        )
        OR (
          X.MSG_PROC_FLAG <> Y.MSG_PROC_FLAG
        )
      )
      OR (
        (
          X.OPERATION IS NULL AND NOT Y.OPERATION IS NULL
        )
        OR (
          NOT X.OPERATION IS NULL AND Y.OPERATION IS NULL
        )
        OR (
          X.OPERATION <> Y.OPERATION
        )
      )
      OR (
        (
          X.OUR_CHARGES_REFUSED IS NULL AND NOT Y.OUR_CHARGES_REFUSED IS NULL
        )
        OR (
          NOT X.OUR_CHARGES_REFUSED IS NULL AND Y.OUR_CHARGES_REFUSED IS NULL
        )
        OR (
          X.OUR_CHARGES_REFUSED <> Y.OUR_CHARGES_REFUSED
        )
      )
      OR (
        (
          X.OUR_LC_REF IS NULL AND NOT Y.OUR_LC_REF IS NULL
        )
        OR (
          NOT X.OUR_LC_REF IS NULL AND Y.OUR_LC_REF IS NULL
        )
        OR (
          X.OUR_LC_REF <> Y.OUR_LC_REF
        )
      )
      OR (
        (
          X.OURLC_APPLICANT IS NULL AND NOT Y.OURLC_APPLICANT IS NULL
        )
        OR (
          NOT X.OURLC_APPLICANT IS NULL AND Y.OURLC_APPLICANT IS NULL
        )
        OR (
          X.OURLC_APPLICANT <> Y.OURLC_APPLICANT
        )
      )
      OR (
        (
          X.OVD_OVERDRAFT_FLAG IS NULL AND NOT Y.OVD_OVERDRAFT_FLAG IS NULL
        )
        OR (
          NOT X.OVD_OVERDRAFT_FLAG IS NULL AND Y.OVD_OVERDRAFT_FLAG IS NULL
        )
        OR (
          X.OVD_OVERDRAFT_FLAG <> Y.OVD_OVERDRAFT_FLAG
        )
      )
      OR (
        (
          X.PASS_INTEREST_TO_THEM IS NULL AND NOT Y.PASS_INTEREST_TO_THEM IS NULL
        )
        OR (
          NOT X.PASS_INTEREST_TO_THEM IS NULL AND Y.PASS_INTEREST_TO_THEM IS NULL
        )
        OR (
          X.PASS_INTEREST_TO_THEM <> Y.PASS_INTEREST_TO_THEM
        )
      )
      OR (
        (
          X.PASS_OUR_CHG_TO_THEM IS NULL AND NOT Y.PASS_OUR_CHG_TO_THEM IS NULL
        )
        OR (
          NOT X.PASS_OUR_CHG_TO_THEM IS NULL AND Y.PASS_OUR_CHG_TO_THEM IS NULL
        )
        OR (
          X.PASS_OUR_CHG_TO_THEM <> Y.PASS_OUR_CHG_TO_THEM
        )
      )
      OR (
        (
          X.PRODUCT_TYPE IS NULL AND NOT Y.PRODUCT_TYPE IS NULL
        )
        OR (
          NOT X.PRODUCT_TYPE IS NULL AND Y.PRODUCT_TYPE IS NULL
        )
        OR (
          X.PRODUCT_TYPE <> Y.PRODUCT_TYPE
        )
      )
      OR (
        (
          X.PUR_TO_COLL_FLAG IS NULL AND NOT Y.PUR_TO_COLL_FLAG IS NULL
        )
        OR (
          NOT X.PUR_TO_COLL_FLAG IS NULL AND Y.PUR_TO_COLL_FLAG IS NULL
        )
        OR (
          X.PUR_TO_COLL_FLAG <> Y.PUR_TO_COLL_FLAG
        )
      )
      OR (
        (
          X.PURCHASE_AMT IS NULL AND NOT Y.PURCHASE_AMT IS NULL
        )
        OR (
          NOT X.PURCHASE_AMT IS NULL AND Y.PURCHASE_AMT IS NULL
        )
        OR (
          X.PURCHASE_AMT <> Y.PURCHASE_AMT
        )
      )
      OR (
        (
          X.REBATE_AMT IS NULL AND NOT Y.REBATE_AMT IS NULL
        )
        OR (
          NOT X.REBATE_AMT IS NULL AND Y.REBATE_AMT IS NULL
        )
        OR (
          X.REBATE_AMT <> Y.REBATE_AMT
        )
      )
      OR (
        (
          X.REDISCOUNT_FLAG IS NULL AND NOT Y.REDISCOUNT_FLAG IS NULL
        )
        OR (
          NOT X.REDISCOUNT_FLAG IS NULL AND Y.REDISCOUNT_FLAG IS NULL
        )
        OR (
          X.REDISCOUNT_FLAG <> Y.REDISCOUNT_FLAG
        )
      )
      OR (
        (
          X.REFUND_INT_CODE IS NULL AND NOT Y.REFUND_INT_CODE IS NULL
        )
        OR (
          NOT X.REFUND_INT_CODE IS NULL AND Y.REFUND_INT_CODE IS NULL
        )
        OR (
          X.REFUND_INT_CODE <> Y.REFUND_INT_CODE
        )
      )
      OR (
        (
          X.REFUND_INT_RATE IS NULL AND NOT Y.REFUND_INT_RATE IS NULL
        )
        OR (
          NOT X.REFUND_INT_RATE IS NULL AND Y.REFUND_INT_RATE IS NULL
        )
        OR (
          X.REFUND_INT_RATE <> Y.REFUND_INT_RATE
        )
      )
      OR (
        (
          X.REFUND_INTEREST IS NULL AND NOT Y.REFUND_INTEREST IS NULL
        )
        OR (
          NOT X.REFUND_INTEREST IS NULL AND Y.REFUND_INTEREST IS NULL
        )
        OR (
          X.REFUND_INTEREST <> Y.REFUND_INTEREST
        )
      )
      OR (
        (
          X.REIMBURSEMENT_DAYS IS NULL AND NOT Y.REIMBURSEMENT_DAYS IS NULL
        )
        OR (
          NOT X.REIMBURSEMENT_DAYS IS NULL AND Y.REIMBURSEMENT_DAYS IS NULL
        )
        OR (
          X.REIMBURSEMENT_DAYS <> Y.REIMBURSEMENT_DAYS
        )
      )
      OR (
        (
          X.ROLLOVER_FLAG IS NULL AND NOT Y.ROLLOVER_FLAG IS NULL
        )
        OR (
          NOT X.ROLLOVER_FLAG IS NULL AND Y.ROLLOVER_FLAG IS NULL
        )
        OR (
          X.ROLLOVER_FLAG <> Y.ROLLOVER_FLAG
        )
      )
      OR (
        (
          X.ROLLOVER_REF_NO IS NULL AND NOT Y.ROLLOVER_REF_NO IS NULL
        )
        OR (
          NOT X.ROLLOVER_REF_NO IS NULL AND Y.ROLLOVER_REF_NO IS NULL
        )
        OR (
          X.ROLLOVER_REF_NO <> Y.ROLLOVER_REF_NO
        )
      )
      OR (
        (
          X.SETTLE_AVAILABLE_AMT IS NULL AND NOT Y.SETTLE_AVAILABLE_AMT IS NULL
        )
        OR (
          NOT X.SETTLE_AVAILABLE_AMT IS NULL AND Y.SETTLE_AVAILABLE_AMT IS NULL
        )
        OR (
          X.SETTLE_AVAILABLE_AMT <> Y.SETTLE_AVAILABLE_AMT
        )
      )
      OR (
        (
          X.SK_CUSTOMER_ID IS NULL AND NOT Y.SK_CUSTOMER_ID IS NULL
        )
        OR (
          NOT X.SK_CUSTOMER_ID IS NULL AND Y.SK_CUSTOMER_ID IS NULL
        )
        OR (
          X.SK_CUSTOMER_ID <> Y.SK_CUSTOMER_ID
        )
      )
      OR (
        (
          X.SK_BCREFNO IS NULL AND NOT Y.SK_BCREFNO IS NULL
        )
        OR (
          NOT X.SK_BCREFNO IS NULL AND Y.SK_BCREFNO IS NULL
        )
        OR (
          X.SK_BCREFNO <> Y.SK_BCREFNO
        )
      )
      OR (
        (
          X.SK_OUR_LC_REF IS NULL AND NOT Y.SK_OUR_LC_REF IS NULL
        )
        OR (
          NOT X.SK_OUR_LC_REF IS NULL AND Y.SK_OUR_LC_REF IS NULL
        )
        OR (
          X.SK_OUR_LC_REF <> Y.SK_OUR_LC_REF
        )
      )
      OR (
        (
          X.SK_TENOR_CODE IS NULL AND NOT Y.SK_TENOR_CODE IS NULL
        )
        OR (
          NOT X.SK_TENOR_CODE IS NULL AND Y.SK_TENOR_CODE IS NULL
        )
        OR (
          X.SK_TENOR_CODE <> Y.SK_TENOR_CODE
        )
      )
      OR (
        (
          X.STAGE IS NULL AND NOT Y.STAGE IS NULL
        )
        OR (
          NOT X.STAGE IS NULL AND Y.STAGE IS NULL
        )
        OR (
          X.STAGE <> Y.STAGE
        )
      )
      OR (
        (
          X.STAGE_ASOF_DATE IS NULL AND NOT Y.STAGE_ASOF_DATE IS NULL
        )
        OR (
          NOT X.STAGE_ASOF_DATE IS NULL AND Y.STAGE_ASOF_DATE IS NULL
        )
        OR (
          X.STAGE_ASOF_DATE <> Y.STAGE_ASOF_DATE
        )
      )
      OR (
        (
          X.STATUS_ASOF_DATE IS NULL AND NOT Y.STATUS_ASOF_DATE IS NULL
        )
        OR (
          NOT X.STATUS_ASOF_DATE IS NULL AND Y.STATUS_ASOF_DATE IS NULL
        )
        OR (
          X.STATUS_ASOF_DATE <> Y.STATUS_ASOF_DATE
        )
      )
      OR (
        (
          X.STATUS_CHK_REQD IS NULL AND NOT Y.STATUS_CHK_REQD IS NULL
        )
        OR (
          NOT X.STATUS_CHK_REQD IS NULL AND Y.STATUS_CHK_REQD IS NULL
        )
        OR (
          X.STATUS_CHK_REQD <> Y.STATUS_CHK_REQD
        )
      )
      OR (
        (
          X.STATUS_CONTROL_FLAG IS NULL AND NOT Y.STATUS_CONTROL_FLAG IS NULL
        )
        OR (
          NOT X.STATUS_CONTROL_FLAG IS NULL AND Y.STATUS_CONTROL_FLAG IS NULL
        )
        OR (
          X.STATUS_CONTROL_FLAG <> Y.STATUS_CONTROL_FLAG
        )
      )
      OR (
        (
          X.SUBSYSTEM_STAT IS NULL AND NOT Y.SUBSYSTEM_STAT IS NULL
        )
        OR (
          NOT X.SUBSYSTEM_STAT IS NULL AND Y.SUBSYSTEM_STAT IS NULL
        )
        OR (
          X.SUBSYSTEM_STAT <> Y.SUBSYSTEM_STAT
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
      OR (
        (
          X.TENOR_DAYS IS NULL AND NOT Y.TENOR_DAYS IS NULL
        )
        OR (
          NOT X.TENOR_DAYS IS NULL AND Y.TENOR_DAYS IS NULL
        )
        OR (
          X.TENOR_DAYS <> Y.TENOR_DAYS
        )
      )
      OR (
        (
          X.TFR_COLLATERAL_FROM_LC IS NULL AND NOT Y.TFR_COLLATERAL_FROM_LC IS NULL
        )
        OR (
          NOT X.TFR_COLLATERAL_FROM_LC IS NULL AND Y.TFR_COLLATERAL_FROM_LC IS NULL
        )
        OR (
          X.TFR_COLLATERAL_FROM_LC <> Y.TFR_COLLATERAL_FROM_LC
        )
      )
      OR (
        (
          X.THEIR_CHARGES_REFUSED IS NULL AND NOT Y.THEIR_CHARGES_REFUSED IS NULL
        )
        OR (
          NOT X.THEIR_CHARGES_REFUSED IS NULL AND Y.THEIR_CHARGES_REFUSED IS NULL
        )
        OR (
          X.THEIR_CHARGES_REFUSED <> Y.THEIR_CHARGES_REFUSED
        )
      )
      OR (
        (
          X.THEIR_CHG_AMT IS NULL AND NOT Y.THEIR_CHG_AMT IS NULL
        )
        OR (
          NOT X.THEIR_CHG_AMT IS NULL AND Y.THEIR_CHG_AMT IS NULL
        )
        OR (
          X.THEIR_CHG_AMT <> Y.THEIR_CHG_AMT
        )
      )
      OR (
        (
          X.THEIR_CHG_CCY IS NULL AND NOT Y.THEIR_CHG_CCY IS NULL
        )
        OR (
          NOT X.THEIR_CHG_CCY IS NULL AND Y.THEIR_CHG_CCY IS NULL
        )
        OR (
          X.THEIR_CHG_CCY <> Y.THEIR_CHG_CCY
        )
      )
      OR (
        (
          X.THEIR_LC_REF IS NULL AND NOT Y.THEIR_LC_REF IS NULL
        )
        OR (
          NOT X.THEIR_LC_REF IS NULL AND Y.THEIR_LC_REF IS NULL
        )
        OR (
          X.THEIR_LC_REF <> Y.THEIR_LC_REF
        )
      )
      OR (
        (
          X.TO_CALC_DATE IS NULL AND NOT Y.TO_CALC_DATE IS NULL
        )
        OR (
          NOT X.TO_CALC_DATE IS NULL AND Y.TO_CALC_DATE IS NULL
        )
        OR (
          X.TO_CALC_DATE <> Y.TO_CALC_DATE
        )
      )
      OR (
        (
          X.TRANSFERRED_COLLATERAL_AMT IS NULL AND NOT Y.TRANSFERRED_COLLATERAL_AMT IS NULL
        )
        OR (
          NOT X.TRANSFERRED_COLLATERAL_AMT IS NULL AND Y.TRANSFERRED_COLLATERAL_AMT IS NULL
        )
        OR (
          X.TRANSFERRED_COLLATERAL_AMT <> Y.TRANSFERRED_COLLATERAL_AMT
        )
      )
      OR (
        (
          X.TRANSIT_DAYS IS NULL AND NOT Y.TRANSIT_DAYS IS NULL
        )
        OR (
          NOT X.TRANSIT_DAYS IS NULL AND Y.TRANSIT_DAYS IS NULL
        )
        OR (
          X.TRANSIT_DAYS <> Y.TRANSIT_DAYS
        )
      )
      OR (
        (
          X.TXN_DATE IS NULL AND NOT Y.TXN_DATE IS NULL
        )
        OR (
          NOT X.TXN_DATE IS NULL AND Y.TXN_DATE IS NULL
        )
        OR (
          X.TXN_DATE <> Y.TXN_DATE
        )
      )
      OR (
        (
          X.ULTI_ROLL_PARENT IS NULL AND NOT Y.ULTI_ROLL_PARENT IS NULL
        )
        OR (
          NOT X.ULTI_ROLL_PARENT IS NULL AND Y.ULTI_ROLL_PARENT IS NULL
        )
        OR (
          X.ULTI_ROLL_PARENT <> Y.ULTI_ROLL_PARENT
        )
      )
      OR (
        (
          X.UNLINKED_FX_RATE IS NULL AND NOT Y.UNLINKED_FX_RATE IS NULL
        )
        OR (
          NOT X.UNLINKED_FX_RATE IS NULL AND Y.UNLINKED_FX_RATE IS NULL
        )
        OR (
          X.UNLINKED_FX_RATE <> Y.UNLINKED_FX_RATE
        )
      )
      OR (
        (
          X.USE_LCREF_IN_MSG IS NULL AND NOT Y.USE_LCREF_IN_MSG IS NULL
        )
        OR (
          NOT X.USE_LCREF_IN_MSG IS NULL AND Y.USE_LCREF_IN_MSG IS NULL
        )
        OR (
          X.USE_LCREF_IN_MSG <> Y.USE_LCREF_IN_MSG
        )
      )
      OR (
        (
          X.USER_DEFINED_STATUS IS NULL AND NOT Y.USER_DEFINED_STATUS IS NULL
        )
        OR (
          NOT X.USER_DEFINED_STATUS IS NULL AND Y.USER_DEFINED_STATUS IS NULL
        )
        OR (
          X.USER_DEFINED_STATUS <> Y.USER_DEFINED_STATUS
        )
      )
      OR (
        (
          X.VALUE_DATE IS NULL AND NOT Y.VALUE_DATE IS NULL
        )
        OR (
          NOT X.VALUE_DATE IS NULL AND Y.VALUE_DATE IS NULL
        )
        OR (
          X.VALUE_DATE <> Y.VALUE_DATE
        )
      )
      OR (
        (
          X.VERIFY_FUNDS IS NULL AND NOT Y.VERIFY_FUNDS IS NULL
        )
        OR (
          NOT X.VERIFY_FUNDS IS NULL AND Y.VERIFY_FUNDS IS NULL
        )
        OR (
          X.VERIFY_FUNDS <> Y.VERIFY_FUNDS
        )
      )
      OR (
        (
          X.VERSION_NO IS NULL AND NOT Y.VERSION_NO IS NULL
        )
        OR (
          NOT X.VERSION_NO IS NULL AND Y.VERSION_NO IS NULL
        )
        OR (
          X.VERSION_NO <> Y.VERSION_NO
        )
      )
      OR (
        (
          X.WAIVE_END_DATE IS NULL AND NOT Y.WAIVE_END_DATE IS NULL
        )
        OR (
          NOT X.WAIVE_END_DATE IS NULL AND Y.WAIVE_END_DATE IS NULL
        )
        OR (
          X.WAIVE_END_DATE <> Y.WAIVE_END_DATE
        )
      )
    )
    THEN 'UPD'
    WHEN X.BCREFNO IS NULL AND X.EVENT_SEQ_NO IS NULL
    THEN 'DEL' /* - IF MULTIPLE PKs are there then concatenate them with AND */
  END AS OPT_FLAG
FROM DEVT_STG_FLX.BCTB_CONTRACT_MASTER AS X
FULL JOIN DEVT_STG_FLX.BCTB_CONTRACT_MASTER_BACKUP AS Y
  ON X.BCREFNO = Y.BCREFNO
  AND X.EVENT_SEQ_NO = Y.EVENT_SEQ_NO /* - IF MULTIPLE PKs are there then concatenate them with AND */
WHERE
  NOT OPT_FLAG IS NULL