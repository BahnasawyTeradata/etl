CREATE OR REPLACE VIEW DEVV_STG_FLX.LCTB_CONTRACT_MASTER_get_delta AS LOCKING ROW FOR ACCESS
SELECT
  COALESCE(X.CONTRACT_REF_NO, Y.CONTRACT_REF_NO) AS CONTRACT_REF_NO,
  COALESCE(X.EVENT_SEQ_NO, Y.EVENT_SEQ_NO) AS EVENT_SEQ_NO,
  X.ACCOUNT_FOR_ISB,
  X.ACKN_DATE,
  X.ACKN_RECVD,
  X.ADDITIONAL_AMTS_COVERED,
  X.ADVICE_OF_REDUCTION,
  X.ALLOW_PREPAY,
  X.AMENDMENT_DATE,
  X.AMENDMENT_NO,
  X.ANCILLARY_MESG,
  X.ANCILLARY_MESG_FUNCTION,
  X.APPLICABLE_RULE,
  X.AUTO_EXTN_PERIOD,
  X.AVAILED_UNDERTAKING_AMOUNT,
  X.BACK_TO_BACK_LC,
  X.BENEFICIARY_CONF_REQD,
  X.BM_CLOSURE_TYPE,
  X.BM_CONTRACT_CCY,
  X.BM_CUST_TYPE,
  X.BM_EVENT_CODE_LC,
  X.BM_EXPIRY_PLACE,
  X.BM_LC_LANG_CODE,
  X.BM_MAY_CONFIRM,
  X.BM_OPERATION_CODE,
  X.BM_TENOR,
  X.BM_TRACK_LIMIT,
  X.CHARGE_AMT_ISB,
  X.CHARGE_CCY_ISB,
  X.CHARGES_FROM_BEN,
  X.CHARGES_FROM_ISB,
  X.CHG_CLM_SWIFT,
  X.CHGCLM_TEMPLATE_ID,
  X.CIF_ID,
  X.CLAIM_DATE,
  X.CLAIM_EXPIRY_DATE,
  X.CLAIM_INDICATOR,
  X.CLOSURE_DATE,
  X.CLOSURE_TYPE,
  X.COLLAT_LOAN_ALLOWED,
  X.COLLATERAL_PCT_OBP,
  X.CONFIRM_AMOUNT,
  X.CONFIRM_PERCENT,
  X.CONTRACT_AMT,
  X.CONTRACT_CCY,
  X.CONTRACT_DERIVED_STATUS,
  X.CREDIT_AVL_WITH,
  X.CREDIT_LINE,
  X.CUMULATIVE,
  X.CUST_NAME,
  X.CUST_REF_DATE,
  X.CUST_TYPE,
  X.DEAL_SOURCE,
  X.EFFECTIVE_DATE,
  X.EVENT_CODE,
  X.EXPIRY_CONDITION,
  X.EXPIRY_DATE,
  X.EXPIRY_PLACE,
  X.EXT_REF_NO,
  X.EXTN_DETAILS,
  X.FIN_AMND_NO,
  X.FIN_FLAG,
  X.FINAL_EXPIRY_DATE,
  X.FREQUENCY,
  X.FUND_ID,
  X.GUARANTEE_TYPE,
  X.GUARANTEE_TYPE_DESC,
  X.INCLUDE_TO_DATE,
  X.INCO_TERM,
  X.SK_CIF_ID,
  X.ISB_DATE,
  X.ISSUE_DATE,
  X.ISSUE_REQUEST,
  X.LC_LANG_CODE,
  X.LIAB_TOLERANCE,
  X.LIMITS_TRACKING_TENOR_TYPE,
  X.LINE_CIF_ID,
  X.LINE_PARTY_TYPE,
  X.LOCAL_GUA_APPL_RULE,
  X.LOCAL_GUA_AUTO_EXTN_PRD,
  X.LOCAL_GUA_CHG_FROM_BEN,
  X.LOCAL_GUA_CLAIM_IND,
  X.LOCAL_GUA_CONTRACT_AMT,
  X.LOCAL_GUA_CONTRACT_CCY,
  X.LOCAL_GUA_CRD_AVL_WITH,
  X.LOCAL_GUA_EXP_COND,
  X.LOCAL_GUA_EXP_TYPE,
  X.LOCAL_GUA_EXPIRY_DATE,
  X.LOCAL_GUA_EXTN_DETAILS,
  X.LOCAL_GUA_FINAL_EXP_DATE,
  X.LOCAL_GUA_FOU,
  X.LOCAL_GUA_ISSUE_DATE,
  X.LOCAL_GUA_NOTIF_DETAILS,
  X.LOCAL_GUA_NOTIF_PRD,
  X.LOCAL_GUA_RULE_NARRAT,
  X.LOCAL_GUA_SUPP_INFO_AMT,
  X.LOCAL_GUA_TRANSFERABLE,
  X.LOCAL_GUA_TRNF_COND,
  X.MAX_CONTRACT_AMT,
  X.MAX_LIABILITY_AMT,
  X.MAY_CONFIRM,
  X.NEGATIVE_TOLERANCE,
  X.NEXT_REINSTATEMENT_DATE,
  X.NON_EXT_NOTIF_DETAILS,
  X.NOTIFICATION_PERIOD,
  X.OPERATION_CODE,
  X.OS_LC_AMOUNT,
  X.PARTIAL_CLOSURE,
  X.PARTIAL_CONFIRMATION_ALLOW,
  X.PAYMENT_DETAILS,
  X.PERIOD_FOR_PRESENTATION,
  X.POSITIVE_TOLERANCE,
  X.PRE_ADV_DATE,
  X.PRESENTATION_NARRATIVE,
  X.PRODUCT_CODE,
  X.REIMBURSEMENT_TYPE,
  X.REINSTATEMENT_TYPE,
  X.RELATED_LC_REF_NO,
  X.REMARKS,
  X.REQ_CONFIRM_PARTY,
  X.REVOLVES_IN,
  X.RULE_NARRATIVE,
  X.SETTLEMENT_METHOD,
  X.SETTLEMENT_TYPE,
  X.SK_LINE_CIF_ID,
  X.SK_CONTRACT_REF_NO,
  X.SK_CONTRACT_REF_NO_EVENT_SEQ_NO,
  X.SK_CREDIT_AVL_WITH,
  X.SK_CUST_NAME,
  X.SK_PAYMENT_DETAILS,
  X.SK_PRODUCT_CODE,
  X.SK_REMARKS,
  X.SK_TENOR,
  X.STANDARD_WORD_LANG,
  X.STANDARD_WORD_REQD,
  X.STATUS_CONTROL_FLAG,
  X.STOP_DATE,
  X.SUBSYSTEMSTAT,
  X.SUPP_INFO_AMT,
  X.TENOR,
  X.TOLERANCE_TEXT,
  X.TRACK_LIMIT,
  X.TRANSFER_CONDITIONS,
  X.TRANSFERABLE,
  X.UNCONFIRM_AMOUNT,
  X.UNDERTAKING_AMOUNT,
  X.UNDERTAKING_EXPIRY_DATE,
  X.UNITS,
  X.URR_PREFERENCE,
  X.USER_DEFINED_STATUS,
  X.USER_LC_REF_NO,
  X.VALIDITY_TYPE,
  X.VERSION_NO,
  X.CUST_REF_NO,
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
    WHEN Y.CONTRACT_REF_NO IS NULL AND Y.EVENT_SEQ_NO IS NULL
    THEN 'INS' /* - IF MULTIPLE PKs are there then concatenate them with AND */
    WHEN X.CONTRACT_REF_NO = Y.CONTRACT_REF_NO
    AND X.EVENT_SEQ_NO = Y.EVENT_SEQ_NO /* - IF MULTIPLE PKs are there then concatenate them with AND */
    AND (
      (
        (
          X.ACCOUNT_FOR_ISB IS NULL AND NOT Y.ACCOUNT_FOR_ISB IS NULL
        )
        OR (
          NOT X.ACCOUNT_FOR_ISB IS NULL AND Y.ACCOUNT_FOR_ISB IS NULL
        )
        OR (
          X.ACCOUNT_FOR_ISB <> Y.ACCOUNT_FOR_ISB
        )
      )
      OR (
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
          X.ADDITIONAL_AMTS_COVERED IS NULL AND NOT Y.ADDITIONAL_AMTS_COVERED IS NULL
        )
        OR (
          NOT X.ADDITIONAL_AMTS_COVERED IS NULL AND Y.ADDITIONAL_AMTS_COVERED IS NULL
        )
        OR (
          X.ADDITIONAL_AMTS_COVERED <> Y.ADDITIONAL_AMTS_COVERED
        )
      )
      OR (
        (
          X.ADVICE_OF_REDUCTION IS NULL AND NOT Y.ADVICE_OF_REDUCTION IS NULL
        )
        OR (
          NOT X.ADVICE_OF_REDUCTION IS NULL AND Y.ADVICE_OF_REDUCTION IS NULL
        )
        OR (
          X.ADVICE_OF_REDUCTION <> Y.ADVICE_OF_REDUCTION
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
          X.AMENDMENT_DATE IS NULL AND NOT Y.AMENDMENT_DATE IS NULL
        )
        OR (
          NOT X.AMENDMENT_DATE IS NULL AND Y.AMENDMENT_DATE IS NULL
        )
        OR (
          X.AMENDMENT_DATE <> Y.AMENDMENT_DATE
        )
      )
      OR (
        (
          X.AMENDMENT_NO IS NULL AND NOT Y.AMENDMENT_NO IS NULL
        )
        OR (
          NOT X.AMENDMENT_NO IS NULL AND Y.AMENDMENT_NO IS NULL
        )
        OR (
          X.AMENDMENT_NO <> Y.AMENDMENT_NO
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
          X.APPLICABLE_RULE IS NULL AND NOT Y.APPLICABLE_RULE IS NULL
        )
        OR (
          NOT X.APPLICABLE_RULE IS NULL AND Y.APPLICABLE_RULE IS NULL
        )
        OR (
          X.APPLICABLE_RULE <> Y.APPLICABLE_RULE
        )
      )
      OR (
        (
          X.AUTO_EXTN_PERIOD IS NULL AND NOT Y.AUTO_EXTN_PERIOD IS NULL
        )
        OR (
          NOT X.AUTO_EXTN_PERIOD IS NULL AND Y.AUTO_EXTN_PERIOD IS NULL
        )
        OR (
          X.AUTO_EXTN_PERIOD <> Y.AUTO_EXTN_PERIOD
        )
      )
      OR (
        (
          X.AVAILED_UNDERTAKING_AMOUNT IS NULL AND NOT Y.AVAILED_UNDERTAKING_AMOUNT IS NULL
        )
        OR (
          NOT X.AVAILED_UNDERTAKING_AMOUNT IS NULL AND Y.AVAILED_UNDERTAKING_AMOUNT IS NULL
        )
        OR (
          X.AVAILED_UNDERTAKING_AMOUNT <> Y.AVAILED_UNDERTAKING_AMOUNT
        )
      )
      OR (
        (
          X.BACK_TO_BACK_LC IS NULL AND NOT Y.BACK_TO_BACK_LC IS NULL
        )
        OR (
          NOT X.BACK_TO_BACK_LC IS NULL AND Y.BACK_TO_BACK_LC IS NULL
        )
        OR (
          X.BACK_TO_BACK_LC <> Y.BACK_TO_BACK_LC
        )
      )
      OR (
        (
          X.BENEFICIARY_CONF_REQD IS NULL AND NOT Y.BENEFICIARY_CONF_REQD IS NULL
        )
        OR (
          NOT X.BENEFICIARY_CONF_REQD IS NULL AND Y.BENEFICIARY_CONF_REQD IS NULL
        )
        OR (
          X.BENEFICIARY_CONF_REQD <> Y.BENEFICIARY_CONF_REQD
        )
      )
      OR (
        (
          X.BM_CLOSURE_TYPE IS NULL AND NOT Y.BM_CLOSURE_TYPE IS NULL
        )
        OR (
          NOT X.BM_CLOSURE_TYPE IS NULL AND Y.BM_CLOSURE_TYPE IS NULL
        )
        OR (
          X.BM_CLOSURE_TYPE <> Y.BM_CLOSURE_TYPE
        )
      )
      OR (
        (
          X.BM_CONTRACT_CCY IS NULL AND NOT Y.BM_CONTRACT_CCY IS NULL
        )
        OR (
          NOT X.BM_CONTRACT_CCY IS NULL AND Y.BM_CONTRACT_CCY IS NULL
        )
        OR (
          X.BM_CONTRACT_CCY <> Y.BM_CONTRACT_CCY
        )
      )
      OR (
        (
          X.BM_CUST_TYPE IS NULL AND NOT Y.BM_CUST_TYPE IS NULL
        )
        OR (
          NOT X.BM_CUST_TYPE IS NULL AND Y.BM_CUST_TYPE IS NULL
        )
        OR (
          X.BM_CUST_TYPE <> Y.BM_CUST_TYPE
        )
      )
      OR (
        (
          X.BM_EVENT_CODE_LC IS NULL AND NOT Y.BM_EVENT_CODE_LC IS NULL
        )
        OR (
          NOT X.BM_EVENT_CODE_LC IS NULL AND Y.BM_EVENT_CODE_LC IS NULL
        )
        OR (
          X.BM_EVENT_CODE_LC <> Y.BM_EVENT_CODE_LC
        )
      )
      OR (
        (
          X.BM_EXPIRY_PLACE IS NULL AND NOT Y.BM_EXPIRY_PLACE IS NULL
        )
        OR (
          NOT X.BM_EXPIRY_PLACE IS NULL AND Y.BM_EXPIRY_PLACE IS NULL
        )
        OR (
          X.BM_EXPIRY_PLACE <> Y.BM_EXPIRY_PLACE
        )
      )
      OR (
        (
          X.BM_LC_LANG_CODE IS NULL AND NOT Y.BM_LC_LANG_CODE IS NULL
        )
        OR (
          NOT X.BM_LC_LANG_CODE IS NULL AND Y.BM_LC_LANG_CODE IS NULL
        )
        OR (
          X.BM_LC_LANG_CODE <> Y.BM_LC_LANG_CODE
        )
      )
      OR (
        (
          X.BM_MAY_CONFIRM IS NULL AND NOT Y.BM_MAY_CONFIRM IS NULL
        )
        OR (
          NOT X.BM_MAY_CONFIRM IS NULL AND Y.BM_MAY_CONFIRM IS NULL
        )
        OR (
          X.BM_MAY_CONFIRM <> Y.BM_MAY_CONFIRM
        )
      )
      OR (
        (
          X.BM_OPERATION_CODE IS NULL AND NOT Y.BM_OPERATION_CODE IS NULL
        )
        OR (
          NOT X.BM_OPERATION_CODE IS NULL AND Y.BM_OPERATION_CODE IS NULL
        )
        OR (
          X.BM_OPERATION_CODE <> Y.BM_OPERATION_CODE
        )
      )
      OR (
        (
          X.BM_TENOR IS NULL AND NOT Y.BM_TENOR IS NULL
        )
        OR (
          NOT X.BM_TENOR IS NULL AND Y.BM_TENOR IS NULL
        )
        OR (
          X.BM_TENOR <> Y.BM_TENOR
        )
      )
      OR (
        (
          X.BM_TRACK_LIMIT IS NULL AND NOT Y.BM_TRACK_LIMIT IS NULL
        )
        OR (
          NOT X.BM_TRACK_LIMIT IS NULL AND Y.BM_TRACK_LIMIT IS NULL
        )
        OR (
          X.BM_TRACK_LIMIT <> Y.BM_TRACK_LIMIT
        )
      )
      OR (
        (
          X.CHARGE_AMT_ISB IS NULL AND NOT Y.CHARGE_AMT_ISB IS NULL
        )
        OR (
          NOT X.CHARGE_AMT_ISB IS NULL AND Y.CHARGE_AMT_ISB IS NULL
        )
        OR (
          X.CHARGE_AMT_ISB <> Y.CHARGE_AMT_ISB
        )
      )
      OR (
        (
          X.CHARGE_CCY_ISB IS NULL AND NOT Y.CHARGE_CCY_ISB IS NULL
        )
        OR (
          NOT X.CHARGE_CCY_ISB IS NULL AND Y.CHARGE_CCY_ISB IS NULL
        )
        OR (
          X.CHARGE_CCY_ISB <> Y.CHARGE_CCY_ISB
        )
      )
      OR (
        (
          X.CHARGES_FROM_BEN IS NULL AND NOT Y.CHARGES_FROM_BEN IS NULL
        )
        OR (
          NOT X.CHARGES_FROM_BEN IS NULL AND Y.CHARGES_FROM_BEN IS NULL
        )
        OR (
          X.CHARGES_FROM_BEN <> Y.CHARGES_FROM_BEN
        )
      )
      OR (
        (
          X.CHARGES_FROM_ISB IS NULL AND NOT Y.CHARGES_FROM_ISB IS NULL
        )
        OR (
          NOT X.CHARGES_FROM_ISB IS NULL AND Y.CHARGES_FROM_ISB IS NULL
        )
        OR (
          X.CHARGES_FROM_ISB <> Y.CHARGES_FROM_ISB
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
          X.CIF_ID IS NULL AND NOT Y.CIF_ID IS NULL
        )
        OR (
          NOT X.CIF_ID IS NULL AND Y.CIF_ID IS NULL
        )
        OR (
          X.CIF_ID <> Y.CIF_ID
        )
      )
      OR (
        (
          X.CLAIM_DATE IS NULL AND NOT Y.CLAIM_DATE IS NULL
        )
        OR (
          NOT X.CLAIM_DATE IS NULL AND Y.CLAIM_DATE IS NULL
        )
        OR (
          X.CLAIM_DATE <> Y.CLAIM_DATE
        )
      )
      OR (
        (
          X.CLAIM_EXPIRY_DATE IS NULL AND NOT Y.CLAIM_EXPIRY_DATE IS NULL
        )
        OR (
          NOT X.CLAIM_EXPIRY_DATE IS NULL AND Y.CLAIM_EXPIRY_DATE IS NULL
        )
        OR (
          X.CLAIM_EXPIRY_DATE <> Y.CLAIM_EXPIRY_DATE
        )
      )
      OR (
        (
          X.CLAIM_INDICATOR IS NULL AND NOT Y.CLAIM_INDICATOR IS NULL
        )
        OR (
          NOT X.CLAIM_INDICATOR IS NULL AND Y.CLAIM_INDICATOR IS NULL
        )
        OR (
          X.CLAIM_INDICATOR <> Y.CLAIM_INDICATOR
        )
      )
      OR (
        (
          X.CLOSURE_DATE IS NULL AND NOT Y.CLOSURE_DATE IS NULL
        )
        OR (
          NOT X.CLOSURE_DATE IS NULL AND Y.CLOSURE_DATE IS NULL
        )
        OR (
          X.CLOSURE_DATE <> Y.CLOSURE_DATE
        )
      )
      OR (
        (
          X.CLOSURE_TYPE IS NULL AND NOT Y.CLOSURE_TYPE IS NULL
        )
        OR (
          NOT X.CLOSURE_TYPE IS NULL AND Y.CLOSURE_TYPE IS NULL
        )
        OR (
          X.CLOSURE_TYPE <> Y.CLOSURE_TYPE
        )
      )
      OR (
        (
          X.COLLAT_LOAN_ALLOWED IS NULL AND NOT Y.COLLAT_LOAN_ALLOWED IS NULL
        )
        OR (
          NOT X.COLLAT_LOAN_ALLOWED IS NULL AND Y.COLLAT_LOAN_ALLOWED IS NULL
        )
        OR (
          X.COLLAT_LOAN_ALLOWED <> Y.COLLAT_LOAN_ALLOWED
        )
      )
      OR (
        (
          X.COLLATERAL_PCT_OBP IS NULL AND NOT Y.COLLATERAL_PCT_OBP IS NULL
        )
        OR (
          NOT X.COLLATERAL_PCT_OBP IS NULL AND Y.COLLATERAL_PCT_OBP IS NULL
        )
        OR (
          X.COLLATERAL_PCT_OBP <> Y.COLLATERAL_PCT_OBP
        )
      )
      OR (
        (
          X.CONFIRM_AMOUNT IS NULL AND NOT Y.CONFIRM_AMOUNT IS NULL
        )
        OR (
          NOT X.CONFIRM_AMOUNT IS NULL AND Y.CONFIRM_AMOUNT IS NULL
        )
        OR (
          X.CONFIRM_AMOUNT <> Y.CONFIRM_AMOUNT
        )
      )
      OR (
        (
          X.CONFIRM_PERCENT IS NULL AND NOT Y.CONFIRM_PERCENT IS NULL
        )
        OR (
          NOT X.CONFIRM_PERCENT IS NULL AND Y.CONFIRM_PERCENT IS NULL
        )
        OR (
          X.CONFIRM_PERCENT <> Y.CONFIRM_PERCENT
        )
      )
      OR (
        (
          X.CONTRACT_AMT IS NULL AND NOT Y.CONTRACT_AMT IS NULL
        )
        OR (
          NOT X.CONTRACT_AMT IS NULL AND Y.CONTRACT_AMT IS NULL
        )
        OR (
          X.CONTRACT_AMT <> Y.CONTRACT_AMT
        )
      )
      OR (
        (
          X.CONTRACT_CCY IS NULL AND NOT Y.CONTRACT_CCY IS NULL
        )
        OR (
          NOT X.CONTRACT_CCY IS NULL AND Y.CONTRACT_CCY IS NULL
        )
        OR (
          X.CONTRACT_CCY <> Y.CONTRACT_CCY
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
          X.CREDIT_AVL_WITH IS NULL AND NOT Y.CREDIT_AVL_WITH IS NULL
        )
        OR (
          NOT X.CREDIT_AVL_WITH IS NULL AND Y.CREDIT_AVL_WITH IS NULL
        )
        OR (
          X.CREDIT_AVL_WITH <> Y.CREDIT_AVL_WITH
        )
      )
      OR (
        (
          X.CREDIT_LINE IS NULL AND NOT Y.CREDIT_LINE IS NULL
        )
        OR (
          NOT X.CREDIT_LINE IS NULL AND Y.CREDIT_LINE IS NULL
        )
        OR (
          X.CREDIT_LINE <> Y.CREDIT_LINE
        )
      )
      OR (
        (
          X.CUMULATIVE IS NULL AND NOT Y.CUMULATIVE IS NULL
        )
        OR (
          NOT X.CUMULATIVE IS NULL AND Y.CUMULATIVE IS NULL
        )
        OR (
          X.CUMULATIVE <> Y.CUMULATIVE
        )
      )
      OR (
        (
          X.CUST_NAME IS NULL AND NOT Y.CUST_NAME IS NULL
        )
        OR (
          NOT X.CUST_NAME IS NULL AND Y.CUST_NAME IS NULL
        )
        OR (
          X.CUST_NAME <> Y.CUST_NAME
        )
      )
      OR (
        (
          X.CUST_REF_DATE IS NULL AND NOT Y.CUST_REF_DATE IS NULL
        )
        OR (
          NOT X.CUST_REF_DATE IS NULL AND Y.CUST_REF_DATE IS NULL
        )
        OR (
          X.CUST_REF_DATE <> Y.CUST_REF_DATE
        )
      )
      OR (
        (
          X.CUST_TYPE IS NULL AND NOT Y.CUST_TYPE IS NULL
        )
        OR (
          NOT X.CUST_TYPE IS NULL AND Y.CUST_TYPE IS NULL
        )
        OR (
          X.CUST_TYPE <> Y.CUST_TYPE
        )
      )
      OR (
        (
          X.DEAL_SOURCE IS NULL AND NOT Y.DEAL_SOURCE IS NULL
        )
        OR (
          NOT X.DEAL_SOURCE IS NULL AND Y.DEAL_SOURCE IS NULL
        )
        OR (
          X.DEAL_SOURCE <> Y.DEAL_SOURCE
        )
      )
      OR (
        (
          X.EFFECTIVE_DATE IS NULL AND NOT Y.EFFECTIVE_DATE IS NULL
        )
        OR (
          NOT X.EFFECTIVE_DATE IS NULL AND Y.EFFECTIVE_DATE IS NULL
        )
        OR (
          X.EFFECTIVE_DATE <> Y.EFFECTIVE_DATE
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
          X.EXPIRY_CONDITION IS NULL AND NOT Y.EXPIRY_CONDITION IS NULL
        )
        OR (
          NOT X.EXPIRY_CONDITION IS NULL AND Y.EXPIRY_CONDITION IS NULL
        )
        OR (
          X.EXPIRY_CONDITION <> Y.EXPIRY_CONDITION
        )
      )
      OR (
        (
          X.EXPIRY_DATE IS NULL AND NOT Y.EXPIRY_DATE IS NULL
        )
        OR (
          NOT X.EXPIRY_DATE IS NULL AND Y.EXPIRY_DATE IS NULL
        )
        OR (
          X.EXPIRY_DATE <> Y.EXPIRY_DATE
        )
      )
      OR (
        (
          X.EXPIRY_PLACE IS NULL AND NOT Y.EXPIRY_PLACE IS NULL
        )
        OR (
          NOT X.EXPIRY_PLACE IS NULL AND Y.EXPIRY_PLACE IS NULL
        )
        OR (
          X.EXPIRY_PLACE <> Y.EXPIRY_PLACE
        )
      )
      OR (
        (
          X.EXT_REF_NO IS NULL AND NOT Y.EXT_REF_NO IS NULL
        )
        OR (
          NOT X.EXT_REF_NO IS NULL AND Y.EXT_REF_NO IS NULL
        )
        OR (
          X.EXT_REF_NO <> Y.EXT_REF_NO
        )
      )
      OR (
        (
          X.EXTN_DETAILS IS NULL AND NOT Y.EXTN_DETAILS IS NULL
        )
        OR (
          NOT X.EXTN_DETAILS IS NULL AND Y.EXTN_DETAILS IS NULL
        )
        OR (
          X.EXTN_DETAILS <> Y.EXTN_DETAILS
        )
      )
      OR (
        (
          X.FIN_AMND_NO IS NULL AND NOT Y.FIN_AMND_NO IS NULL
        )
        OR (
          NOT X.FIN_AMND_NO IS NULL AND Y.FIN_AMND_NO IS NULL
        )
        OR (
          X.FIN_AMND_NO <> Y.FIN_AMND_NO
        )
      )
      OR (
        (
          X.FIN_FLAG IS NULL AND NOT Y.FIN_FLAG IS NULL
        )
        OR (
          NOT X.FIN_FLAG IS NULL AND Y.FIN_FLAG IS NULL
        )
        OR (
          X.FIN_FLAG <> Y.FIN_FLAG
        )
      )
      OR (
        (
          X.FINAL_EXPIRY_DATE IS NULL AND NOT Y.FINAL_EXPIRY_DATE IS NULL
        )
        OR (
          NOT X.FINAL_EXPIRY_DATE IS NULL AND Y.FINAL_EXPIRY_DATE IS NULL
        )
        OR (
          X.FINAL_EXPIRY_DATE <> Y.FINAL_EXPIRY_DATE
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
          X.GUARANTEE_TYPE IS NULL AND NOT Y.GUARANTEE_TYPE IS NULL
        )
        OR (
          NOT X.GUARANTEE_TYPE IS NULL AND Y.GUARANTEE_TYPE IS NULL
        )
        OR (
          X.GUARANTEE_TYPE <> Y.GUARANTEE_TYPE
        )
      )
      OR (
        (
          X.GUARANTEE_TYPE_DESC IS NULL AND NOT Y.GUARANTEE_TYPE_DESC IS NULL
        )
        OR (
          NOT X.GUARANTEE_TYPE_DESC IS NULL AND Y.GUARANTEE_TYPE_DESC IS NULL
        )
        OR (
          X.GUARANTEE_TYPE_DESC <> Y.GUARANTEE_TYPE_DESC
        )
      )
      OR (
        (
          X.INCLUDE_TO_DATE IS NULL AND NOT Y.INCLUDE_TO_DATE IS NULL
        )
        OR (
          NOT X.INCLUDE_TO_DATE IS NULL AND Y.INCLUDE_TO_DATE IS NULL
        )
        OR (
          X.INCLUDE_TO_DATE <> Y.INCLUDE_TO_DATE
        )
      )
      OR (
        (
          X.INCO_TERM IS NULL AND NOT Y.INCO_TERM IS NULL
        )
        OR (
          NOT X.INCO_TERM IS NULL AND Y.INCO_TERM IS NULL
        )
        OR (
          X.INCO_TERM <> Y.INCO_TERM
        )
      )
      OR (
        (
          X.SK_CIF_ID IS NULL AND NOT Y.SK_CIF_ID IS NULL
        )
        OR (
          NOT X.SK_CIF_ID IS NULL AND Y.SK_CIF_ID IS NULL
        )
        OR (
          X.SK_CIF_ID <> Y.SK_CIF_ID
        )
      )
      OR (
        (
          X.ISB_DATE IS NULL AND NOT Y.ISB_DATE IS NULL
        )
        OR (
          NOT X.ISB_DATE IS NULL AND Y.ISB_DATE IS NULL
        )
        OR (
          X.ISB_DATE <> Y.ISB_DATE
        )
      )
      OR (
        (
          X.ISSUE_DATE IS NULL AND NOT Y.ISSUE_DATE IS NULL
        )
        OR (
          NOT X.ISSUE_DATE IS NULL AND Y.ISSUE_DATE IS NULL
        )
        OR (
          X.ISSUE_DATE <> Y.ISSUE_DATE
        )
      )
      OR (
        (
          X.ISSUE_REQUEST IS NULL AND NOT Y.ISSUE_REQUEST IS NULL
        )
        OR (
          NOT X.ISSUE_REQUEST IS NULL AND Y.ISSUE_REQUEST IS NULL
        )
        OR (
          X.ISSUE_REQUEST <> Y.ISSUE_REQUEST
        )
      )
      OR (
        (
          X.LC_LANG_CODE IS NULL AND NOT Y.LC_LANG_CODE IS NULL
        )
        OR (
          NOT X.LC_LANG_CODE IS NULL AND Y.LC_LANG_CODE IS NULL
        )
        OR (
          X.LC_LANG_CODE <> Y.LC_LANG_CODE
        )
      )
      OR (
        (
          X.LIAB_TOLERANCE IS NULL AND NOT Y.LIAB_TOLERANCE IS NULL
        )
        OR (
          NOT X.LIAB_TOLERANCE IS NULL AND Y.LIAB_TOLERANCE IS NULL
        )
        OR (
          X.LIAB_TOLERANCE <> Y.LIAB_TOLERANCE
        )
      )
      OR (
        (
          X.LIMITS_TRACKING_TENOR_TYPE IS NULL AND NOT Y.LIMITS_TRACKING_TENOR_TYPE IS NULL
        )
        OR (
          NOT X.LIMITS_TRACKING_TENOR_TYPE IS NULL AND Y.LIMITS_TRACKING_TENOR_TYPE IS NULL
        )
        OR (
          X.LIMITS_TRACKING_TENOR_TYPE <> Y.LIMITS_TRACKING_TENOR_TYPE
        )
      )
      OR (
        (
          X.LINE_CIF_ID IS NULL AND NOT Y.LINE_CIF_ID IS NULL
        )
        OR (
          NOT X.LINE_CIF_ID IS NULL AND Y.LINE_CIF_ID IS NULL
        )
        OR (
          X.LINE_CIF_ID <> Y.LINE_CIF_ID
        )
      )
      OR (
        (
          X.LINE_PARTY_TYPE IS NULL AND NOT Y.LINE_PARTY_TYPE IS NULL
        )
        OR (
          NOT X.LINE_PARTY_TYPE IS NULL AND Y.LINE_PARTY_TYPE IS NULL
        )
        OR (
          X.LINE_PARTY_TYPE <> Y.LINE_PARTY_TYPE
        )
      )
      OR (
        (
          X.LOCAL_GUA_APPL_RULE IS NULL AND NOT Y.LOCAL_GUA_APPL_RULE IS NULL
        )
        OR (
          NOT X.LOCAL_GUA_APPL_RULE IS NULL AND Y.LOCAL_GUA_APPL_RULE IS NULL
        )
        OR (
          X.LOCAL_GUA_APPL_RULE <> Y.LOCAL_GUA_APPL_RULE
        )
      )
      OR (
        (
          X.LOCAL_GUA_AUTO_EXTN_PRD IS NULL AND NOT Y.LOCAL_GUA_AUTO_EXTN_PRD IS NULL
        )
        OR (
          NOT X.LOCAL_GUA_AUTO_EXTN_PRD IS NULL AND Y.LOCAL_GUA_AUTO_EXTN_PRD IS NULL
        )
        OR (
          X.LOCAL_GUA_AUTO_EXTN_PRD <> Y.LOCAL_GUA_AUTO_EXTN_PRD
        )
      )
      OR (
        (
          X.LOCAL_GUA_CHG_FROM_BEN IS NULL AND NOT Y.LOCAL_GUA_CHG_FROM_BEN IS NULL
        )
        OR (
          NOT X.LOCAL_GUA_CHG_FROM_BEN IS NULL AND Y.LOCAL_GUA_CHG_FROM_BEN IS NULL
        )
        OR (
          X.LOCAL_GUA_CHG_FROM_BEN <> Y.LOCAL_GUA_CHG_FROM_BEN
        )
      )
      OR (
        (
          X.LOCAL_GUA_CLAIM_IND IS NULL AND NOT Y.LOCAL_GUA_CLAIM_IND IS NULL
        )
        OR (
          NOT X.LOCAL_GUA_CLAIM_IND IS NULL AND Y.LOCAL_GUA_CLAIM_IND IS NULL
        )
        OR (
          X.LOCAL_GUA_CLAIM_IND <> Y.LOCAL_GUA_CLAIM_IND
        )
      )
      OR (
        (
          X.LOCAL_GUA_CONTRACT_AMT IS NULL AND NOT Y.LOCAL_GUA_CONTRACT_AMT IS NULL
        )
        OR (
          NOT X.LOCAL_GUA_CONTRACT_AMT IS NULL AND Y.LOCAL_GUA_CONTRACT_AMT IS NULL
        )
        OR (
          X.LOCAL_GUA_CONTRACT_AMT <> Y.LOCAL_GUA_CONTRACT_AMT
        )
      )
      OR (
        (
          X.LOCAL_GUA_CONTRACT_CCY IS NULL AND NOT Y.LOCAL_GUA_CONTRACT_CCY IS NULL
        )
        OR (
          NOT X.LOCAL_GUA_CONTRACT_CCY IS NULL AND Y.LOCAL_GUA_CONTRACT_CCY IS NULL
        )
        OR (
          X.LOCAL_GUA_CONTRACT_CCY <> Y.LOCAL_GUA_CONTRACT_CCY
        )
      )
      OR (
        (
          X.LOCAL_GUA_CRD_AVL_WITH IS NULL AND NOT Y.LOCAL_GUA_CRD_AVL_WITH IS NULL
        )
        OR (
          NOT X.LOCAL_GUA_CRD_AVL_WITH IS NULL AND Y.LOCAL_GUA_CRD_AVL_WITH IS NULL
        )
        OR (
          X.LOCAL_GUA_CRD_AVL_WITH <> Y.LOCAL_GUA_CRD_AVL_WITH
        )
      )
      OR (
        (
          X.LOCAL_GUA_EXP_COND IS NULL AND NOT Y.LOCAL_GUA_EXP_COND IS NULL
        )
        OR (
          NOT X.LOCAL_GUA_EXP_COND IS NULL AND Y.LOCAL_GUA_EXP_COND IS NULL
        )
        OR (
          X.LOCAL_GUA_EXP_COND <> Y.LOCAL_GUA_EXP_COND
        )
      )
      OR (
        (
          X.LOCAL_GUA_EXP_TYPE IS NULL AND NOT Y.LOCAL_GUA_EXP_TYPE IS NULL
        )
        OR (
          NOT X.LOCAL_GUA_EXP_TYPE IS NULL AND Y.LOCAL_GUA_EXP_TYPE IS NULL
        )
        OR (
          X.LOCAL_GUA_EXP_TYPE <> Y.LOCAL_GUA_EXP_TYPE
        )
      )
      OR (
        (
          X.LOCAL_GUA_EXPIRY_DATE IS NULL AND NOT Y.LOCAL_GUA_EXPIRY_DATE IS NULL
        )
        OR (
          NOT X.LOCAL_GUA_EXPIRY_DATE IS NULL AND Y.LOCAL_GUA_EXPIRY_DATE IS NULL
        )
        OR (
          X.LOCAL_GUA_EXPIRY_DATE <> Y.LOCAL_GUA_EXPIRY_DATE
        )
      )
      OR (
        (
          X.LOCAL_GUA_EXTN_DETAILS IS NULL AND NOT Y.LOCAL_GUA_EXTN_DETAILS IS NULL
        )
        OR (
          NOT X.LOCAL_GUA_EXTN_DETAILS IS NULL AND Y.LOCAL_GUA_EXTN_DETAILS IS NULL
        )
        OR (
          X.LOCAL_GUA_EXTN_DETAILS <> Y.LOCAL_GUA_EXTN_DETAILS
        )
      )
      OR (
        (
          X.LOCAL_GUA_FINAL_EXP_DATE IS NULL AND NOT Y.LOCAL_GUA_FINAL_EXP_DATE IS NULL
        )
        OR (
          NOT X.LOCAL_GUA_FINAL_EXP_DATE IS NULL AND Y.LOCAL_GUA_FINAL_EXP_DATE IS NULL
        )
        OR (
          X.LOCAL_GUA_FINAL_EXP_DATE <> Y.LOCAL_GUA_FINAL_EXP_DATE
        )
      )
      OR (
        (
          X.LOCAL_GUA_FOU IS NULL AND NOT Y.LOCAL_GUA_FOU IS NULL
        )
        OR (
          NOT X.LOCAL_GUA_FOU IS NULL AND Y.LOCAL_GUA_FOU IS NULL
        )
        OR (
          X.LOCAL_GUA_FOU <> Y.LOCAL_GUA_FOU
        )
      )
      OR (
        (
          X.LOCAL_GUA_ISSUE_DATE IS NULL AND NOT Y.LOCAL_GUA_ISSUE_DATE IS NULL
        )
        OR (
          NOT X.LOCAL_GUA_ISSUE_DATE IS NULL AND Y.LOCAL_GUA_ISSUE_DATE IS NULL
        )
        OR (
          X.LOCAL_GUA_ISSUE_DATE <> Y.LOCAL_GUA_ISSUE_DATE
        )
      )
      OR (
        (
          X.LOCAL_GUA_NOTIF_DETAILS IS NULL AND NOT Y.LOCAL_GUA_NOTIF_DETAILS IS NULL
        )
        OR (
          NOT X.LOCAL_GUA_NOTIF_DETAILS IS NULL AND Y.LOCAL_GUA_NOTIF_DETAILS IS NULL
        )
        OR (
          X.LOCAL_GUA_NOTIF_DETAILS <> Y.LOCAL_GUA_NOTIF_DETAILS
        )
      )
      OR (
        (
          X.LOCAL_GUA_NOTIF_PRD IS NULL AND NOT Y.LOCAL_GUA_NOTIF_PRD IS NULL
        )
        OR (
          NOT X.LOCAL_GUA_NOTIF_PRD IS NULL AND Y.LOCAL_GUA_NOTIF_PRD IS NULL
        )
        OR (
          X.LOCAL_GUA_NOTIF_PRD <> Y.LOCAL_GUA_NOTIF_PRD
        )
      )
      OR (
        (
          X.LOCAL_GUA_RULE_NARRAT IS NULL AND NOT Y.LOCAL_GUA_RULE_NARRAT IS NULL
        )
        OR (
          NOT X.LOCAL_GUA_RULE_NARRAT IS NULL AND Y.LOCAL_GUA_RULE_NARRAT IS NULL
        )
        OR (
          X.LOCAL_GUA_RULE_NARRAT <> Y.LOCAL_GUA_RULE_NARRAT
        )
      )
      OR (
        (
          X.LOCAL_GUA_SUPP_INFO_AMT IS NULL AND NOT Y.LOCAL_GUA_SUPP_INFO_AMT IS NULL
        )
        OR (
          NOT X.LOCAL_GUA_SUPP_INFO_AMT IS NULL AND Y.LOCAL_GUA_SUPP_INFO_AMT IS NULL
        )
        OR (
          X.LOCAL_GUA_SUPP_INFO_AMT <> Y.LOCAL_GUA_SUPP_INFO_AMT
        )
      )
      OR (
        (
          X.LOCAL_GUA_TRANSFERABLE IS NULL AND NOT Y.LOCAL_GUA_TRANSFERABLE IS NULL
        )
        OR (
          NOT X.LOCAL_GUA_TRANSFERABLE IS NULL AND Y.LOCAL_GUA_TRANSFERABLE IS NULL
        )
        OR (
          X.LOCAL_GUA_TRANSFERABLE <> Y.LOCAL_GUA_TRANSFERABLE
        )
      )
      OR (
        (
          X.LOCAL_GUA_TRNF_COND IS NULL AND NOT Y.LOCAL_GUA_TRNF_COND IS NULL
        )
        OR (
          NOT X.LOCAL_GUA_TRNF_COND IS NULL AND Y.LOCAL_GUA_TRNF_COND IS NULL
        )
        OR (
          X.LOCAL_GUA_TRNF_COND <> Y.LOCAL_GUA_TRNF_COND
        )
      )
      OR (
        (
          X.MAX_CONTRACT_AMT IS NULL AND NOT Y.MAX_CONTRACT_AMT IS NULL
        )
        OR (
          NOT X.MAX_CONTRACT_AMT IS NULL AND Y.MAX_CONTRACT_AMT IS NULL
        )
        OR (
          X.MAX_CONTRACT_AMT <> Y.MAX_CONTRACT_AMT
        )
      )
      OR (
        (
          X.MAX_LIABILITY_AMT IS NULL AND NOT Y.MAX_LIABILITY_AMT IS NULL
        )
        OR (
          NOT X.MAX_LIABILITY_AMT IS NULL AND Y.MAX_LIABILITY_AMT IS NULL
        )
        OR (
          X.MAX_LIABILITY_AMT <> Y.MAX_LIABILITY_AMT
        )
      )
      OR (
        (
          X.MAY_CONFIRM IS NULL AND NOT Y.MAY_CONFIRM IS NULL
        )
        OR (
          NOT X.MAY_CONFIRM IS NULL AND Y.MAY_CONFIRM IS NULL
        )
        OR (
          X.MAY_CONFIRM <> Y.MAY_CONFIRM
        )
      )
      OR (
        (
          X.NEGATIVE_TOLERANCE IS NULL AND NOT Y.NEGATIVE_TOLERANCE IS NULL
        )
        OR (
          NOT X.NEGATIVE_TOLERANCE IS NULL AND Y.NEGATIVE_TOLERANCE IS NULL
        )
        OR (
          X.NEGATIVE_TOLERANCE <> Y.NEGATIVE_TOLERANCE
        )
      )
      OR (
        (
          X.NEXT_REINSTATEMENT_DATE IS NULL AND NOT Y.NEXT_REINSTATEMENT_DATE IS NULL
        )
        OR (
          NOT X.NEXT_REINSTATEMENT_DATE IS NULL AND Y.NEXT_REINSTATEMENT_DATE IS NULL
        )
        OR (
          X.NEXT_REINSTATEMENT_DATE <> Y.NEXT_REINSTATEMENT_DATE
        )
      )
      OR (
        (
          X.NON_EXT_NOTIF_DETAILS IS NULL AND NOT Y.NON_EXT_NOTIF_DETAILS IS NULL
        )
        OR (
          NOT X.NON_EXT_NOTIF_DETAILS IS NULL AND Y.NON_EXT_NOTIF_DETAILS IS NULL
        )
        OR (
          X.NON_EXT_NOTIF_DETAILS <> Y.NON_EXT_NOTIF_DETAILS
        )
      )
      OR (
        (
          X.NOTIFICATION_PERIOD IS NULL AND NOT Y.NOTIFICATION_PERIOD IS NULL
        )
        OR (
          NOT X.NOTIFICATION_PERIOD IS NULL AND Y.NOTIFICATION_PERIOD IS NULL
        )
        OR (
          X.NOTIFICATION_PERIOD <> Y.NOTIFICATION_PERIOD
        )
      )
      OR (
        (
          X.OPERATION_CODE IS NULL AND NOT Y.OPERATION_CODE IS NULL
        )
        OR (
          NOT X.OPERATION_CODE IS NULL AND Y.OPERATION_CODE IS NULL
        )
        OR (
          X.OPERATION_CODE <> Y.OPERATION_CODE
        )
      )
      OR (
        (
          X.OS_LC_AMOUNT IS NULL AND NOT Y.OS_LC_AMOUNT IS NULL
        )
        OR (
          NOT X.OS_LC_AMOUNT IS NULL AND Y.OS_LC_AMOUNT IS NULL
        )
        OR (
          X.OS_LC_AMOUNT <> Y.OS_LC_AMOUNT
        )
      )
      OR (
        (
          X.PARTIAL_CLOSURE IS NULL AND NOT Y.PARTIAL_CLOSURE IS NULL
        )
        OR (
          NOT X.PARTIAL_CLOSURE IS NULL AND Y.PARTIAL_CLOSURE IS NULL
        )
        OR (
          X.PARTIAL_CLOSURE <> Y.PARTIAL_CLOSURE
        )
      )
      OR (
        (
          X.PARTIAL_CONFIRMATION_ALLOW IS NULL AND NOT Y.PARTIAL_CONFIRMATION_ALLOW IS NULL
        )
        OR (
          NOT X.PARTIAL_CONFIRMATION_ALLOW IS NULL AND Y.PARTIAL_CONFIRMATION_ALLOW IS NULL
        )
        OR (
          X.PARTIAL_CONFIRMATION_ALLOW <> Y.PARTIAL_CONFIRMATION_ALLOW
        )
      )
      OR (
        (
          X.PAYMENT_DETAILS IS NULL AND NOT Y.PAYMENT_DETAILS IS NULL
        )
        OR (
          NOT X.PAYMENT_DETAILS IS NULL AND Y.PAYMENT_DETAILS IS NULL
        )
        OR (
          X.PAYMENT_DETAILS <> Y.PAYMENT_DETAILS
        )
      )
      OR (
        (
          X.PERIOD_FOR_PRESENTATION IS NULL AND NOT Y.PERIOD_FOR_PRESENTATION IS NULL
        )
        OR (
          NOT X.PERIOD_FOR_PRESENTATION IS NULL AND Y.PERIOD_FOR_PRESENTATION IS NULL
        )
        OR (
          X.PERIOD_FOR_PRESENTATION <> Y.PERIOD_FOR_PRESENTATION
        )
      )
      OR (
        (
          X.POSITIVE_TOLERANCE IS NULL AND NOT Y.POSITIVE_TOLERANCE IS NULL
        )
        OR (
          NOT X.POSITIVE_TOLERANCE IS NULL AND Y.POSITIVE_TOLERANCE IS NULL
        )
        OR (
          X.POSITIVE_TOLERANCE <> Y.POSITIVE_TOLERANCE
        )
      )
      OR (
        (
          X.PRE_ADV_DATE IS NULL AND NOT Y.PRE_ADV_DATE IS NULL
        )
        OR (
          NOT X.PRE_ADV_DATE IS NULL AND Y.PRE_ADV_DATE IS NULL
        )
        OR (
          X.PRE_ADV_DATE <> Y.PRE_ADV_DATE
        )
      )
      OR (
        (
          X.PRESENTATION_NARRATIVE IS NULL AND NOT Y.PRESENTATION_NARRATIVE IS NULL
        )
        OR (
          NOT X.PRESENTATION_NARRATIVE IS NULL AND Y.PRESENTATION_NARRATIVE IS NULL
        )
        OR (
          X.PRESENTATION_NARRATIVE <> Y.PRESENTATION_NARRATIVE
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
          X.REIMBURSEMENT_TYPE IS NULL AND NOT Y.REIMBURSEMENT_TYPE IS NULL
        )
        OR (
          NOT X.REIMBURSEMENT_TYPE IS NULL AND Y.REIMBURSEMENT_TYPE IS NULL
        )
        OR (
          X.REIMBURSEMENT_TYPE <> Y.REIMBURSEMENT_TYPE
        )
      )
      OR (
        (
          X.REINSTATEMENT_TYPE IS NULL AND NOT Y.REINSTATEMENT_TYPE IS NULL
        )
        OR (
          NOT X.REINSTATEMENT_TYPE IS NULL AND Y.REINSTATEMENT_TYPE IS NULL
        )
        OR (
          X.REINSTATEMENT_TYPE <> Y.REINSTATEMENT_TYPE
        )
      )
      OR (
        (
          X.RELATED_LC_REF_NO IS NULL AND NOT Y.RELATED_LC_REF_NO IS NULL
        )
        OR (
          NOT X.RELATED_LC_REF_NO IS NULL AND Y.RELATED_LC_REF_NO IS NULL
        )
        OR (
          X.RELATED_LC_REF_NO <> Y.RELATED_LC_REF_NO
        )
      )
      OR (
        (
          X.REMARKS IS NULL AND NOT Y.REMARKS IS NULL
        )
        OR (
          NOT X.REMARKS IS NULL AND Y.REMARKS IS NULL
        )
        OR (
          X.REMARKS <> Y.REMARKS
        )
      )
      OR (
        (
          X.REQ_CONFIRM_PARTY IS NULL AND NOT Y.REQ_CONFIRM_PARTY IS NULL
        )
        OR (
          NOT X.REQ_CONFIRM_PARTY IS NULL AND Y.REQ_CONFIRM_PARTY IS NULL
        )
        OR (
          X.REQ_CONFIRM_PARTY <> Y.REQ_CONFIRM_PARTY
        )
      )
      OR (
        (
          X.REVOLVES_IN IS NULL AND NOT Y.REVOLVES_IN IS NULL
        )
        OR (
          NOT X.REVOLVES_IN IS NULL AND Y.REVOLVES_IN IS NULL
        )
        OR (
          X.REVOLVES_IN <> Y.REVOLVES_IN
        )
      )
      OR (
        (
          X.RULE_NARRATIVE IS NULL AND NOT Y.RULE_NARRATIVE IS NULL
        )
        OR (
          NOT X.RULE_NARRATIVE IS NULL AND Y.RULE_NARRATIVE IS NULL
        )
        OR (
          X.RULE_NARRATIVE <> Y.RULE_NARRATIVE
        )
      )
      OR (
        (
          X.SETTLEMENT_METHOD IS NULL AND NOT Y.SETTLEMENT_METHOD IS NULL
        )
        OR (
          NOT X.SETTLEMENT_METHOD IS NULL AND Y.SETTLEMENT_METHOD IS NULL
        )
        OR (
          X.SETTLEMENT_METHOD <> Y.SETTLEMENT_METHOD
        )
      )
      OR (
        (
          X.SETTLEMENT_TYPE IS NULL AND NOT Y.SETTLEMENT_TYPE IS NULL
        )
        OR (
          NOT X.SETTLEMENT_TYPE IS NULL AND Y.SETTLEMENT_TYPE IS NULL
        )
        OR (
          X.SETTLEMENT_TYPE <> Y.SETTLEMENT_TYPE
        )
      )
      OR (
        (
          X.SK_LINE_CIF_ID IS NULL AND NOT Y.SK_LINE_CIF_ID IS NULL
        )
        OR (
          NOT X.SK_LINE_CIF_ID IS NULL AND Y.SK_LINE_CIF_ID IS NULL
        )
        OR (
          X.SK_LINE_CIF_ID <> Y.SK_LINE_CIF_ID
        )
      )
      OR (
        (
          X.SK_CONTRACT_REF_NO IS NULL AND NOT Y.SK_CONTRACT_REF_NO IS NULL
        )
        OR (
          NOT X.SK_CONTRACT_REF_NO IS NULL AND Y.SK_CONTRACT_REF_NO IS NULL
        )
        OR (
          X.SK_CONTRACT_REF_NO <> Y.SK_CONTRACT_REF_NO
        )
      )
      OR (
        (
          X.SK_CONTRACT_REF_NO_EVENT_SEQ_NO IS NULL
          AND NOT Y.SK_CONTRACT_REF_NO_EVENT_SEQ_NO IS NULL
        )
        OR (
          NOT X.SK_CONTRACT_REF_NO_EVENT_SEQ_NO IS NULL
          AND Y.SK_CONTRACT_REF_NO_EVENT_SEQ_NO IS NULL
        )
        OR (
          X.SK_CONTRACT_REF_NO_EVENT_SEQ_NO <> Y.SK_CONTRACT_REF_NO_EVENT_SEQ_NO
        )
      )
      OR (
        (
          X.SK_CREDIT_AVL_WITH IS NULL AND NOT Y.SK_CREDIT_AVL_WITH IS NULL
        )
        OR (
          NOT X.SK_CREDIT_AVL_WITH IS NULL AND Y.SK_CREDIT_AVL_WITH IS NULL
        )
        OR (
          X.SK_CREDIT_AVL_WITH <> Y.SK_CREDIT_AVL_WITH
        )
      )
      OR (
        (
          X.SK_CUST_NAME IS NULL AND NOT Y.SK_CUST_NAME IS NULL
        )
        OR (
          NOT X.SK_CUST_NAME IS NULL AND Y.SK_CUST_NAME IS NULL
        )
        OR (
          X.SK_CUST_NAME <> Y.SK_CUST_NAME
        )
      )
      OR (
        (
          X.SK_PAYMENT_DETAILS IS NULL AND NOT Y.SK_PAYMENT_DETAILS IS NULL
        )
        OR (
          NOT X.SK_PAYMENT_DETAILS IS NULL AND Y.SK_PAYMENT_DETAILS IS NULL
        )
        OR (
          X.SK_PAYMENT_DETAILS <> Y.SK_PAYMENT_DETAILS
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
          X.SK_REMARKS IS NULL AND NOT Y.SK_REMARKS IS NULL
        )
        OR (
          NOT X.SK_REMARKS IS NULL AND Y.SK_REMARKS IS NULL
        )
        OR (
          X.SK_REMARKS <> Y.SK_REMARKS
        )
      )
      OR (
        (
          X.SK_TENOR IS NULL AND NOT Y.SK_TENOR IS NULL
        )
        OR (
          NOT X.SK_TENOR IS NULL AND Y.SK_TENOR IS NULL
        )
        OR (
          X.SK_TENOR <> Y.SK_TENOR
        )
      )
      OR (
        (
          X.STANDARD_WORD_LANG IS NULL AND NOT Y.STANDARD_WORD_LANG IS NULL
        )
        OR (
          NOT X.STANDARD_WORD_LANG IS NULL AND Y.STANDARD_WORD_LANG IS NULL
        )
        OR (
          X.STANDARD_WORD_LANG <> Y.STANDARD_WORD_LANG
        )
      )
      OR (
        (
          X.STANDARD_WORD_REQD IS NULL AND NOT Y.STANDARD_WORD_REQD IS NULL
        )
        OR (
          NOT X.STANDARD_WORD_REQD IS NULL AND Y.STANDARD_WORD_REQD IS NULL
        )
        OR (
          X.STANDARD_WORD_REQD <> Y.STANDARD_WORD_REQD
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
          X.STOP_DATE IS NULL AND NOT Y.STOP_DATE IS NULL
        )
        OR (
          NOT X.STOP_DATE IS NULL AND Y.STOP_DATE IS NULL
        )
        OR (
          X.STOP_DATE <> Y.STOP_DATE
        )
      )
      OR (
        (
          X.SUBSYSTEMSTAT IS NULL AND NOT Y.SUBSYSTEMSTAT IS NULL
        )
        OR (
          NOT X.SUBSYSTEMSTAT IS NULL AND Y.SUBSYSTEMSTAT IS NULL
        )
        OR (
          X.SUBSYSTEMSTAT <> Y.SUBSYSTEMSTAT
        )
      )
      OR (
        (
          X.SUPP_INFO_AMT IS NULL AND NOT Y.SUPP_INFO_AMT IS NULL
        )
        OR (
          NOT X.SUPP_INFO_AMT IS NULL AND Y.SUPP_INFO_AMT IS NULL
        )
        OR (
          X.SUPP_INFO_AMT <> Y.SUPP_INFO_AMT
        )
      )
      OR (
        (
          X.TENOR IS NULL AND NOT Y.TENOR IS NULL
        )
        OR (
          NOT X.TENOR IS NULL AND Y.TENOR IS NULL
        )
        OR (
          X.TENOR <> Y.TENOR
        )
      )
      OR (
        (
          X.TOLERANCE_TEXT IS NULL AND NOT Y.TOLERANCE_TEXT IS NULL
        )
        OR (
          NOT X.TOLERANCE_TEXT IS NULL AND Y.TOLERANCE_TEXT IS NULL
        )
        OR (
          X.TOLERANCE_TEXT <> Y.TOLERANCE_TEXT
        )
      )
      OR (
        (
          X.TRACK_LIMIT IS NULL AND NOT Y.TRACK_LIMIT IS NULL
        )
        OR (
          NOT X.TRACK_LIMIT IS NULL AND Y.TRACK_LIMIT IS NULL
        )
        OR (
          X.TRACK_LIMIT <> Y.TRACK_LIMIT
        )
      )
      OR (
        (
          X.TRANSFER_CONDITIONS IS NULL AND NOT Y.TRANSFER_CONDITIONS IS NULL
        )
        OR (
          NOT X.TRANSFER_CONDITIONS IS NULL AND Y.TRANSFER_CONDITIONS IS NULL
        )
        OR (
          X.TRANSFER_CONDITIONS <> Y.TRANSFER_CONDITIONS
        )
      )
      OR (
        (
          X.TRANSFERABLE IS NULL AND NOT Y.TRANSFERABLE IS NULL
        )
        OR (
          NOT X.TRANSFERABLE IS NULL AND Y.TRANSFERABLE IS NULL
        )
        OR (
          X.TRANSFERABLE <> Y.TRANSFERABLE
        )
      )
      OR (
        (
          X.UNCONFIRM_AMOUNT IS NULL AND NOT Y.UNCONFIRM_AMOUNT IS NULL
        )
        OR (
          NOT X.UNCONFIRM_AMOUNT IS NULL AND Y.UNCONFIRM_AMOUNT IS NULL
        )
        OR (
          X.UNCONFIRM_AMOUNT <> Y.UNCONFIRM_AMOUNT
        )
      )
      OR (
        (
          X.UNDERTAKING_AMOUNT IS NULL AND NOT Y.UNDERTAKING_AMOUNT IS NULL
        )
        OR (
          NOT X.UNDERTAKING_AMOUNT IS NULL AND Y.UNDERTAKING_AMOUNT IS NULL
        )
        OR (
          X.UNDERTAKING_AMOUNT <> Y.UNDERTAKING_AMOUNT
        )
      )
      OR (
        (
          X.UNDERTAKING_EXPIRY_DATE IS NULL AND NOT Y.UNDERTAKING_EXPIRY_DATE IS NULL
        )
        OR (
          NOT X.UNDERTAKING_EXPIRY_DATE IS NULL AND Y.UNDERTAKING_EXPIRY_DATE IS NULL
        )
        OR (
          X.UNDERTAKING_EXPIRY_DATE <> Y.UNDERTAKING_EXPIRY_DATE
        )
      )
      OR (
        (
          X.UNITS IS NULL AND NOT Y.UNITS IS NULL
        )
        OR (
          NOT X.UNITS IS NULL AND Y.UNITS IS NULL
        )
        OR (
          X.UNITS <> Y.UNITS
        )
      )
      OR (
        (
          X.URR_PREFERENCE IS NULL AND NOT Y.URR_PREFERENCE IS NULL
        )
        OR (
          NOT X.URR_PREFERENCE IS NULL AND Y.URR_PREFERENCE IS NULL
        )
        OR (
          X.URR_PREFERENCE <> Y.URR_PREFERENCE
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
          X.USER_LC_REF_NO IS NULL AND NOT Y.USER_LC_REF_NO IS NULL
        )
        OR (
          NOT X.USER_LC_REF_NO IS NULL AND Y.USER_LC_REF_NO IS NULL
        )
        OR (
          X.USER_LC_REF_NO <> Y.USER_LC_REF_NO
        )
      )
      OR (
        (
          X.VALIDITY_TYPE IS NULL AND NOT Y.VALIDITY_TYPE IS NULL
        )
        OR (
          NOT X.VALIDITY_TYPE IS NULL AND Y.VALIDITY_TYPE IS NULL
        )
        OR (
          X.VALIDITY_TYPE <> Y.VALIDITY_TYPE
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
          X.CUST_REF_NO IS NULL AND NOT Y.CUST_REF_NO IS NULL
        )
        OR (
          NOT X.CUST_REF_NO IS NULL AND Y.CUST_REF_NO IS NULL
        )
        OR (
          X.CUST_REF_NO <> Y.CUST_REF_NO
        )
      )
    )
    THEN 'UPD'
    WHEN X.CONTRACT_REF_NO IS NULL AND X.EVENT_SEQ_NO IS NULL
    THEN 'DEL' /* - IF MULTIPLE PKs are there then concatenate them with AND */
  END AS OPT_FLAG
FROM DEVT_STG_FLX.LCTB_CONTRACT_MASTER AS X
FULL JOIN DEVT_STG_FLX.LCTB_CONTRACT_MASTER_BACKUP AS Y
  ON X.CONTRACT_REF_NO = Y.CONTRACT_REF_NO
  AND X.EVENT_SEQ_NO = Y.EVENT_SEQ_NO /* - IF MULTIPLE PKs are there then concatenate them with AND */
WHERE
  NOT OPT_FLAG IS NULL