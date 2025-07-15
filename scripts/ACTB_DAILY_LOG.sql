CREATE OR REPLACE VIEW DEVV_STG_FLX.ACTB_DAILY_LOG_get_delta AS LOCKING ROW FOR ACCESS
SELECT
  COALESCE(X.AC_ENTRY_SR_NO, Y.AC_ENTRY_SR_NO) AS AC_ENTRY_SR_NO,
  X.AC_BRANCH,
  X.AC_CCY,
  X.AC_NO,
  X.AML_EXCEPTION,
  X.AMOUNT_TAG,
  X.AUTH_ID,
  X.AUTH_STAT,
  X.AVLDAYS,
  X.BALANCE_UPD,
  X.BANK_CODE,
  X.BATCH_NO,
  X.BM_AC_CCY,
  X.BM_AMOUNT_TAG_MODULE,
  X.BM_AUTH_STAT,
  X.BM_DONT_SHOWIN_STMT,
  X.BM_DRCR_IND,
  X.BM_IB,
  X.BM_MIS_HEAD,
  X.BM_MODULE,
  X.BM_MODULE_EVENT_CODE,
  X.BM_PRODUCT_ACCRUAL,
  X.BM_TRN_CODE,
  X.BM_TRN_CODE_EV,
  X.CATEGORY,
  X.CURR_NO,
  X.CUST_GL,
  X.CUST_GL_UPDATE,
  X.DELETE_STAT,
  X.DISTRIBUTED,
  X.DONT_SHOWIN_STMT,
  X.DRCR_IND,
  X.ENTRY_SEQ_NO,
  X.EVENT,
  X.EVENT_SR_NO,
  X.EXCH_RATE,
  X.EXTERNAL_REF_NO,
  X.FCY_AMOUNT,
  X.FINANCIAL_CYCLE,
  X.FLG_POSITION_STATUS,
  X.GLMIS_UPDATE_FLAG,
  X.GLMIS_UPDATE_STATUS,
  X.GLMIS_VAL_UPD_FLAG,
  X.IB,
  X.IC_BAL_INCLUSION,
  X.IL_BVT_PROCESSED,
  X.INSTRUMENT_CODE,
  X.LCY_AMOUNT,
  X.MIS_FLAG,
  X.MIS_HEAD,
  X.MIS_SPREAD,
  X.MODULE,
  X.NETTING_IND,
  X.NODE,
  X.NODE_SR_NO,
  X.ON_LINE,
  X.ORIG_PNL_GL,
  X.PERIOD_CODE,
  X.PRINT_STAT,
  X.PROCESSED_FLAG,
  X.PRODUCT,
  X.PRODUCT_ACCRUAL,
  X.REL_EVENT_SR_NO,
  X.RELATED_ACCOUNT,
  X.RELATED_CUSTOMER,
  X.RELATED_REFERENCE,
  X.SK_AC_ENTRY_SR_NO,
  X.SK_AC_NO,
  X.SK_RELATED_CUSTOMER,
  X.SK_AC_BRANCH,
  X.SK_AUTH_ID,
  X.SK_USER_ID,
  X.SK_PRODUCT,
  X.SK_RELATED_ACCOUNT_CASA,
  X.SK_RELATED_ACCOUNT_LOAN,
  X.SK_TRN_REF_NO,
  X.STMT_DT,
  X.TRN_CODE,
  X.TRN_DT,
  X.TRN_REF_NO,
  X.TXN_DT_TIME,
  X.TXN_INIT_DATE,
  X.TYPE,
  X.UPDACT,
  X.USER_ID,
  X.VALUE_DT,
  X.VDBAL_UPDATE_FLAG,
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
    WHEN Y.AC_ENTRY_SR_NO IS NULL
    THEN 'INS' /* - IF MULTIPLE PKs are there then concatenate them with AND */
    WHEN X.AC_ENTRY_SR_NO = Y.AC_ENTRY_SR_NO /* - IF MULTIPLE PKs are there then concatenate them with AND */
    AND (
      (
        (
          X.AC_BRANCH IS NULL AND NOT Y.AC_BRANCH IS NULL
        )
        OR (
          NOT X.AC_BRANCH IS NULL AND Y.AC_BRANCH IS NULL
        )
        OR (
          X.AC_BRANCH <> Y.AC_BRANCH
        )
      )
      OR (
        (
          X.AC_CCY IS NULL AND NOT Y.AC_CCY IS NULL
        )
        OR (
          NOT X.AC_CCY IS NULL AND Y.AC_CCY IS NULL
        )
        OR (
          X.AC_CCY <> Y.AC_CCY
        )
      )
      OR (
        (
          X.AC_NO IS NULL AND NOT Y.AC_NO IS NULL
        )
        OR (
          NOT X.AC_NO IS NULL AND Y.AC_NO IS NULL
        )
        OR (
          X.AC_NO <> Y.AC_NO
        )
      )
      OR (
        (
          X.AML_EXCEPTION IS NULL AND NOT Y.AML_EXCEPTION IS NULL
        )
        OR (
          NOT X.AML_EXCEPTION IS NULL AND Y.AML_EXCEPTION IS NULL
        )
        OR (
          X.AML_EXCEPTION <> Y.AML_EXCEPTION
        )
      )
      OR (
        (
          X.AMOUNT_TAG IS NULL AND NOT Y.AMOUNT_TAG IS NULL
        )
        OR (
          NOT X.AMOUNT_TAG IS NULL AND Y.AMOUNT_TAG IS NULL
        )
        OR (
          X.AMOUNT_TAG <> Y.AMOUNT_TAG
        )
      )
      OR (
        (
          X.AUTH_ID IS NULL AND NOT Y.AUTH_ID IS NULL
        )
        OR (
          NOT X.AUTH_ID IS NULL AND Y.AUTH_ID IS NULL
        )
        OR (
          X.AUTH_ID <> Y.AUTH_ID
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
          X.AVLDAYS IS NULL AND NOT Y.AVLDAYS IS NULL
        )
        OR (
          NOT X.AVLDAYS IS NULL AND Y.AVLDAYS IS NULL
        )
        OR (
          X.AVLDAYS <> Y.AVLDAYS
        )
      )
      OR (
        (
          X.BALANCE_UPD IS NULL AND NOT Y.BALANCE_UPD IS NULL
        )
        OR (
          NOT X.BALANCE_UPD IS NULL AND Y.BALANCE_UPD IS NULL
        )
        OR (
          X.BALANCE_UPD <> Y.BALANCE_UPD
        )
      )
      OR (
        (
          X.BANK_CODE IS NULL AND NOT Y.BANK_CODE IS NULL
        )
        OR (
          NOT X.BANK_CODE IS NULL AND Y.BANK_CODE IS NULL
        )
        OR (
          X.BANK_CODE <> Y.BANK_CODE
        )
      )
      OR (
        (
          X.BATCH_NO IS NULL AND NOT Y.BATCH_NO IS NULL
        )
        OR (
          NOT X.BATCH_NO IS NULL AND Y.BATCH_NO IS NULL
        )
        OR (
          X.BATCH_NO <> Y.BATCH_NO
        )
      )
      OR (
        (
          X.BM_AC_CCY IS NULL AND NOT Y.BM_AC_CCY IS NULL
        )
        OR (
          NOT X.BM_AC_CCY IS NULL AND Y.BM_AC_CCY IS NULL
        )
        OR (
          X.BM_AC_CCY <> Y.BM_AC_CCY
        )
      )
      OR (
        (
          X.BM_AMOUNT_TAG_MODULE IS NULL AND NOT Y.BM_AMOUNT_TAG_MODULE IS NULL
        )
        OR (
          NOT X.BM_AMOUNT_TAG_MODULE IS NULL AND Y.BM_AMOUNT_TAG_MODULE IS NULL
        )
        OR (
          X.BM_AMOUNT_TAG_MODULE <> Y.BM_AMOUNT_TAG_MODULE
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
          X.BM_DONT_SHOWIN_STMT IS NULL AND NOT Y.BM_DONT_SHOWIN_STMT IS NULL
        )
        OR (
          NOT X.BM_DONT_SHOWIN_STMT IS NULL AND Y.BM_DONT_SHOWIN_STMT IS NULL
        )
        OR (
          X.BM_DONT_SHOWIN_STMT <> Y.BM_DONT_SHOWIN_STMT
        )
      )
      OR (
        (
          X.BM_DRCR_IND IS NULL AND NOT Y.BM_DRCR_IND IS NULL
        )
        OR (
          NOT X.BM_DRCR_IND IS NULL AND Y.BM_DRCR_IND IS NULL
        )
        OR (
          X.BM_DRCR_IND <> Y.BM_DRCR_IND
        )
      )
      OR (
        (
          X.BM_IB IS NULL AND NOT Y.BM_IB IS NULL
        )
        OR (
          NOT X.BM_IB IS NULL AND Y.BM_IB IS NULL
        )
        OR (
          X.BM_IB <> Y.BM_IB
        )
      )
      OR (
        (
          X.BM_MIS_HEAD IS NULL AND NOT Y.BM_MIS_HEAD IS NULL
        )
        OR (
          NOT X.BM_MIS_HEAD IS NULL AND Y.BM_MIS_HEAD IS NULL
        )
        OR (
          X.BM_MIS_HEAD <> Y.BM_MIS_HEAD
        )
      )
      OR (
        (
          X.BM_MODULE IS NULL AND NOT Y.BM_MODULE IS NULL
        )
        OR (
          NOT X.BM_MODULE IS NULL AND Y.BM_MODULE IS NULL
        )
        OR (
          X.BM_MODULE <> Y.BM_MODULE
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
          X.BM_PRODUCT_ACCRUAL IS NULL AND NOT Y.BM_PRODUCT_ACCRUAL IS NULL
        )
        OR (
          NOT X.BM_PRODUCT_ACCRUAL IS NULL AND Y.BM_PRODUCT_ACCRUAL IS NULL
        )
        OR (
          X.BM_PRODUCT_ACCRUAL <> Y.BM_PRODUCT_ACCRUAL
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
          X.CATEGORY IS NULL AND NOT Y.CATEGORY IS NULL
        )
        OR (
          NOT X.CATEGORY IS NULL AND Y.CATEGORY IS NULL
        )
        OR (
          X.CATEGORY <> Y.CATEGORY
        )
      )
      OR (
        (
          X.CURR_NO IS NULL AND NOT Y.CURR_NO IS NULL
        )
        OR (
          NOT X.CURR_NO IS NULL AND Y.CURR_NO IS NULL
        )
        OR (
          X.CURR_NO <> Y.CURR_NO
        )
      )
      OR (
        (
          X.CUST_GL IS NULL AND NOT Y.CUST_GL IS NULL
        )
        OR (
          NOT X.CUST_GL IS NULL AND Y.CUST_GL IS NULL
        )
        OR (
          X.CUST_GL <> Y.CUST_GL
        )
      )
      OR (
        (
          X.CUST_GL_UPDATE IS NULL AND NOT Y.CUST_GL_UPDATE IS NULL
        )
        OR (
          NOT X.CUST_GL_UPDATE IS NULL AND Y.CUST_GL_UPDATE IS NULL
        )
        OR (
          X.CUST_GL_UPDATE <> Y.CUST_GL_UPDATE
        )
      )
      OR (
        (
          X.DELETE_STAT IS NULL AND NOT Y.DELETE_STAT IS NULL
        )
        OR (
          NOT X.DELETE_STAT IS NULL AND Y.DELETE_STAT IS NULL
        )
        OR (
          X.DELETE_STAT <> Y.DELETE_STAT
        )
      )
      OR (
        (
          X.DISTRIBUTED IS NULL AND NOT Y.DISTRIBUTED IS NULL
        )
        OR (
          NOT X.DISTRIBUTED IS NULL AND Y.DISTRIBUTED IS NULL
        )
        OR (
          X.DISTRIBUTED <> Y.DISTRIBUTED
        )
      )
      OR (
        (
          X.DONT_SHOWIN_STMT IS NULL AND NOT Y.DONT_SHOWIN_STMT IS NULL
        )
        OR (
          NOT X.DONT_SHOWIN_STMT IS NULL AND Y.DONT_SHOWIN_STMT IS NULL
        )
        OR (
          X.DONT_SHOWIN_STMT <> Y.DONT_SHOWIN_STMT
        )
      )
      OR (
        (
          X.DRCR_IND IS NULL AND NOT Y.DRCR_IND IS NULL
        )
        OR (
          NOT X.DRCR_IND IS NULL AND Y.DRCR_IND IS NULL
        )
        OR (
          X.DRCR_IND <> Y.DRCR_IND
        )
      )
      OR (
        (
          X.ENTRY_SEQ_NO IS NULL AND NOT Y.ENTRY_SEQ_NO IS NULL
        )
        OR (
          NOT X.ENTRY_SEQ_NO IS NULL AND Y.ENTRY_SEQ_NO IS NULL
        )
        OR (
          X.ENTRY_SEQ_NO <> Y.ENTRY_SEQ_NO
        )
      )
      OR (
        (
          X.EVENT IS NULL AND NOT Y.EVENT IS NULL
        )
        OR (
          NOT X.EVENT IS NULL AND Y.EVENT IS NULL
        )
        OR (
          X.EVENT <> Y.EVENT
        )
      )
      OR (
        (
          X.EVENT_SR_NO IS NULL AND NOT Y.EVENT_SR_NO IS NULL
        )
        OR (
          NOT X.EVENT_SR_NO IS NULL AND Y.EVENT_SR_NO IS NULL
        )
        OR (
          X.EVENT_SR_NO <> Y.EVENT_SR_NO
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
          X.EXTERNAL_REF_NO IS NULL AND NOT Y.EXTERNAL_REF_NO IS NULL
        )
        OR (
          NOT X.EXTERNAL_REF_NO IS NULL AND Y.EXTERNAL_REF_NO IS NULL
        )
        OR (
          X.EXTERNAL_REF_NO <> Y.EXTERNAL_REF_NO
        )
      )
      OR (
        (
          X.FCY_AMOUNT IS NULL AND NOT Y.FCY_AMOUNT IS NULL
        )
        OR (
          NOT X.FCY_AMOUNT IS NULL AND Y.FCY_AMOUNT IS NULL
        )
        OR (
          X.FCY_AMOUNT <> Y.FCY_AMOUNT
        )
      )
      OR (
        (
          X.FINANCIAL_CYCLE IS NULL AND NOT Y.FINANCIAL_CYCLE IS NULL
        )
        OR (
          NOT X.FINANCIAL_CYCLE IS NULL AND Y.FINANCIAL_CYCLE IS NULL
        )
        OR (
          X.FINANCIAL_CYCLE <> Y.FINANCIAL_CYCLE
        )
      )
      OR (
        (
          X.FLG_POSITION_STATUS IS NULL AND NOT Y.FLG_POSITION_STATUS IS NULL
        )
        OR (
          NOT X.FLG_POSITION_STATUS IS NULL AND Y.FLG_POSITION_STATUS IS NULL
        )
        OR (
          X.FLG_POSITION_STATUS <> Y.FLG_POSITION_STATUS
        )
      )
      OR (
        (
          X.GLMIS_UPDATE_FLAG IS NULL AND NOT Y.GLMIS_UPDATE_FLAG IS NULL
        )
        OR (
          NOT X.GLMIS_UPDATE_FLAG IS NULL AND Y.GLMIS_UPDATE_FLAG IS NULL
        )
        OR (
          X.GLMIS_UPDATE_FLAG <> Y.GLMIS_UPDATE_FLAG
        )
      )
      OR (
        (
          X.GLMIS_UPDATE_STATUS IS NULL AND NOT Y.GLMIS_UPDATE_STATUS IS NULL
        )
        OR (
          NOT X.GLMIS_UPDATE_STATUS IS NULL AND Y.GLMIS_UPDATE_STATUS IS NULL
        )
        OR (
          X.GLMIS_UPDATE_STATUS <> Y.GLMIS_UPDATE_STATUS
        )
      )
      OR (
        (
          X.GLMIS_VAL_UPD_FLAG IS NULL AND NOT Y.GLMIS_VAL_UPD_FLAG IS NULL
        )
        OR (
          NOT X.GLMIS_VAL_UPD_FLAG IS NULL AND Y.GLMIS_VAL_UPD_FLAG IS NULL
        )
        OR (
          X.GLMIS_VAL_UPD_FLAG <> Y.GLMIS_VAL_UPD_FLAG
        )
      )
      OR (
        (
          X.IB IS NULL AND NOT Y.IB IS NULL
        )
        OR (
          NOT X.IB IS NULL AND Y.IB IS NULL
        )
        OR (
          X.IB <> Y.IB
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
          X.IL_BVT_PROCESSED IS NULL AND NOT Y.IL_BVT_PROCESSED IS NULL
        )
        OR (
          NOT X.IL_BVT_PROCESSED IS NULL AND Y.IL_BVT_PROCESSED IS NULL
        )
        OR (
          X.IL_BVT_PROCESSED <> Y.IL_BVT_PROCESSED
        )
      )
      OR (
        (
          X.INSTRUMENT_CODE IS NULL AND NOT Y.INSTRUMENT_CODE IS NULL
        )
        OR (
          NOT X.INSTRUMENT_CODE IS NULL AND Y.INSTRUMENT_CODE IS NULL
        )
        OR (
          X.INSTRUMENT_CODE <> Y.INSTRUMENT_CODE
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
          X.MIS_FLAG IS NULL AND NOT Y.MIS_FLAG IS NULL
        )
        OR (
          NOT X.MIS_FLAG IS NULL AND Y.MIS_FLAG IS NULL
        )
        OR (
          X.MIS_FLAG <> Y.MIS_FLAG
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
          X.MIS_SPREAD IS NULL AND NOT Y.MIS_SPREAD IS NULL
        )
        OR (
          NOT X.MIS_SPREAD IS NULL AND Y.MIS_SPREAD IS NULL
        )
        OR (
          X.MIS_SPREAD <> Y.MIS_SPREAD
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
          X.NODE IS NULL AND NOT Y.NODE IS NULL
        )
        OR (
          NOT X.NODE IS NULL AND Y.NODE IS NULL
        )
        OR (
          X.NODE <> Y.NODE
        )
      )
      OR (
        (
          X.NODE_SR_NO IS NULL AND NOT Y.NODE_SR_NO IS NULL
        )
        OR (
          NOT X.NODE_SR_NO IS NULL AND Y.NODE_SR_NO IS NULL
        )
        OR (
          X.NODE_SR_NO <> Y.NODE_SR_NO
        )
      )
      OR (
        (
          X.ON_LINE IS NULL AND NOT Y.ON_LINE IS NULL
        )
        OR (
          NOT X.ON_LINE IS NULL AND Y.ON_LINE IS NULL
        )
        OR (
          X.ON_LINE <> Y.ON_LINE
        )
      )
      OR (
        (
          X.ORIG_PNL_GL IS NULL AND NOT Y.ORIG_PNL_GL IS NULL
        )
        OR (
          NOT X.ORIG_PNL_GL IS NULL AND Y.ORIG_PNL_GL IS NULL
        )
        OR (
          X.ORIG_PNL_GL <> Y.ORIG_PNL_GL
        )
      )
      OR (
        (
          X.PERIOD_CODE IS NULL AND NOT Y.PERIOD_CODE IS NULL
        )
        OR (
          NOT X.PERIOD_CODE IS NULL AND Y.PERIOD_CODE IS NULL
        )
        OR (
          X.PERIOD_CODE <> Y.PERIOD_CODE
        )
      )
      OR (
        (
          X.PRINT_STAT IS NULL AND NOT Y.PRINT_STAT IS NULL
        )
        OR (
          NOT X.PRINT_STAT IS NULL AND Y.PRINT_STAT IS NULL
        )
        OR (
          X.PRINT_STAT <> Y.PRINT_STAT
        )
      )
      OR (
        (
          X.PROCESSED_FLAG IS NULL AND NOT Y.PROCESSED_FLAG IS NULL
        )
        OR (
          NOT X.PROCESSED_FLAG IS NULL AND Y.PROCESSED_FLAG IS NULL
        )
        OR (
          X.PROCESSED_FLAG <> Y.PROCESSED_FLAG
        )
      )
      OR (
        (
          X.PRODUCT IS NULL AND NOT Y.PRODUCT IS NULL
        )
        OR (
          NOT X.PRODUCT IS NULL AND Y.PRODUCT IS NULL
        )
        OR (
          X.PRODUCT <> Y.PRODUCT
        )
      )
      OR (
        (
          X.PRODUCT_ACCRUAL IS NULL AND NOT Y.PRODUCT_ACCRUAL IS NULL
        )
        OR (
          NOT X.PRODUCT_ACCRUAL IS NULL AND Y.PRODUCT_ACCRUAL IS NULL
        )
        OR (
          X.PRODUCT_ACCRUAL <> Y.PRODUCT_ACCRUAL
        )
      )
      OR (
        (
          X.REL_EVENT_SR_NO IS NULL AND NOT Y.REL_EVENT_SR_NO IS NULL
        )
        OR (
          NOT X.REL_EVENT_SR_NO IS NULL AND Y.REL_EVENT_SR_NO IS NULL
        )
        OR (
          X.REL_EVENT_SR_NO <> Y.REL_EVENT_SR_NO
        )
      )
      OR (
        (
          X.RELATED_ACCOUNT IS NULL AND NOT Y.RELATED_ACCOUNT IS NULL
        )
        OR (
          NOT X.RELATED_ACCOUNT IS NULL AND Y.RELATED_ACCOUNT IS NULL
        )
        OR (
          X.RELATED_ACCOUNT <> Y.RELATED_ACCOUNT
        )
      )
      OR (
        (
          X.RELATED_CUSTOMER IS NULL AND NOT Y.RELATED_CUSTOMER IS NULL
        )
        OR (
          NOT X.RELATED_CUSTOMER IS NULL AND Y.RELATED_CUSTOMER IS NULL
        )
        OR (
          X.RELATED_CUSTOMER <> Y.RELATED_CUSTOMER
        )
      )
      OR (
        (
          X.RELATED_REFERENCE IS NULL AND NOT Y.RELATED_REFERENCE IS NULL
        )
        OR (
          NOT X.RELATED_REFERENCE IS NULL AND Y.RELATED_REFERENCE IS NULL
        )
        OR (
          X.RELATED_REFERENCE <> Y.RELATED_REFERENCE
        )
      )
      OR (
        (
          X.SK_AC_ENTRY_SR_NO IS NULL AND NOT Y.SK_AC_ENTRY_SR_NO IS NULL
        )
        OR (
          NOT X.SK_AC_ENTRY_SR_NO IS NULL AND Y.SK_AC_ENTRY_SR_NO IS NULL
        )
        OR (
          X.SK_AC_ENTRY_SR_NO <> Y.SK_AC_ENTRY_SR_NO
        )
      )
      OR (
        (
          X.SK_AC_NO IS NULL AND NOT Y.SK_AC_NO IS NULL
        )
        OR (
          NOT X.SK_AC_NO IS NULL AND Y.SK_AC_NO IS NULL
        )
        OR (
          X.SK_AC_NO <> Y.SK_AC_NO
        )
      )
      OR (
        (
          X.SK_RELATED_CUSTOMER IS NULL AND NOT Y.SK_RELATED_CUSTOMER IS NULL
        )
        OR (
          NOT X.SK_RELATED_CUSTOMER IS NULL AND Y.SK_RELATED_CUSTOMER IS NULL
        )
        OR (
          X.SK_RELATED_CUSTOMER <> Y.SK_RELATED_CUSTOMER
        )
      )
      OR (
        (
          X.SK_AC_BRANCH IS NULL AND NOT Y.SK_AC_BRANCH IS NULL
        )
        OR (
          NOT X.SK_AC_BRANCH IS NULL AND Y.SK_AC_BRANCH IS NULL
        )
        OR (
          X.SK_AC_BRANCH <> Y.SK_AC_BRANCH
        )
      )
      OR (
        (
          X.SK_AUTH_ID IS NULL AND NOT Y.SK_AUTH_ID IS NULL
        )
        OR (
          NOT X.SK_AUTH_ID IS NULL AND Y.SK_AUTH_ID IS NULL
        )
        OR (
          X.SK_AUTH_ID <> Y.SK_AUTH_ID
        )
      )
      OR (
        (
          X.SK_USER_ID IS NULL AND NOT Y.SK_USER_ID IS NULL
        )
        OR (
          NOT X.SK_USER_ID IS NULL AND Y.SK_USER_ID IS NULL
        )
        OR (
          X.SK_USER_ID <> Y.SK_USER_ID
        )
      )
      OR (
        (
          X.SK_PRODUCT IS NULL AND NOT Y.SK_PRODUCT IS NULL
        )
        OR (
          NOT X.SK_PRODUCT IS NULL AND Y.SK_PRODUCT IS NULL
        )
        OR (
          X.SK_PRODUCT <> Y.SK_PRODUCT
        )
      )
      OR (
        (
          X.SK_RELATED_ACCOUNT_CASA IS NULL AND NOT Y.SK_RELATED_ACCOUNT_CASA IS NULL
        )
        OR (
          NOT X.SK_RELATED_ACCOUNT_CASA IS NULL AND Y.SK_RELATED_ACCOUNT_CASA IS NULL
        )
        OR (
          X.SK_RELATED_ACCOUNT_CASA <> Y.SK_RELATED_ACCOUNT_CASA
        )
      )
      OR (
        (
          X.SK_RELATED_ACCOUNT_LOAN IS NULL AND NOT Y.SK_RELATED_ACCOUNT_LOAN IS NULL
        )
        OR (
          NOT X.SK_RELATED_ACCOUNT_LOAN IS NULL AND Y.SK_RELATED_ACCOUNT_LOAN IS NULL
        )
        OR (
          X.SK_RELATED_ACCOUNT_LOAN <> Y.SK_RELATED_ACCOUNT_LOAN
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
          X.STMT_DT IS NULL AND NOT Y.STMT_DT IS NULL
        )
        OR (
          NOT X.STMT_DT IS NULL AND Y.STMT_DT IS NULL
        )
        OR (
          X.STMT_DT <> Y.STMT_DT
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
          X.TRN_REF_NO IS NULL AND NOT Y.TRN_REF_NO IS NULL
        )
        OR (
          NOT X.TRN_REF_NO IS NULL AND Y.TRN_REF_NO IS NULL
        )
        OR (
          X.TRN_REF_NO <> Y.TRN_REF_NO
        )
      )
      OR (
        (
          X.TXN_DT_TIME IS NULL AND NOT Y.TXN_DT_TIME IS NULL
        )
        OR (
          NOT X.TXN_DT_TIME IS NULL AND Y.TXN_DT_TIME IS NULL
        )
        OR (
          X.TXN_DT_TIME <> Y.TXN_DT_TIME
        )
      )
      OR (
        (
          X.TXN_INIT_DATE IS NULL AND NOT Y.TXN_INIT_DATE IS NULL
        )
        OR (
          NOT X.TXN_INIT_DATE IS NULL AND Y.TXN_INIT_DATE IS NULL
        )
        OR (
          X.TXN_INIT_DATE <> Y.TXN_INIT_DATE
        )
      )
      OR (
        (
          X.TYPE IS NULL AND NOT Y.TYPE IS NULL
        )
        OR (
          NOT X.TYPE IS NULL AND Y.TYPE IS NULL
        )
        OR (
          X.TYPE <> Y.TYPE
        )
      )
      OR (
        (
          X.UPDACT IS NULL AND NOT Y.UPDACT IS NULL
        )
        OR (
          NOT X.UPDACT IS NULL AND Y.UPDACT IS NULL
        )
        OR (
          X.UPDACT <> Y.UPDACT
        )
      )
      OR (
        (
          X.USER_ID IS NULL AND NOT Y.USER_ID IS NULL
        )
        OR (
          NOT X.USER_ID IS NULL AND Y.USER_ID IS NULL
        )
        OR (
          X.USER_ID <> Y.USER_ID
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
          X.VDBAL_UPDATE_FLAG IS NULL AND NOT Y.VDBAL_UPDATE_FLAG IS NULL
        )
        OR (
          NOT X.VDBAL_UPDATE_FLAG IS NULL AND Y.VDBAL_UPDATE_FLAG IS NULL
        )
        OR (
          X.VDBAL_UPDATE_FLAG <> Y.VDBAL_UPDATE_FLAG
        )
      )
    )
    THEN 'UPD'
    WHEN X.AC_ENTRY_SR_NO IS NULL
    THEN 'DEL' /* - IF MULTIPLE PKs are there then concatenate them with AND */
  END AS OPT_FLAG
FROM DEVT_STG_FLX.ACTB_DAILY_LOG AS X
FULL JOIN DEVT_STG_FLX.ACTB_DAILY_LOG_BACKUP AS Y
  ON X.AC_ENTRY_SR_NO = Y.AC_ENTRY_SR_NO /* - IF MULTIPLE PKs are there then concatenate them with AND */
WHERE
  NOT OPT_FLAG IS NULL