CREATE OR REPLACE VIEW DEVV_STG_FLX.STTM_BRANCH_get_delta AS LOCKING ROW FOR ACCESS
SELECT
  COALESCE(X.BRANCH_CODE, Y.BRANCH_CODE) AS BRANCH_CODE,
  X.ALLOW_CORPORATE_ACCESS,
  X.ATM_SUSPENSE_GL,
  X.AUTH_STAT,
  X.AUTO_AUTH,
  X.BACK_VALUE_DAYS,
  X.BACK_VALUED_CHK_REQ,
  X.BANK_CODE,
  X.BM_AUTH_STAT,
  X.BM_MOD_NO,
  X.BM_ONCE_AUTH,
  X.BM_RECORD_STAT,
  X.BM_SECTOR_CODE,
  X.BRANCH_ADDR1,
  X.BRANCH_ADDR2,
  X.BRANCH_ADDR3,
  X.BRANCH_LCY,
  X.BRANCH_NAME,
  X.BRN_AVAIL_STAT,
  X.CASA_AMTBLK_TRNCODE,
  X.CHECKER_DT_STAMP,
  X.CHECKER_ID,
  X.CHEQUE_STALE_DAYS,
  X.CHQNO_MASK,
  X.CIF_ID,
  X.CLEARING_ACC,
  X.CLEARING_BANK_CODE,
  X.CLEARING_BRN,
  X.CLG_BRN_CODE,
  X.COD_ATM_BRANCH,
  X.COD_ATM_STOP,
  X.COD_CUST_TRANSFER,
  X.COD_IB_TRN_CODE,
  X.COD_INST_ID,
  X.COD_START_TANK,
  X.CONSOL_TAX_CERT_REQD,
  X.CONT_SUSPENSE_GL_FCY,
  X.CONTINGENT_SUSPENSE_GLSL,
  X.CONVERSION_GL,
  X.CONVERSION_TXNCODE,
  X.COUNTRY_CODE,
  X.CRSUS_PROD,
  X.CURRENT_CYCLE,
  X.CURRENT_PERIOD,
  X.CURRENT_TAX_CYCLE,
  X.DEF_BANK_OPER_CODE,
  X.DEFERRED_STMT,
  X.DEFERRED_STMT_STATUS,
  X.DRSUS_PROD,
  X.DSN_NAME,
  X.ELCM_REPLICATION,
  X.END_OF_INPUT,
  X.ENTERPRISE_GL,
  X.EOC_STAGE,
  X.FILE_CONS_TRN_CODE,
  X.FUND_BRANCH,
  X.GEN_MT103,
  X.GEN_MT103P,
  X.GENERATE,
  X.GL_CLASS,
  X.HOST_NAME,
  X.IBAN_MASK_ACCOUNT_NUMBER,
  X.IBAN_MASK_BANK_CODE,
  X.ICEOD_STATUS,
  X.INDIVIDUAL_TAX_CERT_REQD,
  X.INTERDICT_CHECK_REQD,
  X.INTERDICT_TIME_OUT,
  X.INTERNAL_SWAP_CUSTOMER,
  X.JOB_STAT,
  X.LDAP_TEMPLATE,
  X.LMTEXP_NOTIFY_DAYS,
  X.MAKER_DT_STAMP,
  X.MAKER_ID,
  X.MAX_CONT_SUSPENSE_AMT,
  X.MAX_REAL_SUSPENSE_AMT,
  X.MINOR_AGE,
  X.MIS_CCY_MISMATCH_GROUP,
  X.MOD_NO,
  X.MSG_GEN_DAYS,
  X.NETTING_SUSPENSE_GL,
  X.NOTICE_PERIOD,
  X.OFFSET_CLEARING_ACCOUNT,
  X.OFFSET_HOURS,
  X.OFFSET_HR,
  X.OFFSET_MIN,
  X.OFFSET_MINS,
  X.ONCE_AUTH,
  X.PARENT_BRANCH,
  X.PC_CLEARING_BRN,
  X.PL_SPLIT_REQD,
  X.POSITION_ASSET_GL,
  X.POSITION_LIABILITY_GL,
  X.PROCEED_WITHOUT_FLOAT,
  X.PROVISIONING_FREQUENCY,
  X.RECORD_STAT,
  X.REFERRAL_HR,
  X.REFERRAL_MIN,
  X.REGIONAL_OFFICE,
  X.REP_HISTORY_PERIOD,
  X.REPORT_DSN,
  X.REV_SUSPENSE_ENTRY_DAYS,
  X.REVAL_BRANCH,
  X.ROUTING_NO,
  X.SECTOR_CODE,
  X.SK_BRANCH_ADDR1,
  X.SK_BRANCH_ADDR3_FAX,
  X.SK_BRANCH_ADDR3_TELEPHONE,
  X.SK_BRANCH_CODE,
  X.SK_PARENT_BRANCH,
  X.SK_SWIFT_ADDR,
  X.SK_TELEX_ADDR,
  X.STATUS_PROCESSING_BASIS,
  X.SUSPENSE_BATCH_NO,
  X.SUSPENSE_ENTRY_REQD,
  X.SUSPENSE_GL_FCY,
  X.SUSPENSE_GLSL,
  X.SUSPENSE_TXN_CODE,
  X.SWIFT_ADDR,
  X.TAX_CERT_DAY,
  X.TAX_CERT_FREQ,
  X.TELEX_ADDR,
  X.TIME_LEVEL,
  X.TRACK_PY_PNL_ADJUSTMENT,
  X.TRANSACTION_CODE,
  X.TRANSFER_TYPE,
  X.UNCOLLECTED_FUNDS_BASIS,
  X.WALKIN_CUSTOMER,
  X.WEEK_HOL1,
  X.WEEK_HOL2,
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
    WHEN Y.BRANCH_CODE IS NULL
    THEN 'INS' /* - IF MULTIPLE PKs are there then concatenate them with AND */
    WHEN X.BRANCH_CODE = Y.BRANCH_CODE /* - IF MULTIPLE PKs are there then concatenate them with AND */
    AND (
      (
        (
          X.ALLOW_CORPORATE_ACCESS IS NULL AND NOT Y.ALLOW_CORPORATE_ACCESS IS NULL
        )
        OR (
          NOT X.ALLOW_CORPORATE_ACCESS IS NULL AND Y.ALLOW_CORPORATE_ACCESS IS NULL
        )
        OR (
          X.ALLOW_CORPORATE_ACCESS <> Y.ALLOW_CORPORATE_ACCESS
        )
      )
      OR (
        (
          X.ATM_SUSPENSE_GL IS NULL AND NOT Y.ATM_SUSPENSE_GL IS NULL
        )
        OR (
          NOT X.ATM_SUSPENSE_GL IS NULL AND Y.ATM_SUSPENSE_GL IS NULL
        )
        OR (
          X.ATM_SUSPENSE_GL <> Y.ATM_SUSPENSE_GL
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
          X.AUTO_AUTH IS NULL AND NOT Y.AUTO_AUTH IS NULL
        )
        OR (
          NOT X.AUTO_AUTH IS NULL AND Y.AUTO_AUTH IS NULL
        )
        OR (
          X.AUTO_AUTH <> Y.AUTO_AUTH
        )
      )
      OR (
        (
          X.BACK_VALUE_DAYS IS NULL AND NOT Y.BACK_VALUE_DAYS IS NULL
        )
        OR (
          NOT X.BACK_VALUE_DAYS IS NULL AND Y.BACK_VALUE_DAYS IS NULL
        )
        OR (
          X.BACK_VALUE_DAYS <> Y.BACK_VALUE_DAYS
        )
      )
      OR (
        (
          X.BACK_VALUED_CHK_REQ IS NULL AND NOT Y.BACK_VALUED_CHK_REQ IS NULL
        )
        OR (
          NOT X.BACK_VALUED_CHK_REQ IS NULL AND Y.BACK_VALUED_CHK_REQ IS NULL
        )
        OR (
          X.BACK_VALUED_CHK_REQ <> Y.BACK_VALUED_CHK_REQ
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
          X.BM_SECTOR_CODE IS NULL AND NOT Y.BM_SECTOR_CODE IS NULL
        )
        OR (
          NOT X.BM_SECTOR_CODE IS NULL AND Y.BM_SECTOR_CODE IS NULL
        )
        OR (
          X.BM_SECTOR_CODE <> Y.BM_SECTOR_CODE
        )
      )
      OR (
        (
          X.BRANCH_ADDR1 IS NULL AND NOT Y.BRANCH_ADDR1 IS NULL
        )
        OR (
          NOT X.BRANCH_ADDR1 IS NULL AND Y.BRANCH_ADDR1 IS NULL
        )
        OR (
          X.BRANCH_ADDR1 <> Y.BRANCH_ADDR1
        )
      )
      OR (
        (
          X.BRANCH_ADDR2 IS NULL AND NOT Y.BRANCH_ADDR2 IS NULL
        )
        OR (
          NOT X.BRANCH_ADDR2 IS NULL AND Y.BRANCH_ADDR2 IS NULL
        )
        OR (
          X.BRANCH_ADDR2 <> Y.BRANCH_ADDR2
        )
      )
      OR (
        (
          X.BRANCH_ADDR3 IS NULL AND NOT Y.BRANCH_ADDR3 IS NULL
        )
        OR (
          NOT X.BRANCH_ADDR3 IS NULL AND Y.BRANCH_ADDR3 IS NULL
        )
        OR (
          X.BRANCH_ADDR3 <> Y.BRANCH_ADDR3
        )
      )
      OR (
        (
          X.BRANCH_LCY IS NULL AND NOT Y.BRANCH_LCY IS NULL
        )
        OR (
          NOT X.BRANCH_LCY IS NULL AND Y.BRANCH_LCY IS NULL
        )
        OR (
          X.BRANCH_LCY <> Y.BRANCH_LCY
        )
      )
      OR (
        (
          X.BRANCH_NAME IS NULL AND NOT Y.BRANCH_NAME IS NULL
        )
        OR (
          NOT X.BRANCH_NAME IS NULL AND Y.BRANCH_NAME IS NULL
        )
        OR (
          X.BRANCH_NAME <> Y.BRANCH_NAME
        )
      )
      OR (
        (
          X.BRN_AVAIL_STAT IS NULL AND NOT Y.BRN_AVAIL_STAT IS NULL
        )
        OR (
          NOT X.BRN_AVAIL_STAT IS NULL AND Y.BRN_AVAIL_STAT IS NULL
        )
        OR (
          X.BRN_AVAIL_STAT <> Y.BRN_AVAIL_STAT
        )
      )
      OR (
        (
          X.CASA_AMTBLK_TRNCODE IS NULL AND NOT Y.CASA_AMTBLK_TRNCODE IS NULL
        )
        OR (
          NOT X.CASA_AMTBLK_TRNCODE IS NULL AND Y.CASA_AMTBLK_TRNCODE IS NULL
        )
        OR (
          X.CASA_AMTBLK_TRNCODE <> Y.CASA_AMTBLK_TRNCODE
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
          X.CHEQUE_STALE_DAYS IS NULL AND NOT Y.CHEQUE_STALE_DAYS IS NULL
        )
        OR (
          NOT X.CHEQUE_STALE_DAYS IS NULL AND Y.CHEQUE_STALE_DAYS IS NULL
        )
        OR (
          X.CHEQUE_STALE_DAYS <> Y.CHEQUE_STALE_DAYS
        )
      )
      OR (
        (
          X.CHQNO_MASK IS NULL AND NOT Y.CHQNO_MASK IS NULL
        )
        OR (
          NOT X.CHQNO_MASK IS NULL AND Y.CHQNO_MASK IS NULL
        )
        OR (
          X.CHQNO_MASK <> Y.CHQNO_MASK
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
          X.CLEARING_ACC IS NULL AND NOT Y.CLEARING_ACC IS NULL
        )
        OR (
          NOT X.CLEARING_ACC IS NULL AND Y.CLEARING_ACC IS NULL
        )
        OR (
          X.CLEARING_ACC <> Y.CLEARING_ACC
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
          X.CLEARING_BRN IS NULL AND NOT Y.CLEARING_BRN IS NULL
        )
        OR (
          NOT X.CLEARING_BRN IS NULL AND Y.CLEARING_BRN IS NULL
        )
        OR (
          X.CLEARING_BRN <> Y.CLEARING_BRN
        )
      )
      OR (
        (
          X.CLG_BRN_CODE IS NULL AND NOT Y.CLG_BRN_CODE IS NULL
        )
        OR (
          NOT X.CLG_BRN_CODE IS NULL AND Y.CLG_BRN_CODE IS NULL
        )
        OR (
          X.CLG_BRN_CODE <> Y.CLG_BRN_CODE
        )
      )
      OR (
        (
          X.COD_ATM_BRANCH IS NULL AND NOT Y.COD_ATM_BRANCH IS NULL
        )
        OR (
          NOT X.COD_ATM_BRANCH IS NULL AND Y.COD_ATM_BRANCH IS NULL
        )
        OR (
          X.COD_ATM_BRANCH <> Y.COD_ATM_BRANCH
        )
      )
      OR (
        (
          X.COD_ATM_STOP IS NULL AND NOT Y.COD_ATM_STOP IS NULL
        )
        OR (
          NOT X.COD_ATM_STOP IS NULL AND Y.COD_ATM_STOP IS NULL
        )
        OR (
          X.COD_ATM_STOP <> Y.COD_ATM_STOP
        )
      )
      OR (
        (
          X.COD_CUST_TRANSFER IS NULL AND NOT Y.COD_CUST_TRANSFER IS NULL
        )
        OR (
          NOT X.COD_CUST_TRANSFER IS NULL AND Y.COD_CUST_TRANSFER IS NULL
        )
        OR (
          X.COD_CUST_TRANSFER <> Y.COD_CUST_TRANSFER
        )
      )
      OR (
        (
          X.COD_IB_TRN_CODE IS NULL AND NOT Y.COD_IB_TRN_CODE IS NULL
        )
        OR (
          NOT X.COD_IB_TRN_CODE IS NULL AND Y.COD_IB_TRN_CODE IS NULL
        )
        OR (
          X.COD_IB_TRN_CODE <> Y.COD_IB_TRN_CODE
        )
      )
      OR (
        (
          X.COD_INST_ID IS NULL AND NOT Y.COD_INST_ID IS NULL
        )
        OR (
          NOT X.COD_INST_ID IS NULL AND Y.COD_INST_ID IS NULL
        )
        OR (
          X.COD_INST_ID <> Y.COD_INST_ID
        )
      )
      OR (
        (
          X.COD_START_TANK IS NULL AND NOT Y.COD_START_TANK IS NULL
        )
        OR (
          NOT X.COD_START_TANK IS NULL AND Y.COD_START_TANK IS NULL
        )
        OR (
          X.COD_START_TANK <> Y.COD_START_TANK
        )
      )
      OR (
        (
          X.CONSOL_TAX_CERT_REQD IS NULL AND NOT Y.CONSOL_TAX_CERT_REQD IS NULL
        )
        OR (
          NOT X.CONSOL_TAX_CERT_REQD IS NULL AND Y.CONSOL_TAX_CERT_REQD IS NULL
        )
        OR (
          X.CONSOL_TAX_CERT_REQD <> Y.CONSOL_TAX_CERT_REQD
        )
      )
      OR (
        (
          X.CONT_SUSPENSE_GL_FCY IS NULL AND NOT Y.CONT_SUSPENSE_GL_FCY IS NULL
        )
        OR (
          NOT X.CONT_SUSPENSE_GL_FCY IS NULL AND Y.CONT_SUSPENSE_GL_FCY IS NULL
        )
        OR (
          X.CONT_SUSPENSE_GL_FCY <> Y.CONT_SUSPENSE_GL_FCY
        )
      )
      OR (
        (
          X.CONTINGENT_SUSPENSE_GLSL IS NULL AND NOT Y.CONTINGENT_SUSPENSE_GLSL IS NULL
        )
        OR (
          NOT X.CONTINGENT_SUSPENSE_GLSL IS NULL AND Y.CONTINGENT_SUSPENSE_GLSL IS NULL
        )
        OR (
          X.CONTINGENT_SUSPENSE_GLSL <> Y.CONTINGENT_SUSPENSE_GLSL
        )
      )
      OR (
        (
          X.CONVERSION_GL IS NULL AND NOT Y.CONVERSION_GL IS NULL
        )
        OR (
          NOT X.CONVERSION_GL IS NULL AND Y.CONVERSION_GL IS NULL
        )
        OR (
          X.CONVERSION_GL <> Y.CONVERSION_GL
        )
      )
      OR (
        (
          X.CONVERSION_TXNCODE IS NULL AND NOT Y.CONVERSION_TXNCODE IS NULL
        )
        OR (
          NOT X.CONVERSION_TXNCODE IS NULL AND Y.CONVERSION_TXNCODE IS NULL
        )
        OR (
          X.CONVERSION_TXNCODE <> Y.CONVERSION_TXNCODE
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
          X.CRSUS_PROD IS NULL AND NOT Y.CRSUS_PROD IS NULL
        )
        OR (
          NOT X.CRSUS_PROD IS NULL AND Y.CRSUS_PROD IS NULL
        )
        OR (
          X.CRSUS_PROD <> Y.CRSUS_PROD
        )
      )
      OR (
        (
          X.CURRENT_CYCLE IS NULL AND NOT Y.CURRENT_CYCLE IS NULL
        )
        OR (
          NOT X.CURRENT_CYCLE IS NULL AND Y.CURRENT_CYCLE IS NULL
        )
        OR (
          X.CURRENT_CYCLE <> Y.CURRENT_CYCLE
        )
      )
      OR (
        (
          X.CURRENT_PERIOD IS NULL AND NOT Y.CURRENT_PERIOD IS NULL
        )
        OR (
          NOT X.CURRENT_PERIOD IS NULL AND Y.CURRENT_PERIOD IS NULL
        )
        OR (
          X.CURRENT_PERIOD <> Y.CURRENT_PERIOD
        )
      )
      OR (
        (
          X.CURRENT_TAX_CYCLE IS NULL AND NOT Y.CURRENT_TAX_CYCLE IS NULL
        )
        OR (
          NOT X.CURRENT_TAX_CYCLE IS NULL AND Y.CURRENT_TAX_CYCLE IS NULL
        )
        OR (
          X.CURRENT_TAX_CYCLE <> Y.CURRENT_TAX_CYCLE
        )
      )
      OR (
        (
          X.DEF_BANK_OPER_CODE IS NULL AND NOT Y.DEF_BANK_OPER_CODE IS NULL
        )
        OR (
          NOT X.DEF_BANK_OPER_CODE IS NULL AND Y.DEF_BANK_OPER_CODE IS NULL
        )
        OR (
          X.DEF_BANK_OPER_CODE <> Y.DEF_BANK_OPER_CODE
        )
      )
      OR (
        (
          X.DEFERRED_STMT IS NULL AND NOT Y.DEFERRED_STMT IS NULL
        )
        OR (
          NOT X.DEFERRED_STMT IS NULL AND Y.DEFERRED_STMT IS NULL
        )
        OR (
          X.DEFERRED_STMT <> Y.DEFERRED_STMT
        )
      )
      OR (
        (
          X.DEFERRED_STMT_STATUS IS NULL AND NOT Y.DEFERRED_STMT_STATUS IS NULL
        )
        OR (
          NOT X.DEFERRED_STMT_STATUS IS NULL AND Y.DEFERRED_STMT_STATUS IS NULL
        )
        OR (
          X.DEFERRED_STMT_STATUS <> Y.DEFERRED_STMT_STATUS
        )
      )
      OR (
        (
          X.DRSUS_PROD IS NULL AND NOT Y.DRSUS_PROD IS NULL
        )
        OR (
          NOT X.DRSUS_PROD IS NULL AND Y.DRSUS_PROD IS NULL
        )
        OR (
          X.DRSUS_PROD <> Y.DRSUS_PROD
        )
      )
      OR (
        (
          X.DSN_NAME IS NULL AND NOT Y.DSN_NAME IS NULL
        )
        OR (
          NOT X.DSN_NAME IS NULL AND Y.DSN_NAME IS NULL
        )
        OR (
          X.DSN_NAME <> Y.DSN_NAME
        )
      )
      OR (
        (
          X.ELCM_REPLICATION IS NULL AND NOT Y.ELCM_REPLICATION IS NULL
        )
        OR (
          NOT X.ELCM_REPLICATION IS NULL AND Y.ELCM_REPLICATION IS NULL
        )
        OR (
          X.ELCM_REPLICATION <> Y.ELCM_REPLICATION
        )
      )
      OR (
        (
          X.END_OF_INPUT IS NULL AND NOT Y.END_OF_INPUT IS NULL
        )
        OR (
          NOT X.END_OF_INPUT IS NULL AND Y.END_OF_INPUT IS NULL
        )
        OR (
          X.END_OF_INPUT <> Y.END_OF_INPUT
        )
      )
      OR (
        (
          X.ENTERPRISE_GL IS NULL AND NOT Y.ENTERPRISE_GL IS NULL
        )
        OR (
          NOT X.ENTERPRISE_GL IS NULL AND Y.ENTERPRISE_GL IS NULL
        )
        OR (
          X.ENTERPRISE_GL <> Y.ENTERPRISE_GL
        )
      )
      OR (
        (
          X.EOC_STAGE IS NULL AND NOT Y.EOC_STAGE IS NULL
        )
        OR (
          NOT X.EOC_STAGE IS NULL AND Y.EOC_STAGE IS NULL
        )
        OR (
          X.EOC_STAGE <> Y.EOC_STAGE
        )
      )
      OR (
        (
          X.FILE_CONS_TRN_CODE IS NULL AND NOT Y.FILE_CONS_TRN_CODE IS NULL
        )
        OR (
          NOT X.FILE_CONS_TRN_CODE IS NULL AND Y.FILE_CONS_TRN_CODE IS NULL
        )
        OR (
          X.FILE_CONS_TRN_CODE <> Y.FILE_CONS_TRN_CODE
        )
      )
      OR (
        (
          X.FUND_BRANCH IS NULL AND NOT Y.FUND_BRANCH IS NULL
        )
        OR (
          NOT X.FUND_BRANCH IS NULL AND Y.FUND_BRANCH IS NULL
        )
        OR (
          X.FUND_BRANCH <> Y.FUND_BRANCH
        )
      )
      OR (
        (
          X.GEN_MT103 IS NULL AND NOT Y.GEN_MT103 IS NULL
        )
        OR (
          NOT X.GEN_MT103 IS NULL AND Y.GEN_MT103 IS NULL
        )
        OR (
          X.GEN_MT103 <> Y.GEN_MT103
        )
      )
      OR (
        (
          X.GEN_MT103P IS NULL AND NOT Y.GEN_MT103P IS NULL
        )
        OR (
          NOT X.GEN_MT103P IS NULL AND Y.GEN_MT103P IS NULL
        )
        OR (
          X.GEN_MT103P <> Y.GEN_MT103P
        )
      )
      OR (
        (
          X.GENERATE IS NULL AND NOT Y.GENERATE IS NULL
        )
        OR (
          NOT X.GENERATE IS NULL AND Y.GENERATE IS NULL
        )
        OR (
          X.GENERATE <> Y.GENERATE
        )
      )
      OR (
        (
          X.GL_CLASS IS NULL AND NOT Y.GL_CLASS IS NULL
        )
        OR (
          NOT X.GL_CLASS IS NULL AND Y.GL_CLASS IS NULL
        )
        OR (
          X.GL_CLASS <> Y.GL_CLASS
        )
      )
      OR (
        (
          X.HOST_NAME IS NULL AND NOT Y.HOST_NAME IS NULL
        )
        OR (
          NOT X.HOST_NAME IS NULL AND Y.HOST_NAME IS NULL
        )
        OR (
          X.HOST_NAME <> Y.HOST_NAME
        )
      )
      OR (
        (
          X.IBAN_MASK_ACCOUNT_NUMBER IS NULL AND NOT Y.IBAN_MASK_ACCOUNT_NUMBER IS NULL
        )
        OR (
          NOT X.IBAN_MASK_ACCOUNT_NUMBER IS NULL AND Y.IBAN_MASK_ACCOUNT_NUMBER IS NULL
        )
        OR (
          X.IBAN_MASK_ACCOUNT_NUMBER <> Y.IBAN_MASK_ACCOUNT_NUMBER
        )
      )
      OR (
        (
          X.IBAN_MASK_BANK_CODE IS NULL AND NOT Y.IBAN_MASK_BANK_CODE IS NULL
        )
        OR (
          NOT X.IBAN_MASK_BANK_CODE IS NULL AND Y.IBAN_MASK_BANK_CODE IS NULL
        )
        OR (
          X.IBAN_MASK_BANK_CODE <> Y.IBAN_MASK_BANK_CODE
        )
      )
      OR (
        (
          X.ICEOD_STATUS IS NULL AND NOT Y.ICEOD_STATUS IS NULL
        )
        OR (
          NOT X.ICEOD_STATUS IS NULL AND Y.ICEOD_STATUS IS NULL
        )
        OR (
          X.ICEOD_STATUS <> Y.ICEOD_STATUS
        )
      )
      OR (
        (
          X.INDIVIDUAL_TAX_CERT_REQD IS NULL AND NOT Y.INDIVIDUAL_TAX_CERT_REQD IS NULL
        )
        OR (
          NOT X.INDIVIDUAL_TAX_CERT_REQD IS NULL AND Y.INDIVIDUAL_TAX_CERT_REQD IS NULL
        )
        OR (
          X.INDIVIDUAL_TAX_CERT_REQD <> Y.INDIVIDUAL_TAX_CERT_REQD
        )
      )
      OR (
        (
          X.INTERDICT_CHECK_REQD IS NULL AND NOT Y.INTERDICT_CHECK_REQD IS NULL
        )
        OR (
          NOT X.INTERDICT_CHECK_REQD IS NULL AND Y.INTERDICT_CHECK_REQD IS NULL
        )
        OR (
          X.INTERDICT_CHECK_REQD <> Y.INTERDICT_CHECK_REQD
        )
      )
      OR (
        (
          X.INTERDICT_TIME_OUT IS NULL AND NOT Y.INTERDICT_TIME_OUT IS NULL
        )
        OR (
          NOT X.INTERDICT_TIME_OUT IS NULL AND Y.INTERDICT_TIME_OUT IS NULL
        )
        OR (
          X.INTERDICT_TIME_OUT <> Y.INTERDICT_TIME_OUT
        )
      )
      OR (
        (
          X.INTERNAL_SWAP_CUSTOMER IS NULL AND NOT Y.INTERNAL_SWAP_CUSTOMER IS NULL
        )
        OR (
          NOT X.INTERNAL_SWAP_CUSTOMER IS NULL AND Y.INTERNAL_SWAP_CUSTOMER IS NULL
        )
        OR (
          X.INTERNAL_SWAP_CUSTOMER <> Y.INTERNAL_SWAP_CUSTOMER
        )
      )
      OR (
        (
          X.JOB_STAT IS NULL AND NOT Y.JOB_STAT IS NULL
        )
        OR (
          NOT X.JOB_STAT IS NULL AND Y.JOB_STAT IS NULL
        )
        OR (
          X.JOB_STAT <> Y.JOB_STAT
        )
      )
      OR (
        (
          X.LDAP_TEMPLATE IS NULL AND NOT Y.LDAP_TEMPLATE IS NULL
        )
        OR (
          NOT X.LDAP_TEMPLATE IS NULL AND Y.LDAP_TEMPLATE IS NULL
        )
        OR (
          X.LDAP_TEMPLATE <> Y.LDAP_TEMPLATE
        )
      )
      OR (
        (
          X.LMTEXP_NOTIFY_DAYS IS NULL AND NOT Y.LMTEXP_NOTIFY_DAYS IS NULL
        )
        OR (
          NOT X.LMTEXP_NOTIFY_DAYS IS NULL AND Y.LMTEXP_NOTIFY_DAYS IS NULL
        )
        OR (
          X.LMTEXP_NOTIFY_DAYS <> Y.LMTEXP_NOTIFY_DAYS
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
          X.MAX_CONT_SUSPENSE_AMT IS NULL AND NOT Y.MAX_CONT_SUSPENSE_AMT IS NULL
        )
        OR (
          NOT X.MAX_CONT_SUSPENSE_AMT IS NULL AND Y.MAX_CONT_SUSPENSE_AMT IS NULL
        )
        OR (
          X.MAX_CONT_SUSPENSE_AMT <> Y.MAX_CONT_SUSPENSE_AMT
        )
      )
      OR (
        (
          X.MAX_REAL_SUSPENSE_AMT IS NULL AND NOT Y.MAX_REAL_SUSPENSE_AMT IS NULL
        )
        OR (
          NOT X.MAX_REAL_SUSPENSE_AMT IS NULL AND Y.MAX_REAL_SUSPENSE_AMT IS NULL
        )
        OR (
          X.MAX_REAL_SUSPENSE_AMT <> Y.MAX_REAL_SUSPENSE_AMT
        )
      )
      OR (
        (
          X.MINOR_AGE IS NULL AND NOT Y.MINOR_AGE IS NULL
        )
        OR (
          NOT X.MINOR_AGE IS NULL AND Y.MINOR_AGE IS NULL
        )
        OR (
          X.MINOR_AGE <> Y.MINOR_AGE
        )
      )
      OR (
        (
          X.MIS_CCY_MISMATCH_GROUP IS NULL AND NOT Y.MIS_CCY_MISMATCH_GROUP IS NULL
        )
        OR (
          NOT X.MIS_CCY_MISMATCH_GROUP IS NULL AND Y.MIS_CCY_MISMATCH_GROUP IS NULL
        )
        OR (
          X.MIS_CCY_MISMATCH_GROUP <> Y.MIS_CCY_MISMATCH_GROUP
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
          X.MSG_GEN_DAYS IS NULL AND NOT Y.MSG_GEN_DAYS IS NULL
        )
        OR (
          NOT X.MSG_GEN_DAYS IS NULL AND Y.MSG_GEN_DAYS IS NULL
        )
        OR (
          X.MSG_GEN_DAYS <> Y.MSG_GEN_DAYS
        )
      )
      OR (
        (
          X.NETTING_SUSPENSE_GL IS NULL AND NOT Y.NETTING_SUSPENSE_GL IS NULL
        )
        OR (
          NOT X.NETTING_SUSPENSE_GL IS NULL AND Y.NETTING_SUSPENSE_GL IS NULL
        )
        OR (
          X.NETTING_SUSPENSE_GL <> Y.NETTING_SUSPENSE_GL
        )
      )
      OR (
        (
          X.NOTICE_PERIOD IS NULL AND NOT Y.NOTICE_PERIOD IS NULL
        )
        OR (
          NOT X.NOTICE_PERIOD IS NULL AND Y.NOTICE_PERIOD IS NULL
        )
        OR (
          X.NOTICE_PERIOD <> Y.NOTICE_PERIOD
        )
      )
      OR (
        (
          X.OFFSET_CLEARING_ACCOUNT IS NULL AND NOT Y.OFFSET_CLEARING_ACCOUNT IS NULL
        )
        OR (
          NOT X.OFFSET_CLEARING_ACCOUNT IS NULL AND Y.OFFSET_CLEARING_ACCOUNT IS NULL
        )
        OR (
          X.OFFSET_CLEARING_ACCOUNT <> Y.OFFSET_CLEARING_ACCOUNT
        )
      )
      OR (
        (
          X.OFFSET_HOURS IS NULL AND NOT Y.OFFSET_HOURS IS NULL
        )
        OR (
          NOT X.OFFSET_HOURS IS NULL AND Y.OFFSET_HOURS IS NULL
        )
        OR (
          X.OFFSET_HOURS <> Y.OFFSET_HOURS
        )
      )
      OR (
        (
          X.OFFSET_HR IS NULL AND NOT Y.OFFSET_HR IS NULL
        )
        OR (
          NOT X.OFFSET_HR IS NULL AND Y.OFFSET_HR IS NULL
        )
        OR (
          X.OFFSET_HR <> Y.OFFSET_HR
        )
      )
      OR (
        (
          X.OFFSET_MIN IS NULL AND NOT Y.OFFSET_MIN IS NULL
        )
        OR (
          NOT X.OFFSET_MIN IS NULL AND Y.OFFSET_MIN IS NULL
        )
        OR (
          X.OFFSET_MIN <> Y.OFFSET_MIN
        )
      )
      OR (
        (
          X.OFFSET_MINS IS NULL AND NOT Y.OFFSET_MINS IS NULL
        )
        OR (
          NOT X.OFFSET_MINS IS NULL AND Y.OFFSET_MINS IS NULL
        )
        OR (
          X.OFFSET_MINS <> Y.OFFSET_MINS
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
          X.PARENT_BRANCH IS NULL AND NOT Y.PARENT_BRANCH IS NULL
        )
        OR (
          NOT X.PARENT_BRANCH IS NULL AND Y.PARENT_BRANCH IS NULL
        )
        OR (
          X.PARENT_BRANCH <> Y.PARENT_BRANCH
        )
      )
      OR (
        (
          X.PC_CLEARING_BRN IS NULL AND NOT Y.PC_CLEARING_BRN IS NULL
        )
        OR (
          NOT X.PC_CLEARING_BRN IS NULL AND Y.PC_CLEARING_BRN IS NULL
        )
        OR (
          X.PC_CLEARING_BRN <> Y.PC_CLEARING_BRN
        )
      )
      OR (
        (
          X.PL_SPLIT_REQD IS NULL AND NOT Y.PL_SPLIT_REQD IS NULL
        )
        OR (
          NOT X.PL_SPLIT_REQD IS NULL AND Y.PL_SPLIT_REQD IS NULL
        )
        OR (
          X.PL_SPLIT_REQD <> Y.PL_SPLIT_REQD
        )
      )
      OR (
        (
          X.POSITION_ASSET_GL IS NULL AND NOT Y.POSITION_ASSET_GL IS NULL
        )
        OR (
          NOT X.POSITION_ASSET_GL IS NULL AND Y.POSITION_ASSET_GL IS NULL
        )
        OR (
          X.POSITION_ASSET_GL <> Y.POSITION_ASSET_GL
        )
      )
      OR (
        (
          X.POSITION_LIABILITY_GL IS NULL AND NOT Y.POSITION_LIABILITY_GL IS NULL
        )
        OR (
          NOT X.POSITION_LIABILITY_GL IS NULL AND Y.POSITION_LIABILITY_GL IS NULL
        )
        OR (
          X.POSITION_LIABILITY_GL <> Y.POSITION_LIABILITY_GL
        )
      )
      OR (
        (
          X.PROCEED_WITHOUT_FLOAT IS NULL AND NOT Y.PROCEED_WITHOUT_FLOAT IS NULL
        )
        OR (
          NOT X.PROCEED_WITHOUT_FLOAT IS NULL AND Y.PROCEED_WITHOUT_FLOAT IS NULL
        )
        OR (
          X.PROCEED_WITHOUT_FLOAT <> Y.PROCEED_WITHOUT_FLOAT
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
          X.REFERRAL_HR IS NULL AND NOT Y.REFERRAL_HR IS NULL
        )
        OR (
          NOT X.REFERRAL_HR IS NULL AND Y.REFERRAL_HR IS NULL
        )
        OR (
          X.REFERRAL_HR <> Y.REFERRAL_HR
        )
      )
      OR (
        (
          X.REFERRAL_MIN IS NULL AND NOT Y.REFERRAL_MIN IS NULL
        )
        OR (
          NOT X.REFERRAL_MIN IS NULL AND Y.REFERRAL_MIN IS NULL
        )
        OR (
          X.REFERRAL_MIN <> Y.REFERRAL_MIN
        )
      )
      OR (
        (
          X.REGIONAL_OFFICE IS NULL AND NOT Y.REGIONAL_OFFICE IS NULL
        )
        OR (
          NOT X.REGIONAL_OFFICE IS NULL AND Y.REGIONAL_OFFICE IS NULL
        )
        OR (
          X.REGIONAL_OFFICE <> Y.REGIONAL_OFFICE
        )
      )
      OR (
        (
          X.REP_HISTORY_PERIOD IS NULL AND NOT Y.REP_HISTORY_PERIOD IS NULL
        )
        OR (
          NOT X.REP_HISTORY_PERIOD IS NULL AND Y.REP_HISTORY_PERIOD IS NULL
        )
        OR (
          X.REP_HISTORY_PERIOD <> Y.REP_HISTORY_PERIOD
        )
      )
      OR (
        (
          X.REPORT_DSN IS NULL AND NOT Y.REPORT_DSN IS NULL
        )
        OR (
          NOT X.REPORT_DSN IS NULL AND Y.REPORT_DSN IS NULL
        )
        OR (
          X.REPORT_DSN <> Y.REPORT_DSN
        )
      )
      OR (
        (
          X.REV_SUSPENSE_ENTRY_DAYS IS NULL AND NOT Y.REV_SUSPENSE_ENTRY_DAYS IS NULL
        )
        OR (
          NOT X.REV_SUSPENSE_ENTRY_DAYS IS NULL AND Y.REV_SUSPENSE_ENTRY_DAYS IS NULL
        )
        OR (
          X.REV_SUSPENSE_ENTRY_DAYS <> Y.REV_SUSPENSE_ENTRY_DAYS
        )
      )
      OR (
        (
          X.REVAL_BRANCH IS NULL AND NOT Y.REVAL_BRANCH IS NULL
        )
        OR (
          NOT X.REVAL_BRANCH IS NULL AND Y.REVAL_BRANCH IS NULL
        )
        OR (
          X.REVAL_BRANCH <> Y.REVAL_BRANCH
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
          X.SECTOR_CODE IS NULL AND NOT Y.SECTOR_CODE IS NULL
        )
        OR (
          NOT X.SECTOR_CODE IS NULL AND Y.SECTOR_CODE IS NULL
        )
        OR (
          X.SECTOR_CODE <> Y.SECTOR_CODE
        )
      )
      OR (
        (
          X.SK_BRANCH_ADDR1 IS NULL AND NOT Y.SK_BRANCH_ADDR1 IS NULL
        )
        OR (
          NOT X.SK_BRANCH_ADDR1 IS NULL AND Y.SK_BRANCH_ADDR1 IS NULL
        )
        OR (
          X.SK_BRANCH_ADDR1 <> Y.SK_BRANCH_ADDR1
        )
      )
      OR (
        (
          X.SK_BRANCH_ADDR3_FAX IS NULL AND NOT Y.SK_BRANCH_ADDR3_FAX IS NULL
        )
        OR (
          NOT X.SK_BRANCH_ADDR3_FAX IS NULL AND Y.SK_BRANCH_ADDR3_FAX IS NULL
        )
        OR (
          X.SK_BRANCH_ADDR3_FAX <> Y.SK_BRANCH_ADDR3_FAX
        )
      )
      OR (
        (
          X.SK_BRANCH_ADDR3_TELEPHONE IS NULL AND NOT Y.SK_BRANCH_ADDR3_TELEPHONE IS NULL
        )
        OR (
          NOT X.SK_BRANCH_ADDR3_TELEPHONE IS NULL AND Y.SK_BRANCH_ADDR3_TELEPHONE IS NULL
        )
        OR (
          X.SK_BRANCH_ADDR3_TELEPHONE <> Y.SK_BRANCH_ADDR3_TELEPHONE
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
          X.SK_PARENT_BRANCH IS NULL AND NOT Y.SK_PARENT_BRANCH IS NULL
        )
        OR (
          NOT X.SK_PARENT_BRANCH IS NULL AND Y.SK_PARENT_BRANCH IS NULL
        )
        OR (
          X.SK_PARENT_BRANCH <> Y.SK_PARENT_BRANCH
        )
      )
      OR (
        (
          X.SK_SWIFT_ADDR IS NULL AND NOT Y.SK_SWIFT_ADDR IS NULL
        )
        OR (
          NOT X.SK_SWIFT_ADDR IS NULL AND Y.SK_SWIFT_ADDR IS NULL
        )
        OR (
          X.SK_SWIFT_ADDR <> Y.SK_SWIFT_ADDR
        )
      )
      OR (
        (
          X.SK_TELEX_ADDR IS NULL AND NOT Y.SK_TELEX_ADDR IS NULL
        )
        OR (
          NOT X.SK_TELEX_ADDR IS NULL AND Y.SK_TELEX_ADDR IS NULL
        )
        OR (
          X.SK_TELEX_ADDR <> Y.SK_TELEX_ADDR
        )
      )
      OR (
        (
          X.STATUS_PROCESSING_BASIS IS NULL AND NOT Y.STATUS_PROCESSING_BASIS IS NULL
        )
        OR (
          NOT X.STATUS_PROCESSING_BASIS IS NULL AND Y.STATUS_PROCESSING_BASIS IS NULL
        )
        OR (
          X.STATUS_PROCESSING_BASIS <> Y.STATUS_PROCESSING_BASIS
        )
      )
      OR (
        (
          X.SUSPENSE_BATCH_NO IS NULL AND NOT Y.SUSPENSE_BATCH_NO IS NULL
        )
        OR (
          NOT X.SUSPENSE_BATCH_NO IS NULL AND Y.SUSPENSE_BATCH_NO IS NULL
        )
        OR (
          X.SUSPENSE_BATCH_NO <> Y.SUSPENSE_BATCH_NO
        )
      )
      OR (
        (
          X.SUSPENSE_ENTRY_REQD IS NULL AND NOT Y.SUSPENSE_ENTRY_REQD IS NULL
        )
        OR (
          NOT X.SUSPENSE_ENTRY_REQD IS NULL AND Y.SUSPENSE_ENTRY_REQD IS NULL
        )
        OR (
          X.SUSPENSE_ENTRY_REQD <> Y.SUSPENSE_ENTRY_REQD
        )
      )
      OR (
        (
          X.SUSPENSE_GL_FCY IS NULL AND NOT Y.SUSPENSE_GL_FCY IS NULL
        )
        OR (
          NOT X.SUSPENSE_GL_FCY IS NULL AND Y.SUSPENSE_GL_FCY IS NULL
        )
        OR (
          X.SUSPENSE_GL_FCY <> Y.SUSPENSE_GL_FCY
        )
      )
      OR (
        (
          X.SUSPENSE_GLSL IS NULL AND NOT Y.SUSPENSE_GLSL IS NULL
        )
        OR (
          NOT X.SUSPENSE_GLSL IS NULL AND Y.SUSPENSE_GLSL IS NULL
        )
        OR (
          X.SUSPENSE_GLSL <> Y.SUSPENSE_GLSL
        )
      )
      OR (
        (
          X.SUSPENSE_TXN_CODE IS NULL AND NOT Y.SUSPENSE_TXN_CODE IS NULL
        )
        OR (
          NOT X.SUSPENSE_TXN_CODE IS NULL AND Y.SUSPENSE_TXN_CODE IS NULL
        )
        OR (
          X.SUSPENSE_TXN_CODE <> Y.SUSPENSE_TXN_CODE
        )
      )
      OR (
        (
          X.SWIFT_ADDR IS NULL AND NOT Y.SWIFT_ADDR IS NULL
        )
        OR (
          NOT X.SWIFT_ADDR IS NULL AND Y.SWIFT_ADDR IS NULL
        )
        OR (
          X.SWIFT_ADDR <> Y.SWIFT_ADDR
        )
      )
      OR (
        (
          X.TAX_CERT_DAY IS NULL AND NOT Y.TAX_CERT_DAY IS NULL
        )
        OR (
          NOT X.TAX_CERT_DAY IS NULL AND Y.TAX_CERT_DAY IS NULL
        )
        OR (
          X.TAX_CERT_DAY <> Y.TAX_CERT_DAY
        )
      )
      OR (
        (
          X.TAX_CERT_FREQ IS NULL AND NOT Y.TAX_CERT_FREQ IS NULL
        )
        OR (
          NOT X.TAX_CERT_FREQ IS NULL AND Y.TAX_CERT_FREQ IS NULL
        )
        OR (
          X.TAX_CERT_FREQ <> Y.TAX_CERT_FREQ
        )
      )
      OR (
        (
          X.TELEX_ADDR IS NULL AND NOT Y.TELEX_ADDR IS NULL
        )
        OR (
          NOT X.TELEX_ADDR IS NULL AND Y.TELEX_ADDR IS NULL
        )
        OR (
          X.TELEX_ADDR <> Y.TELEX_ADDR
        )
      )
      OR (
        (
          X.TIME_LEVEL IS NULL AND NOT Y.TIME_LEVEL IS NULL
        )
        OR (
          NOT X.TIME_LEVEL IS NULL AND Y.TIME_LEVEL IS NULL
        )
        OR (
          X.TIME_LEVEL <> Y.TIME_LEVEL
        )
      )
      OR (
        (
          X.TRACK_PY_PNL_ADJUSTMENT IS NULL AND NOT Y.TRACK_PY_PNL_ADJUSTMENT IS NULL
        )
        OR (
          NOT X.TRACK_PY_PNL_ADJUSTMENT IS NULL AND Y.TRACK_PY_PNL_ADJUSTMENT IS NULL
        )
        OR (
          X.TRACK_PY_PNL_ADJUSTMENT <> Y.TRACK_PY_PNL_ADJUSTMENT
        )
      )
      OR (
        (
          X.TRANSACTION_CODE IS NULL AND NOT Y.TRANSACTION_CODE IS NULL
        )
        OR (
          NOT X.TRANSACTION_CODE IS NULL AND Y.TRANSACTION_CODE IS NULL
        )
        OR (
          X.TRANSACTION_CODE <> Y.TRANSACTION_CODE
        )
      )
      OR (
        (
          X.TRANSFER_TYPE IS NULL AND NOT Y.TRANSFER_TYPE IS NULL
        )
        OR (
          NOT X.TRANSFER_TYPE IS NULL AND Y.TRANSFER_TYPE IS NULL
        )
        OR (
          X.TRANSFER_TYPE <> Y.TRANSFER_TYPE
        )
      )
      OR (
        (
          X.UNCOLLECTED_FUNDS_BASIS IS NULL AND NOT Y.UNCOLLECTED_FUNDS_BASIS IS NULL
        )
        OR (
          NOT X.UNCOLLECTED_FUNDS_BASIS IS NULL AND Y.UNCOLLECTED_FUNDS_BASIS IS NULL
        )
        OR (
          X.UNCOLLECTED_FUNDS_BASIS <> Y.UNCOLLECTED_FUNDS_BASIS
        )
      )
      OR (
        (
          X.WALKIN_CUSTOMER IS NULL AND NOT Y.WALKIN_CUSTOMER IS NULL
        )
        OR (
          NOT X.WALKIN_CUSTOMER IS NULL AND Y.WALKIN_CUSTOMER IS NULL
        )
        OR (
          X.WALKIN_CUSTOMER <> Y.WALKIN_CUSTOMER
        )
      )
      OR (
        (
          X.WEEK_HOL1 IS NULL AND NOT Y.WEEK_HOL1 IS NULL
        )
        OR (
          NOT X.WEEK_HOL1 IS NULL AND Y.WEEK_HOL1 IS NULL
        )
        OR (
          X.WEEK_HOL1 <> Y.WEEK_HOL1
        )
      )
      OR (
        (
          X.WEEK_HOL2 IS NULL AND NOT Y.WEEK_HOL2 IS NULL
        )
        OR (
          NOT X.WEEK_HOL2 IS NULL AND Y.WEEK_HOL2 IS NULL
        )
        OR (
          X.WEEK_HOL2 <> Y.WEEK_HOL2
        )
      )
    )
    THEN 'UPD'
    WHEN X.BRANCH_CODE IS NULL
    THEN 'DEL' /* - IF MULTIPLE PKs are there then concatenate them with AND */
  END AS OPT_FLAG
FROM DEVT_STG_FLX.STTM_BRANCH AS X
FULL JOIN DEVT_STG_FLX.STTM_BRANCH_BACKUP AS Y
  ON X.BRANCH_CODE = Y.BRANCH_CODE /* - IF MULTIPLE PKs are there then concatenate them with AND */
WHERE
  NOT OPT_FLAG IS NULL