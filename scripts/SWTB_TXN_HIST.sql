CREATE OR REPLACE VIEW DEVV_STG_FLX.SWTB_TXN_HIST_get_delta AS LOCKING ROW FOR ACCESS
SELECT
  COALESCE(X.XREF, Y.XREF) AS XREF,
  X.ACQ_INS_ID,
  X.ADD_AMT,
  X.AMOUNT_BLOCK_NO,
  X.BILL_AMT,
  X.BILL_CCY_CODE,
  X.CAPT_DATE,
  X.CONV_DATE,
  X.DB_IN_TIME,
  X.DB_OUT_TIME,
  X.DCN,
  X.ERR_PARAM,
  X.ERROR_CODE,
  X.EXP_DATE,
  X.FROM_ACC,
  X.FWD_INS_ID,
  X.MINI_STMT_DATA,
  X.MSG_FLOW,
  X.MSG_TYPE,
  X.OFFUS_ONUS,
  X.P_KEY,
  X.PAN,
  X.PRE_AUTH_SEQ_NO,
  X.PROC_CODE,
  X.PURGE_DATE,
  X.RECONSILED,
  X.RESP_CODE,
  X.RRN,
  X.SETL_AMT,
  X.SETL_CCY_CODE,
  X.SETL_DATE,
  X.SK_PAN,
  X.SK_TERM_ID,
  X.STAN,
  X.TERM_ID,
  X.TO_ACC,
  X.TRANS_DT_TIME,
  X.TRN_REF_NO,
  X.TXN_AMT,
  X.TXN_CCY_CODE,
  X.TXN_DESC,
  X.WORK_PROGRESS,
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
    WHEN Y.XREF IS NULL
    THEN 'INS' /* - IF MULTIPLE PKs are there then concatenate them with AND */
    WHEN X.XREF = Y.XREF /* - IF MULTIPLE PKs are there then concatenate them with AND */
    AND (
      (
        (
          X.ACQ_INS_ID IS NULL AND NOT Y.ACQ_INS_ID IS NULL
        )
        OR (
          NOT X.ACQ_INS_ID IS NULL AND Y.ACQ_INS_ID IS NULL
        )
        OR (
          X.ACQ_INS_ID <> Y.ACQ_INS_ID
        )
      )
      OR (
        (
          X.ADD_AMT IS NULL AND NOT Y.ADD_AMT IS NULL
        )
        OR (
          NOT X.ADD_AMT IS NULL AND Y.ADD_AMT IS NULL
        )
        OR (
          X.ADD_AMT <> Y.ADD_AMT
        )
      )
      OR (
        (
          X.AMOUNT_BLOCK_NO IS NULL AND NOT Y.AMOUNT_BLOCK_NO IS NULL
        )
        OR (
          NOT X.AMOUNT_BLOCK_NO IS NULL AND Y.AMOUNT_BLOCK_NO IS NULL
        )
        OR (
          X.AMOUNT_BLOCK_NO <> Y.AMOUNT_BLOCK_NO
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
          X.BILL_CCY_CODE IS NULL AND NOT Y.BILL_CCY_CODE IS NULL
        )
        OR (
          NOT X.BILL_CCY_CODE IS NULL AND Y.BILL_CCY_CODE IS NULL
        )
        OR (
          X.BILL_CCY_CODE <> Y.BILL_CCY_CODE
        )
      )
      OR (
        (
          X.CAPT_DATE IS NULL AND NOT Y.CAPT_DATE IS NULL
        )
        OR (
          NOT X.CAPT_DATE IS NULL AND Y.CAPT_DATE IS NULL
        )
        OR (
          X.CAPT_DATE <> Y.CAPT_DATE
        )
      )
      OR (
        (
          X.CONV_DATE IS NULL AND NOT Y.CONV_DATE IS NULL
        )
        OR (
          NOT X.CONV_DATE IS NULL AND Y.CONV_DATE IS NULL
        )
        OR (
          X.CONV_DATE <> Y.CONV_DATE
        )
      )
      OR (
        (
          X.DB_IN_TIME IS NULL AND NOT Y.DB_IN_TIME IS NULL
        )
        OR (
          NOT X.DB_IN_TIME IS NULL AND Y.DB_IN_TIME IS NULL
        )
        OR (
          X.DB_IN_TIME <> Y.DB_IN_TIME
        )
      )
      OR (
        (
          X.DB_OUT_TIME IS NULL AND NOT Y.DB_OUT_TIME IS NULL
        )
        OR (
          NOT X.DB_OUT_TIME IS NULL AND Y.DB_OUT_TIME IS NULL
        )
        OR (
          X.DB_OUT_TIME <> Y.DB_OUT_TIME
        )
      )
      OR (
        (
          X.DCN IS NULL AND NOT Y.DCN IS NULL
        )
        OR (
          NOT X.DCN IS NULL AND Y.DCN IS NULL
        )
        OR (
          X.DCN <> Y.DCN
        )
      )
      OR (
        (
          X.ERR_PARAM IS NULL AND NOT Y.ERR_PARAM IS NULL
        )
        OR (
          NOT X.ERR_PARAM IS NULL AND Y.ERR_PARAM IS NULL
        )
        OR (
          X.ERR_PARAM <> Y.ERR_PARAM
        )
      )
      OR (
        (
          X.ERROR_CODE IS NULL AND NOT Y.ERROR_CODE IS NULL
        )
        OR (
          NOT X.ERROR_CODE IS NULL AND Y.ERROR_CODE IS NULL
        )
        OR (
          X.ERROR_CODE <> Y.ERROR_CODE
        )
      )
      OR (
        (
          X.EXP_DATE IS NULL AND NOT Y.EXP_DATE IS NULL
        )
        OR (
          NOT X.EXP_DATE IS NULL AND Y.EXP_DATE IS NULL
        )
        OR (
          X.EXP_DATE <> Y.EXP_DATE
        )
      )
      OR (
        (
          X.FROM_ACC IS NULL AND NOT Y.FROM_ACC IS NULL
        )
        OR (
          NOT X.FROM_ACC IS NULL AND Y.FROM_ACC IS NULL
        )
        OR (
          X.FROM_ACC <> Y.FROM_ACC
        )
      )
      OR (
        (
          X.FWD_INS_ID IS NULL AND NOT Y.FWD_INS_ID IS NULL
        )
        OR (
          NOT X.FWD_INS_ID IS NULL AND Y.FWD_INS_ID IS NULL
        )
        OR (
          X.FWD_INS_ID <> Y.FWD_INS_ID
        )
      )
      OR (
        (
          X.MINI_STMT_DATA IS NULL AND NOT Y.MINI_STMT_DATA IS NULL
        )
        OR (
          NOT X.MINI_STMT_DATA IS NULL AND Y.MINI_STMT_DATA IS NULL
        )
        OR (
          X.MINI_STMT_DATA <> Y.MINI_STMT_DATA
        )
      )
      OR (
        (
          X.MSG_FLOW IS NULL AND NOT Y.MSG_FLOW IS NULL
        )
        OR (
          NOT X.MSG_FLOW IS NULL AND Y.MSG_FLOW IS NULL
        )
        OR (
          X.MSG_FLOW <> Y.MSG_FLOW
        )
      )
      OR (
        (
          X.MSG_TYPE IS NULL AND NOT Y.MSG_TYPE IS NULL
        )
        OR (
          NOT X.MSG_TYPE IS NULL AND Y.MSG_TYPE IS NULL
        )
        OR (
          X.MSG_TYPE <> Y.MSG_TYPE
        )
      )
      OR (
        (
          X.OFFUS_ONUS IS NULL AND NOT Y.OFFUS_ONUS IS NULL
        )
        OR (
          NOT X.OFFUS_ONUS IS NULL AND Y.OFFUS_ONUS IS NULL
        )
        OR (
          X.OFFUS_ONUS <> Y.OFFUS_ONUS
        )
      )
      OR (
        (
          X.P_KEY IS NULL AND NOT Y.P_KEY IS NULL
        )
        OR (
          NOT X.P_KEY IS NULL AND Y.P_KEY IS NULL
        )
        OR (
          X.P_KEY <> Y.P_KEY
        )
      )
      OR (
        (
          X.PAN IS NULL AND NOT Y.PAN IS NULL
        )
        OR (
          NOT X.PAN IS NULL AND Y.PAN IS NULL
        )
        OR (
          X.PAN <> Y.PAN
        )
      )
      OR (
        (
          X.PRE_AUTH_SEQ_NO IS NULL AND NOT Y.PRE_AUTH_SEQ_NO IS NULL
        )
        OR (
          NOT X.PRE_AUTH_SEQ_NO IS NULL AND Y.PRE_AUTH_SEQ_NO IS NULL
        )
        OR (
          X.PRE_AUTH_SEQ_NO <> Y.PRE_AUTH_SEQ_NO
        )
      )
      OR (
        (
          X.PROC_CODE IS NULL AND NOT Y.PROC_CODE IS NULL
        )
        OR (
          NOT X.PROC_CODE IS NULL AND Y.PROC_CODE IS NULL
        )
        OR (
          X.PROC_CODE <> Y.PROC_CODE
        )
      )
      OR (
        (
          X.PURGE_DATE IS NULL AND NOT Y.PURGE_DATE IS NULL
        )
        OR (
          NOT X.PURGE_DATE IS NULL AND Y.PURGE_DATE IS NULL
        )
        OR (
          X.PURGE_DATE <> Y.PURGE_DATE
        )
      )
      OR (
        (
          X.RECONSILED IS NULL AND NOT Y.RECONSILED IS NULL
        )
        OR (
          NOT X.RECONSILED IS NULL AND Y.RECONSILED IS NULL
        )
        OR (
          X.RECONSILED <> Y.RECONSILED
        )
      )
      OR (
        (
          X.RESP_CODE IS NULL AND NOT Y.RESP_CODE IS NULL
        )
        OR (
          NOT X.RESP_CODE IS NULL AND Y.RESP_CODE IS NULL
        )
        OR (
          X.RESP_CODE <> Y.RESP_CODE
        )
      )
      OR (
        (
          X.RRN IS NULL AND NOT Y.RRN IS NULL
        )
        OR (
          NOT X.RRN IS NULL AND Y.RRN IS NULL
        )
        OR (
          X.RRN <> Y.RRN
        )
      )
      OR (
        (
          X.SETL_AMT IS NULL AND NOT Y.SETL_AMT IS NULL
        )
        OR (
          NOT X.SETL_AMT IS NULL AND Y.SETL_AMT IS NULL
        )
        OR (
          X.SETL_AMT <> Y.SETL_AMT
        )
      )
      OR (
        (
          X.SETL_CCY_CODE IS NULL AND NOT Y.SETL_CCY_CODE IS NULL
        )
        OR (
          NOT X.SETL_CCY_CODE IS NULL AND Y.SETL_CCY_CODE IS NULL
        )
        OR (
          X.SETL_CCY_CODE <> Y.SETL_CCY_CODE
        )
      )
      OR (
        (
          X.SETL_DATE IS NULL AND NOT Y.SETL_DATE IS NULL
        )
        OR (
          NOT X.SETL_DATE IS NULL AND Y.SETL_DATE IS NULL
        )
        OR (
          X.SETL_DATE <> Y.SETL_DATE
        )
      )
      OR (
        (
          X.SK_PAN IS NULL AND NOT Y.SK_PAN IS NULL
        )
        OR (
          NOT X.SK_PAN IS NULL AND Y.SK_PAN IS NULL
        )
        OR (
          X.SK_PAN <> Y.SK_PAN
        )
      )
      OR (
        (
          X.SK_TERM_ID IS NULL AND NOT Y.SK_TERM_ID IS NULL
        )
        OR (
          NOT X.SK_TERM_ID IS NULL AND Y.SK_TERM_ID IS NULL
        )
        OR (
          X.SK_TERM_ID <> Y.SK_TERM_ID
        )
      )
      OR (
        (
          X.STAN IS NULL AND NOT Y.STAN IS NULL
        )
        OR (
          NOT X.STAN IS NULL AND Y.STAN IS NULL
        )
        OR (
          X.STAN <> Y.STAN
        )
      )
      OR (
        (
          X.TERM_ID IS NULL AND NOT Y.TERM_ID IS NULL
        )
        OR (
          NOT X.TERM_ID IS NULL AND Y.TERM_ID IS NULL
        )
        OR (
          X.TERM_ID <> Y.TERM_ID
        )
      )
      OR (
        (
          X.TO_ACC IS NULL AND NOT Y.TO_ACC IS NULL
        )
        OR (
          NOT X.TO_ACC IS NULL AND Y.TO_ACC IS NULL
        )
        OR (
          X.TO_ACC <> Y.TO_ACC
        )
      )
      OR (
        (
          X.TRANS_DT_TIME IS NULL AND NOT Y.TRANS_DT_TIME IS NULL
        )
        OR (
          NOT X.TRANS_DT_TIME IS NULL AND Y.TRANS_DT_TIME IS NULL
        )
        OR (
          X.TRANS_DT_TIME <> Y.TRANS_DT_TIME
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
          X.TXN_AMT IS NULL AND NOT Y.TXN_AMT IS NULL
        )
        OR (
          NOT X.TXN_AMT IS NULL AND Y.TXN_AMT IS NULL
        )
        OR (
          X.TXN_AMT <> Y.TXN_AMT
        )
      )
      OR (
        (
          X.TXN_CCY_CODE IS NULL AND NOT Y.TXN_CCY_CODE IS NULL
        )
        OR (
          NOT X.TXN_CCY_CODE IS NULL AND Y.TXN_CCY_CODE IS NULL
        )
        OR (
          X.TXN_CCY_CODE <> Y.TXN_CCY_CODE
        )
      )
      OR (
        (
          X.TXN_DESC IS NULL AND NOT Y.TXN_DESC IS NULL
        )
        OR (
          NOT X.TXN_DESC IS NULL AND Y.TXN_DESC IS NULL
        )
        OR (
          X.TXN_DESC <> Y.TXN_DESC
        )
      )
      OR (
        (
          X.WORK_PROGRESS IS NULL AND NOT Y.WORK_PROGRESS IS NULL
        )
        OR (
          NOT X.WORK_PROGRESS IS NULL AND Y.WORK_PROGRESS IS NULL
        )
        OR (
          X.WORK_PROGRESS <> Y.WORK_PROGRESS
        )
      )
    )
    THEN 'UPD'
    WHEN X.XREF IS NULL
    THEN 'DEL' /* - IF MULTIPLE PKs are there then concatenate them with AND */
  END AS OPT_FLAG
FROM DEVT_STG_FLX.SWTB_TXN_HIST AS X
FULL JOIN DEVT_STG_FLX.SWTB_TXN_HIST_BACKUP AS Y
  ON X.XREF = Y.XREF /* - IF MULTIPLE PKs are there then concatenate them with AND */
WHERE
  NOT OPT_FLAG IS NULL