CREATE OR REPLACE VIEW DEVV_STG_FLX.CYTM_CCY_DEFN_get_delta AS LOCKING ROW FOR ACCESS
SELECT
  COALESCE(X.CCY_CODE, Y.CCY_CODE) AS CCY_CODE,
  X.ALT_CCY_CODE,
  X.AUTH_STAT,
  X.BM_CCY_CODE,
  X.CCY_DECIMALS,
  X.CCY_EUR_TYPE,
  X.CCY_FORMAT_MASK,
  X.CCY_INT_METHOD,
  X.CCY_NAME,
  X.CCY_ROUND_RULE,
  X.CCY_ROUND_UNIT,
  X.CCY_SPOT_DAYS,
  X.CCY_TOL_LIMIT,
  X.CCY_TYPE,
  X.CHECKER_DT_STAMP,
  X.CHECKER_ID,
  X.CLS_CCY,
  X.COUNTRY,
  X.CR_AUTO_EX_RATE_LMT,
  X.CUT_OFF_DAYS,
  X.CUT_OFF_HR,
  X.CUT_OFF_MIN,
  X.DR_AUTO_EX_RATE_LMT,
  X.EUR_CONVERSION_REQD,
  X.FX_NETTING_DAYS,
  X.GEN_103P,
  X.GEN_CUST_COV,
  X.INDEX_BASE_CCY,
  X.INDEX_FLAG,
  X.ISO_NUM_CCY_CODE,
  X.MAKER_DT_STAMP,
  X.MAKER_ID,
  X.MOD_NO,
  X.ONCE_AUTH,
  X.POSITION_EQVGL,
  X.POSITION_GL,
  X.RECORD_STAT,
  X.SETTLEMENT_MSG_DAYS,
  X.VALIDATE_50F,
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
    WHEN Y.CCY_CODE IS NULL
    THEN 'INS' /* - IF MULTIPLE PKs are there then concatenate them with AND */
    WHEN X.CCY_CODE = Y.CCY_CODE /* - IF MULTIPLE PKs are there then concatenate them with AND */
    AND (
      (
        (
          X.ALT_CCY_CODE IS NULL AND NOT Y.ALT_CCY_CODE IS NULL
        )
        OR (
          NOT X.ALT_CCY_CODE IS NULL AND Y.ALT_CCY_CODE IS NULL
        )
        OR (
          X.ALT_CCY_CODE <> Y.ALT_CCY_CODE
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
          X.BM_CCY_CODE IS NULL AND NOT Y.BM_CCY_CODE IS NULL
        )
        OR (
          NOT X.BM_CCY_CODE IS NULL AND Y.BM_CCY_CODE IS NULL
        )
        OR (
          X.BM_CCY_CODE <> Y.BM_CCY_CODE
        )
      )
      OR (
        (
          X.CCY_DECIMALS IS NULL AND NOT Y.CCY_DECIMALS IS NULL
        )
        OR (
          NOT X.CCY_DECIMALS IS NULL AND Y.CCY_DECIMALS IS NULL
        )
        OR (
          X.CCY_DECIMALS <> Y.CCY_DECIMALS
        )
      )
      OR (
        (
          X.CCY_EUR_TYPE IS NULL AND NOT Y.CCY_EUR_TYPE IS NULL
        )
        OR (
          NOT X.CCY_EUR_TYPE IS NULL AND Y.CCY_EUR_TYPE IS NULL
        )
        OR (
          X.CCY_EUR_TYPE <> Y.CCY_EUR_TYPE
        )
      )
      OR (
        (
          X.CCY_FORMAT_MASK IS NULL AND NOT Y.CCY_FORMAT_MASK IS NULL
        )
        OR (
          NOT X.CCY_FORMAT_MASK IS NULL AND Y.CCY_FORMAT_MASK IS NULL
        )
        OR (
          X.CCY_FORMAT_MASK <> Y.CCY_FORMAT_MASK
        )
      )
      OR (
        (
          X.CCY_INT_METHOD IS NULL AND NOT Y.CCY_INT_METHOD IS NULL
        )
        OR (
          NOT X.CCY_INT_METHOD IS NULL AND Y.CCY_INT_METHOD IS NULL
        )
        OR (
          X.CCY_INT_METHOD <> Y.CCY_INT_METHOD
        )
      )
      OR (
        (
          X.CCY_NAME IS NULL AND NOT Y.CCY_NAME IS NULL
        )
        OR (
          NOT X.CCY_NAME IS NULL AND Y.CCY_NAME IS NULL
        )
        OR (
          X.CCY_NAME <> Y.CCY_NAME
        )
      )
      OR (
        (
          X.CCY_ROUND_RULE IS NULL AND NOT Y.CCY_ROUND_RULE IS NULL
        )
        OR (
          NOT X.CCY_ROUND_RULE IS NULL AND Y.CCY_ROUND_RULE IS NULL
        )
        OR (
          X.CCY_ROUND_RULE <> Y.CCY_ROUND_RULE
        )
      )
      OR (
        (
          X.CCY_ROUND_UNIT IS NULL AND NOT Y.CCY_ROUND_UNIT IS NULL
        )
        OR (
          NOT X.CCY_ROUND_UNIT IS NULL AND Y.CCY_ROUND_UNIT IS NULL
        )
        OR (
          X.CCY_ROUND_UNIT <> Y.CCY_ROUND_UNIT
        )
      )
      OR (
        (
          X.CCY_SPOT_DAYS IS NULL AND NOT Y.CCY_SPOT_DAYS IS NULL
        )
        OR (
          NOT X.CCY_SPOT_DAYS IS NULL AND Y.CCY_SPOT_DAYS IS NULL
        )
        OR (
          X.CCY_SPOT_DAYS <> Y.CCY_SPOT_DAYS
        )
      )
      OR (
        (
          X.CCY_TOL_LIMIT IS NULL AND NOT Y.CCY_TOL_LIMIT IS NULL
        )
        OR (
          NOT X.CCY_TOL_LIMIT IS NULL AND Y.CCY_TOL_LIMIT IS NULL
        )
        OR (
          X.CCY_TOL_LIMIT <> Y.CCY_TOL_LIMIT
        )
      )
      OR (
        (
          X.CCY_TYPE IS NULL AND NOT Y.CCY_TYPE IS NULL
        )
        OR (
          NOT X.CCY_TYPE IS NULL AND Y.CCY_TYPE IS NULL
        )
        OR (
          X.CCY_TYPE <> Y.CCY_TYPE
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
          X.CLS_CCY IS NULL AND NOT Y.CLS_CCY IS NULL
        )
        OR (
          NOT X.CLS_CCY IS NULL AND Y.CLS_CCY IS NULL
        )
        OR (
          X.CLS_CCY <> Y.CLS_CCY
        )
      )
      OR (
        (
          X.COUNTRY IS NULL AND NOT Y.COUNTRY IS NULL
        )
        OR (
          NOT X.COUNTRY IS NULL AND Y.COUNTRY IS NULL
        )
        OR (
          X.COUNTRY <> Y.COUNTRY
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
          X.CUT_OFF_DAYS IS NULL AND NOT Y.CUT_OFF_DAYS IS NULL
        )
        OR (
          NOT X.CUT_OFF_DAYS IS NULL AND Y.CUT_OFF_DAYS IS NULL
        )
        OR (
          X.CUT_OFF_DAYS <> Y.CUT_OFF_DAYS
        )
      )
      OR (
        (
          X.CUT_OFF_HR IS NULL AND NOT Y.CUT_OFF_HR IS NULL
        )
        OR (
          NOT X.CUT_OFF_HR IS NULL AND Y.CUT_OFF_HR IS NULL
        )
        OR (
          X.CUT_OFF_HR <> Y.CUT_OFF_HR
        )
      )
      OR (
        (
          X.CUT_OFF_MIN IS NULL AND NOT Y.CUT_OFF_MIN IS NULL
        )
        OR (
          NOT X.CUT_OFF_MIN IS NULL AND Y.CUT_OFF_MIN IS NULL
        )
        OR (
          X.CUT_OFF_MIN <> Y.CUT_OFF_MIN
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
          X.EUR_CONVERSION_REQD IS NULL AND NOT Y.EUR_CONVERSION_REQD IS NULL
        )
        OR (
          NOT X.EUR_CONVERSION_REQD IS NULL AND Y.EUR_CONVERSION_REQD IS NULL
        )
        OR (
          X.EUR_CONVERSION_REQD <> Y.EUR_CONVERSION_REQD
        )
      )
      OR (
        (
          X.FX_NETTING_DAYS IS NULL AND NOT Y.FX_NETTING_DAYS IS NULL
        )
        OR (
          NOT X.FX_NETTING_DAYS IS NULL AND Y.FX_NETTING_DAYS IS NULL
        )
        OR (
          X.FX_NETTING_DAYS <> Y.FX_NETTING_DAYS
        )
      )
      OR (
        (
          X.GEN_103P IS NULL AND NOT Y.GEN_103P IS NULL
        )
        OR (
          NOT X.GEN_103P IS NULL AND Y.GEN_103P IS NULL
        )
        OR (
          X.GEN_103P <> Y.GEN_103P
        )
      )
      OR (
        (
          X.GEN_CUST_COV IS NULL AND NOT Y.GEN_CUST_COV IS NULL
        )
        OR (
          NOT X.GEN_CUST_COV IS NULL AND Y.GEN_CUST_COV IS NULL
        )
        OR (
          X.GEN_CUST_COV <> Y.GEN_CUST_COV
        )
      )
      OR (
        (
          X.INDEX_BASE_CCY IS NULL AND NOT Y.INDEX_BASE_CCY IS NULL
        )
        OR (
          NOT X.INDEX_BASE_CCY IS NULL AND Y.INDEX_BASE_CCY IS NULL
        )
        OR (
          X.INDEX_BASE_CCY <> Y.INDEX_BASE_CCY
        )
      )
      OR (
        (
          X.INDEX_FLAG IS NULL AND NOT Y.INDEX_FLAG IS NULL
        )
        OR (
          NOT X.INDEX_FLAG IS NULL AND Y.INDEX_FLAG IS NULL
        )
        OR (
          X.INDEX_FLAG <> Y.INDEX_FLAG
        )
      )
      OR (
        (
          X.ISO_NUM_CCY_CODE IS NULL AND NOT Y.ISO_NUM_CCY_CODE IS NULL
        )
        OR (
          NOT X.ISO_NUM_CCY_CODE IS NULL AND Y.ISO_NUM_CCY_CODE IS NULL
        )
        OR (
          X.ISO_NUM_CCY_CODE <> Y.ISO_NUM_CCY_CODE
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
          X.POSITION_EQVGL IS NULL AND NOT Y.POSITION_EQVGL IS NULL
        )
        OR (
          NOT X.POSITION_EQVGL IS NULL AND Y.POSITION_EQVGL IS NULL
        )
        OR (
          X.POSITION_EQVGL <> Y.POSITION_EQVGL
        )
      )
      OR (
        (
          X.POSITION_GL IS NULL AND NOT Y.POSITION_GL IS NULL
        )
        OR (
          NOT X.POSITION_GL IS NULL AND Y.POSITION_GL IS NULL
        )
        OR (
          X.POSITION_GL <> Y.POSITION_GL
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
          X.SETTLEMENT_MSG_DAYS IS NULL AND NOT Y.SETTLEMENT_MSG_DAYS IS NULL
        )
        OR (
          NOT X.SETTLEMENT_MSG_DAYS IS NULL AND Y.SETTLEMENT_MSG_DAYS IS NULL
        )
        OR (
          X.SETTLEMENT_MSG_DAYS <> Y.SETTLEMENT_MSG_DAYS
        )
      )
      OR (
        (
          X.VALIDATE_50F IS NULL AND NOT Y.VALIDATE_50F IS NULL
        )
        OR (
          NOT X.VALIDATE_50F IS NULL AND Y.VALIDATE_50F IS NULL
        )
        OR (
          X.VALIDATE_50F <> Y.VALIDATE_50F
        )
      )
    )
    THEN 'UPD'
    WHEN X.CCY_CODE IS NULL
    THEN 'DEL' /* - IF MULTIPLE PKs are there then concatenate them with AND */
  END AS OPT_FLAG
FROM DEVT_STG_FLX.CYTM_CCY_DEFN AS X
FULL JOIN DEVT_STG_FLX.CYTM_CCY_DEFN_BACKUP AS Y
  ON X.CCY_CODE = Y.CCY_CODE /* - IF MULTIPLE PKs are there then concatenate them with AND */
WHERE
  NOT OPT_FLAG IS NULL