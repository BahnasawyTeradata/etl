CREATE OR REPLACE VIEW DEVV_STG_FLX.GETM_LIAB_get_delta AS LOCKING ROW FOR ACCESS
SELECT
  COALESCE(X.ID, Y.ID) AS ID,
  X.AUTH_STAT,
  X.BM_AUTH_STAT,
  X.BM_LIAB_CCY,
  X.BM_MOD_NO,
  X.BM_RECORD_STAT,
  X.BM_UDF_VALUE_23,
  X.CATEGORY,
  X.CHECKER_DT_STAMP,
  X.CHECKER_ID,
  X.CONVERSION_DATE,
  X.CREDIT_RATING,
  X.FX_CLEAN_RISK_LIMIT,
  X.LAST_REVAL_DATE,
  X.LAST_REVAL_XRATE,
  X.LIAB_BRANCH,
  X.LIAB_CCY,
  X.LIAB_NAME,
  X.LIAB_NO,
  X.MAIN_LIAB_ID,
  X.MAKER_DT_STAMP,
  X.MAKER_ID,
  X.MOD_NO,
  X.NETTING_REQUIRED,
  X.ONCE_AUTH,
  X.OVERALL_LIMIT,
  X.OVERALL_SCORE,
  X.PROCESS_NO,
  X.RECORD_STAT,
  X.REVISION_DATE,
  X.SEC_CLEAN_RISK_LIMIT,
  X.SEC_PSTL_RISK_LIMIT,
  X.SK_LIAB_NO,
  X.SK_LIAB_BRANCH,
  X.SK_CHECKER_ID,
  X.SK_MAKER_ID,
  X.SK_ID,
  X.SK_LIAB_NO_UDF_VALUE_24,
  X.SK_LIAB_NO_UDF_VALUE_9,
  X.SOURCE,
  X.UDF_VALUE_1,
  X.UDF_VALUE_10,
  X.UDF_VALUE_11,
  X.UDF_VALUE_12,
  X.UDF_VALUE_13,
  X.UDF_VALUE_14,
  X.UDF_VALUE_15,
  X.UDF_VALUE_16,
  X.UDF_VALUE_17,
  X.UDF_VALUE_18,
  X.UDF_VALUE_19,
  X.UDF_VALUE_2,
  X.UDF_VALUE_20,
  X.UDF_VALUE_21,
  X.UDF_VALUE_22,
  X.UDF_VALUE_23,
  X.UDF_VALUE_24,
  X.UDF_VALUE_25,
  X.UDF_VALUE_26,
  X.UDF_VALUE_27,
  X.UDF_VALUE_28,
  X.UDF_VALUE_29,
  X.UDF_VALUE_3,
  X.UDF_VALUE_30,
  X.UDF_VALUE_31,
  X.UDF_VALUE_32,
  X.UDF_VALUE_33,
  X.UDF_VALUE_34,
  X.UDF_VALUE_35,
  X.UDF_VALUE_36,
  X.UDF_VALUE_37,
  X.UDF_VALUE_38,
  X.UDF_VALUE_39,
  X.UDF_VALUE_4,
  X.UDF_VALUE_40,
  X.UDF_VALUE_41,
  X.UDF_VALUE_42,
  X.UDF_VALUE_43,
  X.UDF_VALUE_44,
  X.UDF_VALUE_45,
  X.UDF_VALUE_46,
  X.UDF_VALUE_47,
  X.UDF_VALUE_48,
  X.UDF_VALUE_49,
  X.UDF_VALUE_5,
  X.UDF_VALUE_50,
  X.UDF_VALUE_6,
  X.UDF_VALUE_7,
  X.UDF_VALUE_8,
  X.UDF_VALUE_9,
  X.UNADVISED,
  X.USER_DEFINE_STATUS,
  X.USER_REFNO,
  X.UTIL_AMT,
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
    WHEN Y.ID IS NULL
    THEN 'INS' /* - IF MULTIPLE PKs are there then concatenate them with AND */
    WHEN X.ID = Y.ID /* - IF MULTIPLE PKs are there then concatenate them with AND */
    AND (
      (
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
          X.BM_LIAB_CCY IS NULL AND NOT Y.BM_LIAB_CCY IS NULL
        )
        OR (
          NOT X.BM_LIAB_CCY IS NULL AND Y.BM_LIAB_CCY IS NULL
        )
        OR (
          X.BM_LIAB_CCY <> Y.BM_LIAB_CCY
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
          X.BM_UDF_VALUE_23 IS NULL AND NOT Y.BM_UDF_VALUE_23 IS NULL
        )
        OR (
          NOT X.BM_UDF_VALUE_23 IS NULL AND Y.BM_UDF_VALUE_23 IS NULL
        )
        OR (
          X.BM_UDF_VALUE_23 <> Y.BM_UDF_VALUE_23
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
          X.CONVERSION_DATE IS NULL AND NOT Y.CONVERSION_DATE IS NULL
        )
        OR (
          NOT X.CONVERSION_DATE IS NULL AND Y.CONVERSION_DATE IS NULL
        )
        OR (
          X.CONVERSION_DATE <> Y.CONVERSION_DATE
        )
      )
      OR (
        (
          X.CREDIT_RATING IS NULL AND NOT Y.CREDIT_RATING IS NULL
        )
        OR (
          NOT X.CREDIT_RATING IS NULL AND Y.CREDIT_RATING IS NULL
        )
        OR (
          X.CREDIT_RATING <> Y.CREDIT_RATING
        )
      )
      OR (
        (
          X.FX_CLEAN_RISK_LIMIT IS NULL AND NOT Y.FX_CLEAN_RISK_LIMIT IS NULL
        )
        OR (
          NOT X.FX_CLEAN_RISK_LIMIT IS NULL AND Y.FX_CLEAN_RISK_LIMIT IS NULL
        )
        OR (
          X.FX_CLEAN_RISK_LIMIT <> Y.FX_CLEAN_RISK_LIMIT
        )
      )
      OR (
        (
          X.LAST_REVAL_DATE IS NULL AND NOT Y.LAST_REVAL_DATE IS NULL
        )
        OR (
          NOT X.LAST_REVAL_DATE IS NULL AND Y.LAST_REVAL_DATE IS NULL
        )
        OR (
          X.LAST_REVAL_DATE <> Y.LAST_REVAL_DATE
        )
      )
      OR (
        (
          X.LAST_REVAL_XRATE IS NULL AND NOT Y.LAST_REVAL_XRATE IS NULL
        )
        OR (
          NOT X.LAST_REVAL_XRATE IS NULL AND Y.LAST_REVAL_XRATE IS NULL
        )
        OR (
          X.LAST_REVAL_XRATE <> Y.LAST_REVAL_XRATE
        )
      )
      OR (
        (
          X.LIAB_BRANCH IS NULL AND NOT Y.LIAB_BRANCH IS NULL
        )
        OR (
          NOT X.LIAB_BRANCH IS NULL AND Y.LIAB_BRANCH IS NULL
        )
        OR (
          X.LIAB_BRANCH <> Y.LIAB_BRANCH
        )
      )
      OR (
        (
          X.LIAB_CCY IS NULL AND NOT Y.LIAB_CCY IS NULL
        )
        OR (
          NOT X.LIAB_CCY IS NULL AND Y.LIAB_CCY IS NULL
        )
        OR (
          X.LIAB_CCY <> Y.LIAB_CCY
        )
      )
      OR (
        (
          X.LIAB_NAME IS NULL AND NOT Y.LIAB_NAME IS NULL
        )
        OR (
          NOT X.LIAB_NAME IS NULL AND Y.LIAB_NAME IS NULL
        )
        OR (
          X.LIAB_NAME <> Y.LIAB_NAME
        )
      )
      OR (
        (
          X.LIAB_NO IS NULL AND NOT Y.LIAB_NO IS NULL
        )
        OR (
          NOT X.LIAB_NO IS NULL AND Y.LIAB_NO IS NULL
        )
        OR (
          X.LIAB_NO <> Y.LIAB_NO
        )
      )
      OR (
        (
          X.MAIN_LIAB_ID IS NULL AND NOT Y.MAIN_LIAB_ID IS NULL
        )
        OR (
          NOT X.MAIN_LIAB_ID IS NULL AND Y.MAIN_LIAB_ID IS NULL
        )
        OR (
          X.MAIN_LIAB_ID <> Y.MAIN_LIAB_ID
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
          X.OVERALL_LIMIT IS NULL AND NOT Y.OVERALL_LIMIT IS NULL
        )
        OR (
          NOT X.OVERALL_LIMIT IS NULL AND Y.OVERALL_LIMIT IS NULL
        )
        OR (
          X.OVERALL_LIMIT <> Y.OVERALL_LIMIT
        )
      )
      OR (
        (
          X.OVERALL_SCORE IS NULL AND NOT Y.OVERALL_SCORE IS NULL
        )
        OR (
          NOT X.OVERALL_SCORE IS NULL AND Y.OVERALL_SCORE IS NULL
        )
        OR (
          X.OVERALL_SCORE <> Y.OVERALL_SCORE
        )
      )
      OR (
        (
          X.PROCESS_NO IS NULL AND NOT Y.PROCESS_NO IS NULL
        )
        OR (
          NOT X.PROCESS_NO IS NULL AND Y.PROCESS_NO IS NULL
        )
        OR (
          X.PROCESS_NO <> Y.PROCESS_NO
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
          X.REVISION_DATE IS NULL AND NOT Y.REVISION_DATE IS NULL
        )
        OR (
          NOT X.REVISION_DATE IS NULL AND Y.REVISION_DATE IS NULL
        )
        OR (
          X.REVISION_DATE <> Y.REVISION_DATE
        )
      )
      OR (
        (
          X.SEC_CLEAN_RISK_LIMIT IS NULL AND NOT Y.SEC_CLEAN_RISK_LIMIT IS NULL
        )
        OR (
          NOT X.SEC_CLEAN_RISK_LIMIT IS NULL AND Y.SEC_CLEAN_RISK_LIMIT IS NULL
        )
        OR (
          X.SEC_CLEAN_RISK_LIMIT <> Y.SEC_CLEAN_RISK_LIMIT
        )
      )
      OR (
        (
          X.SEC_PSTL_RISK_LIMIT IS NULL AND NOT Y.SEC_PSTL_RISK_LIMIT IS NULL
        )
        OR (
          NOT X.SEC_PSTL_RISK_LIMIT IS NULL AND Y.SEC_PSTL_RISK_LIMIT IS NULL
        )
        OR (
          X.SEC_PSTL_RISK_LIMIT <> Y.SEC_PSTL_RISK_LIMIT
        )
      )
      OR (
        (
          X.SK_LIAB_NO IS NULL AND NOT Y.SK_LIAB_NO IS NULL
        )
        OR (
          NOT X.SK_LIAB_NO IS NULL AND Y.SK_LIAB_NO IS NULL
        )
        OR (
          X.SK_LIAB_NO <> Y.SK_LIAB_NO
        )
      )
      OR (
        (
          X.SK_LIAB_BRANCH IS NULL AND NOT Y.SK_LIAB_BRANCH IS NULL
        )
        OR (
          NOT X.SK_LIAB_BRANCH IS NULL AND Y.SK_LIAB_BRANCH IS NULL
        )
        OR (
          X.SK_LIAB_BRANCH <> Y.SK_LIAB_BRANCH
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
          X.SK_ID IS NULL AND NOT Y.SK_ID IS NULL
        )
        OR (
          NOT X.SK_ID IS NULL AND Y.SK_ID IS NULL
        )
        OR (
          X.SK_ID <> Y.SK_ID
        )
      )
      OR (
        (
          X.SK_LIAB_NO_UDF_VALUE_24 IS NULL AND NOT Y.SK_LIAB_NO_UDF_VALUE_24 IS NULL
        )
        OR (
          NOT X.SK_LIAB_NO_UDF_VALUE_24 IS NULL AND Y.SK_LIAB_NO_UDF_VALUE_24 IS NULL
        )
        OR (
          X.SK_LIAB_NO_UDF_VALUE_24 <> Y.SK_LIAB_NO_UDF_VALUE_24
        )
      )
      OR (
        (
          X.SK_LIAB_NO_UDF_VALUE_9 IS NULL AND NOT Y.SK_LIAB_NO_UDF_VALUE_9 IS NULL
        )
        OR (
          NOT X.SK_LIAB_NO_UDF_VALUE_9 IS NULL AND Y.SK_LIAB_NO_UDF_VALUE_9 IS NULL
        )
        OR (
          X.SK_LIAB_NO_UDF_VALUE_9 <> Y.SK_LIAB_NO_UDF_VALUE_9
        )
      )
      OR (
        (
          X.SOURCE IS NULL AND NOT Y.SOURCE IS NULL
        )
        OR (
          NOT X.SOURCE IS NULL AND Y.SOURCE IS NULL
        )
        OR (
          X.SOURCE <> Y.SOURCE
        )
      )
      OR (
        (
          X.UDF_VALUE_1 IS NULL AND NOT Y.UDF_VALUE_1 IS NULL
        )
        OR (
          NOT X.UDF_VALUE_1 IS NULL AND Y.UDF_VALUE_1 IS NULL
        )
        OR (
          X.UDF_VALUE_1 <> Y.UDF_VALUE_1
        )
      )
      OR (
        (
          X.UDF_VALUE_10 IS NULL AND NOT Y.UDF_VALUE_10 IS NULL
        )
        OR (
          NOT X.UDF_VALUE_10 IS NULL AND Y.UDF_VALUE_10 IS NULL
        )
        OR (
          X.UDF_VALUE_10 <> Y.UDF_VALUE_10
        )
      )
      OR (
        (
          X.UDF_VALUE_11 IS NULL AND NOT Y.UDF_VALUE_11 IS NULL
        )
        OR (
          NOT X.UDF_VALUE_11 IS NULL AND Y.UDF_VALUE_11 IS NULL
        )
        OR (
          X.UDF_VALUE_11 <> Y.UDF_VALUE_11
        )
      )
      OR (
        (
          X.UDF_VALUE_12 IS NULL AND NOT Y.UDF_VALUE_12 IS NULL
        )
        OR (
          NOT X.UDF_VALUE_12 IS NULL AND Y.UDF_VALUE_12 IS NULL
        )
        OR (
          X.UDF_VALUE_12 <> Y.UDF_VALUE_12
        )
      )
      OR (
        (
          X.UDF_VALUE_13 IS NULL AND NOT Y.UDF_VALUE_13 IS NULL
        )
        OR (
          NOT X.UDF_VALUE_13 IS NULL AND Y.UDF_VALUE_13 IS NULL
        )
        OR (
          X.UDF_VALUE_13 <> Y.UDF_VALUE_13
        )
      )
      OR (
        (
          X.UDF_VALUE_14 IS NULL AND NOT Y.UDF_VALUE_14 IS NULL
        )
        OR (
          NOT X.UDF_VALUE_14 IS NULL AND Y.UDF_VALUE_14 IS NULL
        )
        OR (
          X.UDF_VALUE_14 <> Y.UDF_VALUE_14
        )
      )
      OR (
        (
          X.UDF_VALUE_15 IS NULL AND NOT Y.UDF_VALUE_15 IS NULL
        )
        OR (
          NOT X.UDF_VALUE_15 IS NULL AND Y.UDF_VALUE_15 IS NULL
        )
        OR (
          X.UDF_VALUE_15 <> Y.UDF_VALUE_15
        )
      )
      OR (
        (
          X.UDF_VALUE_16 IS NULL AND NOT Y.UDF_VALUE_16 IS NULL
        )
        OR (
          NOT X.UDF_VALUE_16 IS NULL AND Y.UDF_VALUE_16 IS NULL
        )
        OR (
          X.UDF_VALUE_16 <> Y.UDF_VALUE_16
        )
      )
      OR (
        (
          X.UDF_VALUE_17 IS NULL AND NOT Y.UDF_VALUE_17 IS NULL
        )
        OR (
          NOT X.UDF_VALUE_17 IS NULL AND Y.UDF_VALUE_17 IS NULL
        )
        OR (
          X.UDF_VALUE_17 <> Y.UDF_VALUE_17
        )
      )
      OR (
        (
          X.UDF_VALUE_18 IS NULL AND NOT Y.UDF_VALUE_18 IS NULL
        )
        OR (
          NOT X.UDF_VALUE_18 IS NULL AND Y.UDF_VALUE_18 IS NULL
        )
        OR (
          X.UDF_VALUE_18 <> Y.UDF_VALUE_18
        )
      )
      OR (
        (
          X.UDF_VALUE_19 IS NULL AND NOT Y.UDF_VALUE_19 IS NULL
        )
        OR (
          NOT X.UDF_VALUE_19 IS NULL AND Y.UDF_VALUE_19 IS NULL
        )
        OR (
          X.UDF_VALUE_19 <> Y.UDF_VALUE_19
        )
      )
      OR (
        (
          X.UDF_VALUE_2 IS NULL AND NOT Y.UDF_VALUE_2 IS NULL
        )
        OR (
          NOT X.UDF_VALUE_2 IS NULL AND Y.UDF_VALUE_2 IS NULL
        )
        OR (
          X.UDF_VALUE_2 <> Y.UDF_VALUE_2
        )
      )
      OR (
        (
          X.UDF_VALUE_20 IS NULL AND NOT Y.UDF_VALUE_20 IS NULL
        )
        OR (
          NOT X.UDF_VALUE_20 IS NULL AND Y.UDF_VALUE_20 IS NULL
        )
        OR (
          X.UDF_VALUE_20 <> Y.UDF_VALUE_20
        )
      )
      OR (
        (
          X.UDF_VALUE_21 IS NULL AND NOT Y.UDF_VALUE_21 IS NULL
        )
        OR (
          NOT X.UDF_VALUE_21 IS NULL AND Y.UDF_VALUE_21 IS NULL
        )
        OR (
          X.UDF_VALUE_21 <> Y.UDF_VALUE_21
        )
      )
      OR (
        (
          X.UDF_VALUE_22 IS NULL AND NOT Y.UDF_VALUE_22 IS NULL
        )
        OR (
          NOT X.UDF_VALUE_22 IS NULL AND Y.UDF_VALUE_22 IS NULL
        )
        OR (
          X.UDF_VALUE_22 <> Y.UDF_VALUE_22
        )
      )
      OR (
        (
          X.UDF_VALUE_23 IS NULL AND NOT Y.UDF_VALUE_23 IS NULL
        )
        OR (
          NOT X.UDF_VALUE_23 IS NULL AND Y.UDF_VALUE_23 IS NULL
        )
        OR (
          X.UDF_VALUE_23 <> Y.UDF_VALUE_23
        )
      )
      OR (
        (
          X.UDF_VALUE_24 IS NULL AND NOT Y.UDF_VALUE_24 IS NULL
        )
        OR (
          NOT X.UDF_VALUE_24 IS NULL AND Y.UDF_VALUE_24 IS NULL
        )
        OR (
          X.UDF_VALUE_24 <> Y.UDF_VALUE_24
        )
      )
      OR (
        (
          X.UDF_VALUE_25 IS NULL AND NOT Y.UDF_VALUE_25 IS NULL
        )
        OR (
          NOT X.UDF_VALUE_25 IS NULL AND Y.UDF_VALUE_25 IS NULL
        )
        OR (
          X.UDF_VALUE_25 <> Y.UDF_VALUE_25
        )
      )
      OR (
        (
          X.UDF_VALUE_26 IS NULL AND NOT Y.UDF_VALUE_26 IS NULL
        )
        OR (
          NOT X.UDF_VALUE_26 IS NULL AND Y.UDF_VALUE_26 IS NULL
        )
        OR (
          X.UDF_VALUE_26 <> Y.UDF_VALUE_26
        )
      )
      OR (
        (
          X.UDF_VALUE_27 IS NULL AND NOT Y.UDF_VALUE_27 IS NULL
        )
        OR (
          NOT X.UDF_VALUE_27 IS NULL AND Y.UDF_VALUE_27 IS NULL
        )
        OR (
          X.UDF_VALUE_27 <> Y.UDF_VALUE_27
        )
      )
      OR (
        (
          X.UDF_VALUE_28 IS NULL AND NOT Y.UDF_VALUE_28 IS NULL
        )
        OR (
          NOT X.UDF_VALUE_28 IS NULL AND Y.UDF_VALUE_28 IS NULL
        )
        OR (
          X.UDF_VALUE_28 <> Y.UDF_VALUE_28
        )
      )
      OR (
        (
          X.UDF_VALUE_29 IS NULL AND NOT Y.UDF_VALUE_29 IS NULL
        )
        OR (
          NOT X.UDF_VALUE_29 IS NULL AND Y.UDF_VALUE_29 IS NULL
        )
        OR (
          X.UDF_VALUE_29 <> Y.UDF_VALUE_29
        )
      )
      OR (
        (
          X.UDF_VALUE_3 IS NULL AND NOT Y.UDF_VALUE_3 IS NULL
        )
        OR (
          NOT X.UDF_VALUE_3 IS NULL AND Y.UDF_VALUE_3 IS NULL
        )
        OR (
          X.UDF_VALUE_3 <> Y.UDF_VALUE_3
        )
      )
      OR (
        (
          X.UDF_VALUE_30 IS NULL AND NOT Y.UDF_VALUE_30 IS NULL
        )
        OR (
          NOT X.UDF_VALUE_30 IS NULL AND Y.UDF_VALUE_30 IS NULL
        )
        OR (
          X.UDF_VALUE_30 <> Y.UDF_VALUE_30
        )
      )
      OR (
        (
          X.UDF_VALUE_31 IS NULL AND NOT Y.UDF_VALUE_31 IS NULL
        )
        OR (
          NOT X.UDF_VALUE_31 IS NULL AND Y.UDF_VALUE_31 IS NULL
        )
        OR (
          X.UDF_VALUE_31 <> Y.UDF_VALUE_31
        )
      )
      OR (
        (
          X.UDF_VALUE_32 IS NULL AND NOT Y.UDF_VALUE_32 IS NULL
        )
        OR (
          NOT X.UDF_VALUE_32 IS NULL AND Y.UDF_VALUE_32 IS NULL
        )
        OR (
          X.UDF_VALUE_32 <> Y.UDF_VALUE_32
        )
      )
      OR (
        (
          X.UDF_VALUE_33 IS NULL AND NOT Y.UDF_VALUE_33 IS NULL
        )
        OR (
          NOT X.UDF_VALUE_33 IS NULL AND Y.UDF_VALUE_33 IS NULL
        )
        OR (
          X.UDF_VALUE_33 <> Y.UDF_VALUE_33
        )
      )
      OR (
        (
          X.UDF_VALUE_34 IS NULL AND NOT Y.UDF_VALUE_34 IS NULL
        )
        OR (
          NOT X.UDF_VALUE_34 IS NULL AND Y.UDF_VALUE_34 IS NULL
        )
        OR (
          X.UDF_VALUE_34 <> Y.UDF_VALUE_34
        )
      )
      OR (
        (
          X.UDF_VALUE_35 IS NULL AND NOT Y.UDF_VALUE_35 IS NULL
        )
        OR (
          NOT X.UDF_VALUE_35 IS NULL AND Y.UDF_VALUE_35 IS NULL
        )
        OR (
          X.UDF_VALUE_35 <> Y.UDF_VALUE_35
        )
      )
      OR (
        (
          X.UDF_VALUE_36 IS NULL AND NOT Y.UDF_VALUE_36 IS NULL
        )
        OR (
          NOT X.UDF_VALUE_36 IS NULL AND Y.UDF_VALUE_36 IS NULL
        )
        OR (
          X.UDF_VALUE_36 <> Y.UDF_VALUE_36
        )
      )
      OR (
        (
          X.UDF_VALUE_37 IS NULL AND NOT Y.UDF_VALUE_37 IS NULL
        )
        OR (
          NOT X.UDF_VALUE_37 IS NULL AND Y.UDF_VALUE_37 IS NULL
        )
        OR (
          X.UDF_VALUE_37 <> Y.UDF_VALUE_37
        )
      )
      OR (
        (
          X.UDF_VALUE_38 IS NULL AND NOT Y.UDF_VALUE_38 IS NULL
        )
        OR (
          NOT X.UDF_VALUE_38 IS NULL AND Y.UDF_VALUE_38 IS NULL
        )
        OR (
          X.UDF_VALUE_38 <> Y.UDF_VALUE_38
        )
      )
      OR (
        (
          X.UDF_VALUE_39 IS NULL AND NOT Y.UDF_VALUE_39 IS NULL
        )
        OR (
          NOT X.UDF_VALUE_39 IS NULL AND Y.UDF_VALUE_39 IS NULL
        )
        OR (
          X.UDF_VALUE_39 <> Y.UDF_VALUE_39
        )
      )
      OR (
        (
          X.UDF_VALUE_4 IS NULL AND NOT Y.UDF_VALUE_4 IS NULL
        )
        OR (
          NOT X.UDF_VALUE_4 IS NULL AND Y.UDF_VALUE_4 IS NULL
        )
        OR (
          X.UDF_VALUE_4 <> Y.UDF_VALUE_4
        )
      )
      OR (
        (
          X.UDF_VALUE_40 IS NULL AND NOT Y.UDF_VALUE_40 IS NULL
        )
        OR (
          NOT X.UDF_VALUE_40 IS NULL AND Y.UDF_VALUE_40 IS NULL
        )
        OR (
          X.UDF_VALUE_40 <> Y.UDF_VALUE_40
        )
      )
      OR (
        (
          X.UDF_VALUE_41 IS NULL AND NOT Y.UDF_VALUE_41 IS NULL
        )
        OR (
          NOT X.UDF_VALUE_41 IS NULL AND Y.UDF_VALUE_41 IS NULL
        )
        OR (
          X.UDF_VALUE_41 <> Y.UDF_VALUE_41
        )
      )
      OR (
        (
          X.UDF_VALUE_42 IS NULL AND NOT Y.UDF_VALUE_42 IS NULL
        )
        OR (
          NOT X.UDF_VALUE_42 IS NULL AND Y.UDF_VALUE_42 IS NULL
        )
        OR (
          X.UDF_VALUE_42 <> Y.UDF_VALUE_42
        )
      )
      OR (
        (
          X.UDF_VALUE_43 IS NULL AND NOT Y.UDF_VALUE_43 IS NULL
        )
        OR (
          NOT X.UDF_VALUE_43 IS NULL AND Y.UDF_VALUE_43 IS NULL
        )
        OR (
          X.UDF_VALUE_43 <> Y.UDF_VALUE_43
        )
      )
      OR (
        (
          X.UDF_VALUE_44 IS NULL AND NOT Y.UDF_VALUE_44 IS NULL
        )
        OR (
          NOT X.UDF_VALUE_44 IS NULL AND Y.UDF_VALUE_44 IS NULL
        )
        OR (
          X.UDF_VALUE_44 <> Y.UDF_VALUE_44
        )
      )
      OR (
        (
          X.UDF_VALUE_45 IS NULL AND NOT Y.UDF_VALUE_45 IS NULL
        )
        OR (
          NOT X.UDF_VALUE_45 IS NULL AND Y.UDF_VALUE_45 IS NULL
        )
        OR (
          X.UDF_VALUE_45 <> Y.UDF_VALUE_45
        )
      )
      OR (
        (
          X.UDF_VALUE_46 IS NULL AND NOT Y.UDF_VALUE_46 IS NULL
        )
        OR (
          NOT X.UDF_VALUE_46 IS NULL AND Y.UDF_VALUE_46 IS NULL
        )
        OR (
          X.UDF_VALUE_46 <> Y.UDF_VALUE_46
        )
      )
      OR (
        (
          X.UDF_VALUE_47 IS NULL AND NOT Y.UDF_VALUE_47 IS NULL
        )
        OR (
          NOT X.UDF_VALUE_47 IS NULL AND Y.UDF_VALUE_47 IS NULL
        )
        OR (
          X.UDF_VALUE_47 <> Y.UDF_VALUE_47
        )
      )
      OR (
        (
          X.UDF_VALUE_48 IS NULL AND NOT Y.UDF_VALUE_48 IS NULL
        )
        OR (
          NOT X.UDF_VALUE_48 IS NULL AND Y.UDF_VALUE_48 IS NULL
        )
        OR (
          X.UDF_VALUE_48 <> Y.UDF_VALUE_48
        )
      )
      OR (
        (
          X.UDF_VALUE_49 IS NULL AND NOT Y.UDF_VALUE_49 IS NULL
        )
        OR (
          NOT X.UDF_VALUE_49 IS NULL AND Y.UDF_VALUE_49 IS NULL
        )
        OR (
          X.UDF_VALUE_49 <> Y.UDF_VALUE_49
        )
      )
      OR (
        (
          X.UDF_VALUE_5 IS NULL AND NOT Y.UDF_VALUE_5 IS NULL
        )
        OR (
          NOT X.UDF_VALUE_5 IS NULL AND Y.UDF_VALUE_5 IS NULL
        )
        OR (
          X.UDF_VALUE_5 <> Y.UDF_VALUE_5
        )
      )
      OR (
        (
          X.UDF_VALUE_50 IS NULL AND NOT Y.UDF_VALUE_50 IS NULL
        )
        OR (
          NOT X.UDF_VALUE_50 IS NULL AND Y.UDF_VALUE_50 IS NULL
        )
        OR (
          X.UDF_VALUE_50 <> Y.UDF_VALUE_50
        )
      )
      OR (
        (
          X.UDF_VALUE_6 IS NULL AND NOT Y.UDF_VALUE_6 IS NULL
        )
        OR (
          NOT X.UDF_VALUE_6 IS NULL AND Y.UDF_VALUE_6 IS NULL
        )
        OR (
          X.UDF_VALUE_6 <> Y.UDF_VALUE_6
        )
      )
      OR (
        (
          X.UDF_VALUE_7 IS NULL AND NOT Y.UDF_VALUE_7 IS NULL
        )
        OR (
          NOT X.UDF_VALUE_7 IS NULL AND Y.UDF_VALUE_7 IS NULL
        )
        OR (
          X.UDF_VALUE_7 <> Y.UDF_VALUE_7
        )
      )
      OR (
        (
          X.UDF_VALUE_8 IS NULL AND NOT Y.UDF_VALUE_8 IS NULL
        )
        OR (
          NOT X.UDF_VALUE_8 IS NULL AND Y.UDF_VALUE_8 IS NULL
        )
        OR (
          X.UDF_VALUE_8 <> Y.UDF_VALUE_8
        )
      )
      OR (
        (
          X.UDF_VALUE_9 IS NULL AND NOT Y.UDF_VALUE_9 IS NULL
        )
        OR (
          NOT X.UDF_VALUE_9 IS NULL AND Y.UDF_VALUE_9 IS NULL
        )
        OR (
          X.UDF_VALUE_9 <> Y.UDF_VALUE_9
        )
      )
      OR (
        (
          X.UNADVISED IS NULL AND NOT Y.UNADVISED IS NULL
        )
        OR (
          NOT X.UNADVISED IS NULL AND Y.UNADVISED IS NULL
        )
        OR (
          X.UNADVISED <> Y.UNADVISED
        )
      )
      OR (
        (
          X.USER_DEFINE_STATUS IS NULL AND NOT Y.USER_DEFINE_STATUS IS NULL
        )
        OR (
          NOT X.USER_DEFINE_STATUS IS NULL AND Y.USER_DEFINE_STATUS IS NULL
        )
        OR (
          X.USER_DEFINE_STATUS <> Y.USER_DEFINE_STATUS
        )
      )
      OR (
        (
          X.USER_REFNO IS NULL AND NOT Y.USER_REFNO IS NULL
        )
        OR (
          NOT X.USER_REFNO IS NULL AND Y.USER_REFNO IS NULL
        )
        OR (
          X.USER_REFNO <> Y.USER_REFNO
        )
      )
      OR (
        (
          X.UTIL_AMT IS NULL AND NOT Y.UTIL_AMT IS NULL
        )
        OR (
          NOT X.UTIL_AMT IS NULL AND Y.UTIL_AMT IS NULL
        )
        OR (
          X.UTIL_AMT <> Y.UTIL_AMT
        )
      )
    )
    THEN 'UPD'
    WHEN X.ID IS NULL
    THEN 'DEL' /* - IF MULTIPLE PKs are there then concatenate them with AND */
  END AS OPT_FLAG
FROM DEVT_STG_FLX.GETM_LIAB AS X
FULL JOIN DEVT_STG_FLX.GETM_LIAB_BACKUP AS Y
  ON X.ID = Y.ID /* - IF MULTIPLE PKs are there then concatenate them with AND */
WHERE
  NOT OPT_FLAG IS NULL