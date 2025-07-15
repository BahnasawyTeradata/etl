CREATE OR REPLACE VIEW DEVV_STG_FLX.GETM_FACILITY_get_delta AS LOCKING ROW FOR ACCESS
SELECT
  COALESCE(X.ID, Y.ID) AS ID,
  X.AMOUNT_REINSTATED_TODAY,
  X.AMOUNT_UTILISED_TODAY,
  X.APPROVED_AMT,
  X.AUTH_STAT,
  X.AVAILABILITY_FLAG,
  X.AVAILABLE_AMOUNT,
  X.AVAILMENT_DATE,
  X.BLOCK_AMOUNT,
  X.BM_AUTH_STAT,
  X.BM_AVAILABILITY_FLAG,
  X.BM_CATEGORY,
  X.BM_LINE_CODE,
  X.BM_LINE_CURRENCY,
  X.BM_ONCE_AUTH,
  X.BM_RECORD_STAT,
  X.BOOKING_DATE,
  X.BRANCH_REST_TYPE,
  X.BRN,
  X.BULK_PMT_REQD,
  X.CATEGORY,
  X.CCY_REST_TYPE,
  X.CCY_RESTRICTION,
  X.CHECKER_DT_STAMP,
  X.CHECKER_ID,
  X.COLLATERAL_CONTRIBUTION,
  X.COLLATERAL_PCT,
  X.COMMITMENT_BRANCH,
  X.COMMITMENT_PRODUCT,
  X.COMMITMENT_REF_NO,
  X.COMMITMENT_SETTL_ACC,
  X.COMMITMENT_SETTL_BRN,
  X.CONVERSION_DATE,
  X.CUSTOMER_REST_TYPE,
  X.DATE_OF_FIRST_OD,
  X.DATE_OF_LAST_OD,
  X.DAY_LIGHT_ED_DT,
  X.DAY_LIGHT_LIMIT,
  X.DAY_LIGHT_ST_DT,
  X.DAYLIGHT_OD_LIMIT,
  X.DESCRIPTION,
  X.DSP_EFF_LINE_AMOUNT,
  X.EXCEP_BREACH,
  X.EXCEP_TXN_AMT,
  X.EXCESS_TENOR,
  X.EXP_REST_TYPE,
  X.EXT_SYSTEM_REST_TYPE,
  X.EXTERNAL_REFNO,
  X.FUNDED,
  X.INTEREST_CALC_ACC,
  X.INTEREST_REQD,
  X.INTERNAL_REMARKS,
  X.LAST_NEW_UTIL_DATE,
  X.LAST_REVAL_DATE,
  X.LAST_REVAL_XRATE,
  X.LENDING_CATEGORY_CODE,
  X.LIAB_BR,
  X.LIAB_ID,
  X.LIMIT_AMOUNT,
  X.LINE_CODE,
  X.LINE_CURRENCY,
  X.LINE_EXPIRY_DATE,
  X.LINE_SERIAL,
  X.LINE_START_DATE,
  X.LMT_AMT_BASIS,
  X.MAIN_LINE_ID,
  X.MAKER_DT_STAMP,
  X.MAKER_ID,
  X.MATURED_UTIL,
  X.MOD_NO,
  X.NETTING_AMOUNT,
  X.NETTING_REQUIRED,
  X.ONCE_AUTH,
  X.PPC_PROJECT_ID,
  X.PPC_REF_NO,
  X.PROCESS_NO,
  X.PRODUCT_REST_TYPE,
  X.PRODUCT_TYPE,
  X.RECORD_STAT,
  X.REPORTING_AMOUNT,
  X.REVOLVING_AMT,
  X.REVOLVING_LINE,
  X.SCHEDULE_PROCESS_DATE,
  X.SHADOW_LIMIT,
  X.SK_AVAILABILITY_FLAG,
  X.SK_UDF_VALUE_2,
  X.SK_UDF_VALUE_3,
  X.SK_UDF_VALUE_4,
  X.SK_BRANCH_REST_TYPE,
  X.SK_CCY_REST_TYPE,
  X.SK_CCY_RESTRICTION,
  X.SK_BRN,
  X.SK_COLLATERAL_PCT,
  X.SK_LIAB_BR,
  X.SK_CUSTOMER_REST_TYPE,
  X.SK_CHECKER_ID,
  X.SK_ID,
  X.SK_MAKER_ID,
  X.SK_LMT_AMT_BASIS,
  X.SK_PRODUCT_REST_TYPE,
  X.SK_REVOLVING_LINE,
  X.SOURCE,
  X.TANKED_UTIL,
  X.TENOR_REST_TYPE,
  X.TRANSFER_AMOUNT,
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
  X.UNCOLLECTED_AMOUNT,
  X.UNCOLLECTED_FUNDS_LIMIT,
  X.USER_DEF_STAT_CHANGED_DT,
  X.USER_DEFINE_STATUS,
  X.USER_REFNO,
  X.UTILISATION,
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
          X.AMOUNT_REINSTATED_TODAY IS NULL AND NOT Y.AMOUNT_REINSTATED_TODAY IS NULL
        )
        OR (
          NOT X.AMOUNT_REINSTATED_TODAY IS NULL AND Y.AMOUNT_REINSTATED_TODAY IS NULL
        )
        OR (
          X.AMOUNT_REINSTATED_TODAY <> Y.AMOUNT_REINSTATED_TODAY
        )
      )
      OR (
        (
          X.AMOUNT_UTILISED_TODAY IS NULL AND NOT Y.AMOUNT_UTILISED_TODAY IS NULL
        )
        OR (
          NOT X.AMOUNT_UTILISED_TODAY IS NULL AND Y.AMOUNT_UTILISED_TODAY IS NULL
        )
        OR (
          X.AMOUNT_UTILISED_TODAY <> Y.AMOUNT_UTILISED_TODAY
        )
      )
      OR (
        (
          X.APPROVED_AMT IS NULL AND NOT Y.APPROVED_AMT IS NULL
        )
        OR (
          NOT X.APPROVED_AMT IS NULL AND Y.APPROVED_AMT IS NULL
        )
        OR (
          X.APPROVED_AMT <> Y.APPROVED_AMT
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
          X.AVAILABILITY_FLAG IS NULL AND NOT Y.AVAILABILITY_FLAG IS NULL
        )
        OR (
          NOT X.AVAILABILITY_FLAG IS NULL AND Y.AVAILABILITY_FLAG IS NULL
        )
        OR (
          X.AVAILABILITY_FLAG <> Y.AVAILABILITY_FLAG
        )
      )
      OR (
        (
          X.AVAILABLE_AMOUNT IS NULL AND NOT Y.AVAILABLE_AMOUNT IS NULL
        )
        OR (
          NOT X.AVAILABLE_AMOUNT IS NULL AND Y.AVAILABLE_AMOUNT IS NULL
        )
        OR (
          X.AVAILABLE_AMOUNT <> Y.AVAILABLE_AMOUNT
        )
      )
      OR (
        (
          X.AVAILMENT_DATE IS NULL AND NOT Y.AVAILMENT_DATE IS NULL
        )
        OR (
          NOT X.AVAILMENT_DATE IS NULL AND Y.AVAILMENT_DATE IS NULL
        )
        OR (
          X.AVAILMENT_DATE <> Y.AVAILMENT_DATE
        )
      )
      OR (
        (
          X.BLOCK_AMOUNT IS NULL AND NOT Y.BLOCK_AMOUNT IS NULL
        )
        OR (
          NOT X.BLOCK_AMOUNT IS NULL AND Y.BLOCK_AMOUNT IS NULL
        )
        OR (
          X.BLOCK_AMOUNT <> Y.BLOCK_AMOUNT
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
          X.BM_AVAILABILITY_FLAG IS NULL AND NOT Y.BM_AVAILABILITY_FLAG IS NULL
        )
        OR (
          NOT X.BM_AVAILABILITY_FLAG IS NULL AND Y.BM_AVAILABILITY_FLAG IS NULL
        )
        OR (
          X.BM_AVAILABILITY_FLAG <> Y.BM_AVAILABILITY_FLAG
        )
      )
      OR (
        (
          X.BM_CATEGORY IS NULL AND NOT Y.BM_CATEGORY IS NULL
        )
        OR (
          NOT X.BM_CATEGORY IS NULL AND Y.BM_CATEGORY IS NULL
        )
        OR (
          X.BM_CATEGORY <> Y.BM_CATEGORY
        )
      )
      OR (
        (
          X.BM_LINE_CODE IS NULL AND NOT Y.BM_LINE_CODE IS NULL
        )
        OR (
          NOT X.BM_LINE_CODE IS NULL AND Y.BM_LINE_CODE IS NULL
        )
        OR (
          X.BM_LINE_CODE <> Y.BM_LINE_CODE
        )
      )
      OR (
        (
          X.BM_LINE_CURRENCY IS NULL AND NOT Y.BM_LINE_CURRENCY IS NULL
        )
        OR (
          NOT X.BM_LINE_CURRENCY IS NULL AND Y.BM_LINE_CURRENCY IS NULL
        )
        OR (
          X.BM_LINE_CURRENCY <> Y.BM_LINE_CURRENCY
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
          X.BOOKING_DATE IS NULL AND NOT Y.BOOKING_DATE IS NULL
        )
        OR (
          NOT X.BOOKING_DATE IS NULL AND Y.BOOKING_DATE IS NULL
        )
        OR (
          X.BOOKING_DATE <> Y.BOOKING_DATE
        )
      )
      OR (
        (
          X.BRANCH_REST_TYPE IS NULL AND NOT Y.BRANCH_REST_TYPE IS NULL
        )
        OR (
          NOT X.BRANCH_REST_TYPE IS NULL AND Y.BRANCH_REST_TYPE IS NULL
        )
        OR (
          X.BRANCH_REST_TYPE <> Y.BRANCH_REST_TYPE
        )
      )
      OR (
        (
          X.BRN IS NULL AND NOT Y.BRN IS NULL
        )
        OR (
          NOT X.BRN IS NULL AND Y.BRN IS NULL
        )
        OR (
          X.BRN <> Y.BRN
        )
      )
      OR (
        (
          X.BULK_PMT_REQD IS NULL AND NOT Y.BULK_PMT_REQD IS NULL
        )
        OR (
          NOT X.BULK_PMT_REQD IS NULL AND Y.BULK_PMT_REQD IS NULL
        )
        OR (
          X.BULK_PMT_REQD <> Y.BULK_PMT_REQD
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
          X.CCY_REST_TYPE IS NULL AND NOT Y.CCY_REST_TYPE IS NULL
        )
        OR (
          NOT X.CCY_REST_TYPE IS NULL AND Y.CCY_REST_TYPE IS NULL
        )
        OR (
          X.CCY_REST_TYPE <> Y.CCY_REST_TYPE
        )
      )
      OR (
        (
          X.CCY_RESTRICTION IS NULL AND NOT Y.CCY_RESTRICTION IS NULL
        )
        OR (
          NOT X.CCY_RESTRICTION IS NULL AND Y.CCY_RESTRICTION IS NULL
        )
        OR (
          X.CCY_RESTRICTION <> Y.CCY_RESTRICTION
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
          X.COLLATERAL_CONTRIBUTION IS NULL AND NOT Y.COLLATERAL_CONTRIBUTION IS NULL
        )
        OR (
          NOT X.COLLATERAL_CONTRIBUTION IS NULL AND Y.COLLATERAL_CONTRIBUTION IS NULL
        )
        OR (
          X.COLLATERAL_CONTRIBUTION <> Y.COLLATERAL_CONTRIBUTION
        )
      )
      OR (
        (
          X.COLLATERAL_PCT IS NULL AND NOT Y.COLLATERAL_PCT IS NULL
        )
        OR (
          NOT X.COLLATERAL_PCT IS NULL AND Y.COLLATERAL_PCT IS NULL
        )
        OR (
          X.COLLATERAL_PCT <> Y.COLLATERAL_PCT
        )
      )
      OR (
        (
          X.COMMITMENT_BRANCH IS NULL AND NOT Y.COMMITMENT_BRANCH IS NULL
        )
        OR (
          NOT X.COMMITMENT_BRANCH IS NULL AND Y.COMMITMENT_BRANCH IS NULL
        )
        OR (
          X.COMMITMENT_BRANCH <> Y.COMMITMENT_BRANCH
        )
      )
      OR (
        (
          X.COMMITMENT_PRODUCT IS NULL AND NOT Y.COMMITMENT_PRODUCT IS NULL
        )
        OR (
          NOT X.COMMITMENT_PRODUCT IS NULL AND Y.COMMITMENT_PRODUCT IS NULL
        )
        OR (
          X.COMMITMENT_PRODUCT <> Y.COMMITMENT_PRODUCT
        )
      )
      OR (
        (
          X.COMMITMENT_REF_NO IS NULL AND NOT Y.COMMITMENT_REF_NO IS NULL
        )
        OR (
          NOT X.COMMITMENT_REF_NO IS NULL AND Y.COMMITMENT_REF_NO IS NULL
        )
        OR (
          X.COMMITMENT_REF_NO <> Y.COMMITMENT_REF_NO
        )
      )
      OR (
        (
          X.COMMITMENT_SETTL_ACC IS NULL AND NOT Y.COMMITMENT_SETTL_ACC IS NULL
        )
        OR (
          NOT X.COMMITMENT_SETTL_ACC IS NULL AND Y.COMMITMENT_SETTL_ACC IS NULL
        )
        OR (
          X.COMMITMENT_SETTL_ACC <> Y.COMMITMENT_SETTL_ACC
        )
      )
      OR (
        (
          X.COMMITMENT_SETTL_BRN IS NULL AND NOT Y.COMMITMENT_SETTL_BRN IS NULL
        )
        OR (
          NOT X.COMMITMENT_SETTL_BRN IS NULL AND Y.COMMITMENT_SETTL_BRN IS NULL
        )
        OR (
          X.COMMITMENT_SETTL_BRN <> Y.COMMITMENT_SETTL_BRN
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
          X.CUSTOMER_REST_TYPE IS NULL AND NOT Y.CUSTOMER_REST_TYPE IS NULL
        )
        OR (
          NOT X.CUSTOMER_REST_TYPE IS NULL AND Y.CUSTOMER_REST_TYPE IS NULL
        )
        OR (
          X.CUSTOMER_REST_TYPE <> Y.CUSTOMER_REST_TYPE
        )
      )
      OR (
        (
          X.DATE_OF_FIRST_OD IS NULL AND NOT Y.DATE_OF_FIRST_OD IS NULL
        )
        OR (
          NOT X.DATE_OF_FIRST_OD IS NULL AND Y.DATE_OF_FIRST_OD IS NULL
        )
        OR (
          X.DATE_OF_FIRST_OD <> Y.DATE_OF_FIRST_OD
        )
      )
      OR (
        (
          X.DATE_OF_LAST_OD IS NULL AND NOT Y.DATE_OF_LAST_OD IS NULL
        )
        OR (
          NOT X.DATE_OF_LAST_OD IS NULL AND Y.DATE_OF_LAST_OD IS NULL
        )
        OR (
          X.DATE_OF_LAST_OD <> Y.DATE_OF_LAST_OD
        )
      )
      OR (
        (
          X.DAY_LIGHT_ED_DT IS NULL AND NOT Y.DAY_LIGHT_ED_DT IS NULL
        )
        OR (
          NOT X.DAY_LIGHT_ED_DT IS NULL AND Y.DAY_LIGHT_ED_DT IS NULL
        )
        OR (
          X.DAY_LIGHT_ED_DT <> Y.DAY_LIGHT_ED_DT
        )
      )
      OR (
        (
          X.DAY_LIGHT_LIMIT IS NULL AND NOT Y.DAY_LIGHT_LIMIT IS NULL
        )
        OR (
          NOT X.DAY_LIGHT_LIMIT IS NULL AND Y.DAY_LIGHT_LIMIT IS NULL
        )
        OR (
          X.DAY_LIGHT_LIMIT <> Y.DAY_LIGHT_LIMIT
        )
      )
      OR (
        (
          X.DAY_LIGHT_ST_DT IS NULL AND NOT Y.DAY_LIGHT_ST_DT IS NULL
        )
        OR (
          NOT X.DAY_LIGHT_ST_DT IS NULL AND Y.DAY_LIGHT_ST_DT IS NULL
        )
        OR (
          X.DAY_LIGHT_ST_DT <> Y.DAY_LIGHT_ST_DT
        )
      )
      OR (
        (
          X.DAYLIGHT_OD_LIMIT IS NULL AND NOT Y.DAYLIGHT_OD_LIMIT IS NULL
        )
        OR (
          NOT X.DAYLIGHT_OD_LIMIT IS NULL AND Y.DAYLIGHT_OD_LIMIT IS NULL
        )
        OR (
          X.DAYLIGHT_OD_LIMIT <> Y.DAYLIGHT_OD_LIMIT
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
          X.DSP_EFF_LINE_AMOUNT IS NULL AND NOT Y.DSP_EFF_LINE_AMOUNT IS NULL
        )
        OR (
          NOT X.DSP_EFF_LINE_AMOUNT IS NULL AND Y.DSP_EFF_LINE_AMOUNT IS NULL
        )
        OR (
          X.DSP_EFF_LINE_AMOUNT <> Y.DSP_EFF_LINE_AMOUNT
        )
      )
      OR (
        (
          X.EXCEP_BREACH IS NULL AND NOT Y.EXCEP_BREACH IS NULL
        )
        OR (
          NOT X.EXCEP_BREACH IS NULL AND Y.EXCEP_BREACH IS NULL
        )
        OR (
          X.EXCEP_BREACH <> Y.EXCEP_BREACH
        )
      )
      OR (
        (
          X.EXCEP_TXN_AMT IS NULL AND NOT Y.EXCEP_TXN_AMT IS NULL
        )
        OR (
          NOT X.EXCEP_TXN_AMT IS NULL AND Y.EXCEP_TXN_AMT IS NULL
        )
        OR (
          X.EXCEP_TXN_AMT <> Y.EXCEP_TXN_AMT
        )
      )
      OR (
        (
          X.EXCESS_TENOR IS NULL AND NOT Y.EXCESS_TENOR IS NULL
        )
        OR (
          NOT X.EXCESS_TENOR IS NULL AND Y.EXCESS_TENOR IS NULL
        )
        OR (
          X.EXCESS_TENOR <> Y.EXCESS_TENOR
        )
      )
      OR (
        (
          X.EXP_REST_TYPE IS NULL AND NOT Y.EXP_REST_TYPE IS NULL
        )
        OR (
          NOT X.EXP_REST_TYPE IS NULL AND Y.EXP_REST_TYPE IS NULL
        )
        OR (
          X.EXP_REST_TYPE <> Y.EXP_REST_TYPE
        )
      )
      OR (
        (
          X.EXT_SYSTEM_REST_TYPE IS NULL AND NOT Y.EXT_SYSTEM_REST_TYPE IS NULL
        )
        OR (
          NOT X.EXT_SYSTEM_REST_TYPE IS NULL AND Y.EXT_SYSTEM_REST_TYPE IS NULL
        )
        OR (
          X.EXT_SYSTEM_REST_TYPE <> Y.EXT_SYSTEM_REST_TYPE
        )
      )
      OR (
        (
          X.EXTERNAL_REFNO IS NULL AND NOT Y.EXTERNAL_REFNO IS NULL
        )
        OR (
          NOT X.EXTERNAL_REFNO IS NULL AND Y.EXTERNAL_REFNO IS NULL
        )
        OR (
          X.EXTERNAL_REFNO <> Y.EXTERNAL_REFNO
        )
      )
      OR (
        (
          X.FUNDED IS NULL AND NOT Y.FUNDED IS NULL
        )
        OR (
          NOT X.FUNDED IS NULL AND Y.FUNDED IS NULL
        )
        OR (
          X.FUNDED <> Y.FUNDED
        )
      )
      OR (
        (
          X.INTEREST_CALC_ACC IS NULL AND NOT Y.INTEREST_CALC_ACC IS NULL
        )
        OR (
          NOT X.INTEREST_CALC_ACC IS NULL AND Y.INTEREST_CALC_ACC IS NULL
        )
        OR (
          X.INTEREST_CALC_ACC <> Y.INTEREST_CALC_ACC
        )
      )
      OR (
        (
          X.INTEREST_REQD IS NULL AND NOT Y.INTEREST_REQD IS NULL
        )
        OR (
          NOT X.INTEREST_REQD IS NULL AND Y.INTEREST_REQD IS NULL
        )
        OR (
          X.INTEREST_REQD <> Y.INTEREST_REQD
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
          X.LAST_NEW_UTIL_DATE IS NULL AND NOT Y.LAST_NEW_UTIL_DATE IS NULL
        )
        OR (
          NOT X.LAST_NEW_UTIL_DATE IS NULL AND Y.LAST_NEW_UTIL_DATE IS NULL
        )
        OR (
          X.LAST_NEW_UTIL_DATE <> Y.LAST_NEW_UTIL_DATE
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
          X.LENDING_CATEGORY_CODE IS NULL AND NOT Y.LENDING_CATEGORY_CODE IS NULL
        )
        OR (
          NOT X.LENDING_CATEGORY_CODE IS NULL AND Y.LENDING_CATEGORY_CODE IS NULL
        )
        OR (
          X.LENDING_CATEGORY_CODE <> Y.LENDING_CATEGORY_CODE
        )
      )
      OR (
        (
          X.LIAB_BR IS NULL AND NOT Y.LIAB_BR IS NULL
        )
        OR (
          NOT X.LIAB_BR IS NULL AND Y.LIAB_BR IS NULL
        )
        OR (
          X.LIAB_BR <> Y.LIAB_BR
        )
      )
      OR (
        (
          X.LIAB_ID IS NULL AND NOT Y.LIAB_ID IS NULL
        )
        OR (
          NOT X.LIAB_ID IS NULL AND Y.LIAB_ID IS NULL
        )
        OR (
          X.LIAB_ID <> Y.LIAB_ID
        )
      )
      OR (
        (
          X.LIMIT_AMOUNT IS NULL AND NOT Y.LIMIT_AMOUNT IS NULL
        )
        OR (
          NOT X.LIMIT_AMOUNT IS NULL AND Y.LIMIT_AMOUNT IS NULL
        )
        OR (
          X.LIMIT_AMOUNT <> Y.LIMIT_AMOUNT
        )
      )
      OR (
        (
          X.LINE_CODE IS NULL AND NOT Y.LINE_CODE IS NULL
        )
        OR (
          NOT X.LINE_CODE IS NULL AND Y.LINE_CODE IS NULL
        )
        OR (
          X.LINE_CODE <> Y.LINE_CODE
        )
      )
      OR (
        (
          X.LINE_CURRENCY IS NULL AND NOT Y.LINE_CURRENCY IS NULL
        )
        OR (
          NOT X.LINE_CURRENCY IS NULL AND Y.LINE_CURRENCY IS NULL
        )
        OR (
          X.LINE_CURRENCY <> Y.LINE_CURRENCY
        )
      )
      OR (
        (
          X.LINE_EXPIRY_DATE IS NULL AND NOT Y.LINE_EXPIRY_DATE IS NULL
        )
        OR (
          NOT X.LINE_EXPIRY_DATE IS NULL AND Y.LINE_EXPIRY_DATE IS NULL
        )
        OR (
          X.LINE_EXPIRY_DATE <> Y.LINE_EXPIRY_DATE
        )
      )
      OR (
        (
          X.LINE_SERIAL IS NULL AND NOT Y.LINE_SERIAL IS NULL
        )
        OR (
          NOT X.LINE_SERIAL IS NULL AND Y.LINE_SERIAL IS NULL
        )
        OR (
          X.LINE_SERIAL <> Y.LINE_SERIAL
        )
      )
      OR (
        (
          X.LINE_START_DATE IS NULL AND NOT Y.LINE_START_DATE IS NULL
        )
        OR (
          NOT X.LINE_START_DATE IS NULL AND Y.LINE_START_DATE IS NULL
        )
        OR (
          X.LINE_START_DATE <> Y.LINE_START_DATE
        )
      )
      OR (
        (
          X.LMT_AMT_BASIS IS NULL AND NOT Y.LMT_AMT_BASIS IS NULL
        )
        OR (
          NOT X.LMT_AMT_BASIS IS NULL AND Y.LMT_AMT_BASIS IS NULL
        )
        OR (
          X.LMT_AMT_BASIS <> Y.LMT_AMT_BASIS
        )
      )
      OR (
        (
          X.MAIN_LINE_ID IS NULL AND NOT Y.MAIN_LINE_ID IS NULL
        )
        OR (
          NOT X.MAIN_LINE_ID IS NULL AND Y.MAIN_LINE_ID IS NULL
        )
        OR (
          X.MAIN_LINE_ID <> Y.MAIN_LINE_ID
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
          X.MATURED_UTIL IS NULL AND NOT Y.MATURED_UTIL IS NULL
        )
        OR (
          NOT X.MATURED_UTIL IS NULL AND Y.MATURED_UTIL IS NULL
        )
        OR (
          X.MATURED_UTIL <> Y.MATURED_UTIL
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
          X.NETTING_AMOUNT IS NULL AND NOT Y.NETTING_AMOUNT IS NULL
        )
        OR (
          NOT X.NETTING_AMOUNT IS NULL AND Y.NETTING_AMOUNT IS NULL
        )
        OR (
          X.NETTING_AMOUNT <> Y.NETTING_AMOUNT
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
          X.PPC_PROJECT_ID IS NULL AND NOT Y.PPC_PROJECT_ID IS NULL
        )
        OR (
          NOT X.PPC_PROJECT_ID IS NULL AND Y.PPC_PROJECT_ID IS NULL
        )
        OR (
          X.PPC_PROJECT_ID <> Y.PPC_PROJECT_ID
        )
      )
      OR (
        (
          X.PPC_REF_NO IS NULL AND NOT Y.PPC_REF_NO IS NULL
        )
        OR (
          NOT X.PPC_REF_NO IS NULL AND Y.PPC_REF_NO IS NULL
        )
        OR (
          X.PPC_REF_NO <> Y.PPC_REF_NO
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
          X.PRODUCT_REST_TYPE IS NULL AND NOT Y.PRODUCT_REST_TYPE IS NULL
        )
        OR (
          NOT X.PRODUCT_REST_TYPE IS NULL AND Y.PRODUCT_REST_TYPE IS NULL
        )
        OR (
          X.PRODUCT_REST_TYPE <> Y.PRODUCT_REST_TYPE
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
          X.REPORTING_AMOUNT IS NULL AND NOT Y.REPORTING_AMOUNT IS NULL
        )
        OR (
          NOT X.REPORTING_AMOUNT IS NULL AND Y.REPORTING_AMOUNT IS NULL
        )
        OR (
          X.REPORTING_AMOUNT <> Y.REPORTING_AMOUNT
        )
      )
      OR (
        (
          X.REVOLVING_AMT IS NULL AND NOT Y.REVOLVING_AMT IS NULL
        )
        OR (
          NOT X.REVOLVING_AMT IS NULL AND Y.REVOLVING_AMT IS NULL
        )
        OR (
          X.REVOLVING_AMT <> Y.REVOLVING_AMT
        )
      )
      OR (
        (
          X.REVOLVING_LINE IS NULL AND NOT Y.REVOLVING_LINE IS NULL
        )
        OR (
          NOT X.REVOLVING_LINE IS NULL AND Y.REVOLVING_LINE IS NULL
        )
        OR (
          X.REVOLVING_LINE <> Y.REVOLVING_LINE
        )
      )
      OR (
        (
          X.SCHEDULE_PROCESS_DATE IS NULL AND NOT Y.SCHEDULE_PROCESS_DATE IS NULL
        )
        OR (
          NOT X.SCHEDULE_PROCESS_DATE IS NULL AND Y.SCHEDULE_PROCESS_DATE IS NULL
        )
        OR (
          X.SCHEDULE_PROCESS_DATE <> Y.SCHEDULE_PROCESS_DATE
        )
      )
      OR (
        (
          X.SHADOW_LIMIT IS NULL AND NOT Y.SHADOW_LIMIT IS NULL
        )
        OR (
          NOT X.SHADOW_LIMIT IS NULL AND Y.SHADOW_LIMIT IS NULL
        )
        OR (
          X.SHADOW_LIMIT <> Y.SHADOW_LIMIT
        )
      )
      OR (
        (
          X.SK_AVAILABILITY_FLAG IS NULL AND NOT Y.SK_AVAILABILITY_FLAG IS NULL
        )
        OR (
          NOT X.SK_AVAILABILITY_FLAG IS NULL AND Y.SK_AVAILABILITY_FLAG IS NULL
        )
        OR (
          X.SK_AVAILABILITY_FLAG <> Y.SK_AVAILABILITY_FLAG
        )
      )
      OR (
        (
          X.SK_UDF_VALUE_2 IS NULL AND NOT Y.SK_UDF_VALUE_2 IS NULL
        )
        OR (
          NOT X.SK_UDF_VALUE_2 IS NULL AND Y.SK_UDF_VALUE_2 IS NULL
        )
        OR (
          X.SK_UDF_VALUE_2 <> Y.SK_UDF_VALUE_2
        )
      )
      OR (
        (
          X.SK_UDF_VALUE_3 IS NULL AND NOT Y.SK_UDF_VALUE_3 IS NULL
        )
        OR (
          NOT X.SK_UDF_VALUE_3 IS NULL AND Y.SK_UDF_VALUE_3 IS NULL
        )
        OR (
          X.SK_UDF_VALUE_3 <> Y.SK_UDF_VALUE_3
        )
      )
      OR (
        (
          X.SK_UDF_VALUE_4 IS NULL AND NOT Y.SK_UDF_VALUE_4 IS NULL
        )
        OR (
          NOT X.SK_UDF_VALUE_4 IS NULL AND Y.SK_UDF_VALUE_4 IS NULL
        )
        OR (
          X.SK_UDF_VALUE_4 <> Y.SK_UDF_VALUE_4
        )
      )
      OR (
        (
          X.SK_BRANCH_REST_TYPE IS NULL AND NOT Y.SK_BRANCH_REST_TYPE IS NULL
        )
        OR (
          NOT X.SK_BRANCH_REST_TYPE IS NULL AND Y.SK_BRANCH_REST_TYPE IS NULL
        )
        OR (
          X.SK_BRANCH_REST_TYPE <> Y.SK_BRANCH_REST_TYPE
        )
      )
      OR (
        (
          X.SK_CCY_REST_TYPE IS NULL AND NOT Y.SK_CCY_REST_TYPE IS NULL
        )
        OR (
          NOT X.SK_CCY_REST_TYPE IS NULL AND Y.SK_CCY_REST_TYPE IS NULL
        )
        OR (
          X.SK_CCY_REST_TYPE <> Y.SK_CCY_REST_TYPE
        )
      )
      OR (
        (
          X.SK_CCY_RESTRICTION IS NULL AND NOT Y.SK_CCY_RESTRICTION IS NULL
        )
        OR (
          NOT X.SK_CCY_RESTRICTION IS NULL AND Y.SK_CCY_RESTRICTION IS NULL
        )
        OR (
          X.SK_CCY_RESTRICTION <> Y.SK_CCY_RESTRICTION
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
          X.SK_COLLATERAL_PCT IS NULL AND NOT Y.SK_COLLATERAL_PCT IS NULL
        )
        OR (
          NOT X.SK_COLLATERAL_PCT IS NULL AND Y.SK_COLLATERAL_PCT IS NULL
        )
        OR (
          X.SK_COLLATERAL_PCT <> Y.SK_COLLATERAL_PCT
        )
      )
      OR (
        (
          X.SK_LIAB_BR IS NULL AND NOT Y.SK_LIAB_BR IS NULL
        )
        OR (
          NOT X.SK_LIAB_BR IS NULL AND Y.SK_LIAB_BR IS NULL
        )
        OR (
          X.SK_LIAB_BR <> Y.SK_LIAB_BR
        )
      )
      OR (
        (
          X.SK_CUSTOMER_REST_TYPE IS NULL AND NOT Y.SK_CUSTOMER_REST_TYPE IS NULL
        )
        OR (
          NOT X.SK_CUSTOMER_REST_TYPE IS NULL AND Y.SK_CUSTOMER_REST_TYPE IS NULL
        )
        OR (
          X.SK_CUSTOMER_REST_TYPE <> Y.SK_CUSTOMER_REST_TYPE
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
          X.SK_LMT_AMT_BASIS IS NULL AND NOT Y.SK_LMT_AMT_BASIS IS NULL
        )
        OR (
          NOT X.SK_LMT_AMT_BASIS IS NULL AND Y.SK_LMT_AMT_BASIS IS NULL
        )
        OR (
          X.SK_LMT_AMT_BASIS <> Y.SK_LMT_AMT_BASIS
        )
      )
      OR (
        (
          X.SK_PRODUCT_REST_TYPE IS NULL AND NOT Y.SK_PRODUCT_REST_TYPE IS NULL
        )
        OR (
          NOT X.SK_PRODUCT_REST_TYPE IS NULL AND Y.SK_PRODUCT_REST_TYPE IS NULL
        )
        OR (
          X.SK_PRODUCT_REST_TYPE <> Y.SK_PRODUCT_REST_TYPE
        )
      )
      OR (
        (
          X.SK_REVOLVING_LINE IS NULL AND NOT Y.SK_REVOLVING_LINE IS NULL
        )
        OR (
          NOT X.SK_REVOLVING_LINE IS NULL AND Y.SK_REVOLVING_LINE IS NULL
        )
        OR (
          X.SK_REVOLVING_LINE <> Y.SK_REVOLVING_LINE
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
          X.TANKED_UTIL IS NULL AND NOT Y.TANKED_UTIL IS NULL
        )
        OR (
          NOT X.TANKED_UTIL IS NULL AND Y.TANKED_UTIL IS NULL
        )
        OR (
          X.TANKED_UTIL <> Y.TANKED_UTIL
        )
      )
      OR (
        (
          X.TENOR_REST_TYPE IS NULL AND NOT Y.TENOR_REST_TYPE IS NULL
        )
        OR (
          NOT X.TENOR_REST_TYPE IS NULL AND Y.TENOR_REST_TYPE IS NULL
        )
        OR (
          X.TENOR_REST_TYPE <> Y.TENOR_REST_TYPE
        )
      )
      OR (
        (
          X.TRANSFER_AMOUNT IS NULL AND NOT Y.TRANSFER_AMOUNT IS NULL
        )
        OR (
          NOT X.TRANSFER_AMOUNT IS NULL AND Y.TRANSFER_AMOUNT IS NULL
        )
        OR (
          X.TRANSFER_AMOUNT <> Y.TRANSFER_AMOUNT
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
          X.UNCOLLECTED_AMOUNT IS NULL AND NOT Y.UNCOLLECTED_AMOUNT IS NULL
        )
        OR (
          NOT X.UNCOLLECTED_AMOUNT IS NULL AND Y.UNCOLLECTED_AMOUNT IS NULL
        )
        OR (
          X.UNCOLLECTED_AMOUNT <> Y.UNCOLLECTED_AMOUNT
        )
      )
      OR (
        (
          X.UNCOLLECTED_FUNDS_LIMIT IS NULL AND NOT Y.UNCOLLECTED_FUNDS_LIMIT IS NULL
        )
        OR (
          NOT X.UNCOLLECTED_FUNDS_LIMIT IS NULL AND Y.UNCOLLECTED_FUNDS_LIMIT IS NULL
        )
        OR (
          X.UNCOLLECTED_FUNDS_LIMIT <> Y.UNCOLLECTED_FUNDS_LIMIT
        )
      )
      OR (
        (
          X.USER_DEF_STAT_CHANGED_DT IS NULL AND NOT Y.USER_DEF_STAT_CHANGED_DT IS NULL
        )
        OR (
          NOT X.USER_DEF_STAT_CHANGED_DT IS NULL AND Y.USER_DEF_STAT_CHANGED_DT IS NULL
        )
        OR (
          X.USER_DEF_STAT_CHANGED_DT <> Y.USER_DEF_STAT_CHANGED_DT
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
          X.UTILISATION IS NULL AND NOT Y.UTILISATION IS NULL
        )
        OR (
          NOT X.UTILISATION IS NULL AND Y.UTILISATION IS NULL
        )
        OR (
          X.UTILISATION <> Y.UTILISATION
        )
      )
    )
    THEN 'UPD'
    WHEN X.ID IS NULL
    THEN 'DEL' /* - IF MULTIPLE PKs are there then concatenate them with AND */
  END AS OPT_FLAG
FROM DEVT_STG_FLX.GETM_FACILITY AS X
FULL JOIN DEVT_STG_FLX.GETM_FACILITY_BACKUP AS Y
  ON X.ID = Y.ID /* - IF MULTIPLE PKs are there then concatenate them with AND */
WHERE
  NOT OPT_FLAG IS NULL