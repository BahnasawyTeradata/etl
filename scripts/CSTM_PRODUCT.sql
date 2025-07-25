CREATE OR REPLACE VIEW DEVV_STG_FLX.CSTM_PRODUCT_get_delta AS LOCKING ROW FOR ACCESS
SELECT
  COALESCE(X.PRODUCT_CODE, Y.PRODUCT_CODE) AS PRODUCT_CODE,
  X.ASSET_CATEGORIES_LIST,
  X.AUTH_STAT,
  X.BM_AUTH_STAT,
  X.BM_MODULE_ID_PRODUCT,
  X.BM_PRODUCT_GROUP,
  X.BM_PRODUCT_TYPE_MODULE,
  X.BM_RECORD_STAT,
  X.BRANCHES_LIST,
  X.CATEGORIES_LIST,
  X.CHECKER_DT_STAMP,
  X.CHECKER_ID,
  X.CURRENCIES_LIST,
  X.GEN_MT103P,
  X.INCLUDE_FOR_TDS_CALC,
  X.INSTRUMENT_PRODUCT_ALLOW,
  X.LOCATION_LIST,
  X.MAKER_DT_STAMP,
  X.MAKER_ID,
  X.MAXIMUM_RATE_VARIANCE,
  X.MOD_NO,
  X.MODULE,
  X.NO_OF_LEGS,
  X.NORMAL_RATE_VARIANCE,
  X.ONCE_AUTH,
  X.PART_OF_PRODUCT,
  X.POOL_CODE,
  X.PORTFOLIO_PRODUCT_ALLOW,
  X.PRODUCT_DESCRIPTION,
  X.PRODUCT_END_DATE,
  X.PRODUCT_GROUP,
  X.PRODUCT_REMARKS,
  X.PRODUCT_SLOGAN,
  X.PRODUCT_START_DATE,
  X.PRODUCT_TYPE,
  X.RATE_CODE_PREFERRED,
  X.RATE_TYPE_PREFERRED,
  X.RECORD_STAT,
  X.RTH_CLASS,
  X.SK_PRODUCT_CODE,
  X.WAREHOUSE_CODE,
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
    WHEN Y.PRODUCT_CODE IS NULL
    THEN 'INS' /* - IF MULTIPLE PKs are there then concatenate them with AND */
    WHEN X.PRODUCT_CODE = Y.PRODUCT_CODE /* - IF MULTIPLE PKs are there then concatenate them with AND */
    AND (
      (
        (
          X.ASSET_CATEGORIES_LIST IS NULL AND NOT Y.ASSET_CATEGORIES_LIST IS NULL
        )
        OR (
          NOT X.ASSET_CATEGORIES_LIST IS NULL AND Y.ASSET_CATEGORIES_LIST IS NULL
        )
        OR (
          X.ASSET_CATEGORIES_LIST <> Y.ASSET_CATEGORIES_LIST
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
          X.BM_MODULE_ID_PRODUCT IS NULL AND NOT Y.BM_MODULE_ID_PRODUCT IS NULL
        )
        OR (
          NOT X.BM_MODULE_ID_PRODUCT IS NULL AND Y.BM_MODULE_ID_PRODUCT IS NULL
        )
        OR (
          X.BM_MODULE_ID_PRODUCT <> Y.BM_MODULE_ID_PRODUCT
        )
      )
      OR (
        (
          X.BM_PRODUCT_GROUP IS NULL AND NOT Y.BM_PRODUCT_GROUP IS NULL
        )
        OR (
          NOT X.BM_PRODUCT_GROUP IS NULL AND Y.BM_PRODUCT_GROUP IS NULL
        )
        OR (
          X.BM_PRODUCT_GROUP <> Y.BM_PRODUCT_GROUP
        )
      )
      OR (
        (
          X.BM_PRODUCT_TYPE_MODULE IS NULL AND NOT Y.BM_PRODUCT_TYPE_MODULE IS NULL
        )
        OR (
          NOT X.BM_PRODUCT_TYPE_MODULE IS NULL AND Y.BM_PRODUCT_TYPE_MODULE IS NULL
        )
        OR (
          X.BM_PRODUCT_TYPE_MODULE <> Y.BM_PRODUCT_TYPE_MODULE
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
          X.BRANCHES_LIST IS NULL AND NOT Y.BRANCHES_LIST IS NULL
        )
        OR (
          NOT X.BRANCHES_LIST IS NULL AND Y.BRANCHES_LIST IS NULL
        )
        OR (
          X.BRANCHES_LIST <> Y.BRANCHES_LIST
        )
      )
      OR (
        (
          X.CATEGORIES_LIST IS NULL AND NOT Y.CATEGORIES_LIST IS NULL
        )
        OR (
          NOT X.CATEGORIES_LIST IS NULL AND Y.CATEGORIES_LIST IS NULL
        )
        OR (
          X.CATEGORIES_LIST <> Y.CATEGORIES_LIST
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
          X.CURRENCIES_LIST IS NULL AND NOT Y.CURRENCIES_LIST IS NULL
        )
        OR (
          NOT X.CURRENCIES_LIST IS NULL AND Y.CURRENCIES_LIST IS NULL
        )
        OR (
          X.CURRENCIES_LIST <> Y.CURRENCIES_LIST
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
          X.INCLUDE_FOR_TDS_CALC IS NULL AND NOT Y.INCLUDE_FOR_TDS_CALC IS NULL
        )
        OR (
          NOT X.INCLUDE_FOR_TDS_CALC IS NULL AND Y.INCLUDE_FOR_TDS_CALC IS NULL
        )
        OR (
          X.INCLUDE_FOR_TDS_CALC <> Y.INCLUDE_FOR_TDS_CALC
        )
      )
      OR (
        (
          X.INSTRUMENT_PRODUCT_ALLOW IS NULL AND NOT Y.INSTRUMENT_PRODUCT_ALLOW IS NULL
        )
        OR (
          NOT X.INSTRUMENT_PRODUCT_ALLOW IS NULL AND Y.INSTRUMENT_PRODUCT_ALLOW IS NULL
        )
        OR (
          X.INSTRUMENT_PRODUCT_ALLOW <> Y.INSTRUMENT_PRODUCT_ALLOW
        )
      )
      OR (
        (
          X.LOCATION_LIST IS NULL AND NOT Y.LOCATION_LIST IS NULL
        )
        OR (
          NOT X.LOCATION_LIST IS NULL AND Y.LOCATION_LIST IS NULL
        )
        OR (
          X.LOCATION_LIST <> Y.LOCATION_LIST
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
          X.MAXIMUM_RATE_VARIANCE IS NULL AND NOT Y.MAXIMUM_RATE_VARIANCE IS NULL
        )
        OR (
          NOT X.MAXIMUM_RATE_VARIANCE IS NULL AND Y.MAXIMUM_RATE_VARIANCE IS NULL
        )
        OR (
          X.MAXIMUM_RATE_VARIANCE <> Y.MAXIMUM_RATE_VARIANCE
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
          X.NO_OF_LEGS IS NULL AND NOT Y.NO_OF_LEGS IS NULL
        )
        OR (
          NOT X.NO_OF_LEGS IS NULL AND Y.NO_OF_LEGS IS NULL
        )
        OR (
          X.NO_OF_LEGS <> Y.NO_OF_LEGS
        )
      )
      OR (
        (
          X.NORMAL_RATE_VARIANCE IS NULL AND NOT Y.NORMAL_RATE_VARIANCE IS NULL
        )
        OR (
          NOT X.NORMAL_RATE_VARIANCE IS NULL AND Y.NORMAL_RATE_VARIANCE IS NULL
        )
        OR (
          X.NORMAL_RATE_VARIANCE <> Y.NORMAL_RATE_VARIANCE
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
          X.PART_OF_PRODUCT IS NULL AND NOT Y.PART_OF_PRODUCT IS NULL
        )
        OR (
          NOT X.PART_OF_PRODUCT IS NULL AND Y.PART_OF_PRODUCT IS NULL
        )
        OR (
          X.PART_OF_PRODUCT <> Y.PART_OF_PRODUCT
        )
      )
      OR (
        (
          X.POOL_CODE IS NULL AND NOT Y.POOL_CODE IS NULL
        )
        OR (
          NOT X.POOL_CODE IS NULL AND Y.POOL_CODE IS NULL
        )
        OR (
          X.POOL_CODE <> Y.POOL_CODE
        )
      )
      OR (
        (
          X.PORTFOLIO_PRODUCT_ALLOW IS NULL AND NOT Y.PORTFOLIO_PRODUCT_ALLOW IS NULL
        )
        OR (
          NOT X.PORTFOLIO_PRODUCT_ALLOW IS NULL AND Y.PORTFOLIO_PRODUCT_ALLOW IS NULL
        )
        OR (
          X.PORTFOLIO_PRODUCT_ALLOW <> Y.PORTFOLIO_PRODUCT_ALLOW
        )
      )
      OR (
        (
          X.PRODUCT_DESCRIPTION IS NULL AND NOT Y.PRODUCT_DESCRIPTION IS NULL
        )
        OR (
          NOT X.PRODUCT_DESCRIPTION IS NULL AND Y.PRODUCT_DESCRIPTION IS NULL
        )
        OR (
          X.PRODUCT_DESCRIPTION <> Y.PRODUCT_DESCRIPTION
        )
      )
      OR (
        (
          X.PRODUCT_END_DATE IS NULL AND NOT Y.PRODUCT_END_DATE IS NULL
        )
        OR (
          NOT X.PRODUCT_END_DATE IS NULL AND Y.PRODUCT_END_DATE IS NULL
        )
        OR (
          X.PRODUCT_END_DATE <> Y.PRODUCT_END_DATE
        )
      )
      OR (
        (
          X.PRODUCT_GROUP IS NULL AND NOT Y.PRODUCT_GROUP IS NULL
        )
        OR (
          NOT X.PRODUCT_GROUP IS NULL AND Y.PRODUCT_GROUP IS NULL
        )
        OR (
          X.PRODUCT_GROUP <> Y.PRODUCT_GROUP
        )
      )
      OR (
        (
          X.PRODUCT_REMARKS IS NULL AND NOT Y.PRODUCT_REMARKS IS NULL
        )
        OR (
          NOT X.PRODUCT_REMARKS IS NULL AND Y.PRODUCT_REMARKS IS NULL
        )
        OR (
          X.PRODUCT_REMARKS <> Y.PRODUCT_REMARKS
        )
      )
      OR (
        (
          X.PRODUCT_SLOGAN IS NULL AND NOT Y.PRODUCT_SLOGAN IS NULL
        )
        OR (
          NOT X.PRODUCT_SLOGAN IS NULL AND Y.PRODUCT_SLOGAN IS NULL
        )
        OR (
          X.PRODUCT_SLOGAN <> Y.PRODUCT_SLOGAN
        )
      )
      OR (
        (
          X.PRODUCT_START_DATE IS NULL AND NOT Y.PRODUCT_START_DATE IS NULL
        )
        OR (
          NOT X.PRODUCT_START_DATE IS NULL AND Y.PRODUCT_START_DATE IS NULL
        )
        OR (
          X.PRODUCT_START_DATE <> Y.PRODUCT_START_DATE
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
          X.RATE_CODE_PREFERRED IS NULL AND NOT Y.RATE_CODE_PREFERRED IS NULL
        )
        OR (
          NOT X.RATE_CODE_PREFERRED IS NULL AND Y.RATE_CODE_PREFERRED IS NULL
        )
        OR (
          X.RATE_CODE_PREFERRED <> Y.RATE_CODE_PREFERRED
        )
      )
      OR (
        (
          X.RATE_TYPE_PREFERRED IS NULL AND NOT Y.RATE_TYPE_PREFERRED IS NULL
        )
        OR (
          NOT X.RATE_TYPE_PREFERRED IS NULL AND Y.RATE_TYPE_PREFERRED IS NULL
        )
        OR (
          X.RATE_TYPE_PREFERRED <> Y.RATE_TYPE_PREFERRED
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
          X.RTH_CLASS IS NULL AND NOT Y.RTH_CLASS IS NULL
        )
        OR (
          NOT X.RTH_CLASS IS NULL AND Y.RTH_CLASS IS NULL
        )
        OR (
          X.RTH_CLASS <> Y.RTH_CLASS
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
          X.WAREHOUSE_CODE IS NULL AND NOT Y.WAREHOUSE_CODE IS NULL
        )
        OR (
          NOT X.WAREHOUSE_CODE IS NULL AND Y.WAREHOUSE_CODE IS NULL
        )
        OR (
          X.WAREHOUSE_CODE <> Y.WAREHOUSE_CODE
        )
      )
    )
    THEN 'UPD'
    WHEN X.PRODUCT_CODE IS NULL
    THEN 'DEL' /* - IF MULTIPLE PKs are there then concatenate them with AND */
  END AS OPT_FLAG
FROM DEVT_STG_FLX.CSTM_PRODUCT AS X
FULL JOIN DEVT_STG_FLX.CSTM_PRODUCT_BACKUP AS Y
  ON X.PRODUCT_CODE = Y.PRODUCT_CODE /* - IF MULTIPLE PKs are there then concatenate them with AND */
WHERE
  NOT OPT_FLAG IS NULL