CREATE OR REPLACE VIEW DEVV_STG_FLX.STTM_CUSTOMER_get_delta AS LOCKING ROW FOR ACCESS
SELECT
  COALESCE(X.CUSTOMER_NO, Y.CUSTOMER_NO) AS CUSTOMER_NO,
  X.ADDRESS_LINE1,
  X.ADDRESS_LINE2,
  X.ADDRESS_LINE3,
  X.ADDRESS_LINE4,
  X.ALG_ID,
  X.AML_CUSTOMER_GRP,
  X.AML_REQUIRED,
  X.AR_AP_TRACKING,
  X.AUTH_STAT,
  X.AUTO_CREATE_ACCOUNT,
  X.AUTO_CUST_AC_NO,
  X.AUTOGEN_STMTPLAN,
  X.BM_AUTH_STAT,
  X.BM_CHARGE_GROUP,
  X.BM_COUNTRY,
  X.BM_CUSTOMER_CATEGORY,
  X.BM_CUSTOMER_TYPE,
  X.BM_CUSTOMER_TYPE_CLAS,
  X.BM_DECEASED,
  X.BM_DEFAULT_MEDIA,
  X.BM_DEFAULT_MEDIA,
  X.BM_EXPOSURE_COUNTRY,
  X.BM_LANGUAGE,
  X.BM_LIMIT_CCY,
  X.BM_MOD_NO,
  X.BM_NATIONALITY,
  X.BM_NATIONALITY_CLS,
  X.BM_ONCE_AUTH,
  X.BM_PRIVATE_CUSTOMER,
  X.BM_RECORD_STAT,
  X.BM_STAFF,
  X.BM_TRACK_LIMITS,
  X.BM_UDF_3,
  X.BM_UDF_4,
  X.BM_UNIQUE_ID_NAME,
  X.CAS_CUST,
  X.CHARGE_GROUP,
  X.CHECKER_DT_STAMP,
  X.CHECKER_ID,
  X.CHK_DIGIT_VALID_REQD,
  X.CIF_CREATION_DATE,
  X.CIF_STATUS,
  X.CIF_STATUS_SINCE,
  X.CLS_CCY_ALLOWED,
  X.CLS_PARTICIPANT,
  X.CONSOL_TAX_CERT_REQD,
  X.COUNTRY,
  X.CREDIT_RATING,
  X.CRM_CUSTOMER,
  X.CUST_CLASSIFICATION,
  X.CUST_CLG_GROUP,
  X.CUSTOMER_CATEGORY,
  X.CUSTOMER_NAME1,
  X.CUSTOMER_TYPE,
  X.DEBTOR_CATEGORY,
  X.DECEASED,
  X.DEFAULT_MEDIA,
  X.ELCM_CUSTOMER,
  X.ELCM_CUSTOMER_NO,
  X.EXPOSURE_CATEGORY,
  X.EXPOSURE_COUNTRY,
  X.EXT_REF_NO,
  X.FAX_NUMBER,
  X.FREQUENCY,
  X.FROZEN,
  X.FT_ACCTING_AS_OF,
  X.FULL_NAME,
  X.FX_CLEAN_RISK_LIMIT,
  X.FX_CUST_CLEAN_RISK_LIMIT,
  X.FX_NETTING_CUSTOMER,
  X.GENERATE_MT920,
  X.GROUP_CODE,
  X.HO_AC_NO,
  X.INDIVIDUAL_TAX_CERT_REQD,
  X.INTRODUCER,
  X.ISSUER_CUSTOMER,
  X.JOINT_VENTURE,
  X.JV_LIMIT_TRACKING,
  X.KYC_DETAILS,
  X.KYC_REF_NO,
  X.LANGUAGE,
  X.LC_COLLATERAL_PCT,
  X.LIAB_BR,
  X.LIAB_NODE,
  X.LIABILITY_NO,
  X.LIMIT_CCY,
  X.LOC_CODE,
  X.LOCAL_BRANCH,
  X.MAILERS_REQUIRED,
  X.MAKER_DT_STAMP,
  X.MAKER_ID,
  X.MFI_CUSTOMER,
  X.MOD_NO,
  X.NATIONALITY,
  X.ONCE_AUTH,
  X.OVERALL_LIMIT,
  X.PAST_DUE_FLAG,
  X.PINCODE,
  X.PRIVATE_CUSTOMER,
  X.RECORD_STAT,
  X.REVISION_DATE,
  X.RISK_CATEGORY,
  X.RISK_PROFILE,
  X.RM_ID,
  X.RP_CUSTOMER,
  X.SEC_CLEAN_RISK_LIMIT,
  X.SEC_CUST_CLEAN_RISK_LIMIT,
  X.SEC_CUST_PSTL_RISK_LIMIT,
  X.SEC_PSTL_RISK_LIMIT,
  X.SHORT_NAME,
  X.SHORT_NAME2,
  X.SK_ADDRESS_LINE1_ADDRESS_LINE2_ADDRESS_LINE3_ADDRESS_LINE4,
  X.SK_CUSTOMER_NO,
  X.SK_LOCAL_BRANCH,
  X.SK_CHECKER_ID,
  X.SK_MAKER_ID,
  X.SK_RM_ID,
  X.SK_UDF_2,
  X.SK_SWIFT_CODE,
  X.SSN,
  X.STAFF,
  X.STMT_DAY,
  X.SWIFT_CODE,
  X.TAX_GROUP,
  X.TRACK_LIMITS,
  X.TREASURY_CUSTOMER,
  X.UDF_1,
  X.UDF_2,
  X.UDF_3,
  X.UDF_4,
  X.UDF_5,
  X.UNADVISED,
  X.UNIQUE_ID_NAME,
  X.UNIQUE_ID_VALUE,
  X.UTILITY_PROVIDER,
  X.UTILITY_PROVIDER_ID,
  X.UTILITY_PROVIDER_TYPE,
  X.WHEREABOUTS_UNKNOWN,
  X.WHT_PCT,
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
    WHEN Y.CUSTOMER_NO IS NULL
    THEN 'INS' /* - IF MULTIPLE PKs are there then concatenate them with AND */
    WHEN X.CUSTOMER_NO = Y.CUSTOMER_NO /* - IF MULTIPLE PKs are there then concatenate them with AND */
    AND (
      (
        (
          X.ADDRESS_LINE1 IS NULL AND NOT Y.ADDRESS_LINE1 IS NULL
        )
        OR (
          NOT X.ADDRESS_LINE1 IS NULL AND Y.ADDRESS_LINE1 IS NULL
        )
        OR (
          X.ADDRESS_LINE1 <> Y.ADDRESS_LINE1
        )
      )
      OR (
        (
          X.ADDRESS_LINE2 IS NULL AND NOT Y.ADDRESS_LINE2 IS NULL
        )
        OR (
          NOT X.ADDRESS_LINE2 IS NULL AND Y.ADDRESS_LINE2 IS NULL
        )
        OR (
          X.ADDRESS_LINE2 <> Y.ADDRESS_LINE2
        )
      )
      OR (
        (
          X.ADDRESS_LINE3 IS NULL AND NOT Y.ADDRESS_LINE3 IS NULL
        )
        OR (
          NOT X.ADDRESS_LINE3 IS NULL AND Y.ADDRESS_LINE3 IS NULL
        )
        OR (
          X.ADDRESS_LINE3 <> Y.ADDRESS_LINE3
        )
      )
      OR (
        (
          X.ADDRESS_LINE4 IS NULL AND NOT Y.ADDRESS_LINE4 IS NULL
        )
        OR (
          NOT X.ADDRESS_LINE4 IS NULL AND Y.ADDRESS_LINE4 IS NULL
        )
        OR (
          X.ADDRESS_LINE4 <> Y.ADDRESS_LINE4
        )
      )
      OR (
        (
          X.ALG_ID IS NULL AND NOT Y.ALG_ID IS NULL
        )
        OR (
          NOT X.ALG_ID IS NULL AND Y.ALG_ID IS NULL
        )
        OR (
          X.ALG_ID <> Y.ALG_ID
        )
      )
      OR (
        (
          X.AML_CUSTOMER_GRP IS NULL AND NOT Y.AML_CUSTOMER_GRP IS NULL
        )
        OR (
          NOT X.AML_CUSTOMER_GRP IS NULL AND Y.AML_CUSTOMER_GRP IS NULL
        )
        OR (
          X.AML_CUSTOMER_GRP <> Y.AML_CUSTOMER_GRP
        )
      )
      OR (
        (
          X.AML_REQUIRED IS NULL AND NOT Y.AML_REQUIRED IS NULL
        )
        OR (
          NOT X.AML_REQUIRED IS NULL AND Y.AML_REQUIRED IS NULL
        )
        OR (
          X.AML_REQUIRED <> Y.AML_REQUIRED
        )
      )
      OR (
        (
          X.AR_AP_TRACKING IS NULL AND NOT Y.AR_AP_TRACKING IS NULL
        )
        OR (
          NOT X.AR_AP_TRACKING IS NULL AND Y.AR_AP_TRACKING IS NULL
        )
        OR (
          X.AR_AP_TRACKING <> Y.AR_AP_TRACKING
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
          X.AUTO_CREATE_ACCOUNT IS NULL AND NOT Y.AUTO_CREATE_ACCOUNT IS NULL
        )
        OR (
          NOT X.AUTO_CREATE_ACCOUNT IS NULL AND Y.AUTO_CREATE_ACCOUNT IS NULL
        )
        OR (
          X.AUTO_CREATE_ACCOUNT <> Y.AUTO_CREATE_ACCOUNT
        )
      )
      OR (
        (
          X.AUTO_CUST_AC_NO IS NULL AND NOT Y.AUTO_CUST_AC_NO IS NULL
        )
        OR (
          NOT X.AUTO_CUST_AC_NO IS NULL AND Y.AUTO_CUST_AC_NO IS NULL
        )
        OR (
          X.AUTO_CUST_AC_NO <> Y.AUTO_CUST_AC_NO
        )
      )
      OR (
        (
          X.AUTOGEN_STMTPLAN IS NULL AND NOT Y.AUTOGEN_STMTPLAN IS NULL
        )
        OR (
          NOT X.AUTOGEN_STMTPLAN IS NULL AND Y.AUTOGEN_STMTPLAN IS NULL
        )
        OR (
          X.AUTOGEN_STMTPLAN <> Y.AUTOGEN_STMTPLAN
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
          X.BM_CHARGE_GROUP IS NULL AND NOT Y.BM_CHARGE_GROUP IS NULL
        )
        OR (
          NOT X.BM_CHARGE_GROUP IS NULL AND Y.BM_CHARGE_GROUP IS NULL
        )
        OR (
          X.BM_CHARGE_GROUP <> Y.BM_CHARGE_GROUP
        )
      )
      OR (
        (
          X.BM_COUNTRY IS NULL AND NOT Y.BM_COUNTRY IS NULL
        )
        OR (
          NOT X.BM_COUNTRY IS NULL AND Y.BM_COUNTRY IS NULL
        )
        OR (
          X.BM_COUNTRY <> Y.BM_COUNTRY
        )
      )
      OR (
        (
          X.BM_CUSTOMER_CATEGORY IS NULL AND NOT Y.BM_CUSTOMER_CATEGORY IS NULL
        )
        OR (
          NOT X.BM_CUSTOMER_CATEGORY IS NULL AND Y.BM_CUSTOMER_CATEGORY IS NULL
        )
        OR (
          X.BM_CUSTOMER_CATEGORY <> Y.BM_CUSTOMER_CATEGORY
        )
      )
      OR (
        (
          X.BM_CUSTOMER_TYPE IS NULL AND NOT Y.BM_CUSTOMER_TYPE IS NULL
        )
        OR (
          NOT X.BM_CUSTOMER_TYPE IS NULL AND Y.BM_CUSTOMER_TYPE IS NULL
        )
        OR (
          X.BM_CUSTOMER_TYPE <> Y.BM_CUSTOMER_TYPE
        )
      )
      OR (
        (
          X.BM_CUSTOMER_TYPE_CLAS IS NULL AND NOT Y.BM_CUSTOMER_TYPE_CLAS IS NULL
        )
        OR (
          NOT X.BM_CUSTOMER_TYPE_CLAS IS NULL AND Y.BM_CUSTOMER_TYPE_CLAS IS NULL
        )
        OR (
          X.BM_CUSTOMER_TYPE_CLAS <> Y.BM_CUSTOMER_TYPE_CLAS
        )
      )
      OR (
        (
          X.BM_DECEASED IS NULL AND NOT Y.BM_DECEASED IS NULL
        )
        OR (
          NOT X.BM_DECEASED IS NULL AND Y.BM_DECEASED IS NULL
        )
        OR (
          X.BM_DECEASED <> Y.BM_DECEASED
        )
      )
      OR (
        (
          X.BM_DEFAULT_MEDIA IS NULL AND NOT Y.BM_DEFAULT_MEDIA IS NULL
        )
        OR (
          NOT X.BM_DEFAULT_MEDIA IS NULL AND Y.BM_DEFAULT_MEDIA IS NULL
        )
        OR (
          X.BM_DEFAULT_MEDIA <> Y.BM_DEFAULT_MEDIA
        )
      )
      OR (
        (
          X.BM_DEFAULT_MEDIA IS NULL AND NOT Y.BM_DEFAULT_MEDIA IS NULL
        )
        OR (
          NOT X.BM_DEFAULT_MEDIA IS NULL AND Y.BM_DEFAULT_MEDIA IS NULL
        )
        OR (
          X.BM_DEFAULT_MEDIA <> Y.BM_DEFAULT_MEDIA
        )
      )
      OR (
        (
          X.BM_EXPOSURE_COUNTRY IS NULL AND NOT Y.BM_EXPOSURE_COUNTRY IS NULL
        )
        OR (
          NOT X.BM_EXPOSURE_COUNTRY IS NULL AND Y.BM_EXPOSURE_COUNTRY IS NULL
        )
        OR (
          X.BM_EXPOSURE_COUNTRY <> Y.BM_EXPOSURE_COUNTRY
        )
      )
      OR (
        (
          X.BM_LANGUAGE IS NULL AND NOT Y.BM_LANGUAGE IS NULL
        )
        OR (
          NOT X.BM_LANGUAGE IS NULL AND Y.BM_LANGUAGE IS NULL
        )
        OR (
          X.BM_LANGUAGE <> Y.BM_LANGUAGE
        )
      )
      OR (
        (
          X.BM_LIMIT_CCY IS NULL AND NOT Y.BM_LIMIT_CCY IS NULL
        )
        OR (
          NOT X.BM_LIMIT_CCY IS NULL AND Y.BM_LIMIT_CCY IS NULL
        )
        OR (
          X.BM_LIMIT_CCY <> Y.BM_LIMIT_CCY
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
          X.BM_NATIONALITY IS NULL AND NOT Y.BM_NATIONALITY IS NULL
        )
        OR (
          NOT X.BM_NATIONALITY IS NULL AND Y.BM_NATIONALITY IS NULL
        )
        OR (
          X.BM_NATIONALITY <> Y.BM_NATIONALITY
        )
      )
      OR (
        (
          X.BM_NATIONALITY_CLS IS NULL AND NOT Y.BM_NATIONALITY_CLS IS NULL
        )
        OR (
          NOT X.BM_NATIONALITY_CLS IS NULL AND Y.BM_NATIONALITY_CLS IS NULL
        )
        OR (
          X.BM_NATIONALITY_CLS <> Y.BM_NATIONALITY_CLS
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
          X.BM_PRIVATE_CUSTOMER IS NULL AND NOT Y.BM_PRIVATE_CUSTOMER IS NULL
        )
        OR (
          NOT X.BM_PRIVATE_CUSTOMER IS NULL AND Y.BM_PRIVATE_CUSTOMER IS NULL
        )
        OR (
          X.BM_PRIVATE_CUSTOMER <> Y.BM_PRIVATE_CUSTOMER
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
          X.BM_STAFF IS NULL AND NOT Y.BM_STAFF IS NULL
        )
        OR (
          NOT X.BM_STAFF IS NULL AND Y.BM_STAFF IS NULL
        )
        OR (
          X.BM_STAFF <> Y.BM_STAFF
        )
      )
      OR (
        (
          X.BM_TRACK_LIMITS IS NULL AND NOT Y.BM_TRACK_LIMITS IS NULL
        )
        OR (
          NOT X.BM_TRACK_LIMITS IS NULL AND Y.BM_TRACK_LIMITS IS NULL
        )
        OR (
          X.BM_TRACK_LIMITS <> Y.BM_TRACK_LIMITS
        )
      )
      OR (
        (
          X.BM_UDF_3 IS NULL AND NOT Y.BM_UDF_3 IS NULL
        )
        OR (
          NOT X.BM_UDF_3 IS NULL AND Y.BM_UDF_3 IS NULL
        )
        OR (
          X.BM_UDF_3 <> Y.BM_UDF_3
        )
      )
      OR (
        (
          X.BM_UDF_4 IS NULL AND NOT Y.BM_UDF_4 IS NULL
        )
        OR (
          NOT X.BM_UDF_4 IS NULL AND Y.BM_UDF_4 IS NULL
        )
        OR (
          X.BM_UDF_4 <> Y.BM_UDF_4
        )
      )
      OR (
        (
          X.BM_UNIQUE_ID_NAME IS NULL AND NOT Y.BM_UNIQUE_ID_NAME IS NULL
        )
        OR (
          NOT X.BM_UNIQUE_ID_NAME IS NULL AND Y.BM_UNIQUE_ID_NAME IS NULL
        )
        OR (
          X.BM_UNIQUE_ID_NAME <> Y.BM_UNIQUE_ID_NAME
        )
      )
      OR (
        (
          X.CAS_CUST IS NULL AND NOT Y.CAS_CUST IS NULL
        )
        OR (
          NOT X.CAS_CUST IS NULL AND Y.CAS_CUST IS NULL
        )
        OR (
          X.CAS_CUST <> Y.CAS_CUST
        )
      )
      OR (
        (
          X.CHARGE_GROUP IS NULL AND NOT Y.CHARGE_GROUP IS NULL
        )
        OR (
          NOT X.CHARGE_GROUP IS NULL AND Y.CHARGE_GROUP IS NULL
        )
        OR (
          X.CHARGE_GROUP <> Y.CHARGE_GROUP
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
          X.CHK_DIGIT_VALID_REQD IS NULL AND NOT Y.CHK_DIGIT_VALID_REQD IS NULL
        )
        OR (
          NOT X.CHK_DIGIT_VALID_REQD IS NULL AND Y.CHK_DIGIT_VALID_REQD IS NULL
        )
        OR (
          X.CHK_DIGIT_VALID_REQD <> Y.CHK_DIGIT_VALID_REQD
        )
      )
      OR (
        (
          X.CIF_CREATION_DATE IS NULL AND NOT Y.CIF_CREATION_DATE IS NULL
        )
        OR (
          NOT X.CIF_CREATION_DATE IS NULL AND Y.CIF_CREATION_DATE IS NULL
        )
        OR (
          X.CIF_CREATION_DATE <> Y.CIF_CREATION_DATE
        )
      )
      OR (
        (
          X.CIF_STATUS IS NULL AND NOT Y.CIF_STATUS IS NULL
        )
        OR (
          NOT X.CIF_STATUS IS NULL AND Y.CIF_STATUS IS NULL
        )
        OR (
          X.CIF_STATUS <> Y.CIF_STATUS
        )
      )
      OR (
        (
          X.CIF_STATUS_SINCE IS NULL AND NOT Y.CIF_STATUS_SINCE IS NULL
        )
        OR (
          NOT X.CIF_STATUS_SINCE IS NULL AND Y.CIF_STATUS_SINCE IS NULL
        )
        OR (
          X.CIF_STATUS_SINCE <> Y.CIF_STATUS_SINCE
        )
      )
      OR (
        (
          X.CLS_CCY_ALLOWED IS NULL AND NOT Y.CLS_CCY_ALLOWED IS NULL
        )
        OR (
          NOT X.CLS_CCY_ALLOWED IS NULL AND Y.CLS_CCY_ALLOWED IS NULL
        )
        OR (
          X.CLS_CCY_ALLOWED <> Y.CLS_CCY_ALLOWED
        )
      )
      OR (
        (
          X.CLS_PARTICIPANT IS NULL AND NOT Y.CLS_PARTICIPANT IS NULL
        )
        OR (
          NOT X.CLS_PARTICIPANT IS NULL AND Y.CLS_PARTICIPANT IS NULL
        )
        OR (
          X.CLS_PARTICIPANT <> Y.CLS_PARTICIPANT
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
          X.CRM_CUSTOMER IS NULL AND NOT Y.CRM_CUSTOMER IS NULL
        )
        OR (
          NOT X.CRM_CUSTOMER IS NULL AND Y.CRM_CUSTOMER IS NULL
        )
        OR (
          X.CRM_CUSTOMER <> Y.CRM_CUSTOMER
        )
      )
      OR (
        (
          X.CUST_CLASSIFICATION IS NULL AND NOT Y.CUST_CLASSIFICATION IS NULL
        )
        OR (
          NOT X.CUST_CLASSIFICATION IS NULL AND Y.CUST_CLASSIFICATION IS NULL
        )
        OR (
          X.CUST_CLASSIFICATION <> Y.CUST_CLASSIFICATION
        )
      )
      OR (
        (
          X.CUST_CLG_GROUP IS NULL AND NOT Y.CUST_CLG_GROUP IS NULL
        )
        OR (
          NOT X.CUST_CLG_GROUP IS NULL AND Y.CUST_CLG_GROUP IS NULL
        )
        OR (
          X.CUST_CLG_GROUP <> Y.CUST_CLG_GROUP
        )
      )
      OR (
        (
          X.CUSTOMER_CATEGORY IS NULL AND NOT Y.CUSTOMER_CATEGORY IS NULL
        )
        OR (
          NOT X.CUSTOMER_CATEGORY IS NULL AND Y.CUSTOMER_CATEGORY IS NULL
        )
        OR (
          X.CUSTOMER_CATEGORY <> Y.CUSTOMER_CATEGORY
        )
      )
      OR (
        (
          X.CUSTOMER_NAME1 IS NULL AND NOT Y.CUSTOMER_NAME1 IS NULL
        )
        OR (
          NOT X.CUSTOMER_NAME1 IS NULL AND Y.CUSTOMER_NAME1 IS NULL
        )
        OR (
          X.CUSTOMER_NAME1 <> Y.CUSTOMER_NAME1
        )
      )
      OR (
        (
          X.CUSTOMER_TYPE IS NULL AND NOT Y.CUSTOMER_TYPE IS NULL
        )
        OR (
          NOT X.CUSTOMER_TYPE IS NULL AND Y.CUSTOMER_TYPE IS NULL
        )
        OR (
          X.CUSTOMER_TYPE <> Y.CUSTOMER_TYPE
        )
      )
      OR (
        (
          X.DEBTOR_CATEGORY IS NULL AND NOT Y.DEBTOR_CATEGORY IS NULL
        )
        OR (
          NOT X.DEBTOR_CATEGORY IS NULL AND Y.DEBTOR_CATEGORY IS NULL
        )
        OR (
          X.DEBTOR_CATEGORY <> Y.DEBTOR_CATEGORY
        )
      )
      OR (
        (
          X.DECEASED IS NULL AND NOT Y.DECEASED IS NULL
        )
        OR (
          NOT X.DECEASED IS NULL AND Y.DECEASED IS NULL
        )
        OR (
          X.DECEASED <> Y.DECEASED
        )
      )
      OR (
        (
          X.DEFAULT_MEDIA IS NULL AND NOT Y.DEFAULT_MEDIA IS NULL
        )
        OR (
          NOT X.DEFAULT_MEDIA IS NULL AND Y.DEFAULT_MEDIA IS NULL
        )
        OR (
          X.DEFAULT_MEDIA <> Y.DEFAULT_MEDIA
        )
      )
      OR (
        (
          X.ELCM_CUSTOMER IS NULL AND NOT Y.ELCM_CUSTOMER IS NULL
        )
        OR (
          NOT X.ELCM_CUSTOMER IS NULL AND Y.ELCM_CUSTOMER IS NULL
        )
        OR (
          X.ELCM_CUSTOMER <> Y.ELCM_CUSTOMER
        )
      )
      OR (
        (
          X.ELCM_CUSTOMER_NO IS NULL AND NOT Y.ELCM_CUSTOMER_NO IS NULL
        )
        OR (
          NOT X.ELCM_CUSTOMER_NO IS NULL AND Y.ELCM_CUSTOMER_NO IS NULL
        )
        OR (
          X.ELCM_CUSTOMER_NO <> Y.ELCM_CUSTOMER_NO
        )
      )
      OR (
        (
          X.EXPOSURE_CATEGORY IS NULL AND NOT Y.EXPOSURE_CATEGORY IS NULL
        )
        OR (
          NOT X.EXPOSURE_CATEGORY IS NULL AND Y.EXPOSURE_CATEGORY IS NULL
        )
        OR (
          X.EXPOSURE_CATEGORY <> Y.EXPOSURE_CATEGORY
        )
      )
      OR (
        (
          X.EXPOSURE_COUNTRY IS NULL AND NOT Y.EXPOSURE_COUNTRY IS NULL
        )
        OR (
          NOT X.EXPOSURE_COUNTRY IS NULL AND Y.EXPOSURE_COUNTRY IS NULL
        )
        OR (
          X.EXPOSURE_COUNTRY <> Y.EXPOSURE_COUNTRY
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
          X.FAX_NUMBER IS NULL AND NOT Y.FAX_NUMBER IS NULL
        )
        OR (
          NOT X.FAX_NUMBER IS NULL AND Y.FAX_NUMBER IS NULL
        )
        OR (
          X.FAX_NUMBER <> Y.FAX_NUMBER
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
          X.FROZEN IS NULL AND NOT Y.FROZEN IS NULL
        )
        OR (
          NOT X.FROZEN IS NULL AND Y.FROZEN IS NULL
        )
        OR (
          X.FROZEN <> Y.FROZEN
        )
      )
      OR (
        (
          X.FT_ACCTING_AS_OF IS NULL AND NOT Y.FT_ACCTING_AS_OF IS NULL
        )
        OR (
          NOT X.FT_ACCTING_AS_OF IS NULL AND Y.FT_ACCTING_AS_OF IS NULL
        )
        OR (
          X.FT_ACCTING_AS_OF <> Y.FT_ACCTING_AS_OF
        )
      )
      OR (
        (
          X.FULL_NAME IS NULL AND NOT Y.FULL_NAME IS NULL
        )
        OR (
          NOT X.FULL_NAME IS NULL AND Y.FULL_NAME IS NULL
        )
        OR (
          X.FULL_NAME <> Y.FULL_NAME
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
          X.FX_CUST_CLEAN_RISK_LIMIT IS NULL AND NOT Y.FX_CUST_CLEAN_RISK_LIMIT IS NULL
        )
        OR (
          NOT X.FX_CUST_CLEAN_RISK_LIMIT IS NULL AND Y.FX_CUST_CLEAN_RISK_LIMIT IS NULL
        )
        OR (
          X.FX_CUST_CLEAN_RISK_LIMIT <> Y.FX_CUST_CLEAN_RISK_LIMIT
        )
      )
      OR (
        (
          X.FX_NETTING_CUSTOMER IS NULL AND NOT Y.FX_NETTING_CUSTOMER IS NULL
        )
        OR (
          NOT X.FX_NETTING_CUSTOMER IS NULL AND Y.FX_NETTING_CUSTOMER IS NULL
        )
        OR (
          X.FX_NETTING_CUSTOMER <> Y.FX_NETTING_CUSTOMER
        )
      )
      OR (
        (
          X.GENERATE_MT920 IS NULL AND NOT Y.GENERATE_MT920 IS NULL
        )
        OR (
          NOT X.GENERATE_MT920 IS NULL AND Y.GENERATE_MT920 IS NULL
        )
        OR (
          X.GENERATE_MT920 <> Y.GENERATE_MT920
        )
      )
      OR (
        (
          X.GROUP_CODE IS NULL AND NOT Y.GROUP_CODE IS NULL
        )
        OR (
          NOT X.GROUP_CODE IS NULL AND Y.GROUP_CODE IS NULL
        )
        OR (
          X.GROUP_CODE <> Y.GROUP_CODE
        )
      )
      OR (
        (
          X.HO_AC_NO IS NULL AND NOT Y.HO_AC_NO IS NULL
        )
        OR (
          NOT X.HO_AC_NO IS NULL AND Y.HO_AC_NO IS NULL
        )
        OR (
          X.HO_AC_NO <> Y.HO_AC_NO
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
          X.INTRODUCER IS NULL AND NOT Y.INTRODUCER IS NULL
        )
        OR (
          NOT X.INTRODUCER IS NULL AND Y.INTRODUCER IS NULL
        )
        OR (
          X.INTRODUCER <> Y.INTRODUCER
        )
      )
      OR (
        (
          X.ISSUER_CUSTOMER IS NULL AND NOT Y.ISSUER_CUSTOMER IS NULL
        )
        OR (
          NOT X.ISSUER_CUSTOMER IS NULL AND Y.ISSUER_CUSTOMER IS NULL
        )
        OR (
          X.ISSUER_CUSTOMER <> Y.ISSUER_CUSTOMER
        )
      )
      OR (
        (
          X.JOINT_VENTURE IS NULL AND NOT Y.JOINT_VENTURE IS NULL
        )
        OR (
          NOT X.JOINT_VENTURE IS NULL AND Y.JOINT_VENTURE IS NULL
        )
        OR (
          X.JOINT_VENTURE <> Y.JOINT_VENTURE
        )
      )
      OR (
        (
          X.JV_LIMIT_TRACKING IS NULL AND NOT Y.JV_LIMIT_TRACKING IS NULL
        )
        OR (
          NOT X.JV_LIMIT_TRACKING IS NULL AND Y.JV_LIMIT_TRACKING IS NULL
        )
        OR (
          X.JV_LIMIT_TRACKING <> Y.JV_LIMIT_TRACKING
        )
      )
      OR (
        (
          X.KYC_DETAILS IS NULL AND NOT Y.KYC_DETAILS IS NULL
        )
        OR (
          NOT X.KYC_DETAILS IS NULL AND Y.KYC_DETAILS IS NULL
        )
        OR (
          X.KYC_DETAILS <> Y.KYC_DETAILS
        )
      )
      OR (
        (
          X.KYC_REF_NO IS NULL AND NOT Y.KYC_REF_NO IS NULL
        )
        OR (
          NOT X.KYC_REF_NO IS NULL AND Y.KYC_REF_NO IS NULL
        )
        OR (
          X.KYC_REF_NO <> Y.KYC_REF_NO
        )
      )
      OR (
        (
          X.LANGUAGE IS NULL AND NOT Y.LANGUAGE IS NULL
        )
        OR (
          NOT X.LANGUAGE IS NULL AND Y.LANGUAGE IS NULL
        )
        OR (
          X.LANGUAGE <> Y.LANGUAGE
        )
      )
      OR (
        (
          X.LC_COLLATERAL_PCT IS NULL AND NOT Y.LC_COLLATERAL_PCT IS NULL
        )
        OR (
          NOT X.LC_COLLATERAL_PCT IS NULL AND Y.LC_COLLATERAL_PCT IS NULL
        )
        OR (
          X.LC_COLLATERAL_PCT <> Y.LC_COLLATERAL_PCT
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
          X.LIAB_NODE IS NULL AND NOT Y.LIAB_NODE IS NULL
        )
        OR (
          NOT X.LIAB_NODE IS NULL AND Y.LIAB_NODE IS NULL
        )
        OR (
          X.LIAB_NODE <> Y.LIAB_NODE
        )
      )
      OR (
        (
          X.LIABILITY_NO IS NULL AND NOT Y.LIABILITY_NO IS NULL
        )
        OR (
          NOT X.LIABILITY_NO IS NULL AND Y.LIABILITY_NO IS NULL
        )
        OR (
          X.LIABILITY_NO <> Y.LIABILITY_NO
        )
      )
      OR (
        (
          X.LIMIT_CCY IS NULL AND NOT Y.LIMIT_CCY IS NULL
        )
        OR (
          NOT X.LIMIT_CCY IS NULL AND Y.LIMIT_CCY IS NULL
        )
        OR (
          X.LIMIT_CCY <> Y.LIMIT_CCY
        )
      )
      OR (
        (
          X.LOC_CODE IS NULL AND NOT Y.LOC_CODE IS NULL
        )
        OR (
          NOT X.LOC_CODE IS NULL AND Y.LOC_CODE IS NULL
        )
        OR (
          X.LOC_CODE <> Y.LOC_CODE
        )
      )
      OR (
        (
          X.LOCAL_BRANCH IS NULL AND NOT Y.LOCAL_BRANCH IS NULL
        )
        OR (
          NOT X.LOCAL_BRANCH IS NULL AND Y.LOCAL_BRANCH IS NULL
        )
        OR (
          X.LOCAL_BRANCH <> Y.LOCAL_BRANCH
        )
      )
      OR (
        (
          X.MAILERS_REQUIRED IS NULL AND NOT Y.MAILERS_REQUIRED IS NULL
        )
        OR (
          NOT X.MAILERS_REQUIRED IS NULL AND Y.MAILERS_REQUIRED IS NULL
        )
        OR (
          X.MAILERS_REQUIRED <> Y.MAILERS_REQUIRED
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
          X.MFI_CUSTOMER IS NULL AND NOT Y.MFI_CUSTOMER IS NULL
        )
        OR (
          NOT X.MFI_CUSTOMER IS NULL AND Y.MFI_CUSTOMER IS NULL
        )
        OR (
          X.MFI_CUSTOMER <> Y.MFI_CUSTOMER
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
          X.NATIONALITY IS NULL AND NOT Y.NATIONALITY IS NULL
        )
        OR (
          NOT X.NATIONALITY IS NULL AND Y.NATIONALITY IS NULL
        )
        OR (
          X.NATIONALITY <> Y.NATIONALITY
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
          X.PAST_DUE_FLAG IS NULL AND NOT Y.PAST_DUE_FLAG IS NULL
        )
        OR (
          NOT X.PAST_DUE_FLAG IS NULL AND Y.PAST_DUE_FLAG IS NULL
        )
        OR (
          X.PAST_DUE_FLAG <> Y.PAST_DUE_FLAG
        )
      )
      OR (
        (
          X.PINCODE IS NULL AND NOT Y.PINCODE IS NULL
        )
        OR (
          NOT X.PINCODE IS NULL AND Y.PINCODE IS NULL
        )
        OR (
          X.PINCODE <> Y.PINCODE
        )
      )
      OR (
        (
          X.PRIVATE_CUSTOMER IS NULL AND NOT Y.PRIVATE_CUSTOMER IS NULL
        )
        OR (
          NOT X.PRIVATE_CUSTOMER IS NULL AND Y.PRIVATE_CUSTOMER IS NULL
        )
        OR (
          X.PRIVATE_CUSTOMER <> Y.PRIVATE_CUSTOMER
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
          X.RISK_CATEGORY IS NULL AND NOT Y.RISK_CATEGORY IS NULL
        )
        OR (
          NOT X.RISK_CATEGORY IS NULL AND Y.RISK_CATEGORY IS NULL
        )
        OR (
          X.RISK_CATEGORY <> Y.RISK_CATEGORY
        )
      )
      OR (
        (
          X.RISK_PROFILE IS NULL AND NOT Y.RISK_PROFILE IS NULL
        )
        OR (
          NOT X.RISK_PROFILE IS NULL AND Y.RISK_PROFILE IS NULL
        )
        OR (
          X.RISK_PROFILE <> Y.RISK_PROFILE
        )
      )
      OR (
        (
          X.RM_ID IS NULL AND NOT Y.RM_ID IS NULL
        )
        OR (
          NOT X.RM_ID IS NULL AND Y.RM_ID IS NULL
        )
        OR (
          X.RM_ID <> Y.RM_ID
        )
      )
      OR (
        (
          X.RP_CUSTOMER IS NULL AND NOT Y.RP_CUSTOMER IS NULL
        )
        OR (
          NOT X.RP_CUSTOMER IS NULL AND Y.RP_CUSTOMER IS NULL
        )
        OR (
          X.RP_CUSTOMER <> Y.RP_CUSTOMER
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
          X.SEC_CUST_CLEAN_RISK_LIMIT IS NULL AND NOT Y.SEC_CUST_CLEAN_RISK_LIMIT IS NULL
        )
        OR (
          NOT X.SEC_CUST_CLEAN_RISK_LIMIT IS NULL AND Y.SEC_CUST_CLEAN_RISK_LIMIT IS NULL
        )
        OR (
          X.SEC_CUST_CLEAN_RISK_LIMIT <> Y.SEC_CUST_CLEAN_RISK_LIMIT
        )
      )
      OR (
        (
          X.SEC_CUST_PSTL_RISK_LIMIT IS NULL AND NOT Y.SEC_CUST_PSTL_RISK_LIMIT IS NULL
        )
        OR (
          NOT X.SEC_CUST_PSTL_RISK_LIMIT IS NULL AND Y.SEC_CUST_PSTL_RISK_LIMIT IS NULL
        )
        OR (
          X.SEC_CUST_PSTL_RISK_LIMIT <> Y.SEC_CUST_PSTL_RISK_LIMIT
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
          X.SHORT_NAME IS NULL AND NOT Y.SHORT_NAME IS NULL
        )
        OR (
          NOT X.SHORT_NAME IS NULL AND Y.SHORT_NAME IS NULL
        )
        OR (
          X.SHORT_NAME <> Y.SHORT_NAME
        )
      )
      OR (
        (
          X.SHORT_NAME2 IS NULL AND NOT Y.SHORT_NAME2 IS NULL
        )
        OR (
          NOT X.SHORT_NAME2 IS NULL AND Y.SHORT_NAME2 IS NULL
        )
        OR (
          X.SHORT_NAME2 <> Y.SHORT_NAME2
        )
      )
      OR (
        (
          X.SK_ADDRESS_LINE1_ADDRESS_LINE2_ADDRESS_LINE3_ADDRESS_LINE4 IS NULL
          AND NOT Y.SK_ADDRESS_LINE1_ADDRESS_LINE2_ADDRESS_LINE3_ADDRESS_LINE4 IS NULL
        )
        OR (
          NOT X.SK_ADDRESS_LINE1_ADDRESS_LINE2_ADDRESS_LINE3_ADDRESS_LINE4 IS NULL
          AND Y.SK_ADDRESS_LINE1_ADDRESS_LINE2_ADDRESS_LINE3_ADDRESS_LINE4 IS NULL
        )
        OR (
          X.SK_ADDRESS_LINE1_ADDRESS_LINE2_ADDRESS_LINE3_ADDRESS_LINE4 <> Y.SK_ADDRESS_LINE1_ADDRESS_LINE2_ADDRESS_LINE3_ADDRESS_LINE4
        )
      )
      OR (
        (
          X.SK_CUSTOMER_NO IS NULL AND NOT Y.SK_CUSTOMER_NO IS NULL
        )
        OR (
          NOT X.SK_CUSTOMER_NO IS NULL AND Y.SK_CUSTOMER_NO IS NULL
        )
        OR (
          X.SK_CUSTOMER_NO <> Y.SK_CUSTOMER_NO
        )
      )
      OR (
        (
          X.SK_LOCAL_BRANCH IS NULL AND NOT Y.SK_LOCAL_BRANCH IS NULL
        )
        OR (
          NOT X.SK_LOCAL_BRANCH IS NULL AND Y.SK_LOCAL_BRANCH IS NULL
        )
        OR (
          X.SK_LOCAL_BRANCH <> Y.SK_LOCAL_BRANCH
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
          X.SK_RM_ID IS NULL AND NOT Y.SK_RM_ID IS NULL
        )
        OR (
          NOT X.SK_RM_ID IS NULL AND Y.SK_RM_ID IS NULL
        )
        OR (
          X.SK_RM_ID <> Y.SK_RM_ID
        )
      )
      OR (
        (
          X.SK_UDF_2 IS NULL AND NOT Y.SK_UDF_2 IS NULL
        )
        OR (
          NOT X.SK_UDF_2 IS NULL AND Y.SK_UDF_2 IS NULL
        )
        OR (
          X.SK_UDF_2 <> Y.SK_UDF_2
        )
      )
      OR (
        (
          X.SK_SWIFT_CODE IS NULL AND NOT Y.SK_SWIFT_CODE IS NULL
        )
        OR (
          NOT X.SK_SWIFT_CODE IS NULL AND Y.SK_SWIFT_CODE IS NULL
        )
        OR (
          X.SK_SWIFT_CODE <> Y.SK_SWIFT_CODE
        )
      )
      OR (
        (
          X.SSN IS NULL AND NOT Y.SSN IS NULL
        )
        OR (
          NOT X.SSN IS NULL AND Y.SSN IS NULL
        )
        OR (
          X.SSN <> Y.SSN
        )
      )
      OR (
        (
          X.STAFF IS NULL AND NOT Y.STAFF IS NULL
        )
        OR (
          NOT X.STAFF IS NULL AND Y.STAFF IS NULL
        )
        OR (
          X.STAFF <> Y.STAFF
        )
      )
      OR (
        (
          X.STMT_DAY IS NULL AND NOT Y.STMT_DAY IS NULL
        )
        OR (
          NOT X.STMT_DAY IS NULL AND Y.STMT_DAY IS NULL
        )
        OR (
          X.STMT_DAY <> Y.STMT_DAY
        )
      )
      OR (
        (
          X.SWIFT_CODE IS NULL AND NOT Y.SWIFT_CODE IS NULL
        )
        OR (
          NOT X.SWIFT_CODE IS NULL AND Y.SWIFT_CODE IS NULL
        )
        OR (
          X.SWIFT_CODE <> Y.SWIFT_CODE
        )
      )
      OR (
        (
          X.TAX_GROUP IS NULL AND NOT Y.TAX_GROUP IS NULL
        )
        OR (
          NOT X.TAX_GROUP IS NULL AND Y.TAX_GROUP IS NULL
        )
        OR (
          X.TAX_GROUP <> Y.TAX_GROUP
        )
      )
      OR (
        (
          X.TRACK_LIMITS IS NULL AND NOT Y.TRACK_LIMITS IS NULL
        )
        OR (
          NOT X.TRACK_LIMITS IS NULL AND Y.TRACK_LIMITS IS NULL
        )
        OR (
          X.TRACK_LIMITS <> Y.TRACK_LIMITS
        )
      )
      OR (
        (
          X.TREASURY_CUSTOMER IS NULL AND NOT Y.TREASURY_CUSTOMER IS NULL
        )
        OR (
          NOT X.TREASURY_CUSTOMER IS NULL AND Y.TREASURY_CUSTOMER IS NULL
        )
        OR (
          X.TREASURY_CUSTOMER <> Y.TREASURY_CUSTOMER
        )
      )
      OR (
        (
          X.UDF_1 IS NULL AND NOT Y.UDF_1 IS NULL
        )
        OR (
          NOT X.UDF_1 IS NULL AND Y.UDF_1 IS NULL
        )
        OR (
          X.UDF_1 <> Y.UDF_1
        )
      )
      OR (
        (
          X.UDF_2 IS NULL AND NOT Y.UDF_2 IS NULL
        )
        OR (
          NOT X.UDF_2 IS NULL AND Y.UDF_2 IS NULL
        )
        OR (
          X.UDF_2 <> Y.UDF_2
        )
      )
      OR (
        (
          X.UDF_3 IS NULL AND NOT Y.UDF_3 IS NULL
        )
        OR (
          NOT X.UDF_3 IS NULL AND Y.UDF_3 IS NULL
        )
        OR (
          X.UDF_3 <> Y.UDF_3
        )
      )
      OR (
        (
          X.UDF_4 IS NULL AND NOT Y.UDF_4 IS NULL
        )
        OR (
          NOT X.UDF_4 IS NULL AND Y.UDF_4 IS NULL
        )
        OR (
          X.UDF_4 <> Y.UDF_4
        )
      )
      OR (
        (
          X.UDF_5 IS NULL AND NOT Y.UDF_5 IS NULL
        )
        OR (
          NOT X.UDF_5 IS NULL AND Y.UDF_5 IS NULL
        )
        OR (
          X.UDF_5 <> Y.UDF_5
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
          X.UNIQUE_ID_NAME IS NULL AND NOT Y.UNIQUE_ID_NAME IS NULL
        )
        OR (
          NOT X.UNIQUE_ID_NAME IS NULL AND Y.UNIQUE_ID_NAME IS NULL
        )
        OR (
          X.UNIQUE_ID_NAME <> Y.UNIQUE_ID_NAME
        )
      )
      OR (
        (
          X.UNIQUE_ID_VALUE IS NULL AND NOT Y.UNIQUE_ID_VALUE IS NULL
        )
        OR (
          NOT X.UNIQUE_ID_VALUE IS NULL AND Y.UNIQUE_ID_VALUE IS NULL
        )
        OR (
          X.UNIQUE_ID_VALUE <> Y.UNIQUE_ID_VALUE
        )
      )
      OR (
        (
          X.UTILITY_PROVIDER IS NULL AND NOT Y.UTILITY_PROVIDER IS NULL
        )
        OR (
          NOT X.UTILITY_PROVIDER IS NULL AND Y.UTILITY_PROVIDER IS NULL
        )
        OR (
          X.UTILITY_PROVIDER <> Y.UTILITY_PROVIDER
        )
      )
      OR (
        (
          X.UTILITY_PROVIDER_ID IS NULL AND NOT Y.UTILITY_PROVIDER_ID IS NULL
        )
        OR (
          NOT X.UTILITY_PROVIDER_ID IS NULL AND Y.UTILITY_PROVIDER_ID IS NULL
        )
        OR (
          X.UTILITY_PROVIDER_ID <> Y.UTILITY_PROVIDER_ID
        )
      )
      OR (
        (
          X.UTILITY_PROVIDER_TYPE IS NULL AND NOT Y.UTILITY_PROVIDER_TYPE IS NULL
        )
        OR (
          NOT X.UTILITY_PROVIDER_TYPE IS NULL AND Y.UTILITY_PROVIDER_TYPE IS NULL
        )
        OR (
          X.UTILITY_PROVIDER_TYPE <> Y.UTILITY_PROVIDER_TYPE
        )
      )
      OR (
        (
          X.WHEREABOUTS_UNKNOWN IS NULL AND NOT Y.WHEREABOUTS_UNKNOWN IS NULL
        )
        OR (
          NOT X.WHEREABOUTS_UNKNOWN IS NULL AND Y.WHEREABOUTS_UNKNOWN IS NULL
        )
        OR (
          X.WHEREABOUTS_UNKNOWN <> Y.WHEREABOUTS_UNKNOWN
        )
      )
      OR (
        (
          X.WHT_PCT IS NULL AND NOT Y.WHT_PCT IS NULL
        )
        OR (
          NOT X.WHT_PCT IS NULL AND Y.WHT_PCT IS NULL
        )
        OR (
          X.WHT_PCT <> Y.WHT_PCT
        )
      )
    )
    THEN 'UPD'
    WHEN X.CUSTOMER_NO IS NULL
    THEN 'DEL' /* - IF MULTIPLE PKs are there then concatenate them with AND */
  END AS OPT_FLAG
FROM DEVT_STG_FLX.STTM_CUSTOMER AS X
FULL JOIN DEVT_STG_FLX.STTM_CUSTOMER_BACKUP AS Y
  ON X.CUSTOMER_NO = Y.CUSTOMER_NO /* - IF MULTIPLE PKs are there then concatenate them with AND */
WHERE
  NOT OPT_FLAG IS NULL