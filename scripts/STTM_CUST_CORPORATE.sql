CREATE OR REPLACE VIEW DEVV_STG_FLX.STTM_CUST_CORPORATE_get_delta AS LOCKING ROW FOR ACCESS
SELECT
  COALESCE(X.CUSTOMER_NO, Y.CUSTOMER_NO) AS CUSTOMER_NO,
  X.AMOUNTS_CCY,
  X.BM_AMOUNTS_CCY,
  X.BM_INCORP_COUNTRY,
  X.BM_R_COUNTRY,
  X.BM_R_PINCODE,
  X.BUSINESS_DESCRIPTION,
  X.C_NATIONAL_ID,
  X.CAPITAL,
  X.CORPORATE_NAME,
  X.INCORP_COUNTRY,
  X.INCORP_DATE,
  X.NETWORTH,
  X.OWNERSHIP_TYPE,
  X.PREF_CONTACT_DT,
  X.PREF_CONTACT_TIME,
  X.R_ADDRESS1,
  X.R_ADDRESS2,
  X.R_ADDRESS3,
  X.R_ADDRESS4,
  X.R_COUNTRY,
  X.R_PINCODE,
  X.SK_CUSTOMER_NO,
  X.SK_R_ADDRESS1_R_ADDRESS2_R_ADDRESS3_R_ADDRESS4,
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
          X.AMOUNTS_CCY IS NULL AND NOT Y.AMOUNTS_CCY IS NULL
        )
        OR (
          NOT X.AMOUNTS_CCY IS NULL AND Y.AMOUNTS_CCY IS NULL
        )
        OR (
          X.AMOUNTS_CCY <> Y.AMOUNTS_CCY
        )
      )
      OR (
        (
          X.BM_AMOUNTS_CCY IS NULL AND NOT Y.BM_AMOUNTS_CCY IS NULL
        )
        OR (
          NOT X.BM_AMOUNTS_CCY IS NULL AND Y.BM_AMOUNTS_CCY IS NULL
        )
        OR (
          X.BM_AMOUNTS_CCY <> Y.BM_AMOUNTS_CCY
        )
      )
      OR (
        (
          X.BM_INCORP_COUNTRY IS NULL AND NOT Y.BM_INCORP_COUNTRY IS NULL
        )
        OR (
          NOT X.BM_INCORP_COUNTRY IS NULL AND Y.BM_INCORP_COUNTRY IS NULL
        )
        OR (
          X.BM_INCORP_COUNTRY <> Y.BM_INCORP_COUNTRY
        )
      )
      OR (
        (
          X.BM_R_COUNTRY IS NULL AND NOT Y.BM_R_COUNTRY IS NULL
        )
        OR (
          NOT X.BM_R_COUNTRY IS NULL AND Y.BM_R_COUNTRY IS NULL
        )
        OR (
          X.BM_R_COUNTRY <> Y.BM_R_COUNTRY
        )
      )
      OR (
        (
          X.BM_R_PINCODE IS NULL AND NOT Y.BM_R_PINCODE IS NULL
        )
        OR (
          NOT X.BM_R_PINCODE IS NULL AND Y.BM_R_PINCODE IS NULL
        )
        OR (
          X.BM_R_PINCODE <> Y.BM_R_PINCODE
        )
      )
      OR (
        (
          X.BUSINESS_DESCRIPTION IS NULL AND NOT Y.BUSINESS_DESCRIPTION IS NULL
        )
        OR (
          NOT X.BUSINESS_DESCRIPTION IS NULL AND Y.BUSINESS_DESCRIPTION IS NULL
        )
        OR (
          X.BUSINESS_DESCRIPTION <> Y.BUSINESS_DESCRIPTION
        )
      )
      OR (
        (
          X.C_NATIONAL_ID IS NULL AND NOT Y.C_NATIONAL_ID IS NULL
        )
        OR (
          NOT X.C_NATIONAL_ID IS NULL AND Y.C_NATIONAL_ID IS NULL
        )
        OR (
          X.C_NATIONAL_ID <> Y.C_NATIONAL_ID
        )
      )
      OR (
        (
          X.CAPITAL IS NULL AND NOT Y.CAPITAL IS NULL
        )
        OR (
          NOT X.CAPITAL IS NULL AND Y.CAPITAL IS NULL
        )
        OR (
          X.CAPITAL <> Y.CAPITAL
        )
      )
      OR (
        (
          X.CORPORATE_NAME IS NULL AND NOT Y.CORPORATE_NAME IS NULL
        )
        OR (
          NOT X.CORPORATE_NAME IS NULL AND Y.CORPORATE_NAME IS NULL
        )
        OR (
          X.CORPORATE_NAME <> Y.CORPORATE_NAME
        )
      )
      OR (
        (
          X.INCORP_COUNTRY IS NULL AND NOT Y.INCORP_COUNTRY IS NULL
        )
        OR (
          NOT X.INCORP_COUNTRY IS NULL AND Y.INCORP_COUNTRY IS NULL
        )
        OR (
          X.INCORP_COUNTRY <> Y.INCORP_COUNTRY
        )
      )
      OR (
        (
          X.INCORP_DATE IS NULL AND NOT Y.INCORP_DATE IS NULL
        )
        OR (
          NOT X.INCORP_DATE IS NULL AND Y.INCORP_DATE IS NULL
        )
        OR (
          X.INCORP_DATE <> Y.INCORP_DATE
        )
      )
      OR (
        (
          X.NETWORTH IS NULL AND NOT Y.NETWORTH IS NULL
        )
        OR (
          NOT X.NETWORTH IS NULL AND Y.NETWORTH IS NULL
        )
        OR (
          X.NETWORTH <> Y.NETWORTH
        )
      )
      OR (
        (
          X.OWNERSHIP_TYPE IS NULL AND NOT Y.OWNERSHIP_TYPE IS NULL
        )
        OR (
          NOT X.OWNERSHIP_TYPE IS NULL AND Y.OWNERSHIP_TYPE IS NULL
        )
        OR (
          X.OWNERSHIP_TYPE <> Y.OWNERSHIP_TYPE
        )
      )
      OR (
        (
          X.PREF_CONTACT_DT IS NULL AND NOT Y.PREF_CONTACT_DT IS NULL
        )
        OR (
          NOT X.PREF_CONTACT_DT IS NULL AND Y.PREF_CONTACT_DT IS NULL
        )
        OR (
          X.PREF_CONTACT_DT <> Y.PREF_CONTACT_DT
        )
      )
      OR (
        (
          X.PREF_CONTACT_TIME IS NULL AND NOT Y.PREF_CONTACT_TIME IS NULL
        )
        OR (
          NOT X.PREF_CONTACT_TIME IS NULL AND Y.PREF_CONTACT_TIME IS NULL
        )
        OR (
          X.PREF_CONTACT_TIME <> Y.PREF_CONTACT_TIME
        )
      )
      OR (
        (
          X.R_ADDRESS1 IS NULL AND NOT Y.R_ADDRESS1 IS NULL
        )
        OR (
          NOT X.R_ADDRESS1 IS NULL AND Y.R_ADDRESS1 IS NULL
        )
        OR (
          X.R_ADDRESS1 <> Y.R_ADDRESS1
        )
      )
      OR (
        (
          X.R_ADDRESS2 IS NULL AND NOT Y.R_ADDRESS2 IS NULL
        )
        OR (
          NOT X.R_ADDRESS2 IS NULL AND Y.R_ADDRESS2 IS NULL
        )
        OR (
          X.R_ADDRESS2 <> Y.R_ADDRESS2
        )
      )
      OR (
        (
          X.R_ADDRESS3 IS NULL AND NOT Y.R_ADDRESS3 IS NULL
        )
        OR (
          NOT X.R_ADDRESS3 IS NULL AND Y.R_ADDRESS3 IS NULL
        )
        OR (
          X.R_ADDRESS3 <> Y.R_ADDRESS3
        )
      )
      OR (
        (
          X.R_ADDRESS4 IS NULL AND NOT Y.R_ADDRESS4 IS NULL
        )
        OR (
          NOT X.R_ADDRESS4 IS NULL AND Y.R_ADDRESS4 IS NULL
        )
        OR (
          X.R_ADDRESS4 <> Y.R_ADDRESS4
        )
      )
      OR (
        (
          X.R_COUNTRY IS NULL AND NOT Y.R_COUNTRY IS NULL
        )
        OR (
          NOT X.R_COUNTRY IS NULL AND Y.R_COUNTRY IS NULL
        )
        OR (
          X.R_COUNTRY <> Y.R_COUNTRY
        )
      )
      OR (
        (
          X.R_PINCODE IS NULL AND NOT Y.R_PINCODE IS NULL
        )
        OR (
          NOT X.R_PINCODE IS NULL AND Y.R_PINCODE IS NULL
        )
        OR (
          X.R_PINCODE <> Y.R_PINCODE
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
          X.SK_R_ADDRESS1_R_ADDRESS2_R_ADDRESS3_R_ADDRESS4 IS NULL
          AND NOT Y.SK_R_ADDRESS1_R_ADDRESS2_R_ADDRESS3_R_ADDRESS4 IS NULL
        )
        OR (
          NOT X.SK_R_ADDRESS1_R_ADDRESS2_R_ADDRESS3_R_ADDRESS4 IS NULL
          AND Y.SK_R_ADDRESS1_R_ADDRESS2_R_ADDRESS3_R_ADDRESS4 IS NULL
        )
        OR (
          X.SK_R_ADDRESS1_R_ADDRESS2_R_ADDRESS3_R_ADDRESS4 <> Y.SK_R_ADDRESS1_R_ADDRESS2_R_ADDRESS3_R_ADDRESS4
        )
      )
    )
    THEN 'UPD'
    WHEN X.CUSTOMER_NO IS NULL
    THEN 'DEL' /* - IF MULTIPLE PKs are there then concatenate them with AND */
  END AS OPT_FLAG
FROM DEVT_STG_FLX.STTM_CUST_CORPORATE AS X
FULL JOIN DEVT_STG_FLX.STTM_CUST_CORPORATE_BACKUP AS Y
  ON X.CUSTOMER_NO = Y.CUSTOMER_NO /* - IF MULTIPLE PKs are there then concatenate them with AND */
WHERE
  NOT OPT_FLAG IS NULL