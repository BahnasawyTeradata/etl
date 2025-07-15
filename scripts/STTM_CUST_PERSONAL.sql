CREATE OR REPLACE VIEW DEVV_STG_FLX.STTM_CUST_PERSONAL_get_delta AS LOCKING ROW FOR ACCESS
SELECT
  COALESCE(X.CUSTOMER_NO, Y.CUSTOMER_NO) AS CUSTOMER_NO,
  X.AGE_PROOF_SUBMITTED,
  X.BIRTH_COUNTRY,
  X.BM_AGE_PROOF_SUBMITTED,
  X.BM_BIRTH_COUNTRY,
  X.BM_CUSTOMER_PREFIX1,
  X.BM_CUSTOMER_PREFIX2,
  X.BM_D_COUNTRY,
  X.BM_E_MAIL,
  X.BM_MINOR,
  X.BM_P_COUNTRY,
  X.BM_PLACE_OF_BIRTH,
  X.BM_RESIDENT_STATUS,
  X.BM_SEX,
  X.CUST_COMM_MODE,
  X.CUSTOMER_PREFIX,
  X.CUSTOMER_PREFIX1,
  X.CUSTOMER_PREFIX2,
  X.D_ADDRESS1,
  X.D_ADDRESS2,
  X.D_ADDRESS3,
  X.D_ADDRESS4,
  X.D_COUNTRY,
  X.D_PINCODE,
  X.DATE_OF_BIRTH,
  X.E_MAIL,
  X.FAX,
  X.FAX_ISD_NO,
  X.FIRST_NAME,
  X.HOME_TEL_ISD,
  X.HOME_TEL_NO,
  X.LAST_NAME,
  X.LEGAL_GUARDIAN,
  X.MIDDLE_NAME,
  X.MINOR,
  X.MOB_ISD_NO,
  X.MOBILE_NUMBER,
  X.P_ADDRESS1,
  X.P_ADDRESS2,
  X.P_ADDRESS3,
  X.P_ADDRESS4,
  X.P_COUNTRY,
  X.P_NATIONAL_ID,
  X.P_PINCODE,
  X.PA_HOLDER_ADDR,
  X.PA_HOLDER_ADDR_COUNTRY,
  X.PA_HOLDER_NAME,
  X.PA_HOLDER_NATIONALTY,
  X.PA_HOLDER_TEL_ISD,
  X.PA_HOLDER_TEL_NO,
  X.PA_ISSUED,
  X.PASSPORT_NO,
  X.PLACE_OF_BIRTH,
  X.PPT_EXP_DATE,
  X.PPT_ISS_DATE,
  X.PREF_CONTACT_DT,
  X.PREF_CONTACT_TIME,
  X.RESIDENT_STATUS,
  X.SEX,
  X.SK_CUSTOMER_NO,
  X.SK_D_ADDRESS1_D_ADDRESS2_D_ADDRESS3_D_ADDRESS4,
  X.SK_E_MAIL,
  X.SK_FAX,
  X.SK_HOME_TEL_NO,
  X.SK_MOBILE_NUMBER,
  X.SK_P_ADDRESS1_P_ADDRESS2_P_ADDRESS3_P_ADDRESS4,
  X.SK_PA_HOLDER_TEL_NO,
  X.SK_TELEPHONE,
  X.TEL_ISD_NO,
  X.TELEPHONE,
  X.US_RES_STATUS,
  X.VST_US_PREV,
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
          X.AGE_PROOF_SUBMITTED IS NULL AND NOT Y.AGE_PROOF_SUBMITTED IS NULL
        )
        OR (
          NOT X.AGE_PROOF_SUBMITTED IS NULL AND Y.AGE_PROOF_SUBMITTED IS NULL
        )
        OR (
          X.AGE_PROOF_SUBMITTED <> Y.AGE_PROOF_SUBMITTED
        )
      )
      OR (
        (
          X.BIRTH_COUNTRY IS NULL AND NOT Y.BIRTH_COUNTRY IS NULL
        )
        OR (
          NOT X.BIRTH_COUNTRY IS NULL AND Y.BIRTH_COUNTRY IS NULL
        )
        OR (
          X.BIRTH_COUNTRY <> Y.BIRTH_COUNTRY
        )
      )
      OR (
        (
          X.BM_AGE_PROOF_SUBMITTED IS NULL AND NOT Y.BM_AGE_PROOF_SUBMITTED IS NULL
        )
        OR (
          NOT X.BM_AGE_PROOF_SUBMITTED IS NULL AND Y.BM_AGE_PROOF_SUBMITTED IS NULL
        )
        OR (
          X.BM_AGE_PROOF_SUBMITTED <> Y.BM_AGE_PROOF_SUBMITTED
        )
      )
      OR (
        (
          X.BM_BIRTH_COUNTRY IS NULL AND NOT Y.BM_BIRTH_COUNTRY IS NULL
        )
        OR (
          NOT X.BM_BIRTH_COUNTRY IS NULL AND Y.BM_BIRTH_COUNTRY IS NULL
        )
        OR (
          X.BM_BIRTH_COUNTRY <> Y.BM_BIRTH_COUNTRY
        )
      )
      OR (
        (
          X.BM_CUSTOMER_PREFIX1 IS NULL AND NOT Y.BM_CUSTOMER_PREFIX1 IS NULL
        )
        OR (
          NOT X.BM_CUSTOMER_PREFIX1 IS NULL AND Y.BM_CUSTOMER_PREFIX1 IS NULL
        )
        OR (
          X.BM_CUSTOMER_PREFIX1 <> Y.BM_CUSTOMER_PREFIX1
        )
      )
      OR (
        (
          X.BM_CUSTOMER_PREFIX2 IS NULL AND NOT Y.BM_CUSTOMER_PREFIX2 IS NULL
        )
        OR (
          NOT X.BM_CUSTOMER_PREFIX2 IS NULL AND Y.BM_CUSTOMER_PREFIX2 IS NULL
        )
        OR (
          X.BM_CUSTOMER_PREFIX2 <> Y.BM_CUSTOMER_PREFIX2
        )
      )
      OR (
        (
          X.BM_D_COUNTRY IS NULL AND NOT Y.BM_D_COUNTRY IS NULL
        )
        OR (
          NOT X.BM_D_COUNTRY IS NULL AND Y.BM_D_COUNTRY IS NULL
        )
        OR (
          X.BM_D_COUNTRY <> Y.BM_D_COUNTRY
        )
      )
      OR (
        (
          X.BM_E_MAIL IS NULL AND NOT Y.BM_E_MAIL IS NULL
        )
        OR (
          NOT X.BM_E_MAIL IS NULL AND Y.BM_E_MAIL IS NULL
        )
        OR (
          X.BM_E_MAIL <> Y.BM_E_MAIL
        )
      )
      OR (
        (
          X.BM_MINOR IS NULL AND NOT Y.BM_MINOR IS NULL
        )
        OR (
          NOT X.BM_MINOR IS NULL AND Y.BM_MINOR IS NULL
        )
        OR (
          X.BM_MINOR <> Y.BM_MINOR
        )
      )
      OR (
        (
          X.BM_P_COUNTRY IS NULL AND NOT Y.BM_P_COUNTRY IS NULL
        )
        OR (
          NOT X.BM_P_COUNTRY IS NULL AND Y.BM_P_COUNTRY IS NULL
        )
        OR (
          X.BM_P_COUNTRY <> Y.BM_P_COUNTRY
        )
      )
      OR (
        (
          X.BM_PLACE_OF_BIRTH IS NULL AND NOT Y.BM_PLACE_OF_BIRTH IS NULL
        )
        OR (
          NOT X.BM_PLACE_OF_BIRTH IS NULL AND Y.BM_PLACE_OF_BIRTH IS NULL
        )
        OR (
          X.BM_PLACE_OF_BIRTH <> Y.BM_PLACE_OF_BIRTH
        )
      )
      OR (
        (
          X.BM_RESIDENT_STATUS IS NULL AND NOT Y.BM_RESIDENT_STATUS IS NULL
        )
        OR (
          NOT X.BM_RESIDENT_STATUS IS NULL AND Y.BM_RESIDENT_STATUS IS NULL
        )
        OR (
          X.BM_RESIDENT_STATUS <> Y.BM_RESIDENT_STATUS
        )
      )
      OR (
        (
          X.BM_SEX IS NULL AND NOT Y.BM_SEX IS NULL
        )
        OR (
          NOT X.BM_SEX IS NULL AND Y.BM_SEX IS NULL
        )
        OR (
          X.BM_SEX <> Y.BM_SEX
        )
      )
      OR (
        (
          X.CUST_COMM_MODE IS NULL AND NOT Y.CUST_COMM_MODE IS NULL
        )
        OR (
          NOT X.CUST_COMM_MODE IS NULL AND Y.CUST_COMM_MODE IS NULL
        )
        OR (
          X.CUST_COMM_MODE <> Y.CUST_COMM_MODE
        )
      )
      OR (
        (
          X.CUSTOMER_PREFIX IS NULL AND NOT Y.CUSTOMER_PREFIX IS NULL
        )
        OR (
          NOT X.CUSTOMER_PREFIX IS NULL AND Y.CUSTOMER_PREFIX IS NULL
        )
        OR (
          X.CUSTOMER_PREFIX <> Y.CUSTOMER_PREFIX
        )
      )
      OR (
        (
          X.CUSTOMER_PREFIX1 IS NULL AND NOT Y.CUSTOMER_PREFIX1 IS NULL
        )
        OR (
          NOT X.CUSTOMER_PREFIX1 IS NULL AND Y.CUSTOMER_PREFIX1 IS NULL
        )
        OR (
          X.CUSTOMER_PREFIX1 <> Y.CUSTOMER_PREFIX1
        )
      )
      OR (
        (
          X.CUSTOMER_PREFIX2 IS NULL AND NOT Y.CUSTOMER_PREFIX2 IS NULL
        )
        OR (
          NOT X.CUSTOMER_PREFIX2 IS NULL AND Y.CUSTOMER_PREFIX2 IS NULL
        )
        OR (
          X.CUSTOMER_PREFIX2 <> Y.CUSTOMER_PREFIX2
        )
      )
      OR (
        (
          X.D_ADDRESS1 IS NULL AND NOT Y.D_ADDRESS1 IS NULL
        )
        OR (
          NOT X.D_ADDRESS1 IS NULL AND Y.D_ADDRESS1 IS NULL
        )
        OR (
          X.D_ADDRESS1 <> Y.D_ADDRESS1
        )
      )
      OR (
        (
          X.D_ADDRESS2 IS NULL AND NOT Y.D_ADDRESS2 IS NULL
        )
        OR (
          NOT X.D_ADDRESS2 IS NULL AND Y.D_ADDRESS2 IS NULL
        )
        OR (
          X.D_ADDRESS2 <> Y.D_ADDRESS2
        )
      )
      OR (
        (
          X.D_ADDRESS3 IS NULL AND NOT Y.D_ADDRESS3 IS NULL
        )
        OR (
          NOT X.D_ADDRESS3 IS NULL AND Y.D_ADDRESS3 IS NULL
        )
        OR (
          X.D_ADDRESS3 <> Y.D_ADDRESS3
        )
      )
      OR (
        (
          X.D_ADDRESS4 IS NULL AND NOT Y.D_ADDRESS4 IS NULL
        )
        OR (
          NOT X.D_ADDRESS4 IS NULL AND Y.D_ADDRESS4 IS NULL
        )
        OR (
          X.D_ADDRESS4 <> Y.D_ADDRESS4
        )
      )
      OR (
        (
          X.D_COUNTRY IS NULL AND NOT Y.D_COUNTRY IS NULL
        )
        OR (
          NOT X.D_COUNTRY IS NULL AND Y.D_COUNTRY IS NULL
        )
        OR (
          X.D_COUNTRY <> Y.D_COUNTRY
        )
      )
      OR (
        (
          X.D_PINCODE IS NULL AND NOT Y.D_PINCODE IS NULL
        )
        OR (
          NOT X.D_PINCODE IS NULL AND Y.D_PINCODE IS NULL
        )
        OR (
          X.D_PINCODE <> Y.D_PINCODE
        )
      )
      OR (
        (
          X.DATE_OF_BIRTH IS NULL AND NOT Y.DATE_OF_BIRTH IS NULL
        )
        OR (
          NOT X.DATE_OF_BIRTH IS NULL AND Y.DATE_OF_BIRTH IS NULL
        )
        OR (
          X.DATE_OF_BIRTH <> Y.DATE_OF_BIRTH
        )
      )
      OR (
        (
          X.E_MAIL IS NULL AND NOT Y.E_MAIL IS NULL
        )
        OR (
          NOT X.E_MAIL IS NULL AND Y.E_MAIL IS NULL
        )
        OR (
          X.E_MAIL <> Y.E_MAIL
        )
      )
      OR (
        (
          X.FAX IS NULL AND NOT Y.FAX IS NULL
        )
        OR (
          NOT X.FAX IS NULL AND Y.FAX IS NULL
        )
        OR (
          X.FAX <> Y.FAX
        )
      )
      OR (
        (
          X.FAX_ISD_NO IS NULL AND NOT Y.FAX_ISD_NO IS NULL
        )
        OR (
          NOT X.FAX_ISD_NO IS NULL AND Y.FAX_ISD_NO IS NULL
        )
        OR (
          X.FAX_ISD_NO <> Y.FAX_ISD_NO
        )
      )
      OR (
        (
          X.FIRST_NAME IS NULL AND NOT Y.FIRST_NAME IS NULL
        )
        OR (
          NOT X.FIRST_NAME IS NULL AND Y.FIRST_NAME IS NULL
        )
        OR (
          X.FIRST_NAME <> Y.FIRST_NAME
        )
      )
      OR (
        (
          X.HOME_TEL_ISD IS NULL AND NOT Y.HOME_TEL_ISD IS NULL
        )
        OR (
          NOT X.HOME_TEL_ISD IS NULL AND Y.HOME_TEL_ISD IS NULL
        )
        OR (
          X.HOME_TEL_ISD <> Y.HOME_TEL_ISD
        )
      )
      OR (
        (
          X.HOME_TEL_NO IS NULL AND NOT Y.HOME_TEL_NO IS NULL
        )
        OR (
          NOT X.HOME_TEL_NO IS NULL AND Y.HOME_TEL_NO IS NULL
        )
        OR (
          X.HOME_TEL_NO <> Y.HOME_TEL_NO
        )
      )
      OR (
        (
          X.LAST_NAME IS NULL AND NOT Y.LAST_NAME IS NULL
        )
        OR (
          NOT X.LAST_NAME IS NULL AND Y.LAST_NAME IS NULL
        )
        OR (
          X.LAST_NAME <> Y.LAST_NAME
        )
      )
      OR (
        (
          X.LEGAL_GUARDIAN IS NULL AND NOT Y.LEGAL_GUARDIAN IS NULL
        )
        OR (
          NOT X.LEGAL_GUARDIAN IS NULL AND Y.LEGAL_GUARDIAN IS NULL
        )
        OR (
          X.LEGAL_GUARDIAN <> Y.LEGAL_GUARDIAN
        )
      )
      OR (
        (
          X.MIDDLE_NAME IS NULL AND NOT Y.MIDDLE_NAME IS NULL
        )
        OR (
          NOT X.MIDDLE_NAME IS NULL AND Y.MIDDLE_NAME IS NULL
        )
        OR (
          X.MIDDLE_NAME <> Y.MIDDLE_NAME
        )
      )
      OR (
        (
          X.MINOR IS NULL AND NOT Y.MINOR IS NULL
        )
        OR (
          NOT X.MINOR IS NULL AND Y.MINOR IS NULL
        )
        OR (
          X.MINOR <> Y.MINOR
        )
      )
      OR (
        (
          X.MOB_ISD_NO IS NULL AND NOT Y.MOB_ISD_NO IS NULL
        )
        OR (
          NOT X.MOB_ISD_NO IS NULL AND Y.MOB_ISD_NO IS NULL
        )
        OR (
          X.MOB_ISD_NO <> Y.MOB_ISD_NO
        )
      )
      OR (
        (
          X.MOBILE_NUMBER IS NULL AND NOT Y.MOBILE_NUMBER IS NULL
        )
        OR (
          NOT X.MOBILE_NUMBER IS NULL AND Y.MOBILE_NUMBER IS NULL
        )
        OR (
          X.MOBILE_NUMBER <> Y.MOBILE_NUMBER
        )
      )
      OR (
        (
          X.P_ADDRESS1 IS NULL AND NOT Y.P_ADDRESS1 IS NULL
        )
        OR (
          NOT X.P_ADDRESS1 IS NULL AND Y.P_ADDRESS1 IS NULL
        )
        OR (
          X.P_ADDRESS1 <> Y.P_ADDRESS1
        )
      )
      OR (
        (
          X.P_ADDRESS2 IS NULL AND NOT Y.P_ADDRESS2 IS NULL
        )
        OR (
          NOT X.P_ADDRESS2 IS NULL AND Y.P_ADDRESS2 IS NULL
        )
        OR (
          X.P_ADDRESS2 <> Y.P_ADDRESS2
        )
      )
      OR (
        (
          X.P_ADDRESS3 IS NULL AND NOT Y.P_ADDRESS3 IS NULL
        )
        OR (
          NOT X.P_ADDRESS3 IS NULL AND Y.P_ADDRESS3 IS NULL
        )
        OR (
          X.P_ADDRESS3 <> Y.P_ADDRESS3
        )
      )
      OR (
        (
          X.P_ADDRESS4 IS NULL AND NOT Y.P_ADDRESS4 IS NULL
        )
        OR (
          NOT X.P_ADDRESS4 IS NULL AND Y.P_ADDRESS4 IS NULL
        )
        OR (
          X.P_ADDRESS4 <> Y.P_ADDRESS4
        )
      )
      OR (
        (
          X.P_COUNTRY IS NULL AND NOT Y.P_COUNTRY IS NULL
        )
        OR (
          NOT X.P_COUNTRY IS NULL AND Y.P_COUNTRY IS NULL
        )
        OR (
          X.P_COUNTRY <> Y.P_COUNTRY
        )
      )
      OR (
        (
          X.P_NATIONAL_ID IS NULL AND NOT Y.P_NATIONAL_ID IS NULL
        )
        OR (
          NOT X.P_NATIONAL_ID IS NULL AND Y.P_NATIONAL_ID IS NULL
        )
        OR (
          X.P_NATIONAL_ID <> Y.P_NATIONAL_ID
        )
      )
      OR (
        (
          X.P_PINCODE IS NULL AND NOT Y.P_PINCODE IS NULL
        )
        OR (
          NOT X.P_PINCODE IS NULL AND Y.P_PINCODE IS NULL
        )
        OR (
          X.P_PINCODE <> Y.P_PINCODE
        )
      )
      OR (
        (
          X.PA_HOLDER_ADDR IS NULL AND NOT Y.PA_HOLDER_ADDR IS NULL
        )
        OR (
          NOT X.PA_HOLDER_ADDR IS NULL AND Y.PA_HOLDER_ADDR IS NULL
        )
        OR (
          X.PA_HOLDER_ADDR <> Y.PA_HOLDER_ADDR
        )
      )
      OR (
        (
          X.PA_HOLDER_ADDR_COUNTRY IS NULL AND NOT Y.PA_HOLDER_ADDR_COUNTRY IS NULL
        )
        OR (
          NOT X.PA_HOLDER_ADDR_COUNTRY IS NULL AND Y.PA_HOLDER_ADDR_COUNTRY IS NULL
        )
        OR (
          X.PA_HOLDER_ADDR_COUNTRY <> Y.PA_HOLDER_ADDR_COUNTRY
        )
      )
      OR (
        (
          X.PA_HOLDER_NAME IS NULL AND NOT Y.PA_HOLDER_NAME IS NULL
        )
        OR (
          NOT X.PA_HOLDER_NAME IS NULL AND Y.PA_HOLDER_NAME IS NULL
        )
        OR (
          X.PA_HOLDER_NAME <> Y.PA_HOLDER_NAME
        )
      )
      OR (
        (
          X.PA_HOLDER_NATIONALTY IS NULL AND NOT Y.PA_HOLDER_NATIONALTY IS NULL
        )
        OR (
          NOT X.PA_HOLDER_NATIONALTY IS NULL AND Y.PA_HOLDER_NATIONALTY IS NULL
        )
        OR (
          X.PA_HOLDER_NATIONALTY <> Y.PA_HOLDER_NATIONALTY
        )
      )
      OR (
        (
          X.PA_HOLDER_TEL_ISD IS NULL AND NOT Y.PA_HOLDER_TEL_ISD IS NULL
        )
        OR (
          NOT X.PA_HOLDER_TEL_ISD IS NULL AND Y.PA_HOLDER_TEL_ISD IS NULL
        )
        OR (
          X.PA_HOLDER_TEL_ISD <> Y.PA_HOLDER_TEL_ISD
        )
      )
      OR (
        (
          X.PA_HOLDER_TEL_NO IS NULL AND NOT Y.PA_HOLDER_TEL_NO IS NULL
        )
        OR (
          NOT X.PA_HOLDER_TEL_NO IS NULL AND Y.PA_HOLDER_TEL_NO IS NULL
        )
        OR (
          X.PA_HOLDER_TEL_NO <> Y.PA_HOLDER_TEL_NO
        )
      )
      OR (
        (
          X.PA_ISSUED IS NULL AND NOT Y.PA_ISSUED IS NULL
        )
        OR (
          NOT X.PA_ISSUED IS NULL AND Y.PA_ISSUED IS NULL
        )
        OR (
          X.PA_ISSUED <> Y.PA_ISSUED
        )
      )
      OR (
        (
          X.PASSPORT_NO IS NULL AND NOT Y.PASSPORT_NO IS NULL
        )
        OR (
          NOT X.PASSPORT_NO IS NULL AND Y.PASSPORT_NO IS NULL
        )
        OR (
          X.PASSPORT_NO <> Y.PASSPORT_NO
        )
      )
      OR (
        (
          X.PLACE_OF_BIRTH IS NULL AND NOT Y.PLACE_OF_BIRTH IS NULL
        )
        OR (
          NOT X.PLACE_OF_BIRTH IS NULL AND Y.PLACE_OF_BIRTH IS NULL
        )
        OR (
          X.PLACE_OF_BIRTH <> Y.PLACE_OF_BIRTH
        )
      )
      OR (
        (
          X.PPT_EXP_DATE IS NULL AND NOT Y.PPT_EXP_DATE IS NULL
        )
        OR (
          NOT X.PPT_EXP_DATE IS NULL AND Y.PPT_EXP_DATE IS NULL
        )
        OR (
          X.PPT_EXP_DATE <> Y.PPT_EXP_DATE
        )
      )
      OR (
        (
          X.PPT_ISS_DATE IS NULL AND NOT Y.PPT_ISS_DATE IS NULL
        )
        OR (
          NOT X.PPT_ISS_DATE IS NULL AND Y.PPT_ISS_DATE IS NULL
        )
        OR (
          X.PPT_ISS_DATE <> Y.PPT_ISS_DATE
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
          X.RESIDENT_STATUS IS NULL AND NOT Y.RESIDENT_STATUS IS NULL
        )
        OR (
          NOT X.RESIDENT_STATUS IS NULL AND Y.RESIDENT_STATUS IS NULL
        )
        OR (
          X.RESIDENT_STATUS <> Y.RESIDENT_STATUS
        )
      )
      OR (
        (
          X.SEX IS NULL AND NOT Y.SEX IS NULL
        )
        OR (
          NOT X.SEX IS NULL AND Y.SEX IS NULL
        )
        OR (
          X.SEX <> Y.SEX
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
          X.SK_D_ADDRESS1_D_ADDRESS2_D_ADDRESS3_D_ADDRESS4 IS NULL
          AND NOT Y.SK_D_ADDRESS1_D_ADDRESS2_D_ADDRESS3_D_ADDRESS4 IS NULL
        )
        OR (
          NOT X.SK_D_ADDRESS1_D_ADDRESS2_D_ADDRESS3_D_ADDRESS4 IS NULL
          AND Y.SK_D_ADDRESS1_D_ADDRESS2_D_ADDRESS3_D_ADDRESS4 IS NULL
        )
        OR (
          X.SK_D_ADDRESS1_D_ADDRESS2_D_ADDRESS3_D_ADDRESS4 <> Y.SK_D_ADDRESS1_D_ADDRESS2_D_ADDRESS3_D_ADDRESS4
        )
      )
      OR (
        (
          X.SK_E_MAIL IS NULL AND NOT Y.SK_E_MAIL IS NULL
        )
        OR (
          NOT X.SK_E_MAIL IS NULL AND Y.SK_E_MAIL IS NULL
        )
        OR (
          X.SK_E_MAIL <> Y.SK_E_MAIL
        )
      )
      OR (
        (
          X.SK_FAX IS NULL AND NOT Y.SK_FAX IS NULL
        )
        OR (
          NOT X.SK_FAX IS NULL AND Y.SK_FAX IS NULL
        )
        OR (
          X.SK_FAX <> Y.SK_FAX
        )
      )
      OR (
        (
          X.SK_HOME_TEL_NO IS NULL AND NOT Y.SK_HOME_TEL_NO IS NULL
        )
        OR (
          NOT X.SK_HOME_TEL_NO IS NULL AND Y.SK_HOME_TEL_NO IS NULL
        )
        OR (
          X.SK_HOME_TEL_NO <> Y.SK_HOME_TEL_NO
        )
      )
      OR (
        (
          X.SK_MOBILE_NUMBER IS NULL AND NOT Y.SK_MOBILE_NUMBER IS NULL
        )
        OR (
          NOT X.SK_MOBILE_NUMBER IS NULL AND Y.SK_MOBILE_NUMBER IS NULL
        )
        OR (
          X.SK_MOBILE_NUMBER <> Y.SK_MOBILE_NUMBER
        )
      )
      OR (
        (
          X.SK_P_ADDRESS1_P_ADDRESS2_P_ADDRESS3_P_ADDRESS4 IS NULL
          AND NOT Y.SK_P_ADDRESS1_P_ADDRESS2_P_ADDRESS3_P_ADDRESS4 IS NULL
        )
        OR (
          NOT X.SK_P_ADDRESS1_P_ADDRESS2_P_ADDRESS3_P_ADDRESS4 IS NULL
          AND Y.SK_P_ADDRESS1_P_ADDRESS2_P_ADDRESS3_P_ADDRESS4 IS NULL
        )
        OR (
          X.SK_P_ADDRESS1_P_ADDRESS2_P_ADDRESS3_P_ADDRESS4 <> Y.SK_P_ADDRESS1_P_ADDRESS2_P_ADDRESS3_P_ADDRESS4
        )
      )
      OR (
        (
          X.SK_PA_HOLDER_TEL_NO IS NULL AND NOT Y.SK_PA_HOLDER_TEL_NO IS NULL
        )
        OR (
          NOT X.SK_PA_HOLDER_TEL_NO IS NULL AND Y.SK_PA_HOLDER_TEL_NO IS NULL
        )
        OR (
          X.SK_PA_HOLDER_TEL_NO <> Y.SK_PA_HOLDER_TEL_NO
        )
      )
      OR (
        (
          X.SK_TELEPHONE IS NULL AND NOT Y.SK_TELEPHONE IS NULL
        )
        OR (
          NOT X.SK_TELEPHONE IS NULL AND Y.SK_TELEPHONE IS NULL
        )
        OR (
          X.SK_TELEPHONE <> Y.SK_TELEPHONE
        )
      )
      OR (
        (
          X.TEL_ISD_NO IS NULL AND NOT Y.TEL_ISD_NO IS NULL
        )
        OR (
          NOT X.TEL_ISD_NO IS NULL AND Y.TEL_ISD_NO IS NULL
        )
        OR (
          X.TEL_ISD_NO <> Y.TEL_ISD_NO
        )
      )
      OR (
        (
          X.TELEPHONE IS NULL AND NOT Y.TELEPHONE IS NULL
        )
        OR (
          NOT X.TELEPHONE IS NULL AND Y.TELEPHONE IS NULL
        )
        OR (
          X.TELEPHONE <> Y.TELEPHONE
        )
      )
      OR (
        (
          X.US_RES_STATUS IS NULL AND NOT Y.US_RES_STATUS IS NULL
        )
        OR (
          NOT X.US_RES_STATUS IS NULL AND Y.US_RES_STATUS IS NULL
        )
        OR (
          X.US_RES_STATUS <> Y.US_RES_STATUS
        )
      )
      OR (
        (
          X.VST_US_PREV IS NULL AND NOT Y.VST_US_PREV IS NULL
        )
        OR (
          NOT X.VST_US_PREV IS NULL AND Y.VST_US_PREV IS NULL
        )
        OR (
          X.VST_US_PREV <> Y.VST_US_PREV
        )
      )
    )
    THEN 'UPD'
    WHEN X.CUSTOMER_NO IS NULL
    THEN 'DEL' /* - IF MULTIPLE PKs are there then concatenate them with AND */
  END AS OPT_FLAG
FROM DEVT_STG_FLX.STTM_CUST_PERSONAL AS X
FULL JOIN DEVT_STG_FLX.STTM_CUST_PERSONAL_BACKUP AS Y
  ON X.CUSTOMER_NO = Y.CUSTOMER_NO /* - IF MULTIPLE PKs are there then concatenate them with AND */
WHERE
  NOT OPT_FLAG IS NULL