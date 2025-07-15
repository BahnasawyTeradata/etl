CREATE OR REPLACE VIEW DEVV_STG_FLX.STTM_CUST_PROFESSIONAL_get_delta AS LOCKING ROW FOR ACCESS
SELECT
  COALESCE(X.CUSTOMER_NO, Y.CUSTOMER_NO) AS CUSTOMER_NO,
  X.BM_EMPLOYER,
  X.BM_EMPLOYMENT_STATUS,
  X.CCY_PERS_INCEXP,
  X.CREDIT_CARDS,
  X.DESIGNATION,
  X.E_ADDRESS1,
  X.E_ADDRESS2,
  X.E_ADDRESS3,
  X.E_COUNTRY,
  X.E_EMAIL,
  X.E_FAX,
  X.E_TELEPHONE,
  X.E_TELEX,
  X.EMPLOYER,
  X.EMPLOYMENT_STATUS,
  X.EMPLOYMENT_TENURE,
  X.HOUSE_VALUE,
  X.INSURANCE,
  X.LOAN_PAYMENT,
  X.OTHER_EXPENSES,
  X.OTHER_INCOME,
  X.PREV_DESIGNATION,
  X.PREV_EMPLOYER,
  X.RENT,
  X.RETIREMENT_AGE,
  X.SALARY,
  X.SK_CUSTOMER_NO,
  X.SK_EMPLOYER,
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
          X.BM_EMPLOYER IS NULL AND NOT Y.BM_EMPLOYER IS NULL
        )
        OR (
          NOT X.BM_EMPLOYER IS NULL AND Y.BM_EMPLOYER IS NULL
        )
        OR (
          X.BM_EMPLOYER <> Y.BM_EMPLOYER
        )
      )
      OR (
        (
          X.BM_EMPLOYMENT_STATUS IS NULL AND NOT Y.BM_EMPLOYMENT_STATUS IS NULL
        )
        OR (
          NOT X.BM_EMPLOYMENT_STATUS IS NULL AND Y.BM_EMPLOYMENT_STATUS IS NULL
        )
        OR (
          X.BM_EMPLOYMENT_STATUS <> Y.BM_EMPLOYMENT_STATUS
        )
      )
      OR (
        (
          X.CCY_PERS_INCEXP IS NULL AND NOT Y.CCY_PERS_INCEXP IS NULL
        )
        OR (
          NOT X.CCY_PERS_INCEXP IS NULL AND Y.CCY_PERS_INCEXP IS NULL
        )
        OR (
          X.CCY_PERS_INCEXP <> Y.CCY_PERS_INCEXP
        )
      )
      OR (
        (
          X.CREDIT_CARDS IS NULL AND NOT Y.CREDIT_CARDS IS NULL
        )
        OR (
          NOT X.CREDIT_CARDS IS NULL AND Y.CREDIT_CARDS IS NULL
        )
        OR (
          X.CREDIT_CARDS <> Y.CREDIT_CARDS
        )
      )
      OR (
        (
          X.DESIGNATION IS NULL AND NOT Y.DESIGNATION IS NULL
        )
        OR (
          NOT X.DESIGNATION IS NULL AND Y.DESIGNATION IS NULL
        )
        OR (
          X.DESIGNATION <> Y.DESIGNATION
        )
      )
      OR (
        (
          X.E_ADDRESS1 IS NULL AND NOT Y.E_ADDRESS1 IS NULL
        )
        OR (
          NOT X.E_ADDRESS1 IS NULL AND Y.E_ADDRESS1 IS NULL
        )
        OR (
          X.E_ADDRESS1 <> Y.E_ADDRESS1
        )
      )
      OR (
        (
          X.E_ADDRESS2 IS NULL AND NOT Y.E_ADDRESS2 IS NULL
        )
        OR (
          NOT X.E_ADDRESS2 IS NULL AND Y.E_ADDRESS2 IS NULL
        )
        OR (
          X.E_ADDRESS2 <> Y.E_ADDRESS2
        )
      )
      OR (
        (
          X.E_ADDRESS3 IS NULL AND NOT Y.E_ADDRESS3 IS NULL
        )
        OR (
          NOT X.E_ADDRESS3 IS NULL AND Y.E_ADDRESS3 IS NULL
        )
        OR (
          X.E_ADDRESS3 <> Y.E_ADDRESS3
        )
      )
      OR (
        (
          X.E_COUNTRY IS NULL AND NOT Y.E_COUNTRY IS NULL
        )
        OR (
          NOT X.E_COUNTRY IS NULL AND Y.E_COUNTRY IS NULL
        )
        OR (
          X.E_COUNTRY <> Y.E_COUNTRY
        )
      )
      OR (
        (
          X.E_EMAIL IS NULL AND NOT Y.E_EMAIL IS NULL
        )
        OR (
          NOT X.E_EMAIL IS NULL AND Y.E_EMAIL IS NULL
        )
        OR (
          X.E_EMAIL <> Y.E_EMAIL
        )
      )
      OR (
        (
          X.E_FAX IS NULL AND NOT Y.E_FAX IS NULL
        )
        OR (
          NOT X.E_FAX IS NULL AND Y.E_FAX IS NULL
        )
        OR (
          X.E_FAX <> Y.E_FAX
        )
      )
      OR (
        (
          X.E_TELEPHONE IS NULL AND NOT Y.E_TELEPHONE IS NULL
        )
        OR (
          NOT X.E_TELEPHONE IS NULL AND Y.E_TELEPHONE IS NULL
        )
        OR (
          X.E_TELEPHONE <> Y.E_TELEPHONE
        )
      )
      OR (
        (
          X.E_TELEX IS NULL AND NOT Y.E_TELEX IS NULL
        )
        OR (
          NOT X.E_TELEX IS NULL AND Y.E_TELEX IS NULL
        )
        OR (
          X.E_TELEX <> Y.E_TELEX
        )
      )
      OR (
        (
          X.EMPLOYER IS NULL AND NOT Y.EMPLOYER IS NULL
        )
        OR (
          NOT X.EMPLOYER IS NULL AND Y.EMPLOYER IS NULL
        )
        OR (
          X.EMPLOYER <> Y.EMPLOYER
        )
      )
      OR (
        (
          X.EMPLOYMENT_STATUS IS NULL AND NOT Y.EMPLOYMENT_STATUS IS NULL
        )
        OR (
          NOT X.EMPLOYMENT_STATUS IS NULL AND Y.EMPLOYMENT_STATUS IS NULL
        )
        OR (
          X.EMPLOYMENT_STATUS <> Y.EMPLOYMENT_STATUS
        )
      )
      OR (
        (
          X.EMPLOYMENT_TENURE IS NULL AND NOT Y.EMPLOYMENT_TENURE IS NULL
        )
        OR (
          NOT X.EMPLOYMENT_TENURE IS NULL AND Y.EMPLOYMENT_TENURE IS NULL
        )
        OR (
          X.EMPLOYMENT_TENURE <> Y.EMPLOYMENT_TENURE
        )
      )
      OR (
        (
          X.HOUSE_VALUE IS NULL AND NOT Y.HOUSE_VALUE IS NULL
        )
        OR (
          NOT X.HOUSE_VALUE IS NULL AND Y.HOUSE_VALUE IS NULL
        )
        OR (
          X.HOUSE_VALUE <> Y.HOUSE_VALUE
        )
      )
      OR (
        (
          X.INSURANCE IS NULL AND NOT Y.INSURANCE IS NULL
        )
        OR (
          NOT X.INSURANCE IS NULL AND Y.INSURANCE IS NULL
        )
        OR (
          X.INSURANCE <> Y.INSURANCE
        )
      )
      OR (
        (
          X.LOAN_PAYMENT IS NULL AND NOT Y.LOAN_PAYMENT IS NULL
        )
        OR (
          NOT X.LOAN_PAYMENT IS NULL AND Y.LOAN_PAYMENT IS NULL
        )
        OR (
          X.LOAN_PAYMENT <> Y.LOAN_PAYMENT
        )
      )
      OR (
        (
          X.OTHER_EXPENSES IS NULL AND NOT Y.OTHER_EXPENSES IS NULL
        )
        OR (
          NOT X.OTHER_EXPENSES IS NULL AND Y.OTHER_EXPENSES IS NULL
        )
        OR (
          X.OTHER_EXPENSES <> Y.OTHER_EXPENSES
        )
      )
      OR (
        (
          X.OTHER_INCOME IS NULL AND NOT Y.OTHER_INCOME IS NULL
        )
        OR (
          NOT X.OTHER_INCOME IS NULL AND Y.OTHER_INCOME IS NULL
        )
        OR (
          X.OTHER_INCOME <> Y.OTHER_INCOME
        )
      )
      OR (
        (
          X.PREV_DESIGNATION IS NULL AND NOT Y.PREV_DESIGNATION IS NULL
        )
        OR (
          NOT X.PREV_DESIGNATION IS NULL AND Y.PREV_DESIGNATION IS NULL
        )
        OR (
          X.PREV_DESIGNATION <> Y.PREV_DESIGNATION
        )
      )
      OR (
        (
          X.PREV_EMPLOYER IS NULL AND NOT Y.PREV_EMPLOYER IS NULL
        )
        OR (
          NOT X.PREV_EMPLOYER IS NULL AND Y.PREV_EMPLOYER IS NULL
        )
        OR (
          X.PREV_EMPLOYER <> Y.PREV_EMPLOYER
        )
      )
      OR (
        (
          X.RENT IS NULL AND NOT Y.RENT IS NULL
        )
        OR (
          NOT X.RENT IS NULL AND Y.RENT IS NULL
        )
        OR (
          X.RENT <> Y.RENT
        )
      )
      OR (
        (
          X.RETIREMENT_AGE IS NULL AND NOT Y.RETIREMENT_AGE IS NULL
        )
        OR (
          NOT X.RETIREMENT_AGE IS NULL AND Y.RETIREMENT_AGE IS NULL
        )
        OR (
          X.RETIREMENT_AGE <> Y.RETIREMENT_AGE
        )
      )
      OR (
        (
          X.SALARY IS NULL AND NOT Y.SALARY IS NULL
        )
        OR (
          NOT X.SALARY IS NULL AND Y.SALARY IS NULL
        )
        OR (
          X.SALARY <> Y.SALARY
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
          X.SK_EMPLOYER IS NULL AND NOT Y.SK_EMPLOYER IS NULL
        )
        OR (
          NOT X.SK_EMPLOYER IS NULL AND Y.SK_EMPLOYER IS NULL
        )
        OR (
          X.SK_EMPLOYER <> Y.SK_EMPLOYER
        )
      )
    )
    THEN 'UPD'
    WHEN X.CUSTOMER_NO IS NULL
    THEN 'DEL' /* - IF MULTIPLE PKs are there then concatenate them with AND */
  END AS OPT_FLAG
FROM DEVT_STG_FLX.STTM_CUST_PROFESSIONAL AS X
FULL JOIN DEVT_STG_FLX.STTM_CUST_PROFESSIONAL_BACKUP AS Y
  ON X.CUSTOMER_NO = Y.CUSTOMER_NO /* - IF MULTIPLE PKs are there then concatenate them with AND */
WHERE
  NOT OPT_FLAG IS NULL