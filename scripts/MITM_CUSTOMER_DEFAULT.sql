CREATE OR REPLACE VIEW DEVV_STG_FLX.MITM_CUSTOMER_DEFAULT_get_delta AS LOCKING ROW FOR ACCESS
SELECT
  COALESCE(X.CUSTOMER, Y.CUSTOMER) AS CUSTOMER,
  X.COMP_MIS_1,
  X.COMP_MIS_10,
  X.COMP_MIS_2,
  X.COMP_MIS_3,
  X.COMP_MIS_4,
  X.COMP_MIS_5,
  X.COMP_MIS_6,
  X.COMP_MIS_7,
  X.COMP_MIS_8,
  X.COMP_MIS_9,
  X.CUST_MIS_1,
  X.CUST_MIS_10,
  X.CUST_MIS_2,
  X.CUST_MIS_3,
  X.CUST_MIS_4,
  X.CUST_MIS_5,
  X.CUST_MIS_6,
  X.CUST_MIS_7,
  X.CUST_MIS_8,
  X.CUST_MIS_9,
  X.MIS_GROUP,
  X.SK_CUSTOMER,
  X.SK_CUST_MIS_6,
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
    WHEN Y.CUSTOMER IS NULL
    THEN 'INS' /* - IF MULTIPLE PKs are there then concatenate them with AND */
    WHEN X.CUSTOMER = Y.CUSTOMER /* - IF MULTIPLE PKs are there then concatenate them with AND */
    AND (
      (
        (
          X.COMP_MIS_1 IS NULL AND NOT Y.COMP_MIS_1 IS NULL
        )
        OR (
          NOT X.COMP_MIS_1 IS NULL AND Y.COMP_MIS_1 IS NULL
        )
        OR (
          X.COMP_MIS_1 <> Y.COMP_MIS_1
        )
      )
      OR (
        (
          X.COMP_MIS_10 IS NULL AND NOT Y.COMP_MIS_10 IS NULL
        )
        OR (
          NOT X.COMP_MIS_10 IS NULL AND Y.COMP_MIS_10 IS NULL
        )
        OR (
          X.COMP_MIS_10 <> Y.COMP_MIS_10
        )
      )
      OR (
        (
          X.COMP_MIS_2 IS NULL AND NOT Y.COMP_MIS_2 IS NULL
        )
        OR (
          NOT X.COMP_MIS_2 IS NULL AND Y.COMP_MIS_2 IS NULL
        )
        OR (
          X.COMP_MIS_2 <> Y.COMP_MIS_2
        )
      )
      OR (
        (
          X.COMP_MIS_3 IS NULL AND NOT Y.COMP_MIS_3 IS NULL
        )
        OR (
          NOT X.COMP_MIS_3 IS NULL AND Y.COMP_MIS_3 IS NULL
        )
        OR (
          X.COMP_MIS_3 <> Y.COMP_MIS_3
        )
      )
      OR (
        (
          X.COMP_MIS_4 IS NULL AND NOT Y.COMP_MIS_4 IS NULL
        )
        OR (
          NOT X.COMP_MIS_4 IS NULL AND Y.COMP_MIS_4 IS NULL
        )
        OR (
          X.COMP_MIS_4 <> Y.COMP_MIS_4
        )
      )
      OR (
        (
          X.COMP_MIS_5 IS NULL AND NOT Y.COMP_MIS_5 IS NULL
        )
        OR (
          NOT X.COMP_MIS_5 IS NULL AND Y.COMP_MIS_5 IS NULL
        )
        OR (
          X.COMP_MIS_5 <> Y.COMP_MIS_5
        )
      )
      OR (
        (
          X.COMP_MIS_6 IS NULL AND NOT Y.COMP_MIS_6 IS NULL
        )
        OR (
          NOT X.COMP_MIS_6 IS NULL AND Y.COMP_MIS_6 IS NULL
        )
        OR (
          X.COMP_MIS_6 <> Y.COMP_MIS_6
        )
      )
      OR (
        (
          X.COMP_MIS_7 IS NULL AND NOT Y.COMP_MIS_7 IS NULL
        )
        OR (
          NOT X.COMP_MIS_7 IS NULL AND Y.COMP_MIS_7 IS NULL
        )
        OR (
          X.COMP_MIS_7 <> Y.COMP_MIS_7
        )
      )
      OR (
        (
          X.COMP_MIS_8 IS NULL AND NOT Y.COMP_MIS_8 IS NULL
        )
        OR (
          NOT X.COMP_MIS_8 IS NULL AND Y.COMP_MIS_8 IS NULL
        )
        OR (
          X.COMP_MIS_8 <> Y.COMP_MIS_8
        )
      )
      OR (
        (
          X.COMP_MIS_9 IS NULL AND NOT Y.COMP_MIS_9 IS NULL
        )
        OR (
          NOT X.COMP_MIS_9 IS NULL AND Y.COMP_MIS_9 IS NULL
        )
        OR (
          X.COMP_MIS_9 <> Y.COMP_MIS_9
        )
      )
      OR (
        (
          X.CUST_MIS_1 IS NULL AND NOT Y.CUST_MIS_1 IS NULL
        )
        OR (
          NOT X.CUST_MIS_1 IS NULL AND Y.CUST_MIS_1 IS NULL
        )
        OR (
          X.CUST_MIS_1 <> Y.CUST_MIS_1
        )
      )
      OR (
        (
          X.CUST_MIS_10 IS NULL AND NOT Y.CUST_MIS_10 IS NULL
        )
        OR (
          NOT X.CUST_MIS_10 IS NULL AND Y.CUST_MIS_10 IS NULL
        )
        OR (
          X.CUST_MIS_10 <> Y.CUST_MIS_10
        )
      )
      OR (
        (
          X.CUST_MIS_2 IS NULL AND NOT Y.CUST_MIS_2 IS NULL
        )
        OR (
          NOT X.CUST_MIS_2 IS NULL AND Y.CUST_MIS_2 IS NULL
        )
        OR (
          X.CUST_MIS_2 <> Y.CUST_MIS_2
        )
      )
      OR (
        (
          X.CUST_MIS_3 IS NULL AND NOT Y.CUST_MIS_3 IS NULL
        )
        OR (
          NOT X.CUST_MIS_3 IS NULL AND Y.CUST_MIS_3 IS NULL
        )
        OR (
          X.CUST_MIS_3 <> Y.CUST_MIS_3
        )
      )
      OR (
        (
          X.CUST_MIS_4 IS NULL AND NOT Y.CUST_MIS_4 IS NULL
        )
        OR (
          NOT X.CUST_MIS_4 IS NULL AND Y.CUST_MIS_4 IS NULL
        )
        OR (
          X.CUST_MIS_4 <> Y.CUST_MIS_4
        )
      )
      OR (
        (
          X.CUST_MIS_5 IS NULL AND NOT Y.CUST_MIS_5 IS NULL
        )
        OR (
          NOT X.CUST_MIS_5 IS NULL AND Y.CUST_MIS_5 IS NULL
        )
        OR (
          X.CUST_MIS_5 <> Y.CUST_MIS_5
        )
      )
      OR (
        (
          X.CUST_MIS_6 IS NULL AND NOT Y.CUST_MIS_6 IS NULL
        )
        OR (
          NOT X.CUST_MIS_6 IS NULL AND Y.CUST_MIS_6 IS NULL
        )
        OR (
          X.CUST_MIS_6 <> Y.CUST_MIS_6
        )
      )
      OR (
        (
          X.CUST_MIS_7 IS NULL AND NOT Y.CUST_MIS_7 IS NULL
        )
        OR (
          NOT X.CUST_MIS_7 IS NULL AND Y.CUST_MIS_7 IS NULL
        )
        OR (
          X.CUST_MIS_7 <> Y.CUST_MIS_7
        )
      )
      OR (
        (
          X.CUST_MIS_8 IS NULL AND NOT Y.CUST_MIS_8 IS NULL
        )
        OR (
          NOT X.CUST_MIS_8 IS NULL AND Y.CUST_MIS_8 IS NULL
        )
        OR (
          X.CUST_MIS_8 <> Y.CUST_MIS_8
        )
      )
      OR (
        (
          X.CUST_MIS_9 IS NULL AND NOT Y.CUST_MIS_9 IS NULL
        )
        OR (
          NOT X.CUST_MIS_9 IS NULL AND Y.CUST_MIS_9 IS NULL
        )
        OR (
          X.CUST_MIS_9 <> Y.CUST_MIS_9
        )
      )
      OR (
        (
          X.MIS_GROUP IS NULL AND NOT Y.MIS_GROUP IS NULL
        )
        OR (
          NOT X.MIS_GROUP IS NULL AND Y.MIS_GROUP IS NULL
        )
        OR (
          X.MIS_GROUP <> Y.MIS_GROUP
        )
      )
      OR (
        (
          X.SK_CUSTOMER IS NULL AND NOT Y.SK_CUSTOMER IS NULL
        )
        OR (
          NOT X.SK_CUSTOMER IS NULL AND Y.SK_CUSTOMER IS NULL
        )
        OR (
          X.SK_CUSTOMER <> Y.SK_CUSTOMER
        )
      )
      OR (
        (
          X.SK_CUST_MIS_6 IS NULL AND NOT Y.SK_CUST_MIS_6 IS NULL
        )
        OR (
          NOT X.SK_CUST_MIS_6 IS NULL AND Y.SK_CUST_MIS_6 IS NULL
        )
        OR (
          X.SK_CUST_MIS_6 <> Y.SK_CUST_MIS_6
        )
      )
    )
    THEN 'UPD'
    WHEN X.CUSTOMER IS NULL
    THEN 'DEL' /* - IF MULTIPLE PKs are there then concatenate them with AND */
  END AS OPT_FLAG
FROM DEVT_STG_FLX.MITM_CUSTOMER_DEFAULT AS X
FULL JOIN DEVT_STG_FLX.MITM_CUSTOMER_DEFAULT_BACKUP AS Y
  ON X.CUSTOMER = Y.CUSTOMER /* - IF MULTIPLE PKs are there then concatenate them with AND */
WHERE
  NOT OPT_FLAG IS NULL