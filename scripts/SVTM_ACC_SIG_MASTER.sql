CREATE OR REPLACE VIEW DEVV_STG_FLX.SVTM_ACC_SIG_MASTER_get_delta AS LOCKING ROW FOR ACCESS
SELECT
  COALESCE(X.ACC_NO, Y.ACC_NO) AS ACC_NO,
  COALESCE(X.BRANCH, Y.BRANCH) AS BRANCH,
  X.ACC_MSG,
  X.MIN_NO_OF_SIG,
  X.SK_ACC_NO,
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
    WHEN Y.ACC_NO IS NULL AND Y.BRANCH IS NULL
    THEN 'INS' /* - IF MULTIPLE PKs are there then concatenate them with AND */
    WHEN X.ACC_NO = Y.ACC_NO
    AND X.BRANCH = Y.BRANCH /* - IF MULTIPLE PKs are there then concatenate them with AND */
    AND (
      (
        (
          X.ACC_MSG IS NULL AND NOT Y.ACC_MSG IS NULL
        )
        OR (
          NOT X.ACC_MSG IS NULL AND Y.ACC_MSG IS NULL
        )
        OR (
          X.ACC_MSG <> Y.ACC_MSG
        )
      )
      OR (
        (
          X.MIN_NO_OF_SIG IS NULL AND NOT Y.MIN_NO_OF_SIG IS NULL
        )
        OR (
          NOT X.MIN_NO_OF_SIG IS NULL AND Y.MIN_NO_OF_SIG IS NULL
        )
        OR (
          X.MIN_NO_OF_SIG <> Y.MIN_NO_OF_SIG
        )
      )
      OR (
        (
          X.SK_ACC_NO IS NULL AND NOT Y.SK_ACC_NO IS NULL
        )
        OR (
          NOT X.SK_ACC_NO IS NULL AND Y.SK_ACC_NO IS NULL
        )
        OR (
          X.SK_ACC_NO <> Y.SK_ACC_NO
        )
      )
    )
    THEN 'UPD'
    WHEN X.ACC_NO IS NULL AND X.BRANCH IS NULL
    THEN 'DEL' /* - IF MULTIPLE PKs are there then concatenate them with AND */
  END AS OPT_FLAG
FROM DEVT_STG_FLX.SVTM_ACC_SIG_MASTER AS X
FULL JOIN DEVT_STG_FLX.SVTM_ACC_SIG_MASTER_BACKUP AS Y
  ON X.ACC_NO = Y.ACC_NO
  AND X.BRANCH = Y.BRANCH /* - IF MULTIPLE PKs are there then concatenate them with AND */
WHERE
  NOT OPT_FLAG IS NULL