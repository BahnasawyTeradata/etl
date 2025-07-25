CREATE OR REPLACE VIEW DEVV_STG_FLX.SMTB_MODULES_get_delta AS LOCKING ROW FOR ACCESS
SELECT
  COALESCE(X.MODULE_ID, Y.MODULE_ID) AS MODULE_ID,
  X.AC_CLASS_APPLICABLE,
  X.ARC_APPLICABLE,
  X.AUTH_STAT,
  X.BM_MODULE_ID,
  X.BM_MODULE_ID_PRODUCT,
  X.CB_CLASS_APPLICABLE,
  X.CH_CLASS_APPLICABLE,
  X.CHECKER_DT_STAMP,
  X.CHECKER_ID,
  X.CR_CLASS_APPLICABLE,
  X.DISC_ACCR_APPLICABLE,
  X.IN_CLASS_APPLICABLE,
  X.INSTALLED,
  X.LICENSE,
  X.MAKER_DT_STAMP,
  X.MAKER_ID,
  X.MOD_NO,
  X.MODULE_DESC,
  X.ONCE_AUTH,
  X.PURGE_AVAILABLE,
  X.RECORD_STAT,
  X.RH_CLASS_APPLICABLE,
  X.TA_CLASS_APPLICABLE,
  X.USER_DEFINED_MODULE,
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
    WHEN Y.MODULE_ID IS NULL
    THEN 'INS' /* - IF MULTIPLE PKs are there then concatenate them with AND */
    WHEN X.MODULE_ID = Y.MODULE_ID /* - IF MULTIPLE PKs are there then concatenate them with AND */
    AND (
      (
        (
          X.AC_CLASS_APPLICABLE IS NULL AND NOT Y.AC_CLASS_APPLICABLE IS NULL
        )
        OR (
          NOT X.AC_CLASS_APPLICABLE IS NULL AND Y.AC_CLASS_APPLICABLE IS NULL
        )
        OR (
          X.AC_CLASS_APPLICABLE <> Y.AC_CLASS_APPLICABLE
        )
      )
      OR (
        (
          X.ARC_APPLICABLE IS NULL AND NOT Y.ARC_APPLICABLE IS NULL
        )
        OR (
          NOT X.ARC_APPLICABLE IS NULL AND Y.ARC_APPLICABLE IS NULL
        )
        OR (
          X.ARC_APPLICABLE <> Y.ARC_APPLICABLE
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
          X.BM_MODULE_ID IS NULL AND NOT Y.BM_MODULE_ID IS NULL
        )
        OR (
          NOT X.BM_MODULE_ID IS NULL AND Y.BM_MODULE_ID IS NULL
        )
        OR (
          X.BM_MODULE_ID <> Y.BM_MODULE_ID
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
          X.CB_CLASS_APPLICABLE IS NULL AND NOT Y.CB_CLASS_APPLICABLE IS NULL
        )
        OR (
          NOT X.CB_CLASS_APPLICABLE IS NULL AND Y.CB_CLASS_APPLICABLE IS NULL
        )
        OR (
          X.CB_CLASS_APPLICABLE <> Y.CB_CLASS_APPLICABLE
        )
      )
      OR (
        (
          X.CH_CLASS_APPLICABLE IS NULL AND NOT Y.CH_CLASS_APPLICABLE IS NULL
        )
        OR (
          NOT X.CH_CLASS_APPLICABLE IS NULL AND Y.CH_CLASS_APPLICABLE IS NULL
        )
        OR (
          X.CH_CLASS_APPLICABLE <> Y.CH_CLASS_APPLICABLE
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
          X.CR_CLASS_APPLICABLE IS NULL AND NOT Y.CR_CLASS_APPLICABLE IS NULL
        )
        OR (
          NOT X.CR_CLASS_APPLICABLE IS NULL AND Y.CR_CLASS_APPLICABLE IS NULL
        )
        OR (
          X.CR_CLASS_APPLICABLE <> Y.CR_CLASS_APPLICABLE
        )
      )
      OR (
        (
          X.DISC_ACCR_APPLICABLE IS NULL AND NOT Y.DISC_ACCR_APPLICABLE IS NULL
        )
        OR (
          NOT X.DISC_ACCR_APPLICABLE IS NULL AND Y.DISC_ACCR_APPLICABLE IS NULL
        )
        OR (
          X.DISC_ACCR_APPLICABLE <> Y.DISC_ACCR_APPLICABLE
        )
      )
      OR (
        (
          X.IN_CLASS_APPLICABLE IS NULL AND NOT Y.IN_CLASS_APPLICABLE IS NULL
        )
        OR (
          NOT X.IN_CLASS_APPLICABLE IS NULL AND Y.IN_CLASS_APPLICABLE IS NULL
        )
        OR (
          X.IN_CLASS_APPLICABLE <> Y.IN_CLASS_APPLICABLE
        )
      )
      OR (
        (
          X.INSTALLED IS NULL AND NOT Y.INSTALLED IS NULL
        )
        OR (
          NOT X.INSTALLED IS NULL AND Y.INSTALLED IS NULL
        )
        OR (
          X.INSTALLED <> Y.INSTALLED
        )
      )
      OR (
        (
          X.LICENSE IS NULL AND NOT Y.LICENSE IS NULL
        )
        OR (
          NOT X.LICENSE IS NULL AND Y.LICENSE IS NULL
        )
        OR (
          X.LICENSE <> Y.LICENSE
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
          X.MODULE_DESC IS NULL AND NOT Y.MODULE_DESC IS NULL
        )
        OR (
          NOT X.MODULE_DESC IS NULL AND Y.MODULE_DESC IS NULL
        )
        OR (
          X.MODULE_DESC <> Y.MODULE_DESC
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
          X.PURGE_AVAILABLE IS NULL AND NOT Y.PURGE_AVAILABLE IS NULL
        )
        OR (
          NOT X.PURGE_AVAILABLE IS NULL AND Y.PURGE_AVAILABLE IS NULL
        )
        OR (
          X.PURGE_AVAILABLE <> Y.PURGE_AVAILABLE
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
          X.RH_CLASS_APPLICABLE IS NULL AND NOT Y.RH_CLASS_APPLICABLE IS NULL
        )
        OR (
          NOT X.RH_CLASS_APPLICABLE IS NULL AND Y.RH_CLASS_APPLICABLE IS NULL
        )
        OR (
          X.RH_CLASS_APPLICABLE <> Y.RH_CLASS_APPLICABLE
        )
      )
      OR (
        (
          X.TA_CLASS_APPLICABLE IS NULL AND NOT Y.TA_CLASS_APPLICABLE IS NULL
        )
        OR (
          NOT X.TA_CLASS_APPLICABLE IS NULL AND Y.TA_CLASS_APPLICABLE IS NULL
        )
        OR (
          X.TA_CLASS_APPLICABLE <> Y.TA_CLASS_APPLICABLE
        )
      )
      OR (
        (
          X.USER_DEFINED_MODULE IS NULL AND NOT Y.USER_DEFINED_MODULE IS NULL
        )
        OR (
          NOT X.USER_DEFINED_MODULE IS NULL AND Y.USER_DEFINED_MODULE IS NULL
        )
        OR (
          X.USER_DEFINED_MODULE <> Y.USER_DEFINED_MODULE
        )
      )
    )
    THEN 'UPD'
    WHEN X.MODULE_ID IS NULL
    THEN 'DEL' /* - IF MULTIPLE PKs are there then concatenate them with AND */
  END AS OPT_FLAG
FROM DEVT_STG_FLX.SMTB_MODULES AS X
FULL JOIN DEVT_STG_FLX.SMTB_MODULES_BACKUP AS Y
  ON X.MODULE_ID = Y.MODULE_ID /* - IF MULTIPLE PKs are there then concatenate them with AND */
WHERE
  NOT OPT_FLAG IS NULL