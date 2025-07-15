CREATE OR REPLACE VIEW DEVV_STG_FLX.SMTB_USER_get_delta AS LOCKING ROW FOR ACCESS
SELECT
  COALESCE(X.USER_ID, Y.USER_ID) AS USER_ID,
  X.ACCESS_CONTROL,
  X.ACCLASS_ALLOWED,
  X.ALERTS_ON_HOME,
  X.AMOUNT_FORMAT,
  X.AUTH_STAT,
  X.AUTO_AUTH,
  X.BM_AUTH_STAT,
  X.BM_MOD_NO,
  X.BM_ONCE_AUTH,
  X.BM_RECORD_STAT,
  X.BM_USER_EMAIL,
  X.BRANCH_USRPWD,
  X.BRANCHES_ALLOWED,
  X.CHECKER_DT_STAMP,
  X.CHECKER_ID,
  X.CUSTOMER_NO,
  X.DASHBOARD_REQD,
  X.DATE_FORMAT,
  X.DEBUG_WINDOW_ENABLED,
  X.DEPT_CODE,
  X.DFLT_MODULE,
  X.EL_USER_ID,
  X.END_DATE,
  X.EXIT_FUNCTION,
  X.EXT_USER_REF,
  X.F10_REQD,
  X.F11_REQD,
  X.F12_REQD,
  X.FORCE_PASSWD_CHANGE,
  X.FWDREW_COUNT,
  X.GL_ALLOWED,
  X.GROUP_CODE_ALLOWED,
  X.HOME_BRANCH,
  X.HOME_PHONE,
  X.LDAP_USER,
  X.LIMITS_CCY,
  X.MAKER_DT_STAMP,
  X.MAKER_ID,
  X.MAX_AUTH_AMT,
  X.MAX_OVERRIDE_AMT,
  X.MAX_TXN_AMT,
  X.MFI_USER,
  X.MOD_NO,
  X.MULTIBRANCH_ACCESS,
  X.ONCE_AUTH,
  X.OTHER_RM_CUST_RESTRICT,
  X.PRODUCTS_ACCESS_ALLOWED,
  X.PRODUCTS_ALLOWED,
  X.PWD_CHANGED_ON,
  X.RECORD_STAT,
  X.REFERENCE_NO,
  X.SALT,
  X.SCREEN_SAVER_TIMEOUT,
  X.SK_HOME_BRANCH,
  X.SK_USER_ID,
  X.SK_USER_EMAIL,
  X.STAFF_AC_RESTR,
  X.START_DATE,
  X.STARTUP_FUNCTION,
  X.STATUS_CHANGED_ON,
  X.TAX_IDENTIFIER,
  X.TELEPHONE_NUMBER,
  X.TILL_ALLOWED,
  X.TIME_LEVEL,
  X.USER_CATEGORY,
  X.USER_EMAIL,
  X.USER_FAX,
  X.USER_ID_BRN,
  X.USER_LANGUAGE,
  X.USER_MANAGER,
  X.USER_MOBILE,
  X.USER_NAME,
  X.USER_PAGER,
  X.USER_PASSWORD,
  X.USER_PASSWORD_BRN,
  X.USER_STATUS,
  X.USER_TXN_LIMIT,
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
    WHEN Y.USER_ID IS NULL
    THEN 'INS' /* - IF MULTIPLE PKs are there then concatenate them with AND */
    WHEN X.USER_ID = Y.USER_ID /* - IF MULTIPLE PKs are there then concatenate them with AND */
    AND (
      (
        (
          X.ACCESS_CONTROL IS NULL AND NOT Y.ACCESS_CONTROL IS NULL
        )
        OR (
          NOT X.ACCESS_CONTROL IS NULL AND Y.ACCESS_CONTROL IS NULL
        )
        OR (
          X.ACCESS_CONTROL <> Y.ACCESS_CONTROL
        )
      )
      OR (
        (
          X.ACCLASS_ALLOWED IS NULL AND NOT Y.ACCLASS_ALLOWED IS NULL
        )
        OR (
          NOT X.ACCLASS_ALLOWED IS NULL AND Y.ACCLASS_ALLOWED IS NULL
        )
        OR (
          X.ACCLASS_ALLOWED <> Y.ACCLASS_ALLOWED
        )
      )
      OR (
        (
          X.ALERTS_ON_HOME IS NULL AND NOT Y.ALERTS_ON_HOME IS NULL
        )
        OR (
          NOT X.ALERTS_ON_HOME IS NULL AND Y.ALERTS_ON_HOME IS NULL
        )
        OR (
          X.ALERTS_ON_HOME <> Y.ALERTS_ON_HOME
        )
      )
      OR (
        (
          X.AMOUNT_FORMAT IS NULL AND NOT Y.AMOUNT_FORMAT IS NULL
        )
        OR (
          NOT X.AMOUNT_FORMAT IS NULL AND Y.AMOUNT_FORMAT IS NULL
        )
        OR (
          X.AMOUNT_FORMAT <> Y.AMOUNT_FORMAT
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
          X.AUTO_AUTH IS NULL AND NOT Y.AUTO_AUTH IS NULL
        )
        OR (
          NOT X.AUTO_AUTH IS NULL AND Y.AUTO_AUTH IS NULL
        )
        OR (
          X.AUTO_AUTH <> Y.AUTO_AUTH
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
          X.BM_USER_EMAIL IS NULL AND NOT Y.BM_USER_EMAIL IS NULL
        )
        OR (
          NOT X.BM_USER_EMAIL IS NULL AND Y.BM_USER_EMAIL IS NULL
        )
        OR (
          X.BM_USER_EMAIL <> Y.BM_USER_EMAIL
        )
      )
      OR (
        (
          X.BRANCH_USRPWD IS NULL AND NOT Y.BRANCH_USRPWD IS NULL
        )
        OR (
          NOT X.BRANCH_USRPWD IS NULL AND Y.BRANCH_USRPWD IS NULL
        )
        OR (
          X.BRANCH_USRPWD <> Y.BRANCH_USRPWD
        )
      )
      OR (
        (
          X.BRANCHES_ALLOWED IS NULL AND NOT Y.BRANCHES_ALLOWED IS NULL
        )
        OR (
          NOT X.BRANCHES_ALLOWED IS NULL AND Y.BRANCHES_ALLOWED IS NULL
        )
        OR (
          X.BRANCHES_ALLOWED <> Y.BRANCHES_ALLOWED
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
          X.CUSTOMER_NO IS NULL AND NOT Y.CUSTOMER_NO IS NULL
        )
        OR (
          NOT X.CUSTOMER_NO IS NULL AND Y.CUSTOMER_NO IS NULL
        )
        OR (
          X.CUSTOMER_NO <> Y.CUSTOMER_NO
        )
      )
      OR (
        (
          X.DASHBOARD_REQD IS NULL AND NOT Y.DASHBOARD_REQD IS NULL
        )
        OR (
          NOT X.DASHBOARD_REQD IS NULL AND Y.DASHBOARD_REQD IS NULL
        )
        OR (
          X.DASHBOARD_REQD <> Y.DASHBOARD_REQD
        )
      )
      OR (
        (
          X.DATE_FORMAT IS NULL AND NOT Y.DATE_FORMAT IS NULL
        )
        OR (
          NOT X.DATE_FORMAT IS NULL AND Y.DATE_FORMAT IS NULL
        )
        OR (
          X.DATE_FORMAT <> Y.DATE_FORMAT
        )
      )
      OR (
        (
          X.DEBUG_WINDOW_ENABLED IS NULL AND NOT Y.DEBUG_WINDOW_ENABLED IS NULL
        )
        OR (
          NOT X.DEBUG_WINDOW_ENABLED IS NULL AND Y.DEBUG_WINDOW_ENABLED IS NULL
        )
        OR (
          X.DEBUG_WINDOW_ENABLED <> Y.DEBUG_WINDOW_ENABLED
        )
      )
      OR (
        (
          X.DEPT_CODE IS NULL AND NOT Y.DEPT_CODE IS NULL
        )
        OR (
          NOT X.DEPT_CODE IS NULL AND Y.DEPT_CODE IS NULL
        )
        OR (
          X.DEPT_CODE <> Y.DEPT_CODE
        )
      )
      OR (
        (
          X.DFLT_MODULE IS NULL AND NOT Y.DFLT_MODULE IS NULL
        )
        OR (
          NOT X.DFLT_MODULE IS NULL AND Y.DFLT_MODULE IS NULL
        )
        OR (
          X.DFLT_MODULE <> Y.DFLT_MODULE
        )
      )
      OR (
        (
          X.EL_USER_ID IS NULL AND NOT Y.EL_USER_ID IS NULL
        )
        OR (
          NOT X.EL_USER_ID IS NULL AND Y.EL_USER_ID IS NULL
        )
        OR (
          X.EL_USER_ID <> Y.EL_USER_ID
        )
      )
      OR (
        (
          X.END_DATE IS NULL AND NOT Y.END_DATE IS NULL
        )
        OR (
          NOT X.END_DATE IS NULL AND Y.END_DATE IS NULL
        )
        OR (
          X.END_DATE <> Y.END_DATE
        )
      )
      OR (
        (
          X.EXIT_FUNCTION IS NULL AND NOT Y.EXIT_FUNCTION IS NULL
        )
        OR (
          NOT X.EXIT_FUNCTION IS NULL AND Y.EXIT_FUNCTION IS NULL
        )
        OR (
          X.EXIT_FUNCTION <> Y.EXIT_FUNCTION
        )
      )
      OR (
        (
          X.EXT_USER_REF IS NULL AND NOT Y.EXT_USER_REF IS NULL
        )
        OR (
          NOT X.EXT_USER_REF IS NULL AND Y.EXT_USER_REF IS NULL
        )
        OR (
          X.EXT_USER_REF <> Y.EXT_USER_REF
        )
      )
      OR (
        (
          X.F10_REQD IS NULL AND NOT Y.F10_REQD IS NULL
        )
        OR (
          NOT X.F10_REQD IS NULL AND Y.F10_REQD IS NULL
        )
        OR (
          X.F10_REQD <> Y.F10_REQD
        )
      )
      OR (
        (
          X.F11_REQD IS NULL AND NOT Y.F11_REQD IS NULL
        )
        OR (
          NOT X.F11_REQD IS NULL AND Y.F11_REQD IS NULL
        )
        OR (
          X.F11_REQD <> Y.F11_REQD
        )
      )
      OR (
        (
          X.F12_REQD IS NULL AND NOT Y.F12_REQD IS NULL
        )
        OR (
          NOT X.F12_REQD IS NULL AND Y.F12_REQD IS NULL
        )
        OR (
          X.F12_REQD <> Y.F12_REQD
        )
      )
      OR (
        (
          X.FORCE_PASSWD_CHANGE IS NULL AND NOT Y.FORCE_PASSWD_CHANGE IS NULL
        )
        OR (
          NOT X.FORCE_PASSWD_CHANGE IS NULL AND Y.FORCE_PASSWD_CHANGE IS NULL
        )
        OR (
          X.FORCE_PASSWD_CHANGE <> Y.FORCE_PASSWD_CHANGE
        )
      )
      OR (
        (
          X.FWDREW_COUNT IS NULL AND NOT Y.FWDREW_COUNT IS NULL
        )
        OR (
          NOT X.FWDREW_COUNT IS NULL AND Y.FWDREW_COUNT IS NULL
        )
        OR (
          X.FWDREW_COUNT <> Y.FWDREW_COUNT
        )
      )
      OR (
        (
          X.GL_ALLOWED IS NULL AND NOT Y.GL_ALLOWED IS NULL
        )
        OR (
          NOT X.GL_ALLOWED IS NULL AND Y.GL_ALLOWED IS NULL
        )
        OR (
          X.GL_ALLOWED <> Y.GL_ALLOWED
        )
      )
      OR (
        (
          X.GROUP_CODE_ALLOWED IS NULL AND NOT Y.GROUP_CODE_ALLOWED IS NULL
        )
        OR (
          NOT X.GROUP_CODE_ALLOWED IS NULL AND Y.GROUP_CODE_ALLOWED IS NULL
        )
        OR (
          X.GROUP_CODE_ALLOWED <> Y.GROUP_CODE_ALLOWED
        )
      )
      OR (
        (
          X.HOME_BRANCH IS NULL AND NOT Y.HOME_BRANCH IS NULL
        )
        OR (
          NOT X.HOME_BRANCH IS NULL AND Y.HOME_BRANCH IS NULL
        )
        OR (
          X.HOME_BRANCH <> Y.HOME_BRANCH
        )
      )
      OR (
        (
          X.HOME_PHONE IS NULL AND NOT Y.HOME_PHONE IS NULL
        )
        OR (
          NOT X.HOME_PHONE IS NULL AND Y.HOME_PHONE IS NULL
        )
        OR (
          X.HOME_PHONE <> Y.HOME_PHONE
        )
      )
      OR (
        (
          X.LDAP_USER IS NULL AND NOT Y.LDAP_USER IS NULL
        )
        OR (
          NOT X.LDAP_USER IS NULL AND Y.LDAP_USER IS NULL
        )
        OR (
          X.LDAP_USER <> Y.LDAP_USER
        )
      )
      OR (
        (
          X.LIMITS_CCY IS NULL AND NOT Y.LIMITS_CCY IS NULL
        )
        OR (
          NOT X.LIMITS_CCY IS NULL AND Y.LIMITS_CCY IS NULL
        )
        OR (
          X.LIMITS_CCY <> Y.LIMITS_CCY
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
          X.MAX_AUTH_AMT IS NULL AND NOT Y.MAX_AUTH_AMT IS NULL
        )
        OR (
          NOT X.MAX_AUTH_AMT IS NULL AND Y.MAX_AUTH_AMT IS NULL
        )
        OR (
          X.MAX_AUTH_AMT <> Y.MAX_AUTH_AMT
        )
      )
      OR (
        (
          X.MAX_OVERRIDE_AMT IS NULL AND NOT Y.MAX_OVERRIDE_AMT IS NULL
        )
        OR (
          NOT X.MAX_OVERRIDE_AMT IS NULL AND Y.MAX_OVERRIDE_AMT IS NULL
        )
        OR (
          X.MAX_OVERRIDE_AMT <> Y.MAX_OVERRIDE_AMT
        )
      )
      OR (
        (
          X.MAX_TXN_AMT IS NULL AND NOT Y.MAX_TXN_AMT IS NULL
        )
        OR (
          NOT X.MAX_TXN_AMT IS NULL AND Y.MAX_TXN_AMT IS NULL
        )
        OR (
          X.MAX_TXN_AMT <> Y.MAX_TXN_AMT
        )
      )
      OR (
        (
          X.MFI_USER IS NULL AND NOT Y.MFI_USER IS NULL
        )
        OR (
          NOT X.MFI_USER IS NULL AND Y.MFI_USER IS NULL
        )
        OR (
          X.MFI_USER <> Y.MFI_USER
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
          X.MULTIBRANCH_ACCESS IS NULL AND NOT Y.MULTIBRANCH_ACCESS IS NULL
        )
        OR (
          NOT X.MULTIBRANCH_ACCESS IS NULL AND Y.MULTIBRANCH_ACCESS IS NULL
        )
        OR (
          X.MULTIBRANCH_ACCESS <> Y.MULTIBRANCH_ACCESS
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
          X.OTHER_RM_CUST_RESTRICT IS NULL AND NOT Y.OTHER_RM_CUST_RESTRICT IS NULL
        )
        OR (
          NOT X.OTHER_RM_CUST_RESTRICT IS NULL AND Y.OTHER_RM_CUST_RESTRICT IS NULL
        )
        OR (
          X.OTHER_RM_CUST_RESTRICT <> Y.OTHER_RM_CUST_RESTRICT
        )
      )
      OR (
        (
          X.PRODUCTS_ACCESS_ALLOWED IS NULL AND NOT Y.PRODUCTS_ACCESS_ALLOWED IS NULL
        )
        OR (
          NOT X.PRODUCTS_ACCESS_ALLOWED IS NULL AND Y.PRODUCTS_ACCESS_ALLOWED IS NULL
        )
        OR (
          X.PRODUCTS_ACCESS_ALLOWED <> Y.PRODUCTS_ACCESS_ALLOWED
        )
      )
      OR (
        (
          X.PRODUCTS_ALLOWED IS NULL AND NOT Y.PRODUCTS_ALLOWED IS NULL
        )
        OR (
          NOT X.PRODUCTS_ALLOWED IS NULL AND Y.PRODUCTS_ALLOWED IS NULL
        )
        OR (
          X.PRODUCTS_ALLOWED <> Y.PRODUCTS_ALLOWED
        )
      )
      OR (
        (
          X.PWD_CHANGED_ON IS NULL AND NOT Y.PWD_CHANGED_ON IS NULL
        )
        OR (
          NOT X.PWD_CHANGED_ON IS NULL AND Y.PWD_CHANGED_ON IS NULL
        )
        OR (
          X.PWD_CHANGED_ON <> Y.PWD_CHANGED_ON
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
          X.REFERENCE_NO IS NULL AND NOT Y.REFERENCE_NO IS NULL
        )
        OR (
          NOT X.REFERENCE_NO IS NULL AND Y.REFERENCE_NO IS NULL
        )
        OR (
          X.REFERENCE_NO <> Y.REFERENCE_NO
        )
      )
      OR (
        (
          X.SALT IS NULL AND NOT Y.SALT IS NULL
        )
        OR (
          NOT X.SALT IS NULL AND Y.SALT IS NULL
        )
        OR (
          X.SALT <> Y.SALT
        )
      )
      OR (
        (
          X.SCREEN_SAVER_TIMEOUT IS NULL AND NOT Y.SCREEN_SAVER_TIMEOUT IS NULL
        )
        OR (
          NOT X.SCREEN_SAVER_TIMEOUT IS NULL AND Y.SCREEN_SAVER_TIMEOUT IS NULL
        )
        OR (
          X.SCREEN_SAVER_TIMEOUT <> Y.SCREEN_SAVER_TIMEOUT
        )
      )
      OR (
        (
          X.SK_HOME_BRANCH IS NULL AND NOT Y.SK_HOME_BRANCH IS NULL
        )
        OR (
          NOT X.SK_HOME_BRANCH IS NULL AND Y.SK_HOME_BRANCH IS NULL
        )
        OR (
          X.SK_HOME_BRANCH <> Y.SK_HOME_BRANCH
        )
      )
      OR (
        (
          X.SK_USER_ID IS NULL AND NOT Y.SK_USER_ID IS NULL
        )
        OR (
          NOT X.SK_USER_ID IS NULL AND Y.SK_USER_ID IS NULL
        )
        OR (
          X.SK_USER_ID <> Y.SK_USER_ID
        )
      )
      OR (
        (
          X.SK_USER_EMAIL IS NULL AND NOT Y.SK_USER_EMAIL IS NULL
        )
        OR (
          NOT X.SK_USER_EMAIL IS NULL AND Y.SK_USER_EMAIL IS NULL
        )
        OR (
          X.SK_USER_EMAIL <> Y.SK_USER_EMAIL
        )
      )
      OR (
        (
          X.STAFF_AC_RESTR IS NULL AND NOT Y.STAFF_AC_RESTR IS NULL
        )
        OR (
          NOT X.STAFF_AC_RESTR IS NULL AND Y.STAFF_AC_RESTR IS NULL
        )
        OR (
          X.STAFF_AC_RESTR <> Y.STAFF_AC_RESTR
        )
      )
      OR (
        (
          X.START_DATE IS NULL AND NOT Y.START_DATE IS NULL
        )
        OR (
          NOT X.START_DATE IS NULL AND Y.START_DATE IS NULL
        )
        OR (
          X.START_DATE <> Y.START_DATE
        )
      )
      OR (
        (
          X.STARTUP_FUNCTION IS NULL AND NOT Y.STARTUP_FUNCTION IS NULL
        )
        OR (
          NOT X.STARTUP_FUNCTION IS NULL AND Y.STARTUP_FUNCTION IS NULL
        )
        OR (
          X.STARTUP_FUNCTION <> Y.STARTUP_FUNCTION
        )
      )
      OR (
        (
          X.STATUS_CHANGED_ON IS NULL AND NOT Y.STATUS_CHANGED_ON IS NULL
        )
        OR (
          NOT X.STATUS_CHANGED_ON IS NULL AND Y.STATUS_CHANGED_ON IS NULL
        )
        OR (
          X.STATUS_CHANGED_ON <> Y.STATUS_CHANGED_ON
        )
      )
      OR (
        (
          X.TAX_IDENTIFIER IS NULL AND NOT Y.TAX_IDENTIFIER IS NULL
        )
        OR (
          NOT X.TAX_IDENTIFIER IS NULL AND Y.TAX_IDENTIFIER IS NULL
        )
        OR (
          X.TAX_IDENTIFIER <> Y.TAX_IDENTIFIER
        )
      )
      OR (
        (
          X.TELEPHONE_NUMBER IS NULL AND NOT Y.TELEPHONE_NUMBER IS NULL
        )
        OR (
          NOT X.TELEPHONE_NUMBER IS NULL AND Y.TELEPHONE_NUMBER IS NULL
        )
        OR (
          X.TELEPHONE_NUMBER <> Y.TELEPHONE_NUMBER
        )
      )
      OR (
        (
          X.TILL_ALLOWED IS NULL AND NOT Y.TILL_ALLOWED IS NULL
        )
        OR (
          NOT X.TILL_ALLOWED IS NULL AND Y.TILL_ALLOWED IS NULL
        )
        OR (
          X.TILL_ALLOWED <> Y.TILL_ALLOWED
        )
      )
      OR (
        (
          X.TIME_LEVEL IS NULL AND NOT Y.TIME_LEVEL IS NULL
        )
        OR (
          NOT X.TIME_LEVEL IS NULL AND Y.TIME_LEVEL IS NULL
        )
        OR (
          X.TIME_LEVEL <> Y.TIME_LEVEL
        )
      )
      OR (
        (
          X.USER_CATEGORY IS NULL AND NOT Y.USER_CATEGORY IS NULL
        )
        OR (
          NOT X.USER_CATEGORY IS NULL AND Y.USER_CATEGORY IS NULL
        )
        OR (
          X.USER_CATEGORY <> Y.USER_CATEGORY
        )
      )
      OR (
        (
          X.USER_EMAIL IS NULL AND NOT Y.USER_EMAIL IS NULL
        )
        OR (
          NOT X.USER_EMAIL IS NULL AND Y.USER_EMAIL IS NULL
        )
        OR (
          X.USER_EMAIL <> Y.USER_EMAIL
        )
      )
      OR (
        (
          X.USER_FAX IS NULL AND NOT Y.USER_FAX IS NULL
        )
        OR (
          NOT X.USER_FAX IS NULL AND Y.USER_FAX IS NULL
        )
        OR (
          X.USER_FAX <> Y.USER_FAX
        )
      )
      OR (
        (
          X.USER_ID_BRN IS NULL AND NOT Y.USER_ID_BRN IS NULL
        )
        OR (
          NOT X.USER_ID_BRN IS NULL AND Y.USER_ID_BRN IS NULL
        )
        OR (
          X.USER_ID_BRN <> Y.USER_ID_BRN
        )
      )
      OR (
        (
          X.USER_LANGUAGE IS NULL AND NOT Y.USER_LANGUAGE IS NULL
        )
        OR (
          NOT X.USER_LANGUAGE IS NULL AND Y.USER_LANGUAGE IS NULL
        )
        OR (
          X.USER_LANGUAGE <> Y.USER_LANGUAGE
        )
      )
      OR (
        (
          X.USER_MANAGER IS NULL AND NOT Y.USER_MANAGER IS NULL
        )
        OR (
          NOT X.USER_MANAGER IS NULL AND Y.USER_MANAGER IS NULL
        )
        OR (
          X.USER_MANAGER <> Y.USER_MANAGER
        )
      )
      OR (
        (
          X.USER_MOBILE IS NULL AND NOT Y.USER_MOBILE IS NULL
        )
        OR (
          NOT X.USER_MOBILE IS NULL AND Y.USER_MOBILE IS NULL
        )
        OR (
          X.USER_MOBILE <> Y.USER_MOBILE
        )
      )
      OR (
        (
          X.USER_NAME IS NULL AND NOT Y.USER_NAME IS NULL
        )
        OR (
          NOT X.USER_NAME IS NULL AND Y.USER_NAME IS NULL
        )
        OR (
          X.USER_NAME <> Y.USER_NAME
        )
      )
      OR (
        (
          X.USER_PAGER IS NULL AND NOT Y.USER_PAGER IS NULL
        )
        OR (
          NOT X.USER_PAGER IS NULL AND Y.USER_PAGER IS NULL
        )
        OR (
          X.USER_PAGER <> Y.USER_PAGER
        )
      )
      OR (
        (
          X.USER_PASSWORD IS NULL AND NOT Y.USER_PASSWORD IS NULL
        )
        OR (
          NOT X.USER_PASSWORD IS NULL AND Y.USER_PASSWORD IS NULL
        )
        OR (
          X.USER_PASSWORD <> Y.USER_PASSWORD
        )
      )
      OR (
        (
          X.USER_PASSWORD_BRN IS NULL AND NOT Y.USER_PASSWORD_BRN IS NULL
        )
        OR (
          NOT X.USER_PASSWORD_BRN IS NULL AND Y.USER_PASSWORD_BRN IS NULL
        )
        OR (
          X.USER_PASSWORD_BRN <> Y.USER_PASSWORD_BRN
        )
      )
      OR (
        (
          X.USER_STATUS IS NULL AND NOT Y.USER_STATUS IS NULL
        )
        OR (
          NOT X.USER_STATUS IS NULL AND Y.USER_STATUS IS NULL
        )
        OR (
          X.USER_STATUS <> Y.USER_STATUS
        )
      )
      OR (
        (
          X.USER_TXN_LIMIT IS NULL AND NOT Y.USER_TXN_LIMIT IS NULL
        )
        OR (
          NOT X.USER_TXN_LIMIT IS NULL AND Y.USER_TXN_LIMIT IS NULL
        )
        OR (
          X.USER_TXN_LIMIT <> Y.USER_TXN_LIMIT
        )
      )
    )
    THEN 'UPD'
    WHEN X.USER_ID IS NULL
    THEN 'DEL' /* - IF MULTIPLE PKs are there then concatenate them with AND */
  END AS OPT_FLAG
FROM DEVT_STG_FLX.SMTB_USER AS X
FULL JOIN DEVT_STG_FLX.SMTB_USER_BACKUP AS Y
  ON X.USER_ID = Y.USER_ID /* - IF MULTIPLE PKs are there then concatenate them with AND */
WHERE
  NOT OPT_FLAG IS NULL