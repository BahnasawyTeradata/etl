CREATE OR REPLACE VIEW DEVV_STG_FLX.CSTM_FUNCTION_USERDEF_FIELDS_get_delta AS LOCKING ROW FOR ACCESS
SELECT
  COALESCE(X.FUNCTION_ID, Y.FUNCTION_ID) AS FUNCTION_ID,
  COALESCE(X.REC_KEY, Y.REC_KEY) AS REC_KEY,
  X.FIELD_VAL_1,
  X.FIELD_VAL_10,
  X.FIELD_VAL_100,
  X.FIELD_VAL_101,
  X.FIELD_VAL_102,
  X.FIELD_VAL_103,
  X.FIELD_VAL_104,
  X.FIELD_VAL_105,
  X.FIELD_VAL_106,
  X.FIELD_VAL_107,
  X.FIELD_VAL_108,
  X.FIELD_VAL_109,
  X.FIELD_VAL_11,
  X.FIELD_VAL_110,
  X.FIELD_VAL_111,
  X.FIELD_VAL_112,
  X.FIELD_VAL_113,
  X.FIELD_VAL_114,
  X.FIELD_VAL_115,
  X.FIELD_VAL_116,
  X.FIELD_VAL_117,
  X.FIELD_VAL_118,
  X.FIELD_VAL_119,
  X.FIELD_VAL_12,
  X.FIELD_VAL_120,
  X.FIELD_VAL_121,
  X.FIELD_VAL_122,
  X.FIELD_VAL_123,
  X.FIELD_VAL_124,
  X.FIELD_VAL_125,
  X.FIELD_VAL_126,
  X.FIELD_VAL_127,
  X.FIELD_VAL_128,
  X.FIELD_VAL_129,
  X.FIELD_VAL_13,
  X.FIELD_VAL_130,
  X.FIELD_VAL_131,
  X.FIELD_VAL_132,
  X.FIELD_VAL_133,
  X.FIELD_VAL_134,
  X.FIELD_VAL_135,
  X.FIELD_VAL_136,
  X.FIELD_VAL_137,
  X.FIELD_VAL_138,
  X.FIELD_VAL_139,
  X.FIELD_VAL_14,
  X.FIELD_VAL_140,
  X.FIELD_VAL_141,
  X.FIELD_VAL_142,
  X.FIELD_VAL_143,
  X.FIELD_VAL_144,
  X.FIELD_VAL_145,
  X.FIELD_VAL_146,
  X.FIELD_VAL_147,
  X.FIELD_VAL_148,
  X.FIELD_VAL_149,
  X.FIELD_VAL_15,
  X.FIELD_VAL_150,
  X.FIELD_VAL_151,
  X.FIELD_VAL_152,
  X.FIELD_VAL_153,
  X.FIELD_VAL_154,
  X.FIELD_VAL_155,
  X.FIELD_VAL_156,
  X.FIELD_VAL_157,
  X.FIELD_VAL_158,
  X.FIELD_VAL_159,
  X.FIELD_VAL_16,
  X.FIELD_VAL_160,
  X.FIELD_VAL_161,
  X.FIELD_VAL_162,
  X.FIELD_VAL_163,
  X.FIELD_VAL_164,
  X.FIELD_VAL_165,
  X.FIELD_VAL_166,
  X.FIELD_VAL_167,
  X.FIELD_VAL_168,
  X.FIELD_VAL_169,
  X.FIELD_VAL_17,
  X.FIELD_VAL_170,
  X.FIELD_VAL_171,
  X.FIELD_VAL_172,
  X.FIELD_VAL_173,
  X.FIELD_VAL_174,
  X.FIELD_VAL_175,
  X.FIELD_VAL_176,
  X.FIELD_VAL_177,
  X.FIELD_VAL_178,
  X.FIELD_VAL_179,
  X.FIELD_VAL_18,
  X.FIELD_VAL_180,
  X.FIELD_VAL_181,
  X.FIELD_VAL_182,
  X.FIELD_VAL_183,
  X.FIELD_VAL_184,
  X.FIELD_VAL_185,
  X.FIELD_VAL_186,
  X.FIELD_VAL_187,
  X.FIELD_VAL_188,
  X.FIELD_VAL_189,
  X.FIELD_VAL_19,
  X.FIELD_VAL_190,
  X.FIELD_VAL_191,
  X.FIELD_VAL_192,
  X.FIELD_VAL_193,
  X.FIELD_VAL_194,
  X.FIELD_VAL_195,
  X.FIELD_VAL_196,
  X.FIELD_VAL_197,
  X.FIELD_VAL_198,
  X.FIELD_VAL_199,
  X.FIELD_VAL_2,
  X.FIELD_VAL_20,
  X.FIELD_VAL_200,
  X.FIELD_VAL_21,
  X.FIELD_VAL_22,
  X.FIELD_VAL_23,
  X.FIELD_VAL_24,
  X.FIELD_VAL_25,
  X.FIELD_VAL_26,
  X.FIELD_VAL_27,
  X.FIELD_VAL_28,
  X.FIELD_VAL_29,
  X.FIELD_VAL_3,
  X.FIELD_VAL_30,
  X.FIELD_VAL_31,
  X.FIELD_VAL_32,
  X.FIELD_VAL_33,
  X.FIELD_VAL_34,
  X.FIELD_VAL_35,
  X.FIELD_VAL_36,
  X.FIELD_VAL_37,
  X.FIELD_VAL_38,
  X.FIELD_VAL_39,
  X.FIELD_VAL_4,
  X.FIELD_VAL_40,
  X.FIELD_VAL_41,
  X.FIELD_VAL_42,
  X.FIELD_VAL_43,
  X.FIELD_VAL_44,
  X.FIELD_VAL_45,
  X.FIELD_VAL_46,
  X.FIELD_VAL_47,
  X.FIELD_VAL_48,
  X.FIELD_VAL_49,
  X.FIELD_VAL_5,
  X.FIELD_VAL_50,
  X.FIELD_VAL_51,
  X.FIELD_VAL_52,
  X.FIELD_VAL_53,
  X.FIELD_VAL_54,
  X.FIELD_VAL_55,
  X.FIELD_VAL_56,
  X.FIELD_VAL_57,
  X.FIELD_VAL_58,
  X.FIELD_VAL_59,
  X.FIELD_VAL_6,
  X.FIELD_VAL_60,
  X.FIELD_VAL_61,
  X.FIELD_VAL_62,
  X.FIELD_VAL_63,
  X.FIELD_VAL_64,
  X.FIELD_VAL_65,
  X.FIELD_VAL_66,
  X.FIELD_VAL_67,
  X.FIELD_VAL_68,
  X.FIELD_VAL_69,
  X.FIELD_VAL_7,
  X.FIELD_VAL_70,
  X.FIELD_VAL_71,
  X.FIELD_VAL_72,
  X.FIELD_VAL_73,
  X.FIELD_VAL_74,
  X.FIELD_VAL_75,
  X.FIELD_VAL_76,
  X.FIELD_VAL_77,
  X.FIELD_VAL_78,
  X.FIELD_VAL_79,
  X.FIELD_VAL_8,
  X.FIELD_VAL_80,
  X.FIELD_VAL_81,
  X.FIELD_VAL_82,
  X.FIELD_VAL_83,
  X.FIELD_VAL_84,
  X.FIELD_VAL_85,
  X.FIELD_VAL_86,
  X.FIELD_VAL_87,
  X.FIELD_VAL_88,
  X.FIELD_VAL_89,
  X.FIELD_VAL_9,
  X.FIELD_VAL_90,
  X.FIELD_VAL_91,
  X.FIELD_VAL_92,
  X.FIELD_VAL_93,
  X.FIELD_VAL_94,
  X.FIELD_VAL_95,
  X.FIELD_VAL_96,
  X.FIELD_VAL_97,
  X.FIELD_VAL_98,
  X.FIELD_VAL_99,
  X.SK_REC_KEY_BRN,
  X.SK_FIELD_VAL_1,
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
    WHEN Y.FUNCTION_ID IS NULL AND Y.REC_KEY IS NULL
    THEN 'INS' /* - IF MULTIPLE PKs are there then concatenate them with AND */
    WHEN X.FUNCTION_ID = Y.FUNCTION_ID
    AND X.REC_KEY = Y.REC_KEY /* - IF MULTIPLE PKs are there then concatenate them with AND */
    AND (
      (
        (
          X.FIELD_VAL_1 IS NULL AND NOT Y.FIELD_VAL_1 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_1 IS NULL AND Y.FIELD_VAL_1 IS NULL
        )
        OR (
          X.FIELD_VAL_1 <> Y.FIELD_VAL_1
        )
      )
      OR (
        (
          X.FIELD_VAL_10 IS NULL AND NOT Y.FIELD_VAL_10 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_10 IS NULL AND Y.FIELD_VAL_10 IS NULL
        )
        OR (
          X.FIELD_VAL_10 <> Y.FIELD_VAL_10
        )
      )
      OR (
        (
          X.FIELD_VAL_100 IS NULL AND NOT Y.FIELD_VAL_100 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_100 IS NULL AND Y.FIELD_VAL_100 IS NULL
        )
        OR (
          X.FIELD_VAL_100 <> Y.FIELD_VAL_100
        )
      )
      OR (
        (
          X.FIELD_VAL_101 IS NULL AND NOT Y.FIELD_VAL_101 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_101 IS NULL AND Y.FIELD_VAL_101 IS NULL
        )
        OR (
          X.FIELD_VAL_101 <> Y.FIELD_VAL_101
        )
      )
      OR (
        (
          X.FIELD_VAL_102 IS NULL AND NOT Y.FIELD_VAL_102 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_102 IS NULL AND Y.FIELD_VAL_102 IS NULL
        )
        OR (
          X.FIELD_VAL_102 <> Y.FIELD_VAL_102
        )
      )
      OR (
        (
          X.FIELD_VAL_103 IS NULL AND NOT Y.FIELD_VAL_103 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_103 IS NULL AND Y.FIELD_VAL_103 IS NULL
        )
        OR (
          X.FIELD_VAL_103 <> Y.FIELD_VAL_103
        )
      )
      OR (
        (
          X.FIELD_VAL_104 IS NULL AND NOT Y.FIELD_VAL_104 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_104 IS NULL AND Y.FIELD_VAL_104 IS NULL
        )
        OR (
          X.FIELD_VAL_104 <> Y.FIELD_VAL_104
        )
      )
      OR (
        (
          X.FIELD_VAL_105 IS NULL AND NOT Y.FIELD_VAL_105 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_105 IS NULL AND Y.FIELD_VAL_105 IS NULL
        )
        OR (
          X.FIELD_VAL_105 <> Y.FIELD_VAL_105
        )
      )
      OR (
        (
          X.FIELD_VAL_106 IS NULL AND NOT Y.FIELD_VAL_106 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_106 IS NULL AND Y.FIELD_VAL_106 IS NULL
        )
        OR (
          X.FIELD_VAL_106 <> Y.FIELD_VAL_106
        )
      )
      OR (
        (
          X.FIELD_VAL_107 IS NULL AND NOT Y.FIELD_VAL_107 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_107 IS NULL AND Y.FIELD_VAL_107 IS NULL
        )
        OR (
          X.FIELD_VAL_107 <> Y.FIELD_VAL_107
        )
      )
      OR (
        (
          X.FIELD_VAL_108 IS NULL AND NOT Y.FIELD_VAL_108 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_108 IS NULL AND Y.FIELD_VAL_108 IS NULL
        )
        OR (
          X.FIELD_VAL_108 <> Y.FIELD_VAL_108
        )
      )
      OR (
        (
          X.FIELD_VAL_109 IS NULL AND NOT Y.FIELD_VAL_109 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_109 IS NULL AND Y.FIELD_VAL_109 IS NULL
        )
        OR (
          X.FIELD_VAL_109 <> Y.FIELD_VAL_109
        )
      )
      OR (
        (
          X.FIELD_VAL_11 IS NULL AND NOT Y.FIELD_VAL_11 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_11 IS NULL AND Y.FIELD_VAL_11 IS NULL
        )
        OR (
          X.FIELD_VAL_11 <> Y.FIELD_VAL_11
        )
      )
      OR (
        (
          X.FIELD_VAL_110 IS NULL AND NOT Y.FIELD_VAL_110 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_110 IS NULL AND Y.FIELD_VAL_110 IS NULL
        )
        OR (
          X.FIELD_VAL_110 <> Y.FIELD_VAL_110
        )
      )
      OR (
        (
          X.FIELD_VAL_111 IS NULL AND NOT Y.FIELD_VAL_111 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_111 IS NULL AND Y.FIELD_VAL_111 IS NULL
        )
        OR (
          X.FIELD_VAL_111 <> Y.FIELD_VAL_111
        )
      )
      OR (
        (
          X.FIELD_VAL_112 IS NULL AND NOT Y.FIELD_VAL_112 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_112 IS NULL AND Y.FIELD_VAL_112 IS NULL
        )
        OR (
          X.FIELD_VAL_112 <> Y.FIELD_VAL_112
        )
      )
      OR (
        (
          X.FIELD_VAL_113 IS NULL AND NOT Y.FIELD_VAL_113 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_113 IS NULL AND Y.FIELD_VAL_113 IS NULL
        )
        OR (
          X.FIELD_VAL_113 <> Y.FIELD_VAL_113
        )
      )
      OR (
        (
          X.FIELD_VAL_114 IS NULL AND NOT Y.FIELD_VAL_114 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_114 IS NULL AND Y.FIELD_VAL_114 IS NULL
        )
        OR (
          X.FIELD_VAL_114 <> Y.FIELD_VAL_114
        )
      )
      OR (
        (
          X.FIELD_VAL_115 IS NULL AND NOT Y.FIELD_VAL_115 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_115 IS NULL AND Y.FIELD_VAL_115 IS NULL
        )
        OR (
          X.FIELD_VAL_115 <> Y.FIELD_VAL_115
        )
      )
      OR (
        (
          X.FIELD_VAL_116 IS NULL AND NOT Y.FIELD_VAL_116 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_116 IS NULL AND Y.FIELD_VAL_116 IS NULL
        )
        OR (
          X.FIELD_VAL_116 <> Y.FIELD_VAL_116
        )
      )
      OR (
        (
          X.FIELD_VAL_117 IS NULL AND NOT Y.FIELD_VAL_117 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_117 IS NULL AND Y.FIELD_VAL_117 IS NULL
        )
        OR (
          X.FIELD_VAL_117 <> Y.FIELD_VAL_117
        )
      )
      OR (
        (
          X.FIELD_VAL_118 IS NULL AND NOT Y.FIELD_VAL_118 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_118 IS NULL AND Y.FIELD_VAL_118 IS NULL
        )
        OR (
          X.FIELD_VAL_118 <> Y.FIELD_VAL_118
        )
      )
      OR (
        (
          X.FIELD_VAL_119 IS NULL AND NOT Y.FIELD_VAL_119 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_119 IS NULL AND Y.FIELD_VAL_119 IS NULL
        )
        OR (
          X.FIELD_VAL_119 <> Y.FIELD_VAL_119
        )
      )
      OR (
        (
          X.FIELD_VAL_12 IS NULL AND NOT Y.FIELD_VAL_12 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_12 IS NULL AND Y.FIELD_VAL_12 IS NULL
        )
        OR (
          X.FIELD_VAL_12 <> Y.FIELD_VAL_12
        )
      )
      OR (
        (
          X.FIELD_VAL_120 IS NULL AND NOT Y.FIELD_VAL_120 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_120 IS NULL AND Y.FIELD_VAL_120 IS NULL
        )
        OR (
          X.FIELD_VAL_120 <> Y.FIELD_VAL_120
        )
      )
      OR (
        (
          X.FIELD_VAL_121 IS NULL AND NOT Y.FIELD_VAL_121 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_121 IS NULL AND Y.FIELD_VAL_121 IS NULL
        )
        OR (
          X.FIELD_VAL_121 <> Y.FIELD_VAL_121
        )
      )
      OR (
        (
          X.FIELD_VAL_122 IS NULL AND NOT Y.FIELD_VAL_122 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_122 IS NULL AND Y.FIELD_VAL_122 IS NULL
        )
        OR (
          X.FIELD_VAL_122 <> Y.FIELD_VAL_122
        )
      )
      OR (
        (
          X.FIELD_VAL_123 IS NULL AND NOT Y.FIELD_VAL_123 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_123 IS NULL AND Y.FIELD_VAL_123 IS NULL
        )
        OR (
          X.FIELD_VAL_123 <> Y.FIELD_VAL_123
        )
      )
      OR (
        (
          X.FIELD_VAL_124 IS NULL AND NOT Y.FIELD_VAL_124 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_124 IS NULL AND Y.FIELD_VAL_124 IS NULL
        )
        OR (
          X.FIELD_VAL_124 <> Y.FIELD_VAL_124
        )
      )
      OR (
        (
          X.FIELD_VAL_125 IS NULL AND NOT Y.FIELD_VAL_125 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_125 IS NULL AND Y.FIELD_VAL_125 IS NULL
        )
        OR (
          X.FIELD_VAL_125 <> Y.FIELD_VAL_125
        )
      )
      OR (
        (
          X.FIELD_VAL_126 IS NULL AND NOT Y.FIELD_VAL_126 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_126 IS NULL AND Y.FIELD_VAL_126 IS NULL
        )
        OR (
          X.FIELD_VAL_126 <> Y.FIELD_VAL_126
        )
      )
      OR (
        (
          X.FIELD_VAL_127 IS NULL AND NOT Y.FIELD_VAL_127 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_127 IS NULL AND Y.FIELD_VAL_127 IS NULL
        )
        OR (
          X.FIELD_VAL_127 <> Y.FIELD_VAL_127
        )
      )
      OR (
        (
          X.FIELD_VAL_128 IS NULL AND NOT Y.FIELD_VAL_128 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_128 IS NULL AND Y.FIELD_VAL_128 IS NULL
        )
        OR (
          X.FIELD_VAL_128 <> Y.FIELD_VAL_128
        )
      )
      OR (
        (
          X.FIELD_VAL_129 IS NULL AND NOT Y.FIELD_VAL_129 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_129 IS NULL AND Y.FIELD_VAL_129 IS NULL
        )
        OR (
          X.FIELD_VAL_129 <> Y.FIELD_VAL_129
        )
      )
      OR (
        (
          X.FIELD_VAL_13 IS NULL AND NOT Y.FIELD_VAL_13 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_13 IS NULL AND Y.FIELD_VAL_13 IS NULL
        )
        OR (
          X.FIELD_VAL_13 <> Y.FIELD_VAL_13
        )
      )
      OR (
        (
          X.FIELD_VAL_130 IS NULL AND NOT Y.FIELD_VAL_130 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_130 IS NULL AND Y.FIELD_VAL_130 IS NULL
        )
        OR (
          X.FIELD_VAL_130 <> Y.FIELD_VAL_130
        )
      )
      OR (
        (
          X.FIELD_VAL_131 IS NULL AND NOT Y.FIELD_VAL_131 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_131 IS NULL AND Y.FIELD_VAL_131 IS NULL
        )
        OR (
          X.FIELD_VAL_131 <> Y.FIELD_VAL_131
        )
      )
      OR (
        (
          X.FIELD_VAL_132 IS NULL AND NOT Y.FIELD_VAL_132 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_132 IS NULL AND Y.FIELD_VAL_132 IS NULL
        )
        OR (
          X.FIELD_VAL_132 <> Y.FIELD_VAL_132
        )
      )
      OR (
        (
          X.FIELD_VAL_133 IS NULL AND NOT Y.FIELD_VAL_133 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_133 IS NULL AND Y.FIELD_VAL_133 IS NULL
        )
        OR (
          X.FIELD_VAL_133 <> Y.FIELD_VAL_133
        )
      )
      OR (
        (
          X.FIELD_VAL_134 IS NULL AND NOT Y.FIELD_VAL_134 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_134 IS NULL AND Y.FIELD_VAL_134 IS NULL
        )
        OR (
          X.FIELD_VAL_134 <> Y.FIELD_VAL_134
        )
      )
      OR (
        (
          X.FIELD_VAL_135 IS NULL AND NOT Y.FIELD_VAL_135 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_135 IS NULL AND Y.FIELD_VAL_135 IS NULL
        )
        OR (
          X.FIELD_VAL_135 <> Y.FIELD_VAL_135
        )
      )
      OR (
        (
          X.FIELD_VAL_136 IS NULL AND NOT Y.FIELD_VAL_136 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_136 IS NULL AND Y.FIELD_VAL_136 IS NULL
        )
        OR (
          X.FIELD_VAL_136 <> Y.FIELD_VAL_136
        )
      )
      OR (
        (
          X.FIELD_VAL_137 IS NULL AND NOT Y.FIELD_VAL_137 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_137 IS NULL AND Y.FIELD_VAL_137 IS NULL
        )
        OR (
          X.FIELD_VAL_137 <> Y.FIELD_VAL_137
        )
      )
      OR (
        (
          X.FIELD_VAL_138 IS NULL AND NOT Y.FIELD_VAL_138 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_138 IS NULL AND Y.FIELD_VAL_138 IS NULL
        )
        OR (
          X.FIELD_VAL_138 <> Y.FIELD_VAL_138
        )
      )
      OR (
        (
          X.FIELD_VAL_139 IS NULL AND NOT Y.FIELD_VAL_139 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_139 IS NULL AND Y.FIELD_VAL_139 IS NULL
        )
        OR (
          X.FIELD_VAL_139 <> Y.FIELD_VAL_139
        )
      )
      OR (
        (
          X.FIELD_VAL_14 IS NULL AND NOT Y.FIELD_VAL_14 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_14 IS NULL AND Y.FIELD_VAL_14 IS NULL
        )
        OR (
          X.FIELD_VAL_14 <> Y.FIELD_VAL_14
        )
      )
      OR (
        (
          X.FIELD_VAL_140 IS NULL AND NOT Y.FIELD_VAL_140 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_140 IS NULL AND Y.FIELD_VAL_140 IS NULL
        )
        OR (
          X.FIELD_VAL_140 <> Y.FIELD_VAL_140
        )
      )
      OR (
        (
          X.FIELD_VAL_141 IS NULL AND NOT Y.FIELD_VAL_141 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_141 IS NULL AND Y.FIELD_VAL_141 IS NULL
        )
        OR (
          X.FIELD_VAL_141 <> Y.FIELD_VAL_141
        )
      )
      OR (
        (
          X.FIELD_VAL_142 IS NULL AND NOT Y.FIELD_VAL_142 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_142 IS NULL AND Y.FIELD_VAL_142 IS NULL
        )
        OR (
          X.FIELD_VAL_142 <> Y.FIELD_VAL_142
        )
      )
      OR (
        (
          X.FIELD_VAL_143 IS NULL AND NOT Y.FIELD_VAL_143 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_143 IS NULL AND Y.FIELD_VAL_143 IS NULL
        )
        OR (
          X.FIELD_VAL_143 <> Y.FIELD_VAL_143
        )
      )
      OR (
        (
          X.FIELD_VAL_144 IS NULL AND NOT Y.FIELD_VAL_144 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_144 IS NULL AND Y.FIELD_VAL_144 IS NULL
        )
        OR (
          X.FIELD_VAL_144 <> Y.FIELD_VAL_144
        )
      )
      OR (
        (
          X.FIELD_VAL_145 IS NULL AND NOT Y.FIELD_VAL_145 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_145 IS NULL AND Y.FIELD_VAL_145 IS NULL
        )
        OR (
          X.FIELD_VAL_145 <> Y.FIELD_VAL_145
        )
      )
      OR (
        (
          X.FIELD_VAL_146 IS NULL AND NOT Y.FIELD_VAL_146 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_146 IS NULL AND Y.FIELD_VAL_146 IS NULL
        )
        OR (
          X.FIELD_VAL_146 <> Y.FIELD_VAL_146
        )
      )
      OR (
        (
          X.FIELD_VAL_147 IS NULL AND NOT Y.FIELD_VAL_147 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_147 IS NULL AND Y.FIELD_VAL_147 IS NULL
        )
        OR (
          X.FIELD_VAL_147 <> Y.FIELD_VAL_147
        )
      )
      OR (
        (
          X.FIELD_VAL_148 IS NULL AND NOT Y.FIELD_VAL_148 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_148 IS NULL AND Y.FIELD_VAL_148 IS NULL
        )
        OR (
          X.FIELD_VAL_148 <> Y.FIELD_VAL_148
        )
      )
      OR (
        (
          X.FIELD_VAL_149 IS NULL AND NOT Y.FIELD_VAL_149 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_149 IS NULL AND Y.FIELD_VAL_149 IS NULL
        )
        OR (
          X.FIELD_VAL_149 <> Y.FIELD_VAL_149
        )
      )
      OR (
        (
          X.FIELD_VAL_15 IS NULL AND NOT Y.FIELD_VAL_15 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_15 IS NULL AND Y.FIELD_VAL_15 IS NULL
        )
        OR (
          X.FIELD_VAL_15 <> Y.FIELD_VAL_15
        )
      )
      OR (
        (
          X.FIELD_VAL_150 IS NULL AND NOT Y.FIELD_VAL_150 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_150 IS NULL AND Y.FIELD_VAL_150 IS NULL
        )
        OR (
          X.FIELD_VAL_150 <> Y.FIELD_VAL_150
        )
      )
      OR (
        (
          X.FIELD_VAL_151 IS NULL AND NOT Y.FIELD_VAL_151 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_151 IS NULL AND Y.FIELD_VAL_151 IS NULL
        )
        OR (
          X.FIELD_VAL_151 <> Y.FIELD_VAL_151
        )
      )
      OR (
        (
          X.FIELD_VAL_152 IS NULL AND NOT Y.FIELD_VAL_152 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_152 IS NULL AND Y.FIELD_VAL_152 IS NULL
        )
        OR (
          X.FIELD_VAL_152 <> Y.FIELD_VAL_152
        )
      )
      OR (
        (
          X.FIELD_VAL_153 IS NULL AND NOT Y.FIELD_VAL_153 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_153 IS NULL AND Y.FIELD_VAL_153 IS NULL
        )
        OR (
          X.FIELD_VAL_153 <> Y.FIELD_VAL_153
        )
      )
      OR (
        (
          X.FIELD_VAL_154 IS NULL AND NOT Y.FIELD_VAL_154 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_154 IS NULL AND Y.FIELD_VAL_154 IS NULL
        )
        OR (
          X.FIELD_VAL_154 <> Y.FIELD_VAL_154
        )
      )
      OR (
        (
          X.FIELD_VAL_155 IS NULL AND NOT Y.FIELD_VAL_155 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_155 IS NULL AND Y.FIELD_VAL_155 IS NULL
        )
        OR (
          X.FIELD_VAL_155 <> Y.FIELD_VAL_155
        )
      )
      OR (
        (
          X.FIELD_VAL_156 IS NULL AND NOT Y.FIELD_VAL_156 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_156 IS NULL AND Y.FIELD_VAL_156 IS NULL
        )
        OR (
          X.FIELD_VAL_156 <> Y.FIELD_VAL_156
        )
      )
      OR (
        (
          X.FIELD_VAL_157 IS NULL AND NOT Y.FIELD_VAL_157 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_157 IS NULL AND Y.FIELD_VAL_157 IS NULL
        )
        OR (
          X.FIELD_VAL_157 <> Y.FIELD_VAL_157
        )
      )
      OR (
        (
          X.FIELD_VAL_158 IS NULL AND NOT Y.FIELD_VAL_158 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_158 IS NULL AND Y.FIELD_VAL_158 IS NULL
        )
        OR (
          X.FIELD_VAL_158 <> Y.FIELD_VAL_158
        )
      )
      OR (
        (
          X.FIELD_VAL_159 IS NULL AND NOT Y.FIELD_VAL_159 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_159 IS NULL AND Y.FIELD_VAL_159 IS NULL
        )
        OR (
          X.FIELD_VAL_159 <> Y.FIELD_VAL_159
        )
      )
      OR (
        (
          X.FIELD_VAL_16 IS NULL AND NOT Y.FIELD_VAL_16 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_16 IS NULL AND Y.FIELD_VAL_16 IS NULL
        )
        OR (
          X.FIELD_VAL_16 <> Y.FIELD_VAL_16
        )
      )
      OR (
        (
          X.FIELD_VAL_160 IS NULL AND NOT Y.FIELD_VAL_160 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_160 IS NULL AND Y.FIELD_VAL_160 IS NULL
        )
        OR (
          X.FIELD_VAL_160 <> Y.FIELD_VAL_160
        )
      )
      OR (
        (
          X.FIELD_VAL_161 IS NULL AND NOT Y.FIELD_VAL_161 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_161 IS NULL AND Y.FIELD_VAL_161 IS NULL
        )
        OR (
          X.FIELD_VAL_161 <> Y.FIELD_VAL_161
        )
      )
      OR (
        (
          X.FIELD_VAL_162 IS NULL AND NOT Y.FIELD_VAL_162 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_162 IS NULL AND Y.FIELD_VAL_162 IS NULL
        )
        OR (
          X.FIELD_VAL_162 <> Y.FIELD_VAL_162
        )
      )
      OR (
        (
          X.FIELD_VAL_163 IS NULL AND NOT Y.FIELD_VAL_163 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_163 IS NULL AND Y.FIELD_VAL_163 IS NULL
        )
        OR (
          X.FIELD_VAL_163 <> Y.FIELD_VAL_163
        )
      )
      OR (
        (
          X.FIELD_VAL_164 IS NULL AND NOT Y.FIELD_VAL_164 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_164 IS NULL AND Y.FIELD_VAL_164 IS NULL
        )
        OR (
          X.FIELD_VAL_164 <> Y.FIELD_VAL_164
        )
      )
      OR (
        (
          X.FIELD_VAL_165 IS NULL AND NOT Y.FIELD_VAL_165 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_165 IS NULL AND Y.FIELD_VAL_165 IS NULL
        )
        OR (
          X.FIELD_VAL_165 <> Y.FIELD_VAL_165
        )
      )
      OR (
        (
          X.FIELD_VAL_166 IS NULL AND NOT Y.FIELD_VAL_166 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_166 IS NULL AND Y.FIELD_VAL_166 IS NULL
        )
        OR (
          X.FIELD_VAL_166 <> Y.FIELD_VAL_166
        )
      )
      OR (
        (
          X.FIELD_VAL_167 IS NULL AND NOT Y.FIELD_VAL_167 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_167 IS NULL AND Y.FIELD_VAL_167 IS NULL
        )
        OR (
          X.FIELD_VAL_167 <> Y.FIELD_VAL_167
        )
      )
      OR (
        (
          X.FIELD_VAL_168 IS NULL AND NOT Y.FIELD_VAL_168 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_168 IS NULL AND Y.FIELD_VAL_168 IS NULL
        )
        OR (
          X.FIELD_VAL_168 <> Y.FIELD_VAL_168
        )
      )
      OR (
        (
          X.FIELD_VAL_169 IS NULL AND NOT Y.FIELD_VAL_169 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_169 IS NULL AND Y.FIELD_VAL_169 IS NULL
        )
        OR (
          X.FIELD_VAL_169 <> Y.FIELD_VAL_169
        )
      )
      OR (
        (
          X.FIELD_VAL_17 IS NULL AND NOT Y.FIELD_VAL_17 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_17 IS NULL AND Y.FIELD_VAL_17 IS NULL
        )
        OR (
          X.FIELD_VAL_17 <> Y.FIELD_VAL_17
        )
      )
      OR (
        (
          X.FIELD_VAL_170 IS NULL AND NOT Y.FIELD_VAL_170 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_170 IS NULL AND Y.FIELD_VAL_170 IS NULL
        )
        OR (
          X.FIELD_VAL_170 <> Y.FIELD_VAL_170
        )
      )
      OR (
        (
          X.FIELD_VAL_171 IS NULL AND NOT Y.FIELD_VAL_171 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_171 IS NULL AND Y.FIELD_VAL_171 IS NULL
        )
        OR (
          X.FIELD_VAL_171 <> Y.FIELD_VAL_171
        )
      )
      OR (
        (
          X.FIELD_VAL_172 IS NULL AND NOT Y.FIELD_VAL_172 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_172 IS NULL AND Y.FIELD_VAL_172 IS NULL
        )
        OR (
          X.FIELD_VAL_172 <> Y.FIELD_VAL_172
        )
      )
      OR (
        (
          X.FIELD_VAL_173 IS NULL AND NOT Y.FIELD_VAL_173 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_173 IS NULL AND Y.FIELD_VAL_173 IS NULL
        )
        OR (
          X.FIELD_VAL_173 <> Y.FIELD_VAL_173
        )
      )
      OR (
        (
          X.FIELD_VAL_174 IS NULL AND NOT Y.FIELD_VAL_174 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_174 IS NULL AND Y.FIELD_VAL_174 IS NULL
        )
        OR (
          X.FIELD_VAL_174 <> Y.FIELD_VAL_174
        )
      )
      OR (
        (
          X.FIELD_VAL_175 IS NULL AND NOT Y.FIELD_VAL_175 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_175 IS NULL AND Y.FIELD_VAL_175 IS NULL
        )
        OR (
          X.FIELD_VAL_175 <> Y.FIELD_VAL_175
        )
      )
      OR (
        (
          X.FIELD_VAL_176 IS NULL AND NOT Y.FIELD_VAL_176 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_176 IS NULL AND Y.FIELD_VAL_176 IS NULL
        )
        OR (
          X.FIELD_VAL_176 <> Y.FIELD_VAL_176
        )
      )
      OR (
        (
          X.FIELD_VAL_177 IS NULL AND NOT Y.FIELD_VAL_177 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_177 IS NULL AND Y.FIELD_VAL_177 IS NULL
        )
        OR (
          X.FIELD_VAL_177 <> Y.FIELD_VAL_177
        )
      )
      OR (
        (
          X.FIELD_VAL_178 IS NULL AND NOT Y.FIELD_VAL_178 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_178 IS NULL AND Y.FIELD_VAL_178 IS NULL
        )
        OR (
          X.FIELD_VAL_178 <> Y.FIELD_VAL_178
        )
      )
      OR (
        (
          X.FIELD_VAL_179 IS NULL AND NOT Y.FIELD_VAL_179 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_179 IS NULL AND Y.FIELD_VAL_179 IS NULL
        )
        OR (
          X.FIELD_VAL_179 <> Y.FIELD_VAL_179
        )
      )
      OR (
        (
          X.FIELD_VAL_18 IS NULL AND NOT Y.FIELD_VAL_18 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_18 IS NULL AND Y.FIELD_VAL_18 IS NULL
        )
        OR (
          X.FIELD_VAL_18 <> Y.FIELD_VAL_18
        )
      )
      OR (
        (
          X.FIELD_VAL_180 IS NULL AND NOT Y.FIELD_VAL_180 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_180 IS NULL AND Y.FIELD_VAL_180 IS NULL
        )
        OR (
          X.FIELD_VAL_180 <> Y.FIELD_VAL_180
        )
      )
      OR (
        (
          X.FIELD_VAL_181 IS NULL AND NOT Y.FIELD_VAL_181 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_181 IS NULL AND Y.FIELD_VAL_181 IS NULL
        )
        OR (
          X.FIELD_VAL_181 <> Y.FIELD_VAL_181
        )
      )
      OR (
        (
          X.FIELD_VAL_182 IS NULL AND NOT Y.FIELD_VAL_182 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_182 IS NULL AND Y.FIELD_VAL_182 IS NULL
        )
        OR (
          X.FIELD_VAL_182 <> Y.FIELD_VAL_182
        )
      )
      OR (
        (
          X.FIELD_VAL_183 IS NULL AND NOT Y.FIELD_VAL_183 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_183 IS NULL AND Y.FIELD_VAL_183 IS NULL
        )
        OR (
          X.FIELD_VAL_183 <> Y.FIELD_VAL_183
        )
      )
      OR (
        (
          X.FIELD_VAL_184 IS NULL AND NOT Y.FIELD_VAL_184 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_184 IS NULL AND Y.FIELD_VAL_184 IS NULL
        )
        OR (
          X.FIELD_VAL_184 <> Y.FIELD_VAL_184
        )
      )
      OR (
        (
          X.FIELD_VAL_185 IS NULL AND NOT Y.FIELD_VAL_185 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_185 IS NULL AND Y.FIELD_VAL_185 IS NULL
        )
        OR (
          X.FIELD_VAL_185 <> Y.FIELD_VAL_185
        )
      )
      OR (
        (
          X.FIELD_VAL_186 IS NULL AND NOT Y.FIELD_VAL_186 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_186 IS NULL AND Y.FIELD_VAL_186 IS NULL
        )
        OR (
          X.FIELD_VAL_186 <> Y.FIELD_VAL_186
        )
      )
      OR (
        (
          X.FIELD_VAL_187 IS NULL AND NOT Y.FIELD_VAL_187 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_187 IS NULL AND Y.FIELD_VAL_187 IS NULL
        )
        OR (
          X.FIELD_VAL_187 <> Y.FIELD_VAL_187
        )
      )
      OR (
        (
          X.FIELD_VAL_188 IS NULL AND NOT Y.FIELD_VAL_188 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_188 IS NULL AND Y.FIELD_VAL_188 IS NULL
        )
        OR (
          X.FIELD_VAL_188 <> Y.FIELD_VAL_188
        )
      )
      OR (
        (
          X.FIELD_VAL_189 IS NULL AND NOT Y.FIELD_VAL_189 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_189 IS NULL AND Y.FIELD_VAL_189 IS NULL
        )
        OR (
          X.FIELD_VAL_189 <> Y.FIELD_VAL_189
        )
      )
      OR (
        (
          X.FIELD_VAL_19 IS NULL AND NOT Y.FIELD_VAL_19 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_19 IS NULL AND Y.FIELD_VAL_19 IS NULL
        )
        OR (
          X.FIELD_VAL_19 <> Y.FIELD_VAL_19
        )
      )
      OR (
        (
          X.FIELD_VAL_190 IS NULL AND NOT Y.FIELD_VAL_190 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_190 IS NULL AND Y.FIELD_VAL_190 IS NULL
        )
        OR (
          X.FIELD_VAL_190 <> Y.FIELD_VAL_190
        )
      )
      OR (
        (
          X.FIELD_VAL_191 IS NULL AND NOT Y.FIELD_VAL_191 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_191 IS NULL AND Y.FIELD_VAL_191 IS NULL
        )
        OR (
          X.FIELD_VAL_191 <> Y.FIELD_VAL_191
        )
      )
      OR (
        (
          X.FIELD_VAL_192 IS NULL AND NOT Y.FIELD_VAL_192 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_192 IS NULL AND Y.FIELD_VAL_192 IS NULL
        )
        OR (
          X.FIELD_VAL_192 <> Y.FIELD_VAL_192
        )
      )
      OR (
        (
          X.FIELD_VAL_193 IS NULL AND NOT Y.FIELD_VAL_193 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_193 IS NULL AND Y.FIELD_VAL_193 IS NULL
        )
        OR (
          X.FIELD_VAL_193 <> Y.FIELD_VAL_193
        )
      )
      OR (
        (
          X.FIELD_VAL_194 IS NULL AND NOT Y.FIELD_VAL_194 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_194 IS NULL AND Y.FIELD_VAL_194 IS NULL
        )
        OR (
          X.FIELD_VAL_194 <> Y.FIELD_VAL_194
        )
      )
      OR (
        (
          X.FIELD_VAL_195 IS NULL AND NOT Y.FIELD_VAL_195 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_195 IS NULL AND Y.FIELD_VAL_195 IS NULL
        )
        OR (
          X.FIELD_VAL_195 <> Y.FIELD_VAL_195
        )
      )
      OR (
        (
          X.FIELD_VAL_196 IS NULL AND NOT Y.FIELD_VAL_196 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_196 IS NULL AND Y.FIELD_VAL_196 IS NULL
        )
        OR (
          X.FIELD_VAL_196 <> Y.FIELD_VAL_196
        )
      )
      OR (
        (
          X.FIELD_VAL_197 IS NULL AND NOT Y.FIELD_VAL_197 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_197 IS NULL AND Y.FIELD_VAL_197 IS NULL
        )
        OR (
          X.FIELD_VAL_197 <> Y.FIELD_VAL_197
        )
      )
      OR (
        (
          X.FIELD_VAL_198 IS NULL AND NOT Y.FIELD_VAL_198 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_198 IS NULL AND Y.FIELD_VAL_198 IS NULL
        )
        OR (
          X.FIELD_VAL_198 <> Y.FIELD_VAL_198
        )
      )
      OR (
        (
          X.FIELD_VAL_199 IS NULL AND NOT Y.FIELD_VAL_199 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_199 IS NULL AND Y.FIELD_VAL_199 IS NULL
        )
        OR (
          X.FIELD_VAL_199 <> Y.FIELD_VAL_199
        )
      )
      OR (
        (
          X.FIELD_VAL_2 IS NULL AND NOT Y.FIELD_VAL_2 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_2 IS NULL AND Y.FIELD_VAL_2 IS NULL
        )
        OR (
          X.FIELD_VAL_2 <> Y.FIELD_VAL_2
        )
      )
      OR (
        (
          X.FIELD_VAL_20 IS NULL AND NOT Y.FIELD_VAL_20 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_20 IS NULL AND Y.FIELD_VAL_20 IS NULL
        )
        OR (
          X.FIELD_VAL_20 <> Y.FIELD_VAL_20
        )
      )
      OR (
        (
          X.FIELD_VAL_200 IS NULL AND NOT Y.FIELD_VAL_200 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_200 IS NULL AND Y.FIELD_VAL_200 IS NULL
        )
        OR (
          X.FIELD_VAL_200 <> Y.FIELD_VAL_200
        )
      )
      OR (
        (
          X.FIELD_VAL_21 IS NULL AND NOT Y.FIELD_VAL_21 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_21 IS NULL AND Y.FIELD_VAL_21 IS NULL
        )
        OR (
          X.FIELD_VAL_21 <> Y.FIELD_VAL_21
        )
      )
      OR (
        (
          X.FIELD_VAL_22 IS NULL AND NOT Y.FIELD_VAL_22 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_22 IS NULL AND Y.FIELD_VAL_22 IS NULL
        )
        OR (
          X.FIELD_VAL_22 <> Y.FIELD_VAL_22
        )
      )
      OR (
        (
          X.FIELD_VAL_23 IS NULL AND NOT Y.FIELD_VAL_23 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_23 IS NULL AND Y.FIELD_VAL_23 IS NULL
        )
        OR (
          X.FIELD_VAL_23 <> Y.FIELD_VAL_23
        )
      )
      OR (
        (
          X.FIELD_VAL_24 IS NULL AND NOT Y.FIELD_VAL_24 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_24 IS NULL AND Y.FIELD_VAL_24 IS NULL
        )
        OR (
          X.FIELD_VAL_24 <> Y.FIELD_VAL_24
        )
      )
      OR (
        (
          X.FIELD_VAL_25 IS NULL AND NOT Y.FIELD_VAL_25 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_25 IS NULL AND Y.FIELD_VAL_25 IS NULL
        )
        OR (
          X.FIELD_VAL_25 <> Y.FIELD_VAL_25
        )
      )
      OR (
        (
          X.FIELD_VAL_26 IS NULL AND NOT Y.FIELD_VAL_26 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_26 IS NULL AND Y.FIELD_VAL_26 IS NULL
        )
        OR (
          X.FIELD_VAL_26 <> Y.FIELD_VAL_26
        )
      )
      OR (
        (
          X.FIELD_VAL_27 IS NULL AND NOT Y.FIELD_VAL_27 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_27 IS NULL AND Y.FIELD_VAL_27 IS NULL
        )
        OR (
          X.FIELD_VAL_27 <> Y.FIELD_VAL_27
        )
      )
      OR (
        (
          X.FIELD_VAL_28 IS NULL AND NOT Y.FIELD_VAL_28 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_28 IS NULL AND Y.FIELD_VAL_28 IS NULL
        )
        OR (
          X.FIELD_VAL_28 <> Y.FIELD_VAL_28
        )
      )
      OR (
        (
          X.FIELD_VAL_29 IS NULL AND NOT Y.FIELD_VAL_29 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_29 IS NULL AND Y.FIELD_VAL_29 IS NULL
        )
        OR (
          X.FIELD_VAL_29 <> Y.FIELD_VAL_29
        )
      )
      OR (
        (
          X.FIELD_VAL_3 IS NULL AND NOT Y.FIELD_VAL_3 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_3 IS NULL AND Y.FIELD_VAL_3 IS NULL
        )
        OR (
          X.FIELD_VAL_3 <> Y.FIELD_VAL_3
        )
      )
      OR (
        (
          X.FIELD_VAL_30 IS NULL AND NOT Y.FIELD_VAL_30 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_30 IS NULL AND Y.FIELD_VAL_30 IS NULL
        )
        OR (
          X.FIELD_VAL_30 <> Y.FIELD_VAL_30
        )
      )
      OR (
        (
          X.FIELD_VAL_31 IS NULL AND NOT Y.FIELD_VAL_31 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_31 IS NULL AND Y.FIELD_VAL_31 IS NULL
        )
        OR (
          X.FIELD_VAL_31 <> Y.FIELD_VAL_31
        )
      )
      OR (
        (
          X.FIELD_VAL_32 IS NULL AND NOT Y.FIELD_VAL_32 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_32 IS NULL AND Y.FIELD_VAL_32 IS NULL
        )
        OR (
          X.FIELD_VAL_32 <> Y.FIELD_VAL_32
        )
      )
      OR (
        (
          X.FIELD_VAL_33 IS NULL AND NOT Y.FIELD_VAL_33 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_33 IS NULL AND Y.FIELD_VAL_33 IS NULL
        )
        OR (
          X.FIELD_VAL_33 <> Y.FIELD_VAL_33
        )
      )
      OR (
        (
          X.FIELD_VAL_34 IS NULL AND NOT Y.FIELD_VAL_34 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_34 IS NULL AND Y.FIELD_VAL_34 IS NULL
        )
        OR (
          X.FIELD_VAL_34 <> Y.FIELD_VAL_34
        )
      )
      OR (
        (
          X.FIELD_VAL_35 IS NULL AND NOT Y.FIELD_VAL_35 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_35 IS NULL AND Y.FIELD_VAL_35 IS NULL
        )
        OR (
          X.FIELD_VAL_35 <> Y.FIELD_VAL_35
        )
      )
      OR (
        (
          X.FIELD_VAL_36 IS NULL AND NOT Y.FIELD_VAL_36 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_36 IS NULL AND Y.FIELD_VAL_36 IS NULL
        )
        OR (
          X.FIELD_VAL_36 <> Y.FIELD_VAL_36
        )
      )
      OR (
        (
          X.FIELD_VAL_37 IS NULL AND NOT Y.FIELD_VAL_37 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_37 IS NULL AND Y.FIELD_VAL_37 IS NULL
        )
        OR (
          X.FIELD_VAL_37 <> Y.FIELD_VAL_37
        )
      )
      OR (
        (
          X.FIELD_VAL_38 IS NULL AND NOT Y.FIELD_VAL_38 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_38 IS NULL AND Y.FIELD_VAL_38 IS NULL
        )
        OR (
          X.FIELD_VAL_38 <> Y.FIELD_VAL_38
        )
      )
      OR (
        (
          X.FIELD_VAL_39 IS NULL AND NOT Y.FIELD_VAL_39 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_39 IS NULL AND Y.FIELD_VAL_39 IS NULL
        )
        OR (
          X.FIELD_VAL_39 <> Y.FIELD_VAL_39
        )
      )
      OR (
        (
          X.FIELD_VAL_4 IS NULL AND NOT Y.FIELD_VAL_4 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_4 IS NULL AND Y.FIELD_VAL_4 IS NULL
        )
        OR (
          X.FIELD_VAL_4 <> Y.FIELD_VAL_4
        )
      )
      OR (
        (
          X.FIELD_VAL_40 IS NULL AND NOT Y.FIELD_VAL_40 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_40 IS NULL AND Y.FIELD_VAL_40 IS NULL
        )
        OR (
          X.FIELD_VAL_40 <> Y.FIELD_VAL_40
        )
      )
      OR (
        (
          X.FIELD_VAL_41 IS NULL AND NOT Y.FIELD_VAL_41 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_41 IS NULL AND Y.FIELD_VAL_41 IS NULL
        )
        OR (
          X.FIELD_VAL_41 <> Y.FIELD_VAL_41
        )
      )
      OR (
        (
          X.FIELD_VAL_42 IS NULL AND NOT Y.FIELD_VAL_42 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_42 IS NULL AND Y.FIELD_VAL_42 IS NULL
        )
        OR (
          X.FIELD_VAL_42 <> Y.FIELD_VAL_42
        )
      )
      OR (
        (
          X.FIELD_VAL_43 IS NULL AND NOT Y.FIELD_VAL_43 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_43 IS NULL AND Y.FIELD_VAL_43 IS NULL
        )
        OR (
          X.FIELD_VAL_43 <> Y.FIELD_VAL_43
        )
      )
      OR (
        (
          X.FIELD_VAL_44 IS NULL AND NOT Y.FIELD_VAL_44 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_44 IS NULL AND Y.FIELD_VAL_44 IS NULL
        )
        OR (
          X.FIELD_VAL_44 <> Y.FIELD_VAL_44
        )
      )
      OR (
        (
          X.FIELD_VAL_45 IS NULL AND NOT Y.FIELD_VAL_45 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_45 IS NULL AND Y.FIELD_VAL_45 IS NULL
        )
        OR (
          X.FIELD_VAL_45 <> Y.FIELD_VAL_45
        )
      )
      OR (
        (
          X.FIELD_VAL_46 IS NULL AND NOT Y.FIELD_VAL_46 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_46 IS NULL AND Y.FIELD_VAL_46 IS NULL
        )
        OR (
          X.FIELD_VAL_46 <> Y.FIELD_VAL_46
        )
      )
      OR (
        (
          X.FIELD_VAL_47 IS NULL AND NOT Y.FIELD_VAL_47 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_47 IS NULL AND Y.FIELD_VAL_47 IS NULL
        )
        OR (
          X.FIELD_VAL_47 <> Y.FIELD_VAL_47
        )
      )
      OR (
        (
          X.FIELD_VAL_48 IS NULL AND NOT Y.FIELD_VAL_48 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_48 IS NULL AND Y.FIELD_VAL_48 IS NULL
        )
        OR (
          X.FIELD_VAL_48 <> Y.FIELD_VAL_48
        )
      )
      OR (
        (
          X.FIELD_VAL_49 IS NULL AND NOT Y.FIELD_VAL_49 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_49 IS NULL AND Y.FIELD_VAL_49 IS NULL
        )
        OR (
          X.FIELD_VAL_49 <> Y.FIELD_VAL_49
        )
      )
      OR (
        (
          X.FIELD_VAL_5 IS NULL AND NOT Y.FIELD_VAL_5 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_5 IS NULL AND Y.FIELD_VAL_5 IS NULL
        )
        OR (
          X.FIELD_VAL_5 <> Y.FIELD_VAL_5
        )
      )
      OR (
        (
          X.FIELD_VAL_50 IS NULL AND NOT Y.FIELD_VAL_50 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_50 IS NULL AND Y.FIELD_VAL_50 IS NULL
        )
        OR (
          X.FIELD_VAL_50 <> Y.FIELD_VAL_50
        )
      )
      OR (
        (
          X.FIELD_VAL_51 IS NULL AND NOT Y.FIELD_VAL_51 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_51 IS NULL AND Y.FIELD_VAL_51 IS NULL
        )
        OR (
          X.FIELD_VAL_51 <> Y.FIELD_VAL_51
        )
      )
      OR (
        (
          X.FIELD_VAL_52 IS NULL AND NOT Y.FIELD_VAL_52 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_52 IS NULL AND Y.FIELD_VAL_52 IS NULL
        )
        OR (
          X.FIELD_VAL_52 <> Y.FIELD_VAL_52
        )
      )
      OR (
        (
          X.FIELD_VAL_53 IS NULL AND NOT Y.FIELD_VAL_53 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_53 IS NULL AND Y.FIELD_VAL_53 IS NULL
        )
        OR (
          X.FIELD_VAL_53 <> Y.FIELD_VAL_53
        )
      )
      OR (
        (
          X.FIELD_VAL_54 IS NULL AND NOT Y.FIELD_VAL_54 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_54 IS NULL AND Y.FIELD_VAL_54 IS NULL
        )
        OR (
          X.FIELD_VAL_54 <> Y.FIELD_VAL_54
        )
      )
      OR (
        (
          X.FIELD_VAL_55 IS NULL AND NOT Y.FIELD_VAL_55 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_55 IS NULL AND Y.FIELD_VAL_55 IS NULL
        )
        OR (
          X.FIELD_VAL_55 <> Y.FIELD_VAL_55
        )
      )
      OR (
        (
          X.FIELD_VAL_56 IS NULL AND NOT Y.FIELD_VAL_56 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_56 IS NULL AND Y.FIELD_VAL_56 IS NULL
        )
        OR (
          X.FIELD_VAL_56 <> Y.FIELD_VAL_56
        )
      )
      OR (
        (
          X.FIELD_VAL_57 IS NULL AND NOT Y.FIELD_VAL_57 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_57 IS NULL AND Y.FIELD_VAL_57 IS NULL
        )
        OR (
          X.FIELD_VAL_57 <> Y.FIELD_VAL_57
        )
      )
      OR (
        (
          X.FIELD_VAL_58 IS NULL AND NOT Y.FIELD_VAL_58 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_58 IS NULL AND Y.FIELD_VAL_58 IS NULL
        )
        OR (
          X.FIELD_VAL_58 <> Y.FIELD_VAL_58
        )
      )
      OR (
        (
          X.FIELD_VAL_59 IS NULL AND NOT Y.FIELD_VAL_59 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_59 IS NULL AND Y.FIELD_VAL_59 IS NULL
        )
        OR (
          X.FIELD_VAL_59 <> Y.FIELD_VAL_59
        )
      )
      OR (
        (
          X.FIELD_VAL_6 IS NULL AND NOT Y.FIELD_VAL_6 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_6 IS NULL AND Y.FIELD_VAL_6 IS NULL
        )
        OR (
          X.FIELD_VAL_6 <> Y.FIELD_VAL_6
        )
      )
      OR (
        (
          X.FIELD_VAL_60 IS NULL AND NOT Y.FIELD_VAL_60 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_60 IS NULL AND Y.FIELD_VAL_60 IS NULL
        )
        OR (
          X.FIELD_VAL_60 <> Y.FIELD_VAL_60
        )
      )
      OR (
        (
          X.FIELD_VAL_61 IS NULL AND NOT Y.FIELD_VAL_61 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_61 IS NULL AND Y.FIELD_VAL_61 IS NULL
        )
        OR (
          X.FIELD_VAL_61 <> Y.FIELD_VAL_61
        )
      )
      OR (
        (
          X.FIELD_VAL_62 IS NULL AND NOT Y.FIELD_VAL_62 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_62 IS NULL AND Y.FIELD_VAL_62 IS NULL
        )
        OR (
          X.FIELD_VAL_62 <> Y.FIELD_VAL_62
        )
      )
      OR (
        (
          X.FIELD_VAL_63 IS NULL AND NOT Y.FIELD_VAL_63 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_63 IS NULL AND Y.FIELD_VAL_63 IS NULL
        )
        OR (
          X.FIELD_VAL_63 <> Y.FIELD_VAL_63
        )
      )
      OR (
        (
          X.FIELD_VAL_64 IS NULL AND NOT Y.FIELD_VAL_64 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_64 IS NULL AND Y.FIELD_VAL_64 IS NULL
        )
        OR (
          X.FIELD_VAL_64 <> Y.FIELD_VAL_64
        )
      )
      OR (
        (
          X.FIELD_VAL_65 IS NULL AND NOT Y.FIELD_VAL_65 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_65 IS NULL AND Y.FIELD_VAL_65 IS NULL
        )
        OR (
          X.FIELD_VAL_65 <> Y.FIELD_VAL_65
        )
      )
      OR (
        (
          X.FIELD_VAL_66 IS NULL AND NOT Y.FIELD_VAL_66 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_66 IS NULL AND Y.FIELD_VAL_66 IS NULL
        )
        OR (
          X.FIELD_VAL_66 <> Y.FIELD_VAL_66
        )
      )
      OR (
        (
          X.FIELD_VAL_67 IS NULL AND NOT Y.FIELD_VAL_67 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_67 IS NULL AND Y.FIELD_VAL_67 IS NULL
        )
        OR (
          X.FIELD_VAL_67 <> Y.FIELD_VAL_67
        )
      )
      OR (
        (
          X.FIELD_VAL_68 IS NULL AND NOT Y.FIELD_VAL_68 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_68 IS NULL AND Y.FIELD_VAL_68 IS NULL
        )
        OR (
          X.FIELD_VAL_68 <> Y.FIELD_VAL_68
        )
      )
      OR (
        (
          X.FIELD_VAL_69 IS NULL AND NOT Y.FIELD_VAL_69 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_69 IS NULL AND Y.FIELD_VAL_69 IS NULL
        )
        OR (
          X.FIELD_VAL_69 <> Y.FIELD_VAL_69
        )
      )
      OR (
        (
          X.FIELD_VAL_7 IS NULL AND NOT Y.FIELD_VAL_7 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_7 IS NULL AND Y.FIELD_VAL_7 IS NULL
        )
        OR (
          X.FIELD_VAL_7 <> Y.FIELD_VAL_7
        )
      )
      OR (
        (
          X.FIELD_VAL_70 IS NULL AND NOT Y.FIELD_VAL_70 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_70 IS NULL AND Y.FIELD_VAL_70 IS NULL
        )
        OR (
          X.FIELD_VAL_70 <> Y.FIELD_VAL_70
        )
      )
      OR (
        (
          X.FIELD_VAL_71 IS NULL AND NOT Y.FIELD_VAL_71 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_71 IS NULL AND Y.FIELD_VAL_71 IS NULL
        )
        OR (
          X.FIELD_VAL_71 <> Y.FIELD_VAL_71
        )
      )
      OR (
        (
          X.FIELD_VAL_72 IS NULL AND NOT Y.FIELD_VAL_72 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_72 IS NULL AND Y.FIELD_VAL_72 IS NULL
        )
        OR (
          X.FIELD_VAL_72 <> Y.FIELD_VAL_72
        )
      )
      OR (
        (
          X.FIELD_VAL_73 IS NULL AND NOT Y.FIELD_VAL_73 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_73 IS NULL AND Y.FIELD_VAL_73 IS NULL
        )
        OR (
          X.FIELD_VAL_73 <> Y.FIELD_VAL_73
        )
      )
      OR (
        (
          X.FIELD_VAL_74 IS NULL AND NOT Y.FIELD_VAL_74 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_74 IS NULL AND Y.FIELD_VAL_74 IS NULL
        )
        OR (
          X.FIELD_VAL_74 <> Y.FIELD_VAL_74
        )
      )
      OR (
        (
          X.FIELD_VAL_75 IS NULL AND NOT Y.FIELD_VAL_75 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_75 IS NULL AND Y.FIELD_VAL_75 IS NULL
        )
        OR (
          X.FIELD_VAL_75 <> Y.FIELD_VAL_75
        )
      )
      OR (
        (
          X.FIELD_VAL_76 IS NULL AND NOT Y.FIELD_VAL_76 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_76 IS NULL AND Y.FIELD_VAL_76 IS NULL
        )
        OR (
          X.FIELD_VAL_76 <> Y.FIELD_VAL_76
        )
      )
      OR (
        (
          X.FIELD_VAL_77 IS NULL AND NOT Y.FIELD_VAL_77 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_77 IS NULL AND Y.FIELD_VAL_77 IS NULL
        )
        OR (
          X.FIELD_VAL_77 <> Y.FIELD_VAL_77
        )
      )
      OR (
        (
          X.FIELD_VAL_78 IS NULL AND NOT Y.FIELD_VAL_78 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_78 IS NULL AND Y.FIELD_VAL_78 IS NULL
        )
        OR (
          X.FIELD_VAL_78 <> Y.FIELD_VAL_78
        )
      )
      OR (
        (
          X.FIELD_VAL_79 IS NULL AND NOT Y.FIELD_VAL_79 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_79 IS NULL AND Y.FIELD_VAL_79 IS NULL
        )
        OR (
          X.FIELD_VAL_79 <> Y.FIELD_VAL_79
        )
      )
      OR (
        (
          X.FIELD_VAL_8 IS NULL AND NOT Y.FIELD_VAL_8 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_8 IS NULL AND Y.FIELD_VAL_8 IS NULL
        )
        OR (
          X.FIELD_VAL_8 <> Y.FIELD_VAL_8
        )
      )
      OR (
        (
          X.FIELD_VAL_80 IS NULL AND NOT Y.FIELD_VAL_80 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_80 IS NULL AND Y.FIELD_VAL_80 IS NULL
        )
        OR (
          X.FIELD_VAL_80 <> Y.FIELD_VAL_80
        )
      )
      OR (
        (
          X.FIELD_VAL_81 IS NULL AND NOT Y.FIELD_VAL_81 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_81 IS NULL AND Y.FIELD_VAL_81 IS NULL
        )
        OR (
          X.FIELD_VAL_81 <> Y.FIELD_VAL_81
        )
      )
      OR (
        (
          X.FIELD_VAL_82 IS NULL AND NOT Y.FIELD_VAL_82 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_82 IS NULL AND Y.FIELD_VAL_82 IS NULL
        )
        OR (
          X.FIELD_VAL_82 <> Y.FIELD_VAL_82
        )
      )
      OR (
        (
          X.FIELD_VAL_83 IS NULL AND NOT Y.FIELD_VAL_83 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_83 IS NULL AND Y.FIELD_VAL_83 IS NULL
        )
        OR (
          X.FIELD_VAL_83 <> Y.FIELD_VAL_83
        )
      )
      OR (
        (
          X.FIELD_VAL_84 IS NULL AND NOT Y.FIELD_VAL_84 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_84 IS NULL AND Y.FIELD_VAL_84 IS NULL
        )
        OR (
          X.FIELD_VAL_84 <> Y.FIELD_VAL_84
        )
      )
      OR (
        (
          X.FIELD_VAL_85 IS NULL AND NOT Y.FIELD_VAL_85 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_85 IS NULL AND Y.FIELD_VAL_85 IS NULL
        )
        OR (
          X.FIELD_VAL_85 <> Y.FIELD_VAL_85
        )
      )
      OR (
        (
          X.FIELD_VAL_86 IS NULL AND NOT Y.FIELD_VAL_86 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_86 IS NULL AND Y.FIELD_VAL_86 IS NULL
        )
        OR (
          X.FIELD_VAL_86 <> Y.FIELD_VAL_86
        )
      )
      OR (
        (
          X.FIELD_VAL_87 IS NULL AND NOT Y.FIELD_VAL_87 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_87 IS NULL AND Y.FIELD_VAL_87 IS NULL
        )
        OR (
          X.FIELD_VAL_87 <> Y.FIELD_VAL_87
        )
      )
      OR (
        (
          X.FIELD_VAL_88 IS NULL AND NOT Y.FIELD_VAL_88 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_88 IS NULL AND Y.FIELD_VAL_88 IS NULL
        )
        OR (
          X.FIELD_VAL_88 <> Y.FIELD_VAL_88
        )
      )
      OR (
        (
          X.FIELD_VAL_89 IS NULL AND NOT Y.FIELD_VAL_89 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_89 IS NULL AND Y.FIELD_VAL_89 IS NULL
        )
        OR (
          X.FIELD_VAL_89 <> Y.FIELD_VAL_89
        )
      )
      OR (
        (
          X.FIELD_VAL_9 IS NULL AND NOT Y.FIELD_VAL_9 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_9 IS NULL AND Y.FIELD_VAL_9 IS NULL
        )
        OR (
          X.FIELD_VAL_9 <> Y.FIELD_VAL_9
        )
      )
      OR (
        (
          X.FIELD_VAL_90 IS NULL AND NOT Y.FIELD_VAL_90 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_90 IS NULL AND Y.FIELD_VAL_90 IS NULL
        )
        OR (
          X.FIELD_VAL_90 <> Y.FIELD_VAL_90
        )
      )
      OR (
        (
          X.FIELD_VAL_91 IS NULL AND NOT Y.FIELD_VAL_91 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_91 IS NULL AND Y.FIELD_VAL_91 IS NULL
        )
        OR (
          X.FIELD_VAL_91 <> Y.FIELD_VAL_91
        )
      )
      OR (
        (
          X.FIELD_VAL_92 IS NULL AND NOT Y.FIELD_VAL_92 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_92 IS NULL AND Y.FIELD_VAL_92 IS NULL
        )
        OR (
          X.FIELD_VAL_92 <> Y.FIELD_VAL_92
        )
      )
      OR (
        (
          X.FIELD_VAL_93 IS NULL AND NOT Y.FIELD_VAL_93 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_93 IS NULL AND Y.FIELD_VAL_93 IS NULL
        )
        OR (
          X.FIELD_VAL_93 <> Y.FIELD_VAL_93
        )
      )
      OR (
        (
          X.FIELD_VAL_94 IS NULL AND NOT Y.FIELD_VAL_94 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_94 IS NULL AND Y.FIELD_VAL_94 IS NULL
        )
        OR (
          X.FIELD_VAL_94 <> Y.FIELD_VAL_94
        )
      )
      OR (
        (
          X.FIELD_VAL_95 IS NULL AND NOT Y.FIELD_VAL_95 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_95 IS NULL AND Y.FIELD_VAL_95 IS NULL
        )
        OR (
          X.FIELD_VAL_95 <> Y.FIELD_VAL_95
        )
      )
      OR (
        (
          X.FIELD_VAL_96 IS NULL AND NOT Y.FIELD_VAL_96 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_96 IS NULL AND Y.FIELD_VAL_96 IS NULL
        )
        OR (
          X.FIELD_VAL_96 <> Y.FIELD_VAL_96
        )
      )
      OR (
        (
          X.FIELD_VAL_97 IS NULL AND NOT Y.FIELD_VAL_97 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_97 IS NULL AND Y.FIELD_VAL_97 IS NULL
        )
        OR (
          X.FIELD_VAL_97 <> Y.FIELD_VAL_97
        )
      )
      OR (
        (
          X.FIELD_VAL_98 IS NULL AND NOT Y.FIELD_VAL_98 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_98 IS NULL AND Y.FIELD_VAL_98 IS NULL
        )
        OR (
          X.FIELD_VAL_98 <> Y.FIELD_VAL_98
        )
      )
      OR (
        (
          X.FIELD_VAL_99 IS NULL AND NOT Y.FIELD_VAL_99 IS NULL
        )
        OR (
          NOT X.FIELD_VAL_99 IS NULL AND Y.FIELD_VAL_99 IS NULL
        )
        OR (
          X.FIELD_VAL_99 <> Y.FIELD_VAL_99
        )
      )
      OR (
        (
          X.SK_REC_KEY_BRN IS NULL AND NOT Y.SK_REC_KEY_BRN IS NULL
        )
        OR (
          NOT X.SK_REC_KEY_BRN IS NULL AND Y.SK_REC_KEY_BRN IS NULL
        )
        OR (
          X.SK_REC_KEY_BRN <> Y.SK_REC_KEY_BRN
        )
      )
      OR (
        (
          X.SK_FIELD_VAL_1 IS NULL AND NOT Y.SK_FIELD_VAL_1 IS NULL
        )
        OR (
          NOT X.SK_FIELD_VAL_1 IS NULL AND Y.SK_FIELD_VAL_1 IS NULL
        )
        OR (
          X.SK_FIELD_VAL_1 <> Y.SK_FIELD_VAL_1
        )
      )
    )
    THEN 'UPD'
    WHEN X.FUNCTION_ID IS NULL AND X.REC_KEY IS NULL
    THEN 'DEL' /* - IF MULTIPLE PKs are there then concatenate them with AND */
  END AS OPT_FLAG
FROM DEVT_STG_FLX.CSTM_FUNCTION_USERDEF_FIELDS AS X
FULL JOIN DEVT_STG_FLX.CSTM_FUNCTION_USERDEF_FIELDS_BACKUP AS Y
  ON X.FUNCTION_ID = Y.FUNCTION_ID
  AND X.REC_KEY = Y.REC_KEY /* - IF MULTIPLE PKs are there then concatenate them with AND */
WHERE
  NOT OPT_FLAG IS NULL