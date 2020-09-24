
SELECT /*+PARALLEL(3)*/ * FROM VW_FI WHERE YEAR_MONTH=202008


DROP TABLE TEMP_RS_344534_7

SELECT /*+PARALLEL(3)*/ * FROM TEMP_RS_344534_7 
 
CREATE TABLE TEMP_RS_344534_7
PARALLEL
NOLOGGING
AS
     SELECT /*+ INDEX(T3)*/
           LEVEL1D,
            LEVEL1,
            LEVEL2D,
            LEVEL2,
            LEVEL3D,
            LEVEL3,
            LEVEL4D,
            LEVEL4,
            LEVEL5D,
            LEVEL5,
            T1.VISION_SBU,
            T1.SUBTO,
            SUBSTR (T1.VISION_OUC, 5, 4) VISION_OUC,
            BS_GL,
            SUM (BALANCE_06) BALANCE_LCY1
       FROM VW_FI T1,
            FIN_DLY_BALANCES T2,
            FIN_DLY_MAPPINGS T3,
            CURRENCY_RATES_DAILY T4,
            CURRENCY_RATES_DAILY T5,
            (SELECT /*+PARALLEL(3)*/ * FROM PWT_FI_BS_SEQ WHERE ROWNUM<2) Q
      WHERE     T1.COUNTRY = 'VN'
            AND T1.LE_BOOK = '01'
            AND T1.COUNTRY = T2.COUNTRY
            AND T1.LE_BOOK = T2.LE_BOOK
            AND T1.YEAR_MONTH = T2.YEAR_MONTH
            AND T1.SEQUENCE_FD = T2.SEQUENCE_FD
            AND T2.COUNTRY = T3.COUNTRY
            AND T2.LE_BOOK = T3.LE_BOOK
            AND T2.YEAR_MONTH = T3.YEAR_MONTH
            AND T2.SEQUENCE_FD = T3.SEQUENCE_FD
            AND T2.DR_CR_BAL_IND = T3.DR_CR_BAL_IND
            AND T1.YEAR_MONTH IN ('202008')
            AND T1.RECORD_TYPE != 9999
            AND T2.BAL_TYPE = 51
            AND (  (SUBSTR (VISION_OUC, 5, 4) = '0011' AND  T1.VISION_SBU = 'FI')) 
            AND T1.COUNTRY = T4.COUNTRY
            AND T1.LE_BOOK = T4.LE_BOOK
            AND T1.YEAR_MONTH = T4.YEAR_MONTH
            AND T1.CURRENCY = T4.CURRENCY
            AND T4.CATEGORY = 'MRATE'
            AND T1.COUNTRY = T5.COUNTRY
            AND T1.LE_BOOK = T5.LE_BOOK
            AND T1.YEAR_MONTH = T5.YEAR_MONTH
            AND T5.CATEGORY = 'MRATE'
            AND T5.CURRENCY = 'USD'
            AND substr(BS_GL,1,1) in (1,2)
            AND BS_GL NOT IN (SELECT /*+PARALLEL(3)*/ GL FROM PWT_FI_BS_SEQ ) 
   GROUP BY LEVEL1D,
            LEVEL1,
            LEVEL2D,
            LEVEL2,
            LEVEL3D,
            LEVEL3,
            LEVEL4D,
            LEVEL4,
            LEVEL5D,
            LEVEL5,
            T1.VISION_SBU,
            T1.SUBTO,
            SUBSTR (VISION_OUC, 5, 4),
            BS_GL 