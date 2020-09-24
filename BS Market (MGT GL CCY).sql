/*--+===========================================================================================================================================+
                                                Balance Sheet Market Current day Vs Prev day
+===========================================================================================================================================+ 

Replace Following Parameters
----------
Year_month :  202004
Current Day:  Balance_27
Prev Day:     Balance_26

--+===========================================================================================================================================+*/ 

  SELECT /*+ PARALLEL (T1 10) (T2 10) (T3 10) */ 
            MGT_LINE,MGT_LINE_DESCRIPTION,            
            BS_GL,( SELECT GL_DESCRIPTION FROM GL_CODES WHERE COUNTRY='VN' AND VISION_GL= BS_GL ) GL_DESCRIPTION,
            T1.CURRENCY, 
            T88.FUND ,T1.VISION_SBU,
            NVL(SUM(CASE WHEN T2.BAL_TYPE = 51
            THEN (
            CASE WHEN  T1.YEAR_MONTH ='202004' THEN 
            CASE WHEN T1.YEAR_MONTH ='202004' AND T88.FUND IS NOT NULL    AND MGT_LINE <>'G031360' THEN 0
            WHEN T1.YEAR_MONTH ='202004' AND T1.VISION_SBU = 'FI'  AND MGT_LINE <>'G031360' THEN 0
            WHEN T1.YEAR_MONTH ='202004' AND T1.VISION_SBU = 'FI'  AND MGT_LINE = 'G031360' THEN 0
            WHEN T1.YEAR_MONTH ='202004' AND T1.VISION_SBU != 'FI' AND MGT_LINE = 'G031360' THEN BALANCE_27 
            ELSE  BALANCE_27    END ELSE 0 END ) 
            ELSE 0 END),0) M1_TODAY,  
            NVL(SUM(CASE WHEN T2.BAL_TYPE = 51
            THEN ( 
            CASE WHEN  T1.YEAR_MONTH ='202004' THEN 
            CASE WHEN T1.YEAR_MONTH ='202004' AND T88.FUND IS NOT NULL    AND MGT_LINE <>'G031360' THEN 0
            WHEN T1.YEAR_MONTH ='202004' AND T1.VISION_SBU = 'FI'  AND MGT_LINE <>'G031360' THEN 0
            WHEN T1.YEAR_MONTH ='202004' AND T1.VISION_SBU = 'FI'  AND MGT_LINE = 'G031360' THEN 0
            WHEN T1.YEAR_MONTH ='202004' AND T1.VISION_SBU != 'FI' AND MGT_LINE = 'G031360' THEN BALANCE_26  
            ELSE  BALANCE_26     END ELSE 0 END ) 
            ELSE 0 END),0) M1_YES,
            NVL(SUM(CASE WHEN T2.BAL_TYPE = 51
            THEN ( CASE WHEN T1.YEAR_MONTH ='202004' THEN BALANCE_27    ELSE 0 END )ELSE 0 END),0) -
            NVL(SUM(CASE WHEN T2.BAL_TYPE = 51
            THEN (
            CASE WHEN  T1.YEAR_MONTH ='202004' THEN 
            CASE WHEN T1.YEAR_MONTH ='202004' AND T88.FUND IS NOT NULL    AND MGT_LINE <>'G031360' THEN 0
            WHEN T1.YEAR_MONTH ='202004' AND T1.VISION_SBU = 'FI'  AND MGT_LINE <>'G031360' THEN 0
            WHEN T1.YEAR_MONTH ='202004' AND T1.VISION_SBU = 'FI'  AND MGT_LINE = 'G031360' THEN 0
            WHEN T1.YEAR_MONTH ='202004' AND T1.VISION_SBU != 'FI' AND MGT_LINE = 'G031360' THEN BALANCE_27 
            ELSE  BALANCE_27    END ELSE 0 END ) 
            ELSE 0 END),0) M2_TODAY,  
            NVL(SUM(CASE WHEN T2.BAL_TYPE = 51
            THEN ( CASE WHEN T1.YEAR_MONTH ='202004'  THEN BALANCE_26    ELSE 0 END ) 
            ELSE 0 END),0) -
            NVL(SUM(CASE WHEN T2.BAL_TYPE = 51
            THEN (
            CASE WHEN  T1.YEAR_MONTH ='202004' THEN 
            CASE WHEN T1.YEAR_MONTH ='202004' AND T88.FUND IS NOT NULL    AND MGT_LINE <>'G031360' THEN 0
            WHEN T1.YEAR_MONTH ='202004' AND T1.VISION_SBU = 'FI'  AND MGT_LINE <>'G031360' THEN 0
            WHEN T1.YEAR_MONTH ='202004' AND T1.VISION_SBU = 'FI'  AND MGT_LINE = 'G031360' THEN 0
            WHEN T1.YEAR_MONTH ='202004' AND T1.VISION_SBU != 'FI' AND MGT_LINE = 'G031360' THEN BALANCE_26 
            ELSE  BALANCE_26    END ELSE 0 END ) 
            ELSE 0 END),0) M2_YES,RATE_27,RATE_26
            FROM
            VW_FIN_DLY_HEADERS T1,
            FIN_DLY_BALANCES T2,
            FIN_DLY_MAPPINGS T3,
            CURRENCY_RATES_DAILY T4,  
            (SELECT DISTINCT COUNTRY,LE_BOOK,CONTRACT_ID,1 FUND
            FROM PWT_FI_FUND_VW W  
            )  T88,
            MGT_EXPANDED T8 
            WHERE
            T1.COUNTRY = T2.COUNTRY
            AND T1.LE_BOOK = T2.LE_BOOK
            AND T1.YEAR_MONTH = T2.YEAR_MONTH
            AND T1.SEQUENCE_FD= T2.SEQUENCE_FD
            AND T2.COUNTRY = T3.COUNTRY
            AND T2.LE_BOOK = T3.LE_BOOK
            AND T2.YEAR_MONTH = T3.YEAR_MONTH
            AND T2.SEQUENCE_FD= T3.SEQUENCE_FD
            AND T2.DR_CR_BAL_IND= T3.DR_CR_BAL_IND  
            AND T1.YEAR_MONTH IN( '202004')
            AND T1.RECORD_TYPE != 9999
            AND T2.BAL_TYPE  IN (51)
            AND T4.COUNTRY    = T1.COUNTRY  
            AND T4.LE_BOOK    = T1.LE_BOOK  
            AND T4.YEAR_MONTH = T1.YEAR_MONTH
            AND T4.CURRENCY   = T1.CURRENCY 
            AND T4.CATEGORY='MRATE'
            AND  MGT_LINE BETWEEN 'G031100' AND 'G036830'
            AND T8.MGT_LINE_LEVEL = 3
            AND T8.SOURCE_BAL_TYPE = 1
            AND T8.SOURCE_TYPE = 0
            AND T3.MRL_LINE = T8.SOURCE_MRL_LINE  
            AND T1.COUNTRY = T88.COUNTRY(+)
            AND T1.LE_BOOK  = T88.LE_BOOK(+)
            AND T1.CONTRACT_ID = T88.CONTRACT_ID(+)  
            GROUP BY BS_GL,T88.FUND ,T1.VISION_SBU,RATE_27,RATE_26 ,MGT_LINE,MGT_LINE_DESCRIPTION,T1.CURRENCY