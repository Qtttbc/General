
     CREATE TABLE TEMP_RS_FI_LOAN  LOGGING PARALLEL AS   
      SELECT /*+ USE_MERGE(t1,t2,t3,t4,t5) */  
          NVL (T2.VISION_SBU, T1.VISION_SBU) VISION_SBU,   
          NVL (T2.VISION_OUC, T1.VISION_OUC) VISION_OUC,   
          NVL (T2.CURRENCY, T1.CURRENCY) CURRENCY,         
          T4.PERIOD,                                       
          SUM (                                            
                (T3.BALANCE_01 * T5.RATE_01 * T4.DAY_01)   
              + (T3.BALANCE_02 * T5.RATE_02 * T4.DAY_02)   
              + (T3.BALANCE_03 * T5.RATE_03 * T4.DAY_03)   
              + (T3.BALANCE_04 * T5.RATE_04 * T4.DAY_04)   
              + (T3.BALANCE_05 * T5.RATE_05 * T4.DAY_05)   
              + (T3.BALANCE_06 * T5.RATE_06 * T4.DAY_06)   
              + (T3.BALANCE_07 * T5.RATE_07 * T4.DAY_07)   
              + (T3.BALANCE_08 * T5.RATE_08 * T4.DAY_08)   
              + (T3.BALANCE_09 * T5.RATE_09 * T4.DAY_09)   
              + (T3.BALANCE_10 * T5.RATE_10 * T4.DAY_10)   
              + (T3.BALANCE_11 * T5.RATE_11 * T4.DAY_11)   
              + (T3.BALANCE_12 * T5.RATE_12 * T4.DAY_12)   
              + (T3.BALANCE_13 * T5.RATE_13 * T4.DAY_13)   
              + (T3.BALANCE_14 * T5.RATE_14 * T4.DAY_14)   
              + (T3.BALANCE_15 * T5.RATE_15 * T4.DAY_15)   
              + (T3.BALANCE_16 * T5.RATE_16 * T4.DAY_16)   
              + (T3.BALANCE_17 * T5.RATE_17 * T4.DAY_17)   
              + (T3.BALANCE_18 * T5.RATE_18 * T4.DAY_18)   
              + (T3.BALANCE_19 * T5.RATE_19 * T4.DAY_19)   
              + (T3.BALANCE_20 * T5.RATE_20 * T4.DAY_20)   
              + (T3.BALANCE_21 * T5.RATE_21 * T4.DAY_21)   
              + (T3.BALANCE_22 * T5.RATE_22 * T4.DAY_22)   
              + (T3.BALANCE_23 * T5.RATE_23 * T4.DAY_23)   
              + (T3.BALANCE_24 * T5.RATE_24 * T4.DAY_24)   
              + (T3.BALANCE_25 * T5.RATE_25 * T4.DAY_25)   
              + (T3.BALANCE_26 * T5.RATE_26 * T4.DAY_26)   
              + (T3.BALANCE_27 * T5.RATE_27 * T4.DAY_27)   
              + (T3.BALANCE_28 * T5.RATE_28 * T4.DAY_28)   
              + (T3.BALANCE_29 * T5.RATE_29 * T4.DAY_29)   
              + (T3.BALANCE_30 * T5.RATE_30 * T4.DAY_30)   
              + (T3.BALANCE_31 * T5.RATE_31 * T4.DAY_31))  
              BALANCE  
      FROM FIN_DLY_HEADERS T1  
          LEFT OUTER JOIN (FIN_DLY_AUDIT T2  
              JOIN    
                 (  SELECT COUNTRY,       
                           LE_BOOK,       
                           YEAR_MONTH,    
                           SEQUENCE_FD,   
                           MAX (ADJ_SEQUENCE) ADJ_SEQUENCE   
                      FROM FIN_DLY_AUDIT         
                     WHERE     COUNTRY = 'VN'    
                           AND LE_BOOK = '01'    
                           AND YEAR_MONTH BETWEEN 202001 AND 202007  
                           AND SEQUENCE_FD > 0   
                           AND RECORD_TYPE = 2   
                  GROUP BY COUNTRY,                  
                           LE_BOOK,                  
                           YEAR_MONTH,               
                           SEQUENCE_FD) T3           
              ON (    T2.COUNTRY = T3.COUNTRY        
                  AND T2.LE_BOOK = T3.LE_BOOK        
                  AND T2.YEAR_MONTH = T3.YEAR_MONTH  
                  AND T2.SEQUENCE_FD = T3.SEQUENCE_FD   
                  AND T2.ADJ_SEQUENCE = T3.ADJ_SEQUENCE))  
             ON (    T1.COUNTRY = T2.COUNTRY            
                 AND T1.LE_BOOK = T2.LE_BOOK            
                 AND T1.YEAR_MONTH = T2.YEAR_MONTH      
                 AND T1.SEQUENCE_FD = T2.SEQUENCE_FD    
                                     )                         /* T1.RECORD_TYPE != 9999 AND  Added for MSB Bank */
          INNER JOIN FIN_DLY_BALANCES T3                
             ON (    T1.COUNTRY = T3.COUNTRY            
                 AND T1.LE_BOOK = T3.LE_BOOK            
                 AND T1.YEAR_MONTH = T3.YEAR_MONTH      
                 AND T1.SEQUENCE_FD = T3.SEQUENCE_FD    
                 AND T3.BAL_TYPE = 51                   
                 AND T1.RECORD_TYPE =2 )                
          INNER JOIN CURRENCY_RATES_DAILY T5            
             ON (    T1.COUNTRY = T5.COUNTRY            
                 AND T1.LE_BOOK = T5.LE_BOOK            
                 AND T1.YEAR_MONTH = T5.YEAR_MONTH      
                 AND T1.CURRENCY = T5.CURRENCY          
                 AND T5.CATEGORY = 'MRATE' )            
          INNER JOIN (SELECT H1.*,                      
                    H2.YEAR || LPAD (H2.MONTH, 2, '0') YEAR_MONTH,     
                       H2.YEAR                  
                    || LPAD (H2.MONTH, 2, '0')  
                    || LPAD (H1.DAYS, 2, '0')   
                       PERIOD                   
                 FROM DENORM_DAYS H1, PERIOD_CONTROLS H2    
                WHERE DAYS <= NO_OF_DAYS) T4   
             ON (T1.YEAR_MONTH = T4.YEAR_MONTH)  
      WHERE T1.COUNTRY = 'VN' AND T1.LE_BOOK = '01' AND T1.YEAR_MONTH BETWEEN 202001 AND 202007     
          AND T1.SOURCE_ID IN (7)  
          AND NVL (T2.VISION_SBU, T1.VISION_SBU)='B'
--          AND T1.BS_GL IN (%s)    
        GROUP BY NVL (T2.VISION_SBU, T1.VISION_SBU),    
          NVL (T2.VISION_OUC, T1.VISION_OUC),   
          NVL (T2.CURRENCY, T1.CURRENCY) ,  
          T4.PERIOD   
        UNION ALL  
        SELECT NVL (T2.VISION_SBU, T1.VISION_SBU) VISION_SBU,    
          NVL (T2.VISION_OUC, T1.VISION_OUC) VISION_OUC,  
          NVL (T2.CURRENCY, T1.CURRENCY) CURRENCY,  
          T4.PERIOD,                                      
          SUM (                                           
               (T3.BALANCE_01 * T5.RATE_01 * T4.DAY_01)   
             + (T3.BALANCE_02 * T5.RATE_02 * T4.DAY_02)   
             + (T3.BALANCE_03 * T5.RATE_03 * T4.DAY_03)   
             + (T3.BALANCE_04 * T5.RATE_04 * T4.DAY_04)   
             + (T3.BALANCE_05 * T5.RATE_05 * T4.DAY_05)   
             + (T3.BALANCE_06 * T5.RATE_06 * T4.DAY_06)   
             + (T3.BALANCE_07 * T5.RATE_07 * T4.DAY_07)   
             + (T3.BALANCE_08 * T5.RATE_08 * T4.DAY_08)   
             + (T3.BALANCE_09 * T5.RATE_09 * T4.DAY_09)   
             + (T3.BALANCE_10 * T5.RATE_10 * T4.DAY_10)   
             + (T3.BALANCE_11 * T5.RATE_11 * T4.DAY_11)   
             + (T3.BALANCE_12 * T5.RATE_12 * T4.DAY_12)   
             + (T3.BALANCE_13 * T5.RATE_13 * T4.DAY_13)   
             + (T3.BALANCE_14 * T5.RATE_14 * T4.DAY_14)   
             + (T3.BALANCE_15 * T5.RATE_15 * T4.DAY_15)   
             + (T3.BALANCE_16 * T5.RATE_16 * T4.DAY_16)   
             + (T3.BALANCE_17 * T5.RATE_17 * T4.DAY_17)   
             + (T3.BALANCE_18 * T5.RATE_18 * T4.DAY_18)   
             + (T3.BALANCE_19 * T5.RATE_19 * T4.DAY_19)   
             + (T3.BALANCE_20 * T5.RATE_20 * T4.DAY_20)   
             + (T3.BALANCE_21 * T5.RATE_21 * T4.DAY_21)   
             + (T3.BALANCE_22 * T5.RATE_22 * T4.DAY_22)   
             + (T3.BALANCE_23 * T5.RATE_23 * T4.DAY_23)   
             + (T3.BALANCE_24 * T5.RATE_24 * T4.DAY_24)   
             + (T3.BALANCE_25 * T5.RATE_25 * T4.DAY_25)   
             + (T3.BALANCE_26 * T5.RATE_26 * T4.DAY_26)   
             + (T3.BALANCE_27 * T5.RATE_27 * T4.DAY_27)   
             + (T3.BALANCE_28 * T5.RATE_28 * T4.DAY_28)   
             + (T3.BALANCE_29 * T5.RATE_29 * T4.DAY_29)   
             + (T3.BALANCE_30 * T5.RATE_30 * T4.DAY_30)   
             + (T3.BALANCE_31 * T5.RATE_31 * T4.DAY_31))  
             BALANCE  
          FROM FIN_DLY_HEADERS_PM T1   
               LEFT OUTER JOIN (   FIN_DLY_AUDIT T2   
               JOIN    
                  (  SELECT COUNTRY,     
                            LE_BOOK,     
                            YEAR_MONTH,  
                            SEQUENCE_FD, 
                            MAX (ADJ_SEQUENCE) ADJ_SEQUENCE  
                       FROM FIN_DLY_AUDIT                    
                      WHERE     COUNTRY = 'VN'               
                            AND LE_BOOK = '01'               
                            AND YEAR_MONTH BETWEEN 202001 AND 202007   
                            AND SEQUENCE_FD > 0              
                            AND RECORD_TYPE =2                   /* RECORD_TYPE != 9999  Added for MSB Bank */
                   GROUP BY COUNTRY,                         
                            LE_BOOK,                         
                            YEAR_MONTH,                      
                            SEQUENCE_FD) T3                  
               ON (    T2.COUNTRY = T3.COUNTRY               
                   AND T2.LE_BOOK = T3.LE_BOOK               
                   AND T2.YEAR_MONTH = T3.YEAR_MONTH         
                   AND T2.SEQUENCE_FD = T3.SEQUENCE_FD       
                   AND T2.ADJ_SEQUENCE = T3.ADJ_SEQUENCE))   
                ON (    T1.COUNTRY = T2.COUNTRY              
                    AND T1.LE_BOOK = T2.LE_BOOK              
                    AND T1.YEAR_MONTH = T2.YEAR_MONTH        
                    AND T1.SEQUENCE_FD = T2.SEQUENCE_FD )              /*AND T1.RECORD_TYPE != 9999*/
             INNER JOIN FIN_DLY_BALANCES_PM T3               
                ON (    T1.COUNTRY = T3.COUNTRY              
                    AND T1.LE_BOOK = T3.LE_BOOK              
                    AND T1.YEAR_MONTH = T3.YEAR_MONTH        
                    AND T1.SEQUENCE_FD = T3.SEQUENCE_FD      
                    AND T3.BAL_TYPE = 51                     
                    AND T1.RECORD_TYPE = 2     )                  
             INNER JOIN CURRENCY_RATES_DAILY T5              
                ON (    T1.COUNTRY = T5.COUNTRY              
                    AND T1.LE_BOOK = T5.LE_BOOK              
                    AND T1.YEAR_MONTH = T5.YEAR_MONTH        
                    AND T1.CURRENCY = T5.CURRENCY            
                    AND T5.CATEGORY = 'MRATE')               
             INNER JOIN (SELECT H1.*,                        
                          H2.YEAR || LPAD (H2.MONTH, 2, '0') YEAR_MONTH,     
                             H2.YEAR                  
                          || LPAD (H2.MONTH, 2, '0')  
                          || LPAD (H1.DAYS, 2, '0')   
                             PERIOD                   
                     FROM DENORM_DAYS H1, PERIOD_CONTROLS H2  
                    WHERE DAYS <= NO_OF_DAYS) T4   
                ON (T1.YEAR_MONTH = T4.YEAR_MONTH)  
               WHERE T1.COUNTRY = 'VN' AND T1.LE_BOOK = '01' AND T1.YEAR_MONTH BETWEEN 202001 AND 202007 
                                         AND T1.SOURCE_ID IN (7)  
                                         AND NVL (T2.VISION_SBU, T1.VISION_SBU)='B'
--                                         AND T1.BS_GL IN (%s)  
                                GROUP BY NVL (T2.VISION_SBU, T1.VISION_SBU),  
                                         NVL (T2.VISION_OUC, T1.VISION_OUC),  
                                         NVL (T2.CURRENCY, T1.CURRENCY) ,  
                                         T4.PERIOD  