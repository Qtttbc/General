/*--+===========================================================================================================================================+
                                                Incentive Check
+===========================================================================================================================================+ 

Replace Following Parameters
----------

Year_month :  202004
BUSINESS_DATE >='01-APR-2020'

--+===========================================================================================================================================+*/ 



-- Incentive Daily Calculation 
--------------------

SELECT  * FROM PWT_SPECIAL_INCENTIVE WHERE 
    PAYMENT_METHOD in ('LB','ML','MI')
AND BUSINESS_DATE >='01-APR-2020'

-- MIS Adjustment for MA report 
---------------------------------
-- Minsurance 
---------------
               
SELECT * 
  FROM MGT_HEADERS A, MGT_BALANCES B, MGT_DETAILS C
WHERE     A.YEAR = B.YEAR
       AND A.MGT_REFERENCE = B.MGT_REFERENCE
       AND A.YEAR = C.YEAR
       AND A.MGT_REFERENCE = C.MGT_REFERENCE
       AND B.MA_SEQUENCE = C.MA_SEQUENCE 
       AND A.YEAR=2020 
       AND A.MGT_REFERENCE IN ('MI202004','PI202004')
               
-- Mloan 
--------
               
SELECT * 
  FROM MGT_HEADERS A, MGT_BALANCES B, MGT_DETAILS C
WHERE     A.YEAR = B.YEAR
       AND A.MGT_REFERENCE = B.MGT_REFERENCE
       AND A.YEAR = C.YEAR
       AND A.MGT_REFERENCE = C.MGT_REFERENCE
       AND B.MA_SEQUENCE = C.MA_SEQUENCE 
       AND A.YEAR=2020 
       AND A.MGT_REFERENCE IN ('ML202004','PL202004') 
       
-- LBP         
--------
SELECT * 
  FROM MGT_HEADERS A, MGT_BALANCES B, MGT_DETAILS C
WHERE     A.YEAR = B.YEAR
       AND A.MGT_REFERENCE = B.MGT_REFERENCE
       AND A.YEAR = C.YEAR
       AND A.MGT_REFERENCE = C.MGT_REFERENCE
       AND B.MA_SEQUENCE = C.MA_SEQUENCE 
       AND A.YEAR=2020  
       AND A.MGT_REFERENCE IN ('LB202004','PB202004')
