CREATE OR REPLACE PROCEDURE VISION.PR_RS_MSBDS011 (
   P_VISION_ID        IN     NUMBER,
   P_SESSION_ID       IN     VARCHAR2,
   P_REPORT_ID        IN     VARCHAR2,
   P_SCALING_FACTOR   IN     NUMBER,
   P_PROMPT_VALUE_1   IN     VARCHAR2,
   P_PROMPT_VALUE_2   IN     VARCHAR2,
   P_PROMPT_VALUE_3   IN     VARCHAR2,
   P_PROMPT_VALUE_4   IN     VARCHAR2,
   P_PROMPT_VALUE_5   IN     VARCHAR2,
   P_PROMPT_VALUE_6   IN     VARCHAR2,
   P_PROMPT_VALUE_7   IN     VARCHAR2,
   P_PROMPT_VALUE_8   IN     VARCHAR2,
   P_STATUS              OUT VARCHAR2,
   P_ERRORMSG            OUT VARCHAR2)
   AUTHID CURRENT_USER
IS
   /*
   **************************************************************************************************************************************************
   Name            :    PR_RS_MSBDS011
   Description             :    Daily BS Dashboard
   Calling Script          :
             FN_RS_Get_Temp_Table_Name      ()
           FN_RS_Get_Column_Caption       ()
           FN_RS_Get_Parameter            ()
           PR_RS_Cleanup_Temp_Tables      ()
           PR_RS_Get_Security_Flds        ()
           FN_RS_ParseString              ()
           PR_RS_Execute_Stmt             ()
           PR_RS_Apply_Security_Profile   ()
           PR_RS_Make_All_Cols_Null       ()
           PR_RS_Transpose_1              ()
           PR_RS_Generate_Totals_1        ()
           PR_RS_Suppress_Zeros_Temp_Tbl  ()
           PR_RS_Fill_Sort_Column         ()
           PR_RS_Post_To_Stg_Table        ()
           PR_RS_Rem_Z_From_Stgtbl_Cols   ()
           PR_RS_Remove_Rpt_Cols          ()
           PR_RS_Ins_Column_Headers_STG   ()
           Pr_Rs_Do_Computations          ()

   Assumptions             :
   Input Values        :    For this report, only the first
             P_Prompt_Value_1    - Holds the Legal_Vehicles obtained at the Prompt Screen
             P_Prompt_Value_2    - Holds the Date(Mon-RRRR) obtained at the Prompt Screen
             Others variables from P_Prompt_Value_3 to 5 will be empty (Null)

   Output Values        :    As given below :
             P_Status = -1, if there is an error; P_ErrorMsg will contain the error string
             P_Status =  0, if procedure executes successfully. P_ErrorMsg will contain nothing in this case
             P_Status =  1, Procedure has fetched NO records for the given query criteria. P_ErrorMsg will contain nothing.

   Tables Involved     :
               Vision_Users
                 Report_Suite
                 Reports_STG
                 Column_Headers_STG
                 LE_Book
                 Vision_Risk_Mart,
            Alpha_Sub_Tab
            Num_Sub_Tab
            Tenor_Buckets



   Modifications History :

   No.      Date               Modifier Initials       Description
   01     XX-XXX-2011    IR            Initial Request
   **************************************************************************************************************************************************
   */



   LABELROWNUM                NUMBER (2);
   LABELCOLNUM                NUMBER (2);
   GRANDTOTALCAPTION          VARCHAR2 (50);
   CURRENCY_PROMPT            VARCHAR2 (50);
   SBU                        VARCHAR2 (1000);
   SBU_EX                     VARCHAR2 (1000);
   NOOFDECIMALS               NUMBER (5);
   NOOFDATACOLUMNS            NUMBER (5);
   TABSPACES                  VARCHAR2 (50);

   EXTRACONDITIONFLDS         VARCHAR2 (5000);
   DATACOLUMNS                VARCHAR2 (100);
   SQLSTATEMENT               VARCHAR2 (32000);
   FORMULA                    VARCHAR2 (100);


   FILTERSTRING               VARCHAR2 (5000);
   ERRORMESSAGE               VARCHAR2 (2000);

   J                          NUMBER (4);

   FRLLINE                    VARCHAR2 (20);
   LEBOOK                     VARCHAR2 (100);
   CCYVALUE                   VARCHAR2 (10);

   LEGAL_VEHICLE              VARCHAR2 (30);
   PROMPT_COUNTRY             VARCHAR2 (2);
   PROMPT_LE_BOOK             VARCHAR2 (2);
   PROMPT_DAY                 VARCHAR2 (5);
   PROMPT_MONTH               VARCHAR2 (5);
   PROMPT_YEAR                VARCHAR2 (5);
   TOTAL_DAYS                 NUMBER (10);

   MGT_LINE                   VARCHAR2 (30);

   CURR_PERIOD                VARCHAR2 (20) := NULL;
   PREV_PERIOD                VARCHAR2 (20) := NULL;
   YTD_PERIOD                 VARCHAR2 (20) := NULL;
   CURR_MONTH_DAYS            NUMBER (2) := 0;
   PREV_MONTH_DAYS            NUMBER (2) := 0;

   TEMPTABLENAME              VARCHAR2 (30);
   TEMPTABLENAME2             VARCHAR2 (30);
   TEMPTABLENAME3             VARCHAR2 (30);
   TEMPTABLENAME4             VARCHAR2 (30);
   TEMPTABLENAME5             VARCHAR2 (30);
   TEMPTABLENAME6             VARCHAR2 (30);
   TEMPTABLENAME7             VARCHAR2 (30);
   TEMPTABLENAME8             VARCHAR2 (30);
   TEMPTABLENAME60            VARCHAR2 (30);
   SEQUENCENAME               VARCHAR2 (30);
   APPLFLAGS                  VARCHAR2 (20);


   LEFTTOPTITLE1              VARCHAR2 (50);
   LEFTTOPTITLE2              VARCHAR2 (50);
   LEFTTOPTITLE3              VARCHAR2 (50);
   LEFTTOPTITLE4              VARCHAR2 (50);
   LEFTTOPTITLE5              VARCHAR2 (50);
   PARAM_ID_1                 REPORT_SUITE.PARAMETER_1%TYPE;
   PARAM_ID_2                 REPORT_SUITE.PARAMETER_2%TYPE;
   PARAM_ID_3                 REPORT_SUITE.PARAMETER_3%TYPE;
   PARAM_ID_4                 REPORT_SUITE.PARAMETER_4%TYPE;
   PARAM_ID_5                 VARCHAR2 (500);
   OUCLEVEL                   VARCHAR2 (50);
   PROMPT_SBU                 VARCHAR2 (50);
   OUTJOIN                    VARCHAR2 (10);
   PROMPT_OUC                 OUC_CODES.VISION_OUC%TYPE;
   P_WEEKSTARTDAY             DATE;
   PREV_MONTH                 VARCHAR2 (20);
   PREV_MONTH_LAST_DAY        VARCHAR2 (20);
   LAST_DAY_PREV_MONTH_YEAR   VARCHAR2 (20);
   LAST_DAY_PREV_YEAR_JAN     VARCHAR2 (20);
   PREV_WEEK                  VARCHAR (20);
   SQL_STAT                   VARCHAR (15000);
   SQL_STAT2                  VARCHAR (15000);
   NEXT_MONTH                 VARCHAR (45);
   CUR_WEEK                   VARCHAR (45);
   BUSINESS_DATE              VARCHAR (45);
   CUR_DAY                    VARCHAR (45);
   CURW_DAY                   VARCHAR (45);
   LP                         NUMBER;
   PREV_YEAR                  VARCHAR (45);

   START_DATE                 DATE;
   END_DATE                   NUMBER;
   END_DATE_1                 NUMBER;
   SQLSTATEMENT2              VARCHAR2 (10000);
   START_DAY                  NUMBER;

   YEARMTH                    NUMBER;
BEGIN
   TEMPTABLENAME :=
      FN_RS_GET_TEMP_TABLE_NAME (P_VISION_ID, P_SESSION_ID, '_1');
   TEMPTABLENAME2 :=
      FN_RS_GET_TEMP_TABLE_NAME (P_VISION_ID, P_SESSION_ID, '_2');
   TEMPTABLENAME3 :=
      FN_RS_GET_TEMP_TABLE_NAME (P_VISION_ID, P_SESSION_ID, '_3');
   TEMPTABLENAME4 :=
      FN_RS_GET_TEMP_TABLE_NAME (P_VISION_ID, P_SESSION_ID, '_4');
   TEMPTABLENAME5 :=
      FN_RS_GET_TEMP_TABLE_NAME (P_VISION_ID, P_SESSION_ID, '_5');
   TEMPTABLENAME6 :=
      FN_RS_GET_TEMP_TABLE_NAME (P_VISION_ID, P_SESSION_ID, '_7');
   TEMPTABLENAME7 :=
      FN_RS_GET_TEMP_TABLE_NAME (P_VISION_ID, P_SESSION_ID, '_8');
   TEMPTABLENAME8 :=
      FN_RS_GET_TEMP_TABLE_NAME (P_VISION_ID, P_SESSION_ID, '_9');
   SEQUENCENAME := FN_RS_GET_TEMP_TABLE_NAME (P_VISION_ID, P_SESSION_ID, '_6');

   LEFTTOPTITLE1 := 'CASA Trend';
   LEFTTOPTITLE2 :=
      NVL (FN_RS_GET_COLUMN_CAPTION (P_REPORT_ID,
                                     2,
                                     P_STATUS,
                                     P_ERRORMSG),
           'y');
   LEFTTOPTITLE3 :=
      NVL (FN_RS_GET_COLUMN_CAPTION (P_REPORT_ID,
                                     3,
                                     P_STATUS,
                                     P_ERRORMSG),
           'z');
   LEFTTOPTITLE4 := 'Fund Transfer Price';
   LEFTTOPTITLE5 := 'Int Margin/P';

   PARAM_ID_1 :=
      FN_RS_GET_PARAMETER (P_REPORT_ID,
                           1,
                           'Y',
                           P_STATUS,
                           P_ERRORMSG);
   PARAM_ID_2 :=
      FN_RS_GET_PARAMETER (P_REPORT_ID,
                           2,
                           'Y',
                           P_STATUS,
                           P_ERRORMSG);
   PARAM_ID_3 :=
      FN_RS_GET_PARAMETER (P_REPORT_ID,
                           3,
                           'Y',
                           P_STATUS,
                           P_ERRORMSG);
   PARAM_ID_4 :=
      FN_RS_GET_PARAMETER (P_REPORT_ID,
                           4,
                           'Y',
                           P_STATUS,
                           P_ERRORMSG);
   PARAM_ID_5 :=
      FN_RS_GET_PARAMETER (P_REPORT_ID,
                           5,
                           'Y',
                           P_STATUS,
                           P_ERRORMSG);


   TABSPACES := '          ';
   NOOFDECIMALS := 0;
   GRANDTOTALCAPTION := 'Total :';


   PR_RS_CLEANUP_TEMP_TABLES (P_VISION_ID,
                              P_SESSION_ID,
                              P_STATUS,
                              P_ERRORMSG);

   IF P_STATUS = -1
   THEN
      RETURN;
   END IF;

   FILTERSTRING := NULL;

   PRINT ('--------------------------------', NULL);

   PR_RS_GET_SECURITY_FLDS (
      P_VISION_ID,
      P_REPORT_ID,
      'T2.VISION_OUC,T2.VISION_SBU,T1.Mgt_Line,T2.CUSTOMER_ID,T2.CONTRACT_ID,T2.OFFICE_ACCOUNT,T2.BS_GL,T2.PL_GL,T2.GL_ENRICH_ID,
         T2.SOURCE_ID,T2.RECORD_TYPE,T3.bal_type',
      'Mgt_Result_Headers T2,MGT_EXPANDED T1,Mgt_Result_Balances T3',
      EXTRACONDITIONFLDS,
      FILTERSTRING,
      LEGAL_VEHICLE,
      APPLFLAGS,
      P_STATUS,
      P_ERRORMSG);

   IF P_STATUS = -1
   THEN
      RETURN;
   END IF;



   EXECUTE IMMEDIATE
      'Select  TO_DATE(BUSINESS_DATE,''DD-MON-RRRR'')  FROM VB_DAY WHERE COUNTRY=''VN'''
      INTO BUSINESS_DATE;


   P_WEEKSTARTDAY := (TO_DATE (P_PROMPT_VALUE_3, 'DD-MON-RRRR'));
   CUR_DAY := TO_CHAR (TO_DATE (BUSINESS_DATE, 'DD-MON-RRRR'), 'DD');
   CURW_DAY :=
      TO_CHAR (TRUNC (TO_DATE (BUSINESS_DATE, 'DD-MON-RRRR'), 'WW'), 'DD');
   OUCLEVEL := P_PROMPT_VALUE_1;
   PROMPT_SBU := P_PROMPT_VALUE_2;
   PROMPT_DAY := TO_CHAR ( (TO_DATE (P_WEEKSTARTDAY, 'DD-MON-RRRR')), 'DD');
   PROMPT_MONTH := TO_CHAR (TO_DATE (P_WEEKSTARTDAY, 'DD-MON-RRRR'), 'MM');
   PROMPT_YEAR := TO_CHAR (TO_DATE (P_WEEKSTARTDAY, 'DD-MON-RRRR'), 'RRRR');
   PREV_WEEK := TO_CHAR ( (TO_DATE (P_WEEKSTARTDAY, 'DD-MON-RRRR') - 3), 'DD');
   NEXT_MONTH := PROMPT_MONTH + 1;
   CUR_WEEK := PROMPT_DAY + 4;
   PREV_MONTH_LAST_DAY :=
      TO_CHAR (
         LAST_DAY (ADD_MONTHS (TO_DATE (P_WEEKSTARTDAY, 'DD-MON-RRRR'), -1)),
         'DD');
   PREV_MONTH :=
      TO_CHAR (
         LAST_DAY (ADD_MONTHS (TO_DATE (P_WEEKSTARTDAY, 'DD-MON-RRRR'), -1)),
         'MM');
   LAST_DAY_PREV_MONTH_YEAR :=
      TO_CHAR (
         LAST_DAY (ADD_MONTHS (TO_DATE (P_WEEKSTARTDAY, 'DD-MON-RRRR'), -1)),
         'RRRR');
   LAST_DAY_PREV_YEAR_JAN :=
      TO_CHAR (
         LAST_DAY (ADD_MONTHS (TO_DATE (P_WEEKSTARTDAY, 'DD-MON-RRRR'), -1)),
         'RRRR');
   PREV_YEAR :=
      TO_CHAR (
         LAST_DAY (ADD_MONTHS (TO_DATE (P_WEEKSTARTDAY, 'DD-MON-RRRR'), -1)),
         'YYYY');
   DBMS_OUTPUT.PUT_LINE ('Prev_Week :' || PREV_WEEK);
   DBMS_OUTPUT.PUT_LINE ('+++++++++++++++++++++++0');
   DBMS_OUTPUT.PUT_LINE ('Business_date :' || BUSINESS_DATE);
   DBMS_OUTPUT.PUT_LINE ('+++++++++++++++++++++++0.1');
   DBMS_OUTPUT.PUT_LINE ('Next_Month :' || NEXT_MONTH);
   DBMS_OUTPUT.PUT_LINE ('+++++++++++++++++++++++0.2');
   DBMS_OUTPUT.PUT_LINE ('cur_week :' || CUR_WEEK);
   DBMS_OUTPUT.PUT_LINE ('+++++++++++++++++++++++0.3');
   DBMS_OUTPUT.PUT_LINE ('Last_Day_Prev_Month :' || PREV_MONTH_LAST_DAY);
   DBMS_OUTPUT.PUT_LINE ('+++++++++++++++++++++++0.4');
   DBMS_OUTPUT.PUT_LINE (
      'Last_Day_Prev_Month_Year :' || LAST_DAY_PREV_MONTH_YEAR);
   DBMS_OUTPUT.PUT_LINE ('+++++++++++++++++++++++0.5');
   DBMS_OUTPUT.PUT_LINE (
      'Last_Day_Prev_Year_Jan :' || LAST_DAY_PREV_YEAR_JAN);

   SQL_STAT := NULL;
   SQL_STAT2 := NULL;


   START_DATE := P_WEEKSTARTDAY + 1;
   START_DAY :=
      TO_NUMBER (TO_CHAR (TO_DATE (START_DATE, 'DD-MON-RRRR'), 'DD'));

   FOR I IN 1 .. 5
   LOOP
      CASE
         WHEN TO_CHAR (START_DATE, 'DY') IN ('MON')
         THEN
            START_DATE := START_DATE - 3;
         WHEN TO_CHAR (START_DATE, 'DY') IN ('SUN')
         THEN
            START_DATE := START_DATE - 2;
         ELSE
            START_DATE := START_DATE - 1;
      END CASE;

      START_DAY :=
         TO_NUMBER (TO_CHAR (TO_DATE (START_DATE, 'DD-MON-RRRR'), 'DD'));
      YEARMTH :=
         TO_NUMBER (TO_CHAR (TO_DATE (START_DATE, 'DD-MON-RRRR'), 'RRRRMM'));

      IF I < 5
      THEN
         SQL_STAT :=
               SQL_STAT
            || 'SUM( CASE WHEN T1.YEAR_MONTH='''
            || YEARMTH
            || ''' THEN  BALANCE_'
            || LPAD (START_DAY, 2, '0')
            || ' * RATE_'
            || LPAD (START_DAY, 2, '0')
            || '  ELSE 0 END )  DATA_COLUMN_'
            || LPAD ( (I + 1), 1, '0')
            || ',';
      ELSIF I = 5
      THEN
         SQL_STAT :=
               SQL_STAT
            || 'SUM(CASE WHEN T1.YEAR_MONTH='''
            || YEARMTH
            || ''' THEN BALANCE_'
            || LPAD (START_DAY, 2, '0')
            || '* RATE_'
            || LPAD (START_DAY, 2, '0')
            || '  ELSE 0 END ) DATA_COLUMN_'
            || LPAD ( (I + 1), 1, '0')
            || '';
         EXIT;
      END IF;
   END LOOP;

   DBMS_OUTPUT.PUT_LINE ('++++++++++++++++++++++++++++++++++++++++++');
   DBMS_OUTPUT.PUT_LINE (SQL_STAT);
   DBMS_OUTPUT.PUT_LINE ('++++++++++++++++++++++++++++++++++++++++++');



   LEGAL_VEHICLE := FN_RS_PARSESTRING (P_PROMPT_VALUE_1, 1, '-');
   PROMPT_COUNTRY := FN_RS_PARSESTRING (P_PROMPT_VALUE_1, 2, '-');
   PROMPT_LE_BOOK := FN_RS_PARSESTRING (P_PROMPT_VALUE_1, 3, '-');
   PROMPT_OUC := FN_RS_PARSESTRING (P_PROMPT_VALUE_1, 4, '-');

   LEBOOK := LEGAL_VEHICLE || '-' || PROMPT_COUNTRY || '-' || PROMPT_LE_BOOK;

   BEGIN
      EXECUTE IMMEDIATE
            'SELECT NO_OF_DAYS FROM PERIOD_CONTROLS WHERE YEAR = '
         || PROMPT_YEAR
         || ' AND  MONTH =  '
         || PROMPT_MONTH
         || ' '
         INTO TOTAL_DAYS;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         P_ERRORMSG := 'No Data Found';
      WHEN OTHERS
      THEN
         P_STATUS := -1;
         P_ERRORMSG :=
               'Error while selecting PERIOD_CONTROLS  ! Error Code ['
            || TO_CHAR (SQLCODE)
            || '], Msg ['
            || SQLERRM
            || ']';
         RETURN;
   END;

   DBMS_OUTPUT.PUT_LINE ('Total Days' || TOTAL_DAYS);

   IF PROMPT_OUC <> 'zzzz'
   THEN
      SQLSTATEMENT :=
            'Create Table '
         || TEMPTABLENAME4
         || ' As
         SELECT COUNTRY,LE_BOOK,VISION_OUC
            FROM OUC_EXPANDED WHERE
            (
            (VISION_OUC   ='''
         || PROMPT_OUC
         || ''' OR
            OUC_LEVEL_01 ='''
         || PROMPT_OUC
         || ''' OR
            OUC_LEVEL_02 ='''
         || PROMPT_OUC
         || ''' OR
            OUC_LEVEL_03 ='''
         || PROMPT_OUC
         || ''' OR
            OUC_LEVEL_04 ='''
         || PROMPT_OUC
         || ''' OR
            OUC_LEVEL_05 ='''
         || PROMPT_OUC
         || ''')
            and country='''
         || PROMPT_COUNTRY
         || '''
            and le_book='''
         || PROMPT_LE_BOOK
         || '''
            )';
      OUTJOIN := '';
   ELSE
      SQLSTATEMENT :=
            'Create Table '
         || TEMPTABLENAME4
         || ' As
         SELECT COUNTRY,LE_BOOK,VISION_OUC
            FROM OUC_EXPANDED';
      OUTJOIN := '(+)';
   END IF;

   DBMS_OUTPUT.PUT_LINE (
      '+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++');
   DBMS_OUTPUT.PUT_LINE (SQLSTATEMENT);
   DBMS_OUTPUT.PUT_LINE (
      '+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++');

   PR_RS_EXECUTE_STMT (SQLSTATEMENT,
                       NULL,
                       P_STATUS,
                       ERRORMESSAGE);

   IF P_STATUS != 0
   THEN
      P_ERRORMSG := 'Error Creating Pre Stage - 01 table. ' || ERRORMESSAGE;
      RETURN;
   END IF;


   IF (P_PROMPT_VALUE_2 = '000')
   THEN
      SBU := '';
   ELSE
      SBU :=
            ' AND T1.VISION_SBU IN
            (SELECT VISION_SBU 
             from VISION_SBU 
             where VISION_SBU= '''
         || PROMPT_SBU
         || '''  or PARENT_SBU= '''
         || PROMPT_SBU
         || ''' or BANK_GROUP = '''
         || PROMPT_SBU
         || ''' ) ';
   END IF;

   IF (P_PROMPT_VALUE_4 = '0')
   THEN
      SBU_EX := '';
   ELSE
      SBU_EX := ' AND T1.VISION_SBU NOT IN
            (SELECT VISION_SBU 
             from VISION_SBU 
             where PARENT_SBU IN (''NA'', ''AOP'')) ';
   END IF;

   SQLSTATEMENT :=
         'Create Table '
      || TEMPTABLENAME
      || ' As
    SELECT  BANK_GROUP, SBU,  SBU_DESC,SUM(PREV_MONTH_END) PREV_MONTH_END,SUM(DATA_COLUMN_2)DATA_COLUMN_2,
    SUM(DATA_COLUMN_3)DATA_COLUMN_3,SUM(DATA_COLUMN_4)DATA_COLUMN_4,SUM(DATA_COLUMN_5)DATA_COLUMN_5,SUM(DATA_COLUMN_6)DATA_COLUMN_6
    FROM (
    Select
    BANK_GROUP,NVL(AO_NAME,T1.ACCOUNT_OFFICER) SBU, NVL(AO_NAME,T1.ACCOUNT_OFFICER)    SBU_DESC,
    0 PREV_MONTH_END, 
    '
      || SQL_STAT
      || '
    FROM
    VW_FIN_DLY_HEADERS T1,
    FIN_DLY_BALANCES T2,
    FIN_DLY_MAPPINGS T3,
    ACCOUNT_OFFICERS W,
    (SELECT *
        FROM MGT_EXPANDED T1
       WHERE     T1.MGT_LINE_LEVEL = 1
             AND MGT_LINE != ''G010000'' ) T5,
    CURRENCY_RATES_DAILY T99,VISION_SBU Q 
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
    AND T3.MRL_LINE = T5.SOURCE_MRL_LINE   
    AND T1.COUNTRY=T99.COUNTRY
    AND T1.LE_BOOK=T99.LE_BOOK
    AND T1.CURRENCY=T99.CURRENCY
    AND CATEGORY=''MRATE''
    AND T1.YEAR_MONTH=T99.YEAR_MONTH
    and t5.SOURCE_BAL_TYPE = 1
    AND T5.SOURCE_TYPE = 0
    AND T1.VISION_SBU=Q.VISION_SBU 
    AND T1.VISION_SBU=''B''
    AND t1.CUSTOMER_ID IN (SELECT CUSTOMER_ID 
                            FROM PWT_CBS_USER_BU_SEGMENT 
                            WHERE UPPER(CUSTOMER_USER_SEGMENT)  like ''%NON%BANK%''
                             AND BU_SEGMENT_STATUS=0)
    AND T5.MGT_LINE IN (''G012331'',''G012215'') 
    AND T1.ACCOUNT_OFFICER=W.ACCOUNT_OFFICER (+)
    AND T1.YEAR_MONTH  IN ('''
      || PROMPT_YEAR
      || PROMPT_MONTH
      || ''','''
      || YEARMTH
      || ''' )
    AND T1.RECORD_Type != 9999
    AND T2.BAL_TYPE = 51
    GROUP BY BANK_GROUP,PARENT_SBU,NVL(AO_NAME,T1.ACCOUNT_OFFICER), PARENT_SBU_DESCRIPTION 
     )
    GROUP BY BANK_GROUP,SBU,  SBU_DESC
    Order by 1    ';


   DBMS_OUTPUT.PUT_LINE (SQLSTATEMENT);


   PR_RS_EXECUTE_STMT (SQLSTATEMENT,
                       NULL,
                       P_STATUS,
                       ERRORMESSAGE);

   IF P_STATUS != 0
   THEN
      P_ERRORMSG := 'Error Creating Stage 2 table. ' || ERRORMESSAGE;
      RETURN;
   END IF;



   IF (SQL%ROWCOUNT = 0)
   THEN
      P_STATUS := 1;
      P_ERRORMSG := 'No Records Obtained for the given Criteria';
      RETURN;
   END IF;



   PR_RS_APPLY_SECURITY_PROFILE (
      P_VISION_ID,
      P_REPORT_ID,
         'BANK_GROUP,SBU,SBU_DESC,
             PREV_MONTH_END /'
      || P_SCALING_FACTOR
      || ' Data_Column_1, 
            Data_Column_6/'
      || P_SCALING_FACTOR
      || '  Data_Column_2,
            Data_Column_5/'
      || P_SCALING_FACTOR
      || '  Data_Column_3,
            Data_Column_4/'
      || P_SCALING_FACTOR
      || '  Data_Column_4,
            Data_Column_3/'
      || P_SCALING_FACTOR
      || '  Data_Column_5,
            Data_Column_2/'
      || P_SCALING_FACTOR
      || '  Data_Column_6',
      TEMPTABLENAME,
      TEMPTABLENAME2,
      P_STATUS,
      P_ERRORMSG);

   IF P_STATUS != 0
   THEN
      IF P_STATUS = 1
      THEN
         P_ERRORMSG := 'No Records Obtained for the given Criteria';
      END IF;

      RETURN;
   END IF;



   IF (SQL%ROWCOUNT = 0)
   THEN
      P_STATUS := 1;
      P_ERRORMSG := 'No Records Obtained for the given Profile Criteria';
      RETURN;
   END IF;


   DBMS_OUTPUT.PUT_LINE ('0 +++++++++++++++++++++++++');

   IF FN_RS_COUNTTEMPRECORDS (TEMPTABLENAME2) = 0
   THEN
      P_STATUS := 1;
      P_ERRORMSG := 'No Records Obtained for the given Criteria';
      RETURN;
   END IF;

   PR_RS_MAKE_ALL_COLS_NULL (TEMPTABLENAME2, P_STATUS, P_ERRORMSG);

   IF P_STATUS < 0
   THEN
      RETURN;
   END IF;

   PR_RS_ROUND_OFF_COLUMNS (NULL,
                            NOOFDECIMALS,
                            TEMPTABLENAME2,
                            P_STATUS,
                            P_ERRORMSG);

   IF P_STATUS != 0
   THEN
      RETURN;
   END IF;

   DBMS_OUTPUT.PUT_LINE ('3 +++++++++++++++++++++++++');



   DBMS_OUTPUT.PUT_LINE ('5 +++++++++++++++++++++++++');



--   PR_RS_GENERATE_TOTALS_1 ('BANK_GROUP',
--                            'Sum',
--                            'Y',
--                            'N',
--                            'Total :',
--                            'BANK_GROUP',
--                            'Y',
--                            'N',
--                            TEMPTABLENAME2,
--                            TEMPTABLENAME3,
--                            P_STATUS,
--                            P_ERRORMSG);
--
--   IF P_STATUS != 0
--   THEN
--      RETURN;
--   END IF;
--
--
--   PR_RS_GENERATE_TOTALS_1 ('BANK_GROUP',
--                            'Sum',
--                            'Y',
--                            'Y',
--                            'Total :',
--                            'BANK_GROUP',
--                            'N',
--                            'Y',
--                            TEMPTABLENAME2,
--                            TEMPTABLENAME3,
--                            P_STATUS,
--                            P_ERRORMSG);
--
--   IF P_STATUS != 0
--   THEN
--      RETURN;
--   END IF;
--
--   COMMIT;
--
--
--   EXECUTE IMMEDIATE
--         'delete from '
--      || TEMPTABLENAME2
--      || ' WHERE UPPER(BANK_GROUP) like ''%BAD%Z%'' OR UPPER(BANK_GROUP) like ''%GOOD%Z1%''';
--
--   EXECUTE IMMEDIATE
--         'UPDATE  '
--      || TEMPTABLENAME2
--      || ' SET SBU_DESC=INITCAP(regexp_replace(BANK_GROUP, ''[-: Zz0-9]'', '''')) ||'' Bank'' WHERE SBU_DESC IS NULL ';
--

   COMMIT;

   DBMS_OUTPUT.PUT_LINE ('4 +++++++++++++++++++++++++');

   PR_RS_FILL_SORT_COLUMN (TEMPTABLENAME2,
                           TEMPTABLENAME3,
                           SEQUENCENAME,
                           'BANK_GROUP,SBU',
                           P_STATUS,
                           P_ERRORMSG);

   IF P_STATUS != 0
   THEN
      RETURN;
   END IF;

   DBMS_OUTPUT.PUT_LINE ('5 +++++++++++++++++++++++++');


   NOOFDATACOLUMNS := FN_RS_COUNTDATACOLS (TEMPTABLENAME2);

   PR_RS_POST_TO_STG_TABLE_1 (P_REPORT_ID,
                              P_SESSION_ID,
                              TEMPTABLENAME3,
                              'SBU_DESC',
                              'SBU',
                              P_STATUS,
                              P_ERRORMSG);

   IF P_STATUS != 0
   THEN
      RETURN;
   END IF;

   DBMS_OUTPUT.PUT_LINE ('6 +++++++++++++++++++++++++');



   PR_RS_REM_Z_FROM_STGTBL_COLS (P_REPORT_ID,
                                 P_SESSION_ID,
                                 P_STATUS,
                                 P_ERRORMSG);

   IF P_STATUS != 0
   THEN
      RETURN;
   END IF;

   PR_RS_FIX_FORMAT_ALL_COLUMNS (P_REPORT_ID,
                                 P_SESSION_ID,
                                 NOOFDATACOLUMNS,
                                 'N',
                                 NOOFDECIMALS,
                                 P_STATUS,
                                 P_ERRORMSG);

   IF P_STATUS != 0
   THEN
      RETURN;
   END IF;



   PR_RS_INS_COLUMN_HEADERS_STG (P_REPORT_ID,
                                 P_SESSION_ID,
                                 1,
                                 1,
                                 0,
                                 LEFTTOPTITLE1,
                                 'Y',
                                 P_STATUS,
                                 P_ERRORMSG);

   IF P_STATUS = -1
   THEN
      RETURN;
   END IF;

   PR_RS_INS_COLUMN_HEADERS_STG (P_REPORT_ID,
                                 P_SESSION_ID,
                                 1,
                                 2,
                                 0,
                                 'Pr Mth End',
                                 'N',
                                 P_STATUS,
                                 P_ERRORMSG);

   IF P_STATUS = -1
   THEN
      RETURN;
   END IF;


   START_DATE := P_WEEKSTARTDAY + 1;
   START_DAY :=
      TO_NUMBER (TO_CHAR (TO_DATE (START_DATE, 'DD-MON-RRRR'), 'DD'));



   FOR I IN 1 .. 5
   LOOP
      CASE
         WHEN TO_CHAR (START_DATE, 'DY') IN ('MON')
         THEN
            START_DATE := START_DATE - 3;
         WHEN TO_CHAR (START_DATE, 'DY') IN ('SUN')
         THEN
            START_DATE := START_DATE - 2;
         ELSE
            START_DATE := START_DATE - 1;
      END CASE;

      START_DAY :=
         TO_NUMBER (TO_CHAR (TO_DATE (START_DATE, 'DD-MON-RRRR'), 'DD'));
   END LOOP;

   START_DATE := START_DATE - 1;

   FOR I IN 1 .. 5
   LOOP
      CASE
         WHEN TO_CHAR (START_DATE, 'DY') IN ('FRI')
         THEN
            START_DATE := START_DATE + 3;
         WHEN TO_CHAR (START_DATE, 'DY') IN ('SAT')
         THEN
            START_DATE := START_DATE + 2;
         ELSE
            START_DATE := START_DATE + 1;
      END CASE;

      START_DAY :=
         TO_NUMBER (TO_CHAR (TO_DATE (START_DATE, 'DD-MON-RRRR'), 'DD'));

      PR_RS_INS_COLUMN_HEADERS_STG (
         P_REPORT_ID,
         P_SESSION_ID,
         1,
         (2 + I),
         0,
         TO_CHAR (TO_DATE (START_DATE, 'DD-MON-RRRR'), 'DD-Mon'),
         'N',
         P_STATUS,
         P_ERRORMSG);

      IF P_STATUS = -1
      THEN
         RETURN;
      END IF;

      DBMS_OUTPUT.PUT_LINE (
            '++++++++++++++++++++++++++'
         || TO_CHAR (TO_DATE (START_DATE, 'DD-MON-RRRR'), 'DD-Mon'));
   END LOOP;



   P_STATUS := 0;
   P_ERRORMSG := 'Go';
   RETURN;
END;
/

CREATE OR REPLACE PROCEDURE VISION.PR_RS_MSBDS021 (
   P_VISION_ID        IN     NUMBER,
   P_SESSION_ID       IN     VARCHAR2,
   P_REPORT_ID        IN     VARCHAR2,
   P_SCALING_FACTOR   IN     NUMBER,
   P_PROMPT_VALUE_1   IN     VARCHAR2,
   P_PROMPT_VALUE_2   IN     VARCHAR2,
   P_PROMPT_VALUE_3   IN     VARCHAR2,
   P_PROMPT_VALUE_4   IN     VARCHAR2,
   P_PROMPT_VALUE_5   IN     VARCHAR2,
   P_PROMPT_VALUE_6   IN     VARCHAR2,
   P_PROMPT_VALUE_7   IN     VARCHAR2,
   P_PROMPT_VALUE_8   IN     VARCHAR2,
   P_STATUS              OUT VARCHAR2,
   P_ERRORMSG            OUT VARCHAR2)
   AUTHID CURRENT_USER
IS
   /*
   **************************************************************************************************************************************************
   Name            :    PR_RS_MSBDS011
   Description             :    Daily BS Dashboard
   Calling Script          :
             FN_RS_Get_Temp_Table_Name      ()
           FN_RS_Get_Column_Caption       ()
           FN_RS_Get_Parameter            ()
           PR_RS_Cleanup_Temp_Tables      ()
           PR_RS_Get_Security_Flds        ()
           FN_RS_ParseString              ()
           PR_RS_Execute_Stmt             ()
           PR_RS_Apply_Security_Profile   ()
           PR_RS_Make_All_Cols_Null       ()
           PR_RS_Transpose_1              ()
           PR_RS_Generate_Totals_1        ()
           PR_RS_Suppress_Zeros_Temp_Tbl  ()
           PR_RS_Fill_Sort_Column         ()
           PR_RS_Post_To_Stg_Table        ()
           PR_RS_Rem_Z_From_Stgtbl_Cols   ()
           PR_RS_Remove_Rpt_Cols          ()
           PR_RS_Ins_Column_Headers_STG   ()
           Pr_Rs_Do_Computations          ()

   Assumptions             :
   Input Values        :    For this report, only the first
             P_Prompt_Value_1    - Holds the Legal_Vehicles obtained at the Prompt Screen
             P_Prompt_Value_2    - Holds the Date(Mon-RRRR) obtained at the Prompt Screen
             Others variables from P_Prompt_Value_3 to 5 will be empty (Null)

   Output Values        :    As given below :
             P_Status = -1, if there is an error; P_ErrorMsg will contain the error string
             P_Status =  0, if procedure executes successfully. P_ErrorMsg will contain nothing in this case
             P_Status =  1, Procedure has fetched NO records for the given query criteria. P_ErrorMsg will contain nothing.

   Tables Involved     :
               Vision_Users
                 Report_Suite
                 Reports_STG
                 Column_Headers_STG
                 LE_Book
                 Vision_Risk_Mart,
            Alpha_Sub_Tab
            Num_Sub_Tab
            Tenor_Buckets



   Modifications History :

   No.      Date               Modifier Initials       Description
   01     XX-XXX-2011    IR            Initial Request
   **************************************************************************************************************************************************
   */



   LABELROWNUM                NUMBER (2);
   LABELCOLNUM                NUMBER (2);
   GRANDTOTALCAPTION          VARCHAR2 (50);
   CURRENCY_PROMPT            VARCHAR2 (50);
   SBU                        VARCHAR2 (1000);
   SBU_EX                     VARCHAR2 (1000);
   NOOFDECIMALS               NUMBER (5);
   NOOFDATACOLUMNS            NUMBER (5);
   TABSPACES                  VARCHAR2 (50);

   EXTRACONDITIONFLDS         VARCHAR2 (5000);
   DATACOLUMNS                VARCHAR2 (100);
   SQLSTATEMENT               VARCHAR2 (32000);
   FORMULA                    VARCHAR2 (100);


   FILTERSTRING               VARCHAR2 (5000);
   ERRORMESSAGE               VARCHAR2 (2000);

   J                          NUMBER (4);

   FRLLINE                    VARCHAR2 (20);
   LEBOOK                     VARCHAR2 (100);
   CCYVALUE                   VARCHAR2 (10);

   LEGAL_VEHICLE              VARCHAR2 (30);
   PROMPT_COUNTRY             VARCHAR2 (2);
   PROMPT_LE_BOOK             VARCHAR2 (2);
   PROMPT_DAY                 VARCHAR2 (5);
   PROMPT_MONTH               VARCHAR2 (5);
   PROMPT_YEAR                VARCHAR2 (5);
   TOTAL_DAYS                 NUMBER (10);

   MGT_LINE                   VARCHAR2 (30);

   CURR_PERIOD                VARCHAR2 (20) := NULL;
   PREV_PERIOD                VARCHAR2 (20) := NULL;
   YTD_PERIOD                 VARCHAR2 (20) := NULL;
   CURR_MONTH_DAYS            NUMBER (2) := 0;
   PREV_MONTH_DAYS            NUMBER (2) := 0;

   TEMPTABLENAME              VARCHAR2 (30);
   TEMPTABLENAME2             VARCHAR2 (30);
   TEMPTABLENAME3             VARCHAR2 (30);
   TEMPTABLENAME4             VARCHAR2 (30);
   TEMPTABLENAME5             VARCHAR2 (30);
   TEMPTABLENAME6             VARCHAR2 (30);
   TEMPTABLENAME7             VARCHAR2 (30);
   TEMPTABLENAME8             VARCHAR2 (30);
   TEMPTABLENAME60            VARCHAR2 (30);
   SEQUENCENAME               VARCHAR2 (30);
   APPLFLAGS                  VARCHAR2 (20);


   LEFTTOPTITLE1              VARCHAR2 (50);
   LEFTTOPTITLE2              VARCHAR2 (50);
   LEFTTOPTITLE3              VARCHAR2 (50);
   LEFTTOPTITLE4              VARCHAR2 (50);
   LEFTTOPTITLE5              VARCHAR2 (50);
   PARAM_ID_1                 REPORT_SUITE.PARAMETER_1%TYPE;
   PARAM_ID_2                 REPORT_SUITE.PARAMETER_2%TYPE;
   PARAM_ID_3                 REPORT_SUITE.PARAMETER_3%TYPE;
   PARAM_ID_4                 REPORT_SUITE.PARAMETER_4%TYPE;
   PARAM_ID_5                 VARCHAR2 (500);
   OUCLEVEL                   VARCHAR2 (50);
   PROMPT_SBU                 VARCHAR2 (50);
   OUTJOIN                    VARCHAR2 (10);
   PROMPT_OUC                 OUC_CODES.VISION_OUC%TYPE;
   P_WEEKSTARTDAY             DATE;
   PREV_MONTH                 VARCHAR2 (20);
   PREV_MONTH_LAST_DAY        VARCHAR2 (20);
   LAST_DAY_PREV_MONTH_YEAR   VARCHAR2 (20);
   LAST_DAY_PREV_YEAR_JAN     VARCHAR2 (20);
   PREV_WEEK                  VARCHAR (20);
   SQL_STAT                   VARCHAR (15000);
   SQL_STAT2                  VARCHAR (15000);
   NEXT_MONTH                 VARCHAR (45);
   CUR_WEEK                   VARCHAR (45);
   BUSINESS_DATE              VARCHAR (45);
   CUR_DAY                    VARCHAR (45);
   CURW_DAY                   VARCHAR (45);
   LP                         NUMBER;
   PREV_YEAR                  VARCHAR (45);

   START_DATE                 DATE;
   END_DATE                   NUMBER;
   END_DATE_1                 NUMBER;
   SQLSTATEMENT2              VARCHAR2 (10000);
   START_DAY                  NUMBER;

   YEARMTH                    NUMBER;
BEGIN
   TEMPTABLENAME :=
      FN_RS_GET_TEMP_TABLE_NAME (P_VISION_ID, P_SESSION_ID, '_1');
   TEMPTABLENAME2 :=
      FN_RS_GET_TEMP_TABLE_NAME (P_VISION_ID, P_SESSION_ID, '_2');
   TEMPTABLENAME3 :=
      FN_RS_GET_TEMP_TABLE_NAME (P_VISION_ID, P_SESSION_ID, '_3');
   TEMPTABLENAME4 :=
      FN_RS_GET_TEMP_TABLE_NAME (P_VISION_ID, P_SESSION_ID, '_4');
   TEMPTABLENAME5 :=
      FN_RS_GET_TEMP_TABLE_NAME (P_VISION_ID, P_SESSION_ID, '_5');
   TEMPTABLENAME6 :=
      FN_RS_GET_TEMP_TABLE_NAME (P_VISION_ID, P_SESSION_ID, '_7');
   TEMPTABLENAME7 :=
      FN_RS_GET_TEMP_TABLE_NAME (P_VISION_ID, P_SESSION_ID, '_8');
   TEMPTABLENAME8 :=
      FN_RS_GET_TEMP_TABLE_NAME (P_VISION_ID, P_SESSION_ID, '_9');
   SEQUENCENAME := FN_RS_GET_TEMP_TABLE_NAME (P_VISION_ID, P_SESSION_ID, '_6');

   LEFTTOPTITLE1 := 'CASA Trend';
   LEFTTOPTITLE2 :=
      NVL (FN_RS_GET_COLUMN_CAPTION (P_REPORT_ID,
                                     2,
                                     P_STATUS,
                                     P_ERRORMSG),
           'y');
   LEFTTOPTITLE3 :=
      NVL (FN_RS_GET_COLUMN_CAPTION (P_REPORT_ID,
                                     3,
                                     P_STATUS,
                                     P_ERRORMSG),
           'z');
   LEFTTOPTITLE4 := 'Fund Transfer Price';
   LEFTTOPTITLE5 := 'Int Margin/P';

   PARAM_ID_1 :=
      FN_RS_GET_PARAMETER (P_REPORT_ID,
                           1,
                           'Y',
                           P_STATUS,
                           P_ERRORMSG);
   PARAM_ID_2 :=
      FN_RS_GET_PARAMETER (P_REPORT_ID,
                           2,
                           'Y',
                           P_STATUS,
                           P_ERRORMSG);
   PARAM_ID_3 :=
      FN_RS_GET_PARAMETER (P_REPORT_ID,
                           3,
                           'Y',
                           P_STATUS,
                           P_ERRORMSG);
   PARAM_ID_4 :=
      FN_RS_GET_PARAMETER (P_REPORT_ID,
                           4,
                           'Y',
                           P_STATUS,
                           P_ERRORMSG);
   PARAM_ID_5 :=
      FN_RS_GET_PARAMETER (P_REPORT_ID,
                           5,
                           'Y',
                           P_STATUS,
                           P_ERRORMSG);


   TABSPACES := '          ';
   NOOFDECIMALS := 0;
   GRANDTOTALCAPTION := 'Total :';


   PR_RS_CLEANUP_TEMP_TABLES (P_VISION_ID,
                              P_SESSION_ID,
                              P_STATUS,
                              P_ERRORMSG);

   IF P_STATUS = -1
   THEN
      RETURN;
   END IF;

   FILTERSTRING := NULL;

   PRINT ('--------------------------------', NULL);

   PR_RS_GET_SECURITY_FLDS (
      P_VISION_ID,
      P_REPORT_ID,
      'T2.VISION_OUC,T2.VISION_SBU,T1.Mgt_Line,T2.CUSTOMER_ID,T2.CONTRACT_ID,T2.OFFICE_ACCOUNT,T2.BS_GL,T2.PL_GL,T2.GL_ENRICH_ID,
         T2.SOURCE_ID,T2.RECORD_TYPE,T3.bal_type',
      'Mgt_Result_Headers T2,MGT_EXPANDED T1,Mgt_Result_Balances T3',
      EXTRACONDITIONFLDS,
      FILTERSTRING,
      LEGAL_VEHICLE,
      APPLFLAGS,
      P_STATUS,
      P_ERRORMSG);

   IF P_STATUS = -1
   THEN
      RETURN;
   END IF;



   EXECUTE IMMEDIATE
      'Select  TO_DATE(BUSINESS_DATE,''DD-MON-RRRR'')  FROM VB_DAY WHERE COUNTRY=''VN'''
      INTO BUSINESS_DATE;


   P_WEEKSTARTDAY := (TO_DATE (P_PROMPT_VALUE_3, 'DD-MON-RRRR'));
   CUR_DAY := TO_CHAR (TO_DATE (BUSINESS_DATE, 'DD-MON-RRRR'), 'DD');
   CURW_DAY :=
      TO_CHAR (TRUNC (TO_DATE (BUSINESS_DATE, 'DD-MON-RRRR'), 'WW'), 'DD');
   OUCLEVEL := P_PROMPT_VALUE_1;
   PROMPT_SBU := P_PROMPT_VALUE_2;
   PROMPT_DAY := TO_CHAR ( (TO_DATE (P_WEEKSTARTDAY, 'DD-MON-RRRR')), 'DD');
   PROMPT_MONTH := TO_CHAR (TO_DATE (P_WEEKSTARTDAY, 'DD-MON-RRRR'), 'MM');
   PROMPT_YEAR := TO_CHAR (TO_DATE (P_WEEKSTARTDAY, 'DD-MON-RRRR'), 'RRRR');
   PREV_WEEK := TO_CHAR ( (TO_DATE (P_WEEKSTARTDAY, 'DD-MON-RRRR') - 3), 'DD');
   NEXT_MONTH := PROMPT_MONTH + 1;
   CUR_WEEK := PROMPT_DAY + 4;
   PREV_MONTH_LAST_DAY :=
      TO_CHAR (
         LAST_DAY (ADD_MONTHS (TO_DATE (P_WEEKSTARTDAY, 'DD-MON-RRRR'), -1)),
         'DD');
   PREV_MONTH :=
      TO_CHAR (
         LAST_DAY (ADD_MONTHS (TO_DATE (P_WEEKSTARTDAY, 'DD-MON-RRRR'), -1)),
         'MM');
   LAST_DAY_PREV_MONTH_YEAR :=
      TO_CHAR (
         LAST_DAY (ADD_MONTHS (TO_DATE (P_WEEKSTARTDAY, 'DD-MON-RRRR'), -1)),
         'RRRR');
   LAST_DAY_PREV_YEAR_JAN :=
      TO_CHAR (
         LAST_DAY (ADD_MONTHS (TO_DATE (P_WEEKSTARTDAY, 'DD-MON-RRRR'), -1)),
         'RRRR');
   PREV_YEAR :=
      TO_CHAR (
         LAST_DAY (ADD_MONTHS (TO_DATE (P_WEEKSTARTDAY, 'DD-MON-RRRR'), -1)),
         'YYYY');
   DBMS_OUTPUT.PUT_LINE ('Prev_Week :' || PREV_WEEK);
   DBMS_OUTPUT.PUT_LINE ('+++++++++++++++++++++++0');
   DBMS_OUTPUT.PUT_LINE ('Business_date :' || BUSINESS_DATE);
   DBMS_OUTPUT.PUT_LINE ('+++++++++++++++++++++++0.1');
   DBMS_OUTPUT.PUT_LINE ('Next_Month :' || NEXT_MONTH);
   DBMS_OUTPUT.PUT_LINE ('+++++++++++++++++++++++0.2');
   DBMS_OUTPUT.PUT_LINE ('cur_week :' || CUR_WEEK);
   DBMS_OUTPUT.PUT_LINE ('+++++++++++++++++++++++0.3');
   DBMS_OUTPUT.PUT_LINE ('Last_Day_Prev_Month :' || PREV_MONTH_LAST_DAY);
   DBMS_OUTPUT.PUT_LINE ('+++++++++++++++++++++++0.4');
   DBMS_OUTPUT.PUT_LINE (
      'Last_Day_Prev_Month_Year :' || LAST_DAY_PREV_MONTH_YEAR);
   DBMS_OUTPUT.PUT_LINE ('+++++++++++++++++++++++0.5');
   DBMS_OUTPUT.PUT_LINE (
      'Last_Day_Prev_Year_Jan :' || LAST_DAY_PREV_YEAR_JAN);

   SQL_STAT := NULL;
   SQL_STAT2 := NULL;


   START_DATE := P_WEEKSTARTDAY + 1;
   START_DAY :=
      TO_NUMBER (TO_CHAR (TO_DATE (START_DATE, 'DD-MON-RRRR'), 'DD'));

   FOR I IN 1 .. 5
   LOOP
      CASE
         WHEN TO_CHAR (START_DATE, 'DY') IN ('MON')
         THEN
            START_DATE := START_DATE - 3;
         WHEN TO_CHAR (START_DATE, 'DY') IN ('SUN')
         THEN
            START_DATE := START_DATE - 2;
         ELSE
            START_DATE := START_DATE - 1;
      END CASE;

      START_DAY :=
         TO_NUMBER (TO_CHAR (TO_DATE (START_DATE, 'DD-MON-RRRR'), 'DD'));
      YEARMTH :=
         TO_NUMBER (TO_CHAR (TO_DATE (START_DATE, 'DD-MON-RRRR'), 'RRRRMM'));

      IF I < 5
      THEN
         SQL_STAT :=
               SQL_STAT
            || 'SUM( CASE WHEN T1.YEAR_MONTH='''
            || YEARMTH
            || ''' THEN  BALANCE_'
            || LPAD (START_DAY, 2, '0')
            || ' * RATE_'
            || LPAD (START_DAY, 2, '0')
            || '  ELSE 0 END )  DATA_COLUMN_'
            || LPAD ( (I + 1), 1, '0')
            || ',';
      ELSIF I = 5
      THEN
         SQL_STAT :=
               SQL_STAT
            || 'SUM(CASE WHEN T1.YEAR_MONTH='''
            || YEARMTH
            || ''' THEN BALANCE_'
            || LPAD (START_DAY, 2, '0')
            || '* RATE_'
            || LPAD (START_DAY, 2, '0')
            || '  ELSE 0 END ) DATA_COLUMN_'
            || LPAD ( (I + 1), 1, '0')
            || '';
         EXIT;
      END IF;
   END LOOP;

   DBMS_OUTPUT.PUT_LINE ('++++++++++++++++++++++++++++++++++++++++++');
   DBMS_OUTPUT.PUT_LINE (SQL_STAT);
   DBMS_OUTPUT.PUT_LINE ('++++++++++++++++++++++++++++++++++++++++++');



   LEGAL_VEHICLE := FN_RS_PARSESTRING (P_PROMPT_VALUE_1, 1, '-');
   PROMPT_COUNTRY := FN_RS_PARSESTRING (P_PROMPT_VALUE_1, 2, '-');
   PROMPT_LE_BOOK := FN_RS_PARSESTRING (P_PROMPT_VALUE_1, 3, '-');
   PROMPT_OUC := FN_RS_PARSESTRING (P_PROMPT_VALUE_1, 4, '-');

   LEBOOK := LEGAL_VEHICLE || '-' || PROMPT_COUNTRY || '-' || PROMPT_LE_BOOK;

   BEGIN
      EXECUTE IMMEDIATE
            'SELECT NO_OF_DAYS FROM PERIOD_CONTROLS WHERE YEAR = '
         || PROMPT_YEAR
         || ' AND  MONTH =  '
         || PROMPT_MONTH
         || ' '
         INTO TOTAL_DAYS;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         P_ERRORMSG := 'No Data Found';
      WHEN OTHERS
      THEN
         P_STATUS := -1;
         P_ERRORMSG :=
               'Error while selecting PERIOD_CONTROLS  ! Error Code ['
            || TO_CHAR (SQLCODE)
            || '], Msg ['
            || SQLERRM
            || ']';
         RETURN;
   END;

   DBMS_OUTPUT.PUT_LINE ('Total Days' || TOTAL_DAYS);

   IF PROMPT_OUC <> 'zzzz'
   THEN
      SQLSTATEMENT :=
            'Create Table '
         || TEMPTABLENAME4
         || ' As
         SELECT COUNTRY,LE_BOOK,VISION_OUC
            FROM OUC_EXPANDED WHERE
            (
            (VISION_OUC   ='''
         || PROMPT_OUC
         || ''' OR
            OUC_LEVEL_01 ='''
         || PROMPT_OUC
         || ''' OR
            OUC_LEVEL_02 ='''
         || PROMPT_OUC
         || ''' OR
            OUC_LEVEL_03 ='''
         || PROMPT_OUC
         || ''' OR
            OUC_LEVEL_04 ='''
         || PROMPT_OUC
         || ''' OR
            OUC_LEVEL_05 ='''
         || PROMPT_OUC
         || ''')
            and country='''
         || PROMPT_COUNTRY
         || '''
            and le_book='''
         || PROMPT_LE_BOOK
         || '''
            )';
      OUTJOIN := '';
   ELSE
      SQLSTATEMENT :=
            'Create Table '
         || TEMPTABLENAME4
         || ' As
         SELECT COUNTRY,LE_BOOK,VISION_OUC
            FROM OUC_EXPANDED';
      OUTJOIN := '(+)';
   END IF;

   DBMS_OUTPUT.PUT_LINE (
      '+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++');
   DBMS_OUTPUT.PUT_LINE (SQLSTATEMENT);
   DBMS_OUTPUT.PUT_LINE (
      '+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++');

   PR_RS_EXECUTE_STMT (SQLSTATEMENT,
                       NULL,
                       P_STATUS,
                       ERRORMESSAGE);

   IF P_STATUS != 0
   THEN
      P_ERRORMSG := 'Error Creating Pre Stage - 01 table. ' || ERRORMESSAGE;
      RETURN;
   END IF;


   IF (P_PROMPT_VALUE_2 = '000')
   THEN
      SBU := '';
   ELSE
      SBU :=
            ' AND T1.VISION_SBU IN
            (SELECT VISION_SBU 
             from VISION_SBU 
             where VISION_SBU= '''
         || PROMPT_SBU
         || '''  or PARENT_SBU= '''
         || PROMPT_SBU
         || ''' or BANK_GROUP = '''
         || PROMPT_SBU
         || ''' ) ';
   END IF;

   IF (P_PROMPT_VALUE_4 = '0')
   THEN
      SBU_EX := '';
   ELSE
      SBU_EX := ' AND T1.VISION_SBU NOT IN
            (SELECT VISION_SBU 
             from VISION_SBU 
             where PARENT_SBU IN (''NA'', ''AOP'')) ';
   END IF;

   SQLSTATEMENT :=
         'Create Table '
      || TEMPTABLENAME
      || ' As
    SELECT  BANK_GROUP, SBU,  SBU_DESC,SUM(PREV_MONTH_END) PREV_MONTH_END,SUM(DATA_COLUMN_2)DATA_COLUMN_2,
    SUM(DATA_COLUMN_3)DATA_COLUMN_3,SUM(DATA_COLUMN_4)DATA_COLUMN_4,SUM(DATA_COLUMN_5)DATA_COLUMN_5,SUM(DATA_COLUMN_6)DATA_COLUMN_6
    FROM (
    Select
    BANK_GROUP,NVL(AO_NAME,T1.ACCOUNT_OFFICER) SBU, NVL(AO_NAME,T1.ACCOUNT_OFFICER)    SBU_DESC,
    0 PREV_MONTH_END, 
    '
      || SQL_STAT
      || '
    FROM
    VW_FIN_DLY_HEADERS T1,
    FIN_DLY_BALANCES T2,
    FIN_DLY_MAPPINGS T3,
    ACCOUNT_OFFICERS W,
    (SELECT *
        FROM MGT_EXPANDED T1
       WHERE     T1.MGT_LINE_LEVEL = 1
             AND MGT_LINE != ''G010000'' ) T5,
    CURRENCY_RATES_DAILY T99,VISION_SBU Q 
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
    AND T3.MRL_LINE = T5.SOURCE_MRL_LINE   
    AND T1.COUNTRY=T99.COUNTRY
    AND T1.LE_BOOK=T99.LE_BOOK
    AND T1.CURRENCY=T99.CURRENCY
    AND CATEGORY=''MRATE''
    AND T1.YEAR_MONTH=T99.YEAR_MONTH
    and t5.SOURCE_BAL_TYPE = 1
    AND T5.SOURCE_TYPE = 0
    AND T1.VISION_SBU=Q.VISION_SBU 
    AND T1.VISION_SBU=''B''
    AND t1.CUSTOMER_ID IN (SELECT CUSTOMER_ID 
                            FROM PWT_CBS_USER_BU_SEGMENT 
                            WHERE UPPER(CUSTOMER_USER_SEGMENT)  like ''%NON%BANK%''
                             AND BU_SEGMENT_STATUS=0)
    AND T5.MGT_LINE IN (''G012335'',''G012440'',''G012220'')
    AND T1.ACCOUNT_OFFICER=W.ACCOUNT_OFFICER  (+)
    AND T1.YEAR_MONTH  IN ('''
      || PROMPT_YEAR
      || PROMPT_MONTH
      || ''','''
      || YEARMTH
      || ''' )
    AND T1.RECORD_Type != 9999
    AND T2.BAL_TYPE = 51
    GROUP BY BANK_GROUP,PARENT_SBU,NVL(AO_NAME,T1.ACCOUNT_OFFICER), PARENT_SBU_DESCRIPTION 
     )
    GROUP BY BANK_GROUP,SBU,  SBU_DESC
    Order by 1    ';


   DBMS_OUTPUT.PUT_LINE (SQLSTATEMENT);


   PR_RS_EXECUTE_STMT (SQLSTATEMENT,
                       NULL,
                       P_STATUS,
                       ERRORMESSAGE);

   IF P_STATUS != 0
   THEN
      P_ERRORMSG := 'Error Creating Stage 2 table. ' || ERRORMESSAGE;
      RETURN;
   END IF;



   IF (SQL%ROWCOUNT = 0)
   THEN
      P_STATUS := 1;
      P_ERRORMSG := 'No Records Obtained for the given Criteria';
      RETURN;
   END IF;



   PR_RS_APPLY_SECURITY_PROFILE (
      P_VISION_ID,
      P_REPORT_ID,
         'BANK_GROUP,SBU,SBU_DESC,
             PREV_MONTH_END /'
      || P_SCALING_FACTOR
      || ' Data_Column_1, 
            Data_Column_6/'
      || P_SCALING_FACTOR
      || '  Data_Column_2,
            Data_Column_5/'
      || P_SCALING_FACTOR
      || '  Data_Column_3,
            Data_Column_4/'
      || P_SCALING_FACTOR
      || '  Data_Column_4,
            Data_Column_3/'
      || P_SCALING_FACTOR
      || '  Data_Column_5,
            Data_Column_2/'
      || P_SCALING_FACTOR
      || '  Data_Column_6',
      TEMPTABLENAME,
      TEMPTABLENAME2,
      P_STATUS,
      P_ERRORMSG);

   IF P_STATUS != 0
   THEN
      IF P_STATUS = 1
      THEN
         P_ERRORMSG := 'No Records Obtained for the given Criteria';
      END IF;

      RETURN;
   END IF;



   IF (SQL%ROWCOUNT = 0)
   THEN
      P_STATUS := 1;
      P_ERRORMSG := 'No Records Obtained for the given Profile Criteria';
      RETURN;
   END IF;


   DBMS_OUTPUT.PUT_LINE ('0 +++++++++++++++++++++++++');

   IF FN_RS_COUNTTEMPRECORDS (TEMPTABLENAME2) = 0
   THEN
      P_STATUS := 1;
      P_ERRORMSG := 'No Records Obtained for the given Criteria';
      RETURN;
   END IF;

   PR_RS_MAKE_ALL_COLS_NULL (TEMPTABLENAME2, P_STATUS, P_ERRORMSG);

   IF P_STATUS < 0
   THEN
      RETURN;
   END IF;

   PR_RS_ROUND_OFF_COLUMNS (NULL,
                            NOOFDECIMALS,
                            TEMPTABLENAME2,
                            P_STATUS,
                            P_ERRORMSG);

   IF P_STATUS != 0
   THEN
      RETURN;
   END IF;

   DBMS_OUTPUT.PUT_LINE ('3 +++++++++++++++++++++++++');



   DBMS_OUTPUT.PUT_LINE ('5 +++++++++++++++++++++++++');



--   PR_RS_GENERATE_TOTALS_1 ('BANK_GROUP',
--                            'Sum',
--                            'Y',
--                            'N',
--                            'Total :',
--                            'BANK_GROUP',
--                            'Y',
--                            'N',
--                            TEMPTABLENAME2,
--                            TEMPTABLENAME3,
--                            P_STATUS,
--                            P_ERRORMSG);
--
--   IF P_STATUS != 0
--   THEN
--      RETURN;
--   END IF;
--
--
--   PR_RS_GENERATE_TOTALS_1 ('BANK_GROUP',
--                            'Sum',
--                            'Y',
--                            'Y',
--                            'Total :',
--                            'BANK_GROUP',
--                            'N',
--                            'Y',
--                            TEMPTABLENAME2,
--                            TEMPTABLENAME3,
--                            P_STATUS,
--                            P_ERRORMSG);
--
--   IF P_STATUS != 0
--   THEN
--      RETURN;
--   END IF;
--
--   COMMIT;
--
--
--   EXECUTE IMMEDIATE
--         'delete from '
--      || TEMPTABLENAME2
--      || ' WHERE UPPER(BANK_GROUP) like ''%BAD%Z%'' OR UPPER(BANK_GROUP) like ''%GOOD%Z1%''';
--
--   EXECUTE IMMEDIATE
--         'UPDATE  '
--      || TEMPTABLENAME2
--      || ' SET SBU_DESC=INITCAP(regexp_replace(BANK_GROUP, ''[-: Zz0-9]'', '''')) ||'' Bank'' WHERE SBU_DESC IS NULL ';
--

   COMMIT;

   DBMS_OUTPUT.PUT_LINE ('4 +++++++++++++++++++++++++');

   PR_RS_FILL_SORT_COLUMN (TEMPTABLENAME2,
                           TEMPTABLENAME3,
                           SEQUENCENAME,
                           'BANK_GROUP,SBU',
                           P_STATUS,
                           P_ERRORMSG);

   IF P_STATUS != 0
   THEN
      RETURN;
   END IF;

   DBMS_OUTPUT.PUT_LINE ('5 +++++++++++++++++++++++++');


   NOOFDATACOLUMNS := FN_RS_COUNTDATACOLS (TEMPTABLENAME2);

   PR_RS_POST_TO_STG_TABLE_1 (P_REPORT_ID,
                              P_SESSION_ID,
                              TEMPTABLENAME3,
                              'SBU_DESC',
                              'SBU',
                              P_STATUS,
                              P_ERRORMSG);

   IF P_STATUS != 0
   THEN
      RETURN;
   END IF;

   DBMS_OUTPUT.PUT_LINE ('6 +++++++++++++++++++++++++');



   PR_RS_REM_Z_FROM_STGTBL_COLS (P_REPORT_ID,
                                 P_SESSION_ID,
                                 P_STATUS,
                                 P_ERRORMSG);

   IF P_STATUS != 0
   THEN
      RETURN;
   END IF;

   PR_RS_FIX_FORMAT_ALL_COLUMNS (P_REPORT_ID,
                                 P_SESSION_ID,
                                 NOOFDATACOLUMNS,
                                 'N',
                                 NOOFDECIMALS,
                                 P_STATUS,
                                 P_ERRORMSG);

   IF P_STATUS != 0
   THEN
      RETURN;
   END IF;



   PR_RS_INS_COLUMN_HEADERS_STG (P_REPORT_ID,
                                 P_SESSION_ID,
                                 1,
                                 1,
                                 0,
                                 LEFTTOPTITLE1,
                                 'Y',
                                 P_STATUS,
                                 P_ERRORMSG);

   IF P_STATUS = -1
   THEN
      RETURN;
   END IF;

   PR_RS_INS_COLUMN_HEADERS_STG (P_REPORT_ID,
                                 P_SESSION_ID,
                                 1,
                                 2,
                                 0,
                                 'Pr Mth End',
                                 'N',
                                 P_STATUS,
                                 P_ERRORMSG);

   IF P_STATUS = -1
   THEN
      RETURN;
   END IF;


   START_DATE := P_WEEKSTARTDAY + 1;
   START_DAY :=
      TO_NUMBER (TO_CHAR (TO_DATE (START_DATE, 'DD-MON-RRRR'), 'DD'));



   FOR I IN 1 .. 5
   LOOP
      CASE
         WHEN TO_CHAR (START_DATE, 'DY') IN ('MON')
         THEN
            START_DATE := START_DATE - 3;
         WHEN TO_CHAR (START_DATE, 'DY') IN ('SUN')
         THEN
            START_DATE := START_DATE - 2;
         ELSE
            START_DATE := START_DATE - 1;
      END CASE;

      START_DAY :=
         TO_NUMBER (TO_CHAR (TO_DATE (START_DATE, 'DD-MON-RRRR'), 'DD'));
   END LOOP;

   START_DATE := START_DATE - 1;

   FOR I IN 1 .. 5
   LOOP
      CASE
         WHEN TO_CHAR (START_DATE, 'DY') IN ('FRI')
         THEN
            START_DATE := START_DATE + 3;
         WHEN TO_CHAR (START_DATE, 'DY') IN ('SAT')
         THEN
            START_DATE := START_DATE + 2;
         ELSE
            START_DATE := START_DATE + 1;
      END CASE;

      START_DAY :=
         TO_NUMBER (TO_CHAR (TO_DATE (START_DATE, 'DD-MON-RRRR'), 'DD'));

      PR_RS_INS_COLUMN_HEADERS_STG (
         P_REPORT_ID,
         P_SESSION_ID,
         1,
         (2 + I),
         0,
         TO_CHAR (TO_DATE (START_DATE, 'DD-MON-RRRR'), 'DD-Mon'),
         'N',
         P_STATUS,
         P_ERRORMSG);

      IF P_STATUS = -1
      THEN
         RETURN;
      END IF;

      DBMS_OUTPUT.PUT_LINE (
            '++++++++++++++++++++++++++'
         || TO_CHAR (TO_DATE (START_DATE, 'DD-MON-RRRR'), 'DD-Mon'));
   END LOOP;



   P_STATUS := 0;
   P_ERRORMSG := 'Go';
   RETURN;
END;
/


CREATE OR REPLACE PROCEDURE VISION.PR_RS_MSBDS031 (
   P_VISION_ID        IN     NUMBER,
   P_SESSION_ID       IN     VARCHAR2,
   P_REPORT_ID        IN     VARCHAR2,
   P_SCALING_FACTOR   IN     NUMBER,
   P_PROMPT_VALUE_1   IN     VARCHAR2,
   P_PROMPT_VALUE_2   IN     VARCHAR2,
   P_PROMPT_VALUE_3   IN     VARCHAR2,
   P_PROMPT_VALUE_4   IN     VARCHAR2,
   P_PROMPT_VALUE_5   IN     VARCHAR2,
   P_PROMPT_VALUE_6   IN     VARCHAR2,
   P_PROMPT_VALUE_7   IN     VARCHAR2,
   P_PROMPT_VALUE_8   IN     VARCHAR2,
   P_STATUS              OUT VARCHAR2,
   P_ERRORMSG            OUT VARCHAR2)
   AUTHID CURRENT_USER
IS
   /*
   **************************************************************************************************************************************************
   Name            :    PR_RS_MSBDS011
   Description             :    Daily BS Dashboard
   Calling Script          :
             FN_RS_Get_Temp_Table_Name      ()
           FN_RS_Get_Column_Caption       ()
           FN_RS_Get_Parameter            ()
           PR_RS_Cleanup_Temp_Tables      ()
           PR_RS_Get_Security_Flds        ()
           FN_RS_ParseString              ()
           PR_RS_Execute_Stmt             ()
           PR_RS_Apply_Security_Profile   ()
           PR_RS_Make_All_Cols_Null       ()
           PR_RS_Transpose_1              ()
           PR_RS_Generate_Totals_1        ()
           PR_RS_Suppress_Zeros_Temp_Tbl  ()
           PR_RS_Fill_Sort_Column         ()
           PR_RS_Post_To_Stg_Table        ()
           PR_RS_Rem_Z_From_Stgtbl_Cols   ()
           PR_RS_Remove_Rpt_Cols          ()
           PR_RS_Ins_Column_Headers_STG   ()
           Pr_Rs_Do_Computations          ()

   Assumptions             :
   Input Values        :    For this report, only the first
             P_Prompt_Value_1    - Holds the Legal_Vehicles obtained at the Prompt Screen
             P_Prompt_Value_2    - Holds the Date(Mon-RRRR) obtained at the Prompt Screen
             Others variables from P_Prompt_Value_3 to 5 will be empty (Null)

   Output Values        :    As given below :
             P_Status = -1, if there is an error; P_ErrorMsg will contain the error string
             P_Status =  0, if procedure executes successfully. P_ErrorMsg will contain nothing in this case
             P_Status =  1, Procedure has fetched NO records for the given query criteria. P_ErrorMsg will contain nothing.

   Tables Involved     :
               Vision_Users
                 Report_Suite
                 Reports_STG
                 Column_Headers_STG
                 LE_Book
                 Vision_Risk_Mart,
            Alpha_Sub_Tab
            Num_Sub_Tab
            Tenor_Buckets



   Modifications History :

   No.      Date               Modifier Initials       Description
   01     XX-XXX-2011    IR            Initial Request
   **************************************************************************************************************************************************
   */



   LABELROWNUM                NUMBER (2);
   LABELCOLNUM                NUMBER (2);
   GRANDTOTALCAPTION          VARCHAR2 (50);
   CURRENCY_PROMPT            VARCHAR2 (50);
   SBU                        VARCHAR2 (1000);
   SBU_EX                     VARCHAR2 (1000);
   NOOFDECIMALS               NUMBER (5);
   NOOFDATACOLUMNS            NUMBER (5);
   TABSPACES                  VARCHAR2 (50);

   EXTRACONDITIONFLDS         VARCHAR2 (5000);
   DATACOLUMNS                VARCHAR2 (100);
   SQLSTATEMENT               VARCHAR2 (32000);
   FORMULA                    VARCHAR2 (100);


   FILTERSTRING               VARCHAR2 (5000);
   ERRORMESSAGE               VARCHAR2 (2000);

   J                          NUMBER (4);

   FRLLINE                    VARCHAR2 (20);
   LEBOOK                     VARCHAR2 (100);
   CCYVALUE                   VARCHAR2 (10);

   LEGAL_VEHICLE              VARCHAR2 (30);
   PROMPT_COUNTRY             VARCHAR2 (2);
   PROMPT_LE_BOOK             VARCHAR2 (2);
   PROMPT_DAY                 VARCHAR2 (5);
   PROMPT_MONTH               VARCHAR2 (5);
   PROMPT_YEAR                VARCHAR2 (5);
   TOTAL_DAYS                 NUMBER (10);

   MGT_LINE                   VARCHAR2 (30);

   CURR_PERIOD                VARCHAR2 (20) := NULL;
   PREV_PERIOD                VARCHAR2 (20) := NULL;
   YTD_PERIOD                 VARCHAR2 (20) := NULL;
   CURR_MONTH_DAYS            NUMBER (2) := 0;
   PREV_MONTH_DAYS            NUMBER (2) := 0;

   TEMPTABLENAME              VARCHAR2 (30);
   TEMPTABLENAME2             VARCHAR2 (30);
   TEMPTABLENAME3             VARCHAR2 (30);
   TEMPTABLENAME4             VARCHAR2 (30);
   TEMPTABLENAME5             VARCHAR2 (30);
   TEMPTABLENAME6             VARCHAR2 (30);
   TEMPTABLENAME7             VARCHAR2 (30);
   TEMPTABLENAME8             VARCHAR2 (30);
   TEMPTABLENAME60            VARCHAR2 (30);
   SEQUENCENAME               VARCHAR2 (30);
   APPLFLAGS                  VARCHAR2 (20);


   LEFTTOPTITLE1              VARCHAR2 (50);
   LEFTTOPTITLE2              VARCHAR2 (50);
   LEFTTOPTITLE3              VARCHAR2 (50);
   LEFTTOPTITLE4              VARCHAR2 (50);
   LEFTTOPTITLE5              VARCHAR2 (50);
   PARAM_ID_1                 REPORT_SUITE.PARAMETER_1%TYPE;
   PARAM_ID_2                 REPORT_SUITE.PARAMETER_2%TYPE;
   PARAM_ID_3                 REPORT_SUITE.PARAMETER_3%TYPE;
   PARAM_ID_4                 REPORT_SUITE.PARAMETER_4%TYPE;
   PARAM_ID_5                 VARCHAR2 (500);
   OUCLEVEL                   VARCHAR2 (50);
   PROMPT_SBU                 VARCHAR2 (50);
   OUTJOIN                    VARCHAR2 (10);
   PROMPT_OUC                 OUC_CODES.VISION_OUC%TYPE;
   P_WEEKSTARTDAY             DATE;
   PREV_MONTH                 VARCHAR2 (20);
   PREV_MONTH_LAST_DAY        VARCHAR2 (20);
   LAST_DAY_PREV_MONTH_YEAR   VARCHAR2 (20);
   LAST_DAY_PREV_YEAR_JAN     VARCHAR2 (20);
   PREV_WEEK                  VARCHAR (20);
   SQL_STAT                   VARCHAR (15000);
   SQL_STAT2                  VARCHAR (15000);
   NEXT_MONTH                 VARCHAR (45);
   CUR_WEEK                   VARCHAR (45);
   BUSINESS_DATE              VARCHAR (45);
   CUR_DAY                    VARCHAR (45);
   CURW_DAY                   VARCHAR (45);
   LP                         NUMBER;
   PREV_YEAR                  VARCHAR (45);

   START_DATE                 DATE;
   END_DATE                   NUMBER;
   END_DATE_1                 NUMBER;
   SQLSTATEMENT2              VARCHAR2 (10000);
   START_DAY                  NUMBER;

   YEARMTH                    NUMBER;
BEGIN
   TEMPTABLENAME :=
      FN_RS_GET_TEMP_TABLE_NAME (P_VISION_ID, P_SESSION_ID, '_1');
   TEMPTABLENAME2 :=
      FN_RS_GET_TEMP_TABLE_NAME (P_VISION_ID, P_SESSION_ID, '_2');
   TEMPTABLENAME3 :=
      FN_RS_GET_TEMP_TABLE_NAME (P_VISION_ID, P_SESSION_ID, '_3');
   TEMPTABLENAME4 :=
      FN_RS_GET_TEMP_TABLE_NAME (P_VISION_ID, P_SESSION_ID, '_4');
   TEMPTABLENAME5 :=
      FN_RS_GET_TEMP_TABLE_NAME (P_VISION_ID, P_SESSION_ID, '_5');
   TEMPTABLENAME6 :=
      FN_RS_GET_TEMP_TABLE_NAME (P_VISION_ID, P_SESSION_ID, '_7');
   TEMPTABLENAME7 :=
      FN_RS_GET_TEMP_TABLE_NAME (P_VISION_ID, P_SESSION_ID, '_8');
   TEMPTABLENAME8 :=
      FN_RS_GET_TEMP_TABLE_NAME (P_VISION_ID, P_SESSION_ID, '_9');
   SEQUENCENAME := FN_RS_GET_TEMP_TABLE_NAME (P_VISION_ID, P_SESSION_ID, '_6');

   LEFTTOPTITLE1 := 'CASA Trend';
   LEFTTOPTITLE2 :=
      NVL (FN_RS_GET_COLUMN_CAPTION (P_REPORT_ID,
                                     2,
                                     P_STATUS,
                                     P_ERRORMSG),
           'y');
   LEFTTOPTITLE3 :=
      NVL (FN_RS_GET_COLUMN_CAPTION (P_REPORT_ID,
                                     3,
                                     P_STATUS,
                                     P_ERRORMSG),
           'z');
   LEFTTOPTITLE4 := 'Fund Transfer Price';
   LEFTTOPTITLE5 := 'Int Margin/P';

   PARAM_ID_1 :=
      FN_RS_GET_PARAMETER (P_REPORT_ID,
                           1,
                           'Y',
                           P_STATUS,
                           P_ERRORMSG);
   PARAM_ID_2 :=
      FN_RS_GET_PARAMETER (P_REPORT_ID,
                           2,
                           'Y',
                           P_STATUS,
                           P_ERRORMSG);
   PARAM_ID_3 :=
      FN_RS_GET_PARAMETER (P_REPORT_ID,
                           3,
                           'Y',
                           P_STATUS,
                           P_ERRORMSG);
   PARAM_ID_4 :=
      FN_RS_GET_PARAMETER (P_REPORT_ID,
                           4,
                           'Y',
                           P_STATUS,
                           P_ERRORMSG);
   PARAM_ID_5 :=
      FN_RS_GET_PARAMETER (P_REPORT_ID,
                           5,
                           'Y',
                           P_STATUS,
                           P_ERRORMSG);


   TABSPACES := '          ';
   NOOFDECIMALS := 0;
   GRANDTOTALCAPTION := 'Total :';


   PR_RS_CLEANUP_TEMP_TABLES (P_VISION_ID,
                              P_SESSION_ID,
                              P_STATUS,
                              P_ERRORMSG);

   IF P_STATUS = -1
   THEN
      RETURN;
   END IF;

   FILTERSTRING := NULL;

   PRINT ('--------------------------------', NULL);

   PR_RS_GET_SECURITY_FLDS (
      P_VISION_ID,
      P_REPORT_ID,
      'T2.VISION_OUC,T2.VISION_SBU,T1.Mgt_Line,T2.CUSTOMER_ID,T2.CONTRACT_ID,T2.OFFICE_ACCOUNT,T2.BS_GL,T2.PL_GL,T2.GL_ENRICH_ID,
         T2.SOURCE_ID,T2.RECORD_TYPE,T3.bal_type',
      'Mgt_Result_Headers T2,MGT_EXPANDED T1,Mgt_Result_Balances T3',
      EXTRACONDITIONFLDS,
      FILTERSTRING,
      LEGAL_VEHICLE,
      APPLFLAGS,
      P_STATUS,
      P_ERRORMSG);

   IF P_STATUS = -1
   THEN
      RETURN;
   END IF;



   EXECUTE IMMEDIATE
      'Select  TO_DATE(BUSINESS_DATE,''DD-MON-RRRR'')  FROM VB_DAY WHERE COUNTRY=''VN'''
      INTO BUSINESS_DATE;


   P_WEEKSTARTDAY := (TO_DATE (P_PROMPT_VALUE_3, 'DD-MON-RRRR'));
   CUR_DAY := TO_CHAR (TO_DATE (BUSINESS_DATE, 'DD-MON-RRRR'), 'DD');
   CURW_DAY :=
      TO_CHAR (TRUNC (TO_DATE (BUSINESS_DATE, 'DD-MON-RRRR'), 'WW'), 'DD');
   OUCLEVEL := P_PROMPT_VALUE_1;
   PROMPT_SBU := P_PROMPT_VALUE_2;
   PROMPT_DAY := TO_CHAR ( (TO_DATE (P_WEEKSTARTDAY, 'DD-MON-RRRR')), 'DD');
   PROMPT_MONTH := TO_CHAR (TO_DATE (P_WEEKSTARTDAY, 'DD-MON-RRRR'), 'MM');
   PROMPT_YEAR := TO_CHAR (TO_DATE (P_WEEKSTARTDAY, 'DD-MON-RRRR'), 'RRRR');
   PREV_WEEK := TO_CHAR ( (TO_DATE (P_WEEKSTARTDAY, 'DD-MON-RRRR') - 3), 'DD');
   NEXT_MONTH := PROMPT_MONTH + 1;
   CUR_WEEK := PROMPT_DAY + 4;
   PREV_MONTH_LAST_DAY :=
      TO_CHAR (
         LAST_DAY (ADD_MONTHS (TO_DATE (P_WEEKSTARTDAY, 'DD-MON-RRRR'), -1)),
         'DD');
   PREV_MONTH :=
      TO_CHAR (
         LAST_DAY (ADD_MONTHS (TO_DATE (P_WEEKSTARTDAY, 'DD-MON-RRRR'), -1)),
         'MM');
   LAST_DAY_PREV_MONTH_YEAR :=
      TO_CHAR (
         LAST_DAY (ADD_MONTHS (TO_DATE (P_WEEKSTARTDAY, 'DD-MON-RRRR'), -1)),
         'RRRR');
   LAST_DAY_PREV_YEAR_JAN :=
      TO_CHAR (
         LAST_DAY (ADD_MONTHS (TO_DATE (P_WEEKSTARTDAY, 'DD-MON-RRRR'), -1)),
         'RRRR');
   PREV_YEAR :=
      TO_CHAR (
         LAST_DAY (ADD_MONTHS (TO_DATE (P_WEEKSTARTDAY, 'DD-MON-RRRR'), -1)),
         'YYYY');
   DBMS_OUTPUT.PUT_LINE ('Prev_Week :' || PREV_WEEK);
   DBMS_OUTPUT.PUT_LINE ('+++++++++++++++++++++++0');
   DBMS_OUTPUT.PUT_LINE ('Business_date :' || BUSINESS_DATE);
   DBMS_OUTPUT.PUT_LINE ('+++++++++++++++++++++++0.1');
   DBMS_OUTPUT.PUT_LINE ('Next_Month :' || NEXT_MONTH);
   DBMS_OUTPUT.PUT_LINE ('+++++++++++++++++++++++0.2');
   DBMS_OUTPUT.PUT_LINE ('cur_week :' || CUR_WEEK);
   DBMS_OUTPUT.PUT_LINE ('+++++++++++++++++++++++0.3');
   DBMS_OUTPUT.PUT_LINE ('Last_Day_Prev_Month :' || PREV_MONTH_LAST_DAY);
   DBMS_OUTPUT.PUT_LINE ('+++++++++++++++++++++++0.4');
   DBMS_OUTPUT.PUT_LINE (
      'Last_Day_Prev_Month_Year :' || LAST_DAY_PREV_MONTH_YEAR);
   DBMS_OUTPUT.PUT_LINE ('+++++++++++++++++++++++0.5');
   DBMS_OUTPUT.PUT_LINE (
      'Last_Day_Prev_Year_Jan :' || LAST_DAY_PREV_YEAR_JAN);

   SQL_STAT := NULL;
   SQL_STAT2 := NULL;


   START_DATE := P_WEEKSTARTDAY + 1;
   START_DAY :=
      TO_NUMBER (TO_CHAR (TO_DATE (START_DATE, 'DD-MON-RRRR'), 'DD'));

   FOR I IN 1 .. 5
   LOOP
      CASE
         WHEN TO_CHAR (START_DATE, 'DY') IN ('MON')
         THEN
            START_DATE := START_DATE - 3;
         WHEN TO_CHAR (START_DATE, 'DY') IN ('SUN')
         THEN
            START_DATE := START_DATE - 2;
         ELSE
            START_DATE := START_DATE - 1;
      END CASE;

      START_DAY :=
         TO_NUMBER (TO_CHAR (TO_DATE (START_DATE, 'DD-MON-RRRR'), 'DD'));
      YEARMTH :=
         TO_NUMBER (TO_CHAR (TO_DATE (START_DATE, 'DD-MON-RRRR'), 'RRRRMM'));

      IF I < 5
      THEN
         SQL_STAT :=
               SQL_STAT
            || 'SUM( CASE WHEN T1.YEAR_MONTH='''
            || YEARMTH
            || ''' THEN  BALANCE_'
            || LPAD (START_DAY, 2, '0')
            || ' * RATE_'
            || LPAD (START_DAY, 2, '0')
            || '  ELSE 0 END )  DATA_COLUMN_'
            || LPAD ( (I + 1), 1, '0')
            || ',';
      ELSIF I = 5
      THEN
         SQL_STAT :=
               SQL_STAT
            || 'SUM(CASE WHEN T1.YEAR_MONTH='''
            || YEARMTH
            || ''' THEN BALANCE_'
            || LPAD (START_DAY, 2, '0')
            || '* RATE_'
            || LPAD (START_DAY, 2, '0')
            || '  ELSE 0 END ) DATA_COLUMN_'
            || LPAD ( (I + 1), 1, '0')
            || '';
         EXIT;
      END IF;
   END LOOP;

   DBMS_OUTPUT.PUT_LINE ('++++++++++++++++++++++++++++++++++++++++++');
   DBMS_OUTPUT.PUT_LINE (SQL_STAT);
   DBMS_OUTPUT.PUT_LINE ('++++++++++++++++++++++++++++++++++++++++++');



   LEGAL_VEHICLE := FN_RS_PARSESTRING (P_PROMPT_VALUE_1, 1, '-');
   PROMPT_COUNTRY := FN_RS_PARSESTRING (P_PROMPT_VALUE_1, 2, '-');
   PROMPT_LE_BOOK := FN_RS_PARSESTRING (P_PROMPT_VALUE_1, 3, '-');
   PROMPT_OUC := FN_RS_PARSESTRING (P_PROMPT_VALUE_1, 4, '-');

   LEBOOK := LEGAL_VEHICLE || '-' || PROMPT_COUNTRY || '-' || PROMPT_LE_BOOK;

   BEGIN
      EXECUTE IMMEDIATE
            'SELECT NO_OF_DAYS FROM PERIOD_CONTROLS WHERE YEAR = '
         || PROMPT_YEAR
         || ' AND  MONTH =  '
         || PROMPT_MONTH
         || ' '
         INTO TOTAL_DAYS;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         P_ERRORMSG := 'No Data Found';
      WHEN OTHERS
      THEN
         P_STATUS := -1;
         P_ERRORMSG :=
               'Error while selecting PERIOD_CONTROLS  ! Error Code ['
            || TO_CHAR (SQLCODE)
            || '], Msg ['
            || SQLERRM
            || ']';
         RETURN;
   END;

   DBMS_OUTPUT.PUT_LINE ('Total Days' || TOTAL_DAYS);

   IF PROMPT_OUC <> 'zzzz'
   THEN
      SQLSTATEMENT :=
            'Create Table '
         || TEMPTABLENAME4
         || ' As
         SELECT COUNTRY,LE_BOOK,VISION_OUC
            FROM OUC_EXPANDED WHERE
            (
            (VISION_OUC   ='''
         || PROMPT_OUC
         || ''' OR
            OUC_LEVEL_01 ='''
         || PROMPT_OUC
         || ''' OR
            OUC_LEVEL_02 ='''
         || PROMPT_OUC
         || ''' OR
            OUC_LEVEL_03 ='''
         || PROMPT_OUC
         || ''' OR
            OUC_LEVEL_04 ='''
         || PROMPT_OUC
         || ''' OR
            OUC_LEVEL_05 ='''
         || PROMPT_OUC
         || ''')
            and country='''
         || PROMPT_COUNTRY
         || '''
            and le_book='''
         || PROMPT_LE_BOOK
         || '''
            )';
      OUTJOIN := '';
   ELSE
      SQLSTATEMENT :=
            'Create Table '
         || TEMPTABLENAME4
         || ' As
         SELECT COUNTRY,LE_BOOK,VISION_OUC
            FROM OUC_EXPANDED';
      OUTJOIN := '(+)';
   END IF;

   DBMS_OUTPUT.PUT_LINE (
      '+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++');
   DBMS_OUTPUT.PUT_LINE (SQLSTATEMENT);
   DBMS_OUTPUT.PUT_LINE (
      '+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++');

   PR_RS_EXECUTE_STMT (SQLSTATEMENT,
                       NULL,
                       P_STATUS,
                       ERRORMESSAGE);

   IF P_STATUS != 0
   THEN
      P_ERRORMSG := 'Error Creating Pre Stage - 01 table. ' || ERRORMESSAGE;
      RETURN;
   END IF;


   IF (P_PROMPT_VALUE_2 = '000')
   THEN
      SBU := '';
   ELSE
      SBU :=
            ' AND T1.VISION_SBU IN
            (SELECT VISION_SBU 
             from VISION_SBU 
             where VISION_SBU= '''
         || PROMPT_SBU
         || '''  or PARENT_SBU= '''
         || PROMPT_SBU
         || ''' or BANK_GROUP = '''
         || PROMPT_SBU
         || ''' ) ';
   END IF;

   IF (P_PROMPT_VALUE_4 = '0')
   THEN
      SBU_EX := '';
   ELSE
      SBU_EX := ' AND T1.VISION_SBU NOT IN
            (SELECT VISION_SBU 
             from VISION_SBU 
             where PARENT_SBU IN (''NA'', ''AOP'')) ';
   END IF;

   SQLSTATEMENT :=
         'Create Table '
      || TEMPTABLENAME
      || ' As
    SELECT  BANK_GROUP, SBU,  SBU_DESC,SUM(PREV_MONTH_END) PREV_MONTH_END,SUM(DATA_COLUMN_2)DATA_COLUMN_2,
    SUM(DATA_COLUMN_3)DATA_COLUMN_3,SUM(DATA_COLUMN_4)DATA_COLUMN_4,SUM(DATA_COLUMN_5)DATA_COLUMN_5,SUM(DATA_COLUMN_6)DATA_COLUMN_6
    FROM (
    Select
    BANK_GROUP,NVL(AO_NAME,T1.ACCOUNT_OFFICER) SBU, NVL(AO_NAME,T1.ACCOUNT_OFFICER)    SBU_DESC,
    0 PREV_MONTH_END, 
    '
      || SQL_STAT
      || '
    FROM
    VW_FIN_DLY_HEADERS T1,
    FIN_DLY_BALANCES T2,
    FIN_DLY_MAPPINGS T3,
    ACCOUNT_OFFICERS W,
    (SELECT *
        FROM MGT_EXPANDED T1
       WHERE     T1.MGT_LINE_LEVEL = 1
             AND MGT_LINE != ''G010000'' ) T5,
    CURRENCY_RATES_DAILY T99,VISION_SBU Q 
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
    AND T3.MRL_LINE = T5.SOURCE_MRL_LINE   
    AND T1.COUNTRY=T99.COUNTRY
    AND T1.LE_BOOK=T99.LE_BOOK
    AND T1.CURRENCY=T99.CURRENCY
    AND CATEGORY=''MRATE''
    AND T1.YEAR_MONTH=T99.YEAR_MONTH
    and t5.SOURCE_BAL_TYPE = 1
    AND T5.SOURCE_TYPE = 0
    AND T1.VISION_SBU=Q.VISION_SBU 
    AND T1.VISION_SBU=''B''
    AND t1.CUSTOMER_ID IN (SELECT CUSTOMER_ID 
                            FROM PWT_CBS_USER_BU_SEGMENT 
                            WHERE UPPER(CUSTOMER_USER_SEGMENT)  like ''%NON%BANK%''
                             AND BU_SEGMENT_STATUS=0)
    AND T5.mgt_line IN (''G011441'')                             
    AND T1.ACCOUNT_OFFICER=W.ACCOUNT_OFFICER  (+)
    AND T1.YEAR_MONTH  IN ('''
      || PROMPT_YEAR
      || PROMPT_MONTH
      || ''','''
      || YEARMTH
      || ''' )
    AND T1.RECORD_Type != 9999
    AND T2.BAL_TYPE = 51
    GROUP BY BANK_GROUP,PARENT_SBU,NVL(AO_NAME,T1.ACCOUNT_OFFICER), PARENT_SBU_DESCRIPTION 
     )
    GROUP BY BANK_GROUP,SBU,  SBU_DESC
    Order by 1    ';


   DBMS_OUTPUT.PUT_LINE (SQLSTATEMENT);


   PR_RS_EXECUTE_STMT (SQLSTATEMENT,
                       NULL,
                       P_STATUS,
                       ERRORMESSAGE);

   IF P_STATUS != 0
   THEN
      P_ERRORMSG := 'Error Creating Stage 2 table. ' || ERRORMESSAGE;
      RETURN;
   END IF;



   IF (SQL%ROWCOUNT = 0)
   THEN
      P_STATUS := 1;
      P_ERRORMSG := 'No Records Obtained for the given Criteria';
      RETURN;
   END IF;



   PR_RS_APPLY_SECURITY_PROFILE (
      P_VISION_ID,
      P_REPORT_ID,
         'BANK_GROUP,SBU,SBU_DESC,
             PREV_MONTH_END /'
      || P_SCALING_FACTOR
      || ' Data_Column_1, 
            Data_Column_6/-'
      || P_SCALING_FACTOR
      || '  Data_Column_2,
            Data_Column_5/-'
      || P_SCALING_FACTOR
      || '  Data_Column_3,
            Data_Column_4/-'
      || P_SCALING_FACTOR
      || '  Data_Column_4,
            Data_Column_3/-'
      || P_SCALING_FACTOR
      || '  Data_Column_5,
            Data_Column_2/-'
      || P_SCALING_FACTOR
      || '  Data_Column_6',
      TEMPTABLENAME,
      TEMPTABLENAME2,
      P_STATUS,
      P_ERRORMSG);

   IF P_STATUS != 0
   THEN
      IF P_STATUS = 1
      THEN
         P_ERRORMSG := 'No Records Obtained for the given Criteria';
      END IF;

      RETURN;
   END IF;



   IF (SQL%ROWCOUNT = 0)
   THEN
      P_STATUS := 1;
      P_ERRORMSG := 'No Records Obtained for the given Profile Criteria';
      RETURN;
   END IF;


   DBMS_OUTPUT.PUT_LINE ('0 +++++++++++++++++++++++++');

   IF FN_RS_COUNTTEMPRECORDS (TEMPTABLENAME2) = 0
   THEN
      P_STATUS := 1;
      P_ERRORMSG := 'No Records Obtained for the given Criteria';
      RETURN;
   END IF;

   PR_RS_MAKE_ALL_COLS_NULL (TEMPTABLENAME2, P_STATUS, P_ERRORMSG);

   IF P_STATUS < 0
   THEN
      RETURN;
   END IF;

   PR_RS_ROUND_OFF_COLUMNS (NULL,
                            NOOFDECIMALS,
                            TEMPTABLENAME2,
                            P_STATUS,
                            P_ERRORMSG);

   IF P_STATUS != 0
   THEN
      RETURN;
   END IF;

   DBMS_OUTPUT.PUT_LINE ('3 +++++++++++++++++++++++++');



   DBMS_OUTPUT.PUT_LINE ('5 +++++++++++++++++++++++++');



--   PR_RS_GENERATE_TOTALS_1 ('BANK_GROUP',
--                            'Sum',
--                            'Y',
--                            'N',
--                            'Total :',
--                            'BANK_GROUP',
--                            'Y',
--                            'N',
--                            TEMPTABLENAME2,
--                            TEMPTABLENAME3,
--                            P_STATUS,
--                            P_ERRORMSG);
--
--   IF P_STATUS != 0
--   THEN
--      RETURN;
--   END IF;
--
--
--   PR_RS_GENERATE_TOTALS_1 ('BANK_GROUP',
--                            'Sum',
--                            'Y',
--                            'Y',
--                            'Total :',
--                            'BANK_GROUP',
--                            'N',
--                            'Y',
--                            TEMPTABLENAME2,
--                            TEMPTABLENAME3,
--                            P_STATUS,
--                            P_ERRORMSG);
--
--   IF P_STATUS != 0
--   THEN
--      RETURN;
--   END IF;
--
--   COMMIT;
--
--
--   EXECUTE IMMEDIATE
--         'delete from '
--      || TEMPTABLENAME2
--      || ' WHERE UPPER(BANK_GROUP) like ''%BAD%Z%'' OR UPPER(BANK_GROUP) like ''%GOOD%Z1%''';
--
--   EXECUTE IMMEDIATE
--         'UPDATE  '
--      || TEMPTABLENAME2
--      || ' SET SBU_DESC=INITCAP(regexp_replace(BANK_GROUP, ''[-: Zz0-9]'', '''')) ||'' Bank'' WHERE SBU_DESC IS NULL ';
--

   COMMIT;

   DBMS_OUTPUT.PUT_LINE ('4 +++++++++++++++++++++++++');

   PR_RS_FILL_SORT_COLUMN (TEMPTABLENAME2,
                           TEMPTABLENAME3,
                           SEQUENCENAME,
                           'BANK_GROUP,SBU',
                           P_STATUS,
                           P_ERRORMSG);

   IF P_STATUS != 0
   THEN
      RETURN;
   END IF;

   DBMS_OUTPUT.PUT_LINE ('5 +++++++++++++++++++++++++');


   NOOFDATACOLUMNS := FN_RS_COUNTDATACOLS (TEMPTABLENAME2);

   PR_RS_POST_TO_STG_TABLE_1 (P_REPORT_ID,
                              P_SESSION_ID,
                              TEMPTABLENAME3,
                              'SBU_DESC',
                              'SBU',
                              P_STATUS,
                              P_ERRORMSG);

   IF P_STATUS != 0
   THEN
      RETURN;
   END IF;

   DBMS_OUTPUT.PUT_LINE ('6 +++++++++++++++++++++++++');



   PR_RS_REM_Z_FROM_STGTBL_COLS (P_REPORT_ID,
                                 P_SESSION_ID,
                                 P_STATUS,
                                 P_ERRORMSG);

   IF P_STATUS != 0
   THEN
      RETURN;
   END IF;

   PR_RS_FIX_FORMAT_ALL_COLUMNS (P_REPORT_ID,
                                 P_SESSION_ID,
                                 NOOFDATACOLUMNS,
                                 'N',
                                 NOOFDECIMALS,
                                 P_STATUS,
                                 P_ERRORMSG);

   IF P_STATUS != 0
   THEN
      RETURN;
   END IF;



   PR_RS_INS_COLUMN_HEADERS_STG (P_REPORT_ID,
                                 P_SESSION_ID,
                                 1,
                                 1,
                                 0,
                                 LEFTTOPTITLE1,
                                 'Y',
                                 P_STATUS,
                                 P_ERRORMSG);

   IF P_STATUS = -1
   THEN
      RETURN;
   END IF;

   PR_RS_INS_COLUMN_HEADERS_STG (P_REPORT_ID,
                                 P_SESSION_ID,
                                 1,
                                 2,
                                 0,
                                 'Pr Mth End',
                                 'N',
                                 P_STATUS,
                                 P_ERRORMSG);

   IF P_STATUS = -1
   THEN
      RETURN;
   END IF;


   START_DATE := P_WEEKSTARTDAY + 1;
   START_DAY :=
      TO_NUMBER (TO_CHAR (TO_DATE (START_DATE, 'DD-MON-RRRR'), 'DD'));



   FOR I IN 1 .. 5
   LOOP
      CASE
         WHEN TO_CHAR (START_DATE, 'DY') IN ('MON')
         THEN
            START_DATE := START_DATE - 3;
         WHEN TO_CHAR (START_DATE, 'DY') IN ('SUN')
         THEN
            START_DATE := START_DATE - 2;
         ELSE
            START_DATE := START_DATE - 1;
      END CASE;

      START_DAY :=
         TO_NUMBER (TO_CHAR (TO_DATE (START_DATE, 'DD-MON-RRRR'), 'DD'));
   END LOOP;

   START_DATE := START_DATE - 1;

   FOR I IN 1 .. 5
   LOOP
      CASE
         WHEN TO_CHAR (START_DATE, 'DY') IN ('FRI')
         THEN
            START_DATE := START_DATE + 3;
         WHEN TO_CHAR (START_DATE, 'DY') IN ('SAT')
         THEN
            START_DATE := START_DATE + 2;
         ELSE
            START_DATE := START_DATE + 1;
      END CASE;

      START_DAY :=
         TO_NUMBER (TO_CHAR (TO_DATE (START_DATE, 'DD-MON-RRRR'), 'DD'));

      PR_RS_INS_COLUMN_HEADERS_STG (
         P_REPORT_ID,
         P_SESSION_ID,
         1,
         (2 + I),
         0,
         TO_CHAR (TO_DATE (START_DATE, 'DD-MON-RRRR'), 'DD-Mon'),
         'N',
         P_STATUS,
         P_ERRORMSG);

      IF P_STATUS = -1
      THEN
         RETURN;
      END IF;

      DBMS_OUTPUT.PUT_LINE (
            '++++++++++++++++++++++++++'
         || TO_CHAR (TO_DATE (START_DATE, 'DD-MON-RRRR'), 'DD-Mon'));
   END LOOP;



   P_STATUS := 0;
   P_ERRORMSG := 'Go';
   RETURN;
END;
/

