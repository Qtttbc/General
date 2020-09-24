DROP VIEW VISION.TEMP_OD_LH_VW;

/* Formatted on 5/22/2020 10:07:25 AM (QP5 v5.252.13127.32867) */
CREATE OR REPLACE FORCE VIEW VISION.TEMP_OD_LH_VW
(
   ACCTNO,
   OUC,
   OFFICER_CODE,
   BRANCH,
   OD_SEQUENCE,
   OD_RATE,
   LIMIT,
   BAL_CUR,
   AGGREEMENT_DATE,
   EXPIRY_DATE,
   RATECODE,
   VISION_OUC
)
   BEQUEATH DEFINER
AS
     SELECT DISTINCT
            ACCTNO,
               LPAD (BRANCH, 4, '0')
            || NVL (
                  SUBSTR (
                     TO_CHAR (
                        CASE
                           WHEN TRIM (OFFICER_CODE) LIKE 'KD%'
                           THEN
                              CASE
                                 WHEN SUBSTR (TRIM (OFFICER_CODE), 3, 1) != '0'
                                 THEN
                                       SUBSTR (TRIM (OFFICER_CODE), 3, 3)
                                    || '0'
                                    || SUBSTR (TRIM (OFFICER_CODE), 6, 1)
                                 ELSE
                                       SUBSTR (TRIM (OFFICER_CODE), 4, 2)
                                    || '0'
                                    || SUBSTR (TRIM (OFFICER_CODE), 6, 1)
                              END
                           WHEN TRIM (OFFICER_CODE) LIKE 'XK%'
                           THEN
                              CASE
                                 WHEN SUBSTR (TRIM (OFFICER_CODE), 3, 1) != '0'
                                 THEN
                                       SUBSTR (TRIM (OFFICER_CODE), 3, 3)
                                    || '1'
                                    || CAST (
                                          (  SUBSTR (TRIM (OFFICER_CODE), 6, 1)
                                           - 1) AS VARCHAR (2))
                                 ELSE
                                       SUBSTR (TRIM (OFFICER_CODE), 4, 2)
                                    || '1'
                                    || CAST (
                                          (  SUBSTR (TRIM (OFFICER_CODE), 6, 1)
                                           - 1) AS VARCHAR (2))
                              END
                           WHEN TRIM (OFFICER_CODE) IN ('CONV',
                                                        'MSB.ALC',
                                                        'MSB.QLTD',
                                                        'MSB.BQLTD2')
                           THEN
                              BRANCH || '00'
                           WHEN TRIM (OFFICER_CODE) IS NULL
                           THEN
                              BRANCH || '00'
                           WHEN TRIM (OFFICER_CODE) IN ('CONV',
                                                        'MSB.ALC',
                                                        'MSB.QLTD',
                                                        'MSB.BQLTD2')
                           THEN
                              BRANCH || '00'
                           WHEN SUBSTR (TRIM (OFFICER_CODE), 1, 1) = 'X'
                           THEN
                              TO_CHAR (
                                   TO_NUMBER (TRIM (T1.BRANCH) * 100)
                                 + 10
                                 + TO_NUMBER (
                                      SUBSTR (TRIM (TRIM (OFFICER_CODE)), 6, 1))
                                 - 1)
                           WHEN     TRIM (OFFICER_CODE) <> 'CONV'
                                AND TRIM (OFFICER_CODE) NOT LIKE '%SME%'
                                AND TRIM (OFFICER_CODE) NOT LIKE 'MSB%'
                                AND TRIM (OFFICER_CODE) NOT IN ('CONV',
                                                                'MSB.ALC',
                                                                'MSB.QLTD',
                                                                'MSB.BQLTD2',
                                                                'CNVOPR')
                                AND SUBSTR (TRIM (OFFICER_CODE), 1, 1) <> 'X'
                           THEN
                              TO_CHAR (
                                   TO_NUMBER (T1.BRANCH) * 100
                                 + TO_NUMBER (
                                      SUBSTR (TRIM (TRIM (OFFICER_CODE)), 6, 1)))
                           ELSE
                              BRANCH || '01'
                        END),
                     -2,
                     2),
                  '00')
            || SUBSTR (
                  TO_CHAR (
                     CASE
                        WHEN TRIM (OFFICER_CODE) LIKE 'KQ%'
                        THEN
                           CASE
                              WHEN SUBSTR (TRIM (OFFICER_CODE), 3, 1) != '0'
                              THEN
                                    SUBSTR (TRIM (OFFICER_CODE), 3, 3)
                                 || '0'
                                 || SUBSTR (TRIM (OFFICER_CODE), 6, 1)
                                 || 'P'
                                 || SUBSTR (TRIM (OFFICER_CODE), 7, 1)
                              ELSE
                                    SUBSTR (TRIM (OFFICER_CODE), 4, 2)
                                 || '0'
                                 || SUBSTR (TRIM (OFFICER_CODE), 6, 1)
                                 || 'P'
                                 || SUBSTR (TRIM (OFFICER_CODE), 7, 1)
                           END
                        WHEN TRIM (OFFICER_CODE) LIKE 'XQ%'
                        THEN
                           CASE
                              WHEN SUBSTR (TRIM (OFFICER_CODE), 3, 1) != '0'
                              THEN
                                    SUBSTR (TRIM (OFFICER_CODE), 3, 3)
                                 || '1'
                                 || CAST (
                                       (SUBSTR (TRIM (OFFICER_CODE), 6, 1) - 1) AS VARCHAR (2))
                                 || 'P'
                                 || SUBSTR (TRIM (OFFICER_CODE), 7, 1)
                              ELSE
                                    SUBSTR (TRIM (OFFICER_CODE), 4, 2)
                                 || '1'
                                 || CAST (
                                       (SUBSTR (TRIM (OFFICER_CODE), 6, 1) - 1) AS VARCHAR (2))
                                 || 'P'
                                 || SUBSTR (TRIM (OFFICER_CODE), 7, 1)
                           END
                        WHEN SUBSTR (TRIM (OFFICER_CODE), -2, 2) = 'LH'
                        THEN
                           ('LH')
                        WHEN     SUBSTR (TRIM (OFFICER_CODE), -2, 1) = 'L'
                             AND SUBSTR (TRIM (OFFICER_CODE), -1, 1) IN ('1',
                                                                         '2',
                                                                         '3',
                                                                         '4',
                                                                         '5',
                                                                         '6',
                                                                         '7',
                                                                         '8',
                                                                         '9')
                             AND SUBSTR (TRIM (OFFICER_CODE), -2, 2) != 'LH'
                        THEN
                           SUBSTR (TRIM (OFFICER_CODE), -2, 2)
                        ELSE
                           '00'
                     END),
                  -2,
                  2)
            || '0000'
               OUC,
            TRIM (OFFICER_CODE),
            BRANCH,
            OD_SEQUENCE,
            OD_RATE,
            LIMIT,
            BAL_CUR,
            AGGREEMENT_DATE,
            EXPIRY_DATE,
            RATECODE,
            VISION_OUC
       FROM (SELECT ACCTNO ACCTNO,
                    TRIM (OFFICER_CODE) OFFICER_CODE,
                    BRANCH,
                    xx.OD_SEQUENCE,
                    OD_RATE,
                    LIMIT,
                    BAL_CUR,
                    AGGREEMENT_DATE,
                    EXPIRY_DATE,
                    RATECODE,
                    VISION_OUC
               FROM TEMP_SI_DAT_DDMAST,
                    (SELECT *
                       FROM (SELECT OTACCT AS ACCTNO1,
                                    OTSEQ AS OD_SEQUENCE,
                                    OTRATE AS OD_RATE,
                                    OTDLMT AS LIMIT,
                                    OTABAL AS BAL_CUR,
                                    TO_DATE (OTAGD7, 'rrrrddd')
                                       AS AGGREEMENT_DATE,
                                    TO_DATE (OTEXP7, 'rrrrddd') AS EXPIRY_DATE,
                                    OTRTN AS RATECODE,
                                    COFF.CFOFFR AS OFFICER_CODE,
                                    COFF.CFOCOM,
                                    A.VISION_OUC,
                                    ROW_NUMBER ()
                                    OVER (PARTITION BY OTACCT
                                          ORDER BY OTSEQ DESC)
                                       RN
                               FROM (SELECT * FROM TEMP_SI_DAT_ODTIER) OD,
                                    (SELECT *
                                       FROM TEMP_SI_DAT_CFOFFL
                                      WHERE     CFATYP = 'D'
                                            AND CFOREL = 'OD'
                                            AND TRIM (CFOFFR) IS NOT NULL) COFF,
                                    CONTRACTs A
                              WHERE     OD.OTACCT = COFF.CFACCN(+)
                                    AND TO_CHAR (OD.OTACCT) = A.CONTRACT_ID(+)
                                    AND TO_CHAR (OTSEQ) =
                                           REGEXP_REPLACE (
                                              TRIM (NVL (COFF.CFOCOM(+), '1')),
                                              '[^0-9]') --AND  TO_DATE (OTAGD7, 'rrrrddd')>='01-apr-2020'
                                                       )          --WHERE RN=1
                                                        ) xx
              WHERE ACCTNO = ACCTNO1) T1
   --WHERE   trim(OFFICER_CODE) LIKE '%LH%'
   ORDER BY 1;
