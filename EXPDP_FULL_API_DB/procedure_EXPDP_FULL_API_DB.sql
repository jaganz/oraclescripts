create or replace PROCEDURE     EXPDP_FULL_API_DB IS

/*
*/

/*
****************************************************************************************
****************************************************************************************
!!!!!!!!!!!!EDIT CONFIG FROM HERE!!!!!!!!!!!!
****************************************************************************************
**************************************************************************************** 
*/

DPHNDLR number; 
ORADIREXIST number;
FSDIREXIST number;
ISTNAME VARCHAR2(10 CHAR);
STRSYSDATE VARCHAR(8 CHAR);
STMT_DYN VARCHAR(2000 CHAR);
ORA_DP_DEST_DIR VARCHAR2(1000 CHAR);
FS_DP_DEST_DIR  VARCHAR2(1000 CHAR);
-- check job ends
TEST_JOB_END number; 
-- wrong job ends
CTR_JOB_END number; 
-- export errors
ABNORMAL_END_JOB VARCHAR(2000 CHAR);

/*EXCEPTION*/
FS_DIR_NOT_FOUND EXCEPTION;
FS_ORADIR_NOT_FOUND EXCEPTION;
PRAGMA EXCEPTION_INIT(FS_ORADIR_NOT_FOUND, -22285);
-- Manage timeout proc DBMS_LOCK_SLEEP 
EXPDP_SLEEP_TIMEOUT EXCEPTION;
-- Manage export errors
EXPDP_ORA_ERR EXCEPTION;

-- TODO: Mail reports



/*
**********************************************************************
SUBFUNCTIONS:
**********************************************************************
*/

/*
 FUNZIONE GLOBALE DI LOG
*/
PROCEDURE DP_LOG(
                  COMANDO     VARCHAR2, 
                  EVENTO      VARCHAR2, 
                  DESCRIZIONE VARCHAR2, 
                  TIPO        VARCHAR2
                 ) IS 

--VAR SUBFUNCTION
INST_CALLER VARCHAR2(10 CHAR);
L_INSERT_INTO_LOG VARCHAR2(2000 CHAR); 
R_INSERT_INTO_LOG VARCHAR2(2000 CHAR);
L_GET_SEQ_NEXTVAL VARCHAR2(200 CHAR);
R_GET_SEQ_NEXTVAL VARCHAR2(200 CHAR);
SQ_NXTVAL number;
                 
BEGIN

   -- Get dbname
   INST_CALLER := TRIM(SYS_CONTEXT('userenv','db_name'));
   
   L_INSERT_INTO_LOG  := 'INSERT INTO MP2.FULL_EXPDP_LOG (ID, ISTANZA, COMANDO, EVENTO, DESCRIZIONE, TIPO, DATA) VALUES (:1, :2, :3, :4, :5, :6, :7)';
 
  
    -- get seq
     execute immediate L_GET_SEQ_NEXTVAL INTO SQ_NXTVAL;
     
     /**/
     execute immediate L_INSERT_INTO_LOG USING SQ_NXTVAL, INST_CALLER, COMANDO, EVENTO, DESCRIZIONE, TIPO, SYSDATE;
    /**/
   
   commit;
   
END;

BEGIN

/**/
ISTNAME := TRIM(SYS_CONTEXT('userenv','db_name'));
STRSYSDATE := TO_CHAR(SYSDATE, 'DDMMYYYY');
/**/

--Phisical path to manage
FS_DP_DEST_DIR := '/backup/export/'||ISTNAME||'/NIGHTLY';

--Logical dir to manage
ORA_DP_DEST_DIR := 'SYS_DATAPUMP_NIGHTLY';

/*
********************************************
********************************************
********************************************
********************************************
START SCRIPT
********************************************
********************************************
********************************************
********************************************
*/

DP_LOG('DB: '||ISTNAME||' START EXPDP', 'START', 'EXPORT DATAPUMP NIGHTLY', 'INFO');

  --CHECK LOGICAL DIR
  SELECT COUNT(*) INTO ORADIREXIST 
  FROM ALL_OBJECTS 
  WHERE OBJECT_NAME = ORA_DP_DEST_DIR;
  
 
  --CHECK PHISICAL DIR
  SELECT DBMS_LOB.FILEEXISTS(BFILENAME(ORA_DP_DEST_DIR, '.'))
  INTO FSDIREXIST FROM DUAL;
  
  IF FSDIREXIST = 0 THEN
    DP_LOG('CHECK PHISICAL DIR', 'PHISICAL DIR '||FS_DP_DEST_DIR|| ' NOT FOUND', 'CHECK PHISICAL DIR', 'ERROR');
    RAISE FS_DIR_NOT_FOUND;
  END IF;

--expdp type
DPHNDLR := DBMS_DATAPUMP.OPEN(
                               OPERATION   =>  'EXPORT', 
                               JOB_MODE    =>  'SCHEMA',
                               REMOTE_LINK =>   NULL,
                               JOB_NAME    =>  'DPFULL_' || ISTNAME || '_' || STRSYSDATE || '_JOB',
                               VERSION     =>  'COMPATIBLE'
                              );

DP_LOG('OPEN', 'HANDLE', '1. HANDLE CREATED', 'INFO');

--expdp file
DBMS_DATAPUMP.ADD_FILE(
                        HANDLE     =>  DPHNDLR,
                        FILENAME   =>  'exp_full_%U_'||ISTNAME||'.dp', 
                        DIRECTORY  =>  ORA_DP_DEST_DIR,
                        FILESIZE   =>  NULL,
                        FILETYPE   =>  DBMS_DATAPUMP.KU$_FILE_TYPE_DUMP_FILE,
                        REUSEFILE  =>  1 
                      );
DP_LOG('ADD_FILE', 'EXPORT FILE DEFINITION', '2. EXPORT FILE: '||FS_DP_DEST_DIR||'/exp_full_'||ISTNAME||'.dp', 'INFO');

--log file
DBMS_DATAPUMP.ADD_FILE(
                        HANDLE     =>  DPHNDLR,
                        FILENAME   =>  'exp_full_'||ISTNAME||'_'||STRSYSDATE||'.log',
                        DIRECTORY  =>  ORA_DP_DEST_DIR,
                        FILESIZE   =>  NULL,
                        FILETYPE   =>  DBMS_DATAPUMP.KU$_FILE_TYPE_LOG_FILE,
                        REUSEFILE  =>  1 
                      );
DP_LOG('ADD_FILE', 'LOG FILE DEFINITION:', '3. LOG FILE: '||FS_DP_DEST_DIR||'/exp_full_'||ISTNAME||'.log', 'INFO');

--COMPRESS OPTION 
DBMS_DATAPUMP.SET_PARAMETER(
                              HANDLE =>  DPHNDLR,
                              NAME   =>  'COMPRESSION',
                              VALUE  =>  'ALL'
                           );

DP_LOG('SET_PARAMETER', 'COMPRESSION ON', '5. ENABLE COMPRESSION', 'INFO');

--EXCLUDE SCHEMAS
DBMS_DATAPUMP.METADATA_FILTER(
                HANDLE =>  DPHNDLR,
                NAME   =>  'SCHEMA_EXPR',
                VALUE  =>  'NOT IN(''SYSMAN'',''SYSTEM'',''PATROL'',''OUTLN'',''IGNITE'')'
                );

DP_LOG('METADATA_FILTER', 'SCHEMA EXCLUSION', '6. SCHEMA EXCLUSION', 'INFO');

--PARALL EXPDP
DBMS_DATAPUMP.SET_PARALLEL(
                              HANDLE =>  DPHNDLR,
                              DEGREE  =>  3
                           );
                           
DP_LOG('SET_PARALLEL', 'PARALLEL WORKERS', '7. ENABLE PARALLEL EXPDP', 'INFO');


--Force to use one specific node cluster (service name: EXPDPONINST01_SERVICE)
DBMS_DATAPUMP.START_JOB (
                         HANDLE       => DPHNDLR,
                         SKIP_CURRENT => 0,
                         ABORT_STEP   => 0,
                         CLUSTER_OK   => 0,
                         SERVICE_NAME => 'EXPDPONINST01_SERVICE'
);


DP_LOG('START_JOB', 'START JOB', '8. START DATAPUMP', 'INFO');                        
                        
--Backgrounding
DBMS_DATAPUMP.DETACH(
                      HANDLE =>  DPHNDLR
                    );

-- subroutine Check jobs ends
TEST_JOB_END := 1;
CTR_JOB_END := 0;
WHILE TEST_JOB_END > 0
LOOP
  DBMS_LOCK.SLEEP(300); /* 5 min */
  SELECT COUNT(*) INTO TEST_JOB_END FROM DBA_DATAPUMP_JOBS WHERE STATE = 'EXECUTING';
  CTR_JOB_END := CTR_JOB_END + 1;
  IF CTR_JOB_END > 180 /* after 15 hourse stop waiting */
    THEN RAISE EXPDP_SLEEP_TIMEOUT;
  END IF;
END LOOP;

-- Check LOG 
TEST_JOB_END := 0;
SELECT COUNT(*) INTO TEST_JOB_END FROM MP2.FULL_EXPDP_LOG_EXT WHERE LOG_EXP LIKE '%successfully completed%';
IF TEST_JOB_END = 0
THEN
  SELECT * INTO ABNORMAL_END_JOB FROM MP2.FULL_EXPDP_LOG_EXT WHERE LOG_EXP LIKE 'Job%';
  RAISE EXPDP_ORA_ERR;
ELSE
  DP_LOG('INSTANCE: '||ISTNAME||' DATAPUMP ENDS', 'END', 'EXPORT DATAPUMP SUCCESSFULLY COMPLETED', 'INFO');
END IF;

    EXCEPTION 
       WHEN FS_DIR_NOT_FOUND THEN       
         --logging
         DP_LOG('EXCEPTION: FS_DIR_NOT_FOUND', 'PHISICAL DIR '||FS_DP_DEST_DIR|| ' NOT FOUND', 'STOP STORED PROCEDURE', 'ERROR');
         --TODO: MAIL
       WHEN FS_ORADIR_NOT_FOUND THEN
         --logging
         DP_LOG('EXCEPTION: FS_ORADIR_NOT_FOUND', 'LOGICAL DIR '||ORA_DP_DEST_DIR|| ' NOT FOUND', 'STOP STORED PROCEDURE', 'ERROR');
         --TODO: MAIL
       WHEN EXPDP_ORA_ERR THEN
         --logging
         DP_LOG('EXCEPTION: EXPORT ENDS WITH ERRORS', 'ERROR IS ' ||ABNORMAL_END_JOB, 'STOP STORED PROCEDURE, PLEASE CHECK TABLE FULL_EXPDP_LOG_EXT;', 'ERROR');
         --TODO: MAIL
       WHEN EXPDP_SLEEP_TIMEOUT THEN
         --logging
         DP_LOG('EXCEPTION: EXPORT, TIMEOUT', 'ERRORE IS: DBMS_LOCK_SLEEP WAITING TOO LONG, CHECK JOB' , 'STOP STORED PROCEDURE, PLEASE CHECK TABLE FULL_EXPDP_LOG_EXT;', 'ERROR');
         --TODO: MAIL
       WHEN OTHERS THEN         
         DP_LOG('EXCEPTION: GENERIC ERROR', 'SQL ERROR IS: ' || CODICEERR, 'STOP PROCEDURE. ERROR IS: '||DESCRIERR, 'ERROR');
		 --TODO: MAIL
 END;