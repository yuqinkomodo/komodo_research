USE SANDBOX_KOMODO ;
USE ROLE ANALYST ;
USE WAREHOUSE LARGE_WH ;
PUT file://~/komodo_research/projects/Metformin_DPP4_V4/code_list/CKD_Codes.csv @stage_metdpp4 auto_compress=true overwrite=true;
DROP TABLE IF EXISTS SANDBOX_KOMODO.YWEI.METDPP4_V4_DEF_CKD ;
CREATE TABLE SANDBOX_KOMODO.YWEI.METDPP4_V4_DEF_CKD (
  Code VARCHAR(255),
  CodeType VARCHAR(255),
  CodeDescription VARCHAR(255),
  Type VARCHAR(255),
  Billable BOOLEAN
)
;
COPY INTO SANDBOX_KOMODO.YWEI.METDPP4_V4_DEF_CKD FROM @stage_metdpp4/CKD_Codes.csv.gz;
