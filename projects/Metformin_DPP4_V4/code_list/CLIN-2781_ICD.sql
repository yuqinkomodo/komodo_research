USE SANDBOX_KOMODO ;
USE ROLE ANALYST ;
USE WAREHOUSE LARGE_WH ;
CREATE OR REPLACE STAGE stage_metdpp4 FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER = 1);
PUT file://~/komodo_research/projects/Metformin_DPP4_V4/code_list/CLIN-2781_ICD.csv @stage_metdpp4 auto_compress=true overwrite=true;
DROP TABLE IF EXISTS SANDBOX_KOMODO.YWEI.METDPP4_V4_DEF ;
CREATE TABLE SANDBOX_KOMODO.YWEI.METDPP4_V4_DEF (
  Code VARCHAR(255),
  CodeType VARCHAR(255),
  Description VARCHAR(255),
  Type VARCHAR(255),
  Notes VARCHAR(255)
)
;
COPY INTO SANDBOX_KOMODO.YWEI.METDPP4_V4_DEF FROM @stage_metdpp4/CLIN-2781_ICD.csv.gz;
