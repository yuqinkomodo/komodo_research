USE SANDBOX_KOMODO ;
USE ROLE ANALYST ;
USE WAREHOUSE LARGE_WH ;
CREATE OR REPLACE STAGE stage_kr FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER = 1);
PUT file://~/komodo_research/projects/cost_compare_truven_lbp/spinal_surgery.csv @stage_kr auto_compress=true overwrite=true;
DROP TABLE IF EXISTS SANDBOX_KOMODO.AYWEI.LBP_DEF_spinal_surgery ;
CREATE TABLE SANDBOX_KOMODO.AYWEI.LBP_DEF_spinal_surgery (
  Code VARCHAR(255),
  CodeType VARCHAR(255),
  Description VARCHAR(255),
  Type VARCHAR(255)
)
;
COPY INTO SANDBOX_KOMODO.AYWEI.LBP_DEF_spinal_surgery FROM @stage_kr/spinal_surgery.csv.gz;
