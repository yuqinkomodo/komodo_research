USE SANDBOX_KOMODO ;
USE ROLE ANALYST ;
USE WAREHOUSE LARGE_WH ;
PUT file://~/komodo_research/projects/ash_2022/subsample_code_list.csv @stage_kr auto_compress=true overwrite=true;
DROP TABLE IF EXISTS SANDBOX_KOMODO.AYWEI.SCD_CODE_LIST_SUB ;
CREATE TABLE SANDBOX_KOMODO.AYWEI.SCD_CODE_LIST_SUB (
  code VARCHAR(255),
  codetype VARCHAR(255),
  code_description VARCHAR(255),
  event VARCHAR(255)
)
;
COPY INTO SANDBOX_KOMODO.AYWEI.SCD_CODE_LIST_SUB FROM @stage_kr/subsample_code_list.csv.gz;
