USE SANDBOX_KOMODO ;
USE ROLE ANALYST ;
USE WAREHOUSE LARGE_WH ;
PUT file://~/komodo_research/projects/ash_2022/code/code_list.csv @stage_kr auto_compress=true overwrite=true;
DROP TABLE IF EXISTS SANDBOX_KOMODO.AYWEI.SCD_CODE_LIST ;
CREATE TABLE SANDBOX_KOMODO.AYWEI.SCD_CODE_LIST (
  code VARCHAR(255),
  code_description VARCHAR(255),
  codetype VARCHAR(255),
  event VARCHAR(255)
)
;
COPY INTO SANDBOX_KOMODO.AYWEI.SCD_CODE_LIST FROM @stage_kr/code_list.csv.gz;
