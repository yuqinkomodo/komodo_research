USE SANDBOX_KOMODO ;
USE ROLE ANALYST ;
USE WAREHOUSE LARGE_WH ;
PUT file://~/komodo_research/projects/Metformin_DPP4_V4/code_list/CLIN-2781_NDC.csv @stage_metdpp4 auto_compress=true overwrite=true;
DROP TABLE IF EXISTS SANDBOX_KOMODO.YWEI.METDPP4_V4_DEF_NDC ;
CREATE TABLE SANDBOX_KOMODO.YWEI.METDPP4_V4_DEF_NDC (
  NDC VARCHAR(255),
  PRODUCT_NAME VARCHAR(255),
  ACTIVE BOOLEAN,
  START_DATE VARCHAR(255),
  END_DATE VARCHAR(255),
  BRAND_GENERIC VARCHAR(255),
  MULTI_INGREDIENT BOOLEAN
)
;
COPY INTO SANDBOX_KOMODO.YWEI.METDPP4_V4_DEF_NDC FROM @stage_metdpp4/CLIN-2781_NDC.csv.gz;