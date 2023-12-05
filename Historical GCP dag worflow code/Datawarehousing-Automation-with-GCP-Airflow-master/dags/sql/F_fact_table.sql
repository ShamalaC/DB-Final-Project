CREATE OR REPLACE TABLE
  `dbproject-406721.playstore_dataset.F_fact_table` AS
SELECT  App_Id,Rating,
  Rating_Count,
  Installs
  FROM `dbproject-406721.playstore_staging_dataset.dataset_dbproject`;

