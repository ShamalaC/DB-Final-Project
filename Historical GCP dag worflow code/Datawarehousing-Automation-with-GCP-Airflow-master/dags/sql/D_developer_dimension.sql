
ALTER TABLE dbproject-406721.playstore_staging_dataset.dataset_dbproject
ADD COLUMN IF NOT EXISTS Content_Rating STRING;

CREATE OR REPLACE TABLE `dbproject-406721.playstore_dataset.D_developer_dimension` AS
SELECT
  IFNULL(Developer, 'Unknown') AS Developer,
  App_Id,
  Free,
  Price
FROM `dbproject-406721.playstore_staging_dataset.dataset_dbproject`;


