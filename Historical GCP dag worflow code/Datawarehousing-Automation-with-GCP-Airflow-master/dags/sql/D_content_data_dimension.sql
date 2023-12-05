ALTER TABLE dbproject-406721.playstore_staging_dataset.dataset_dbproject
ADD COLUMN IF NOT EXISTS Content_Rating STRING;

CREATE OR REPLACE TABLE `dbproject-406721.playstore_dataset.D_content_data_dimension` AS
SELECT
  IFNULL(Content_Rating, 'Everyone') AS Content_Rating,
  App_Id,
  Ad_Supported,
  In_App_Purchases,
  Released,  
  Last_Updated
FROM `dbproject-406721.playstore_staging_dataset.dataset_dbproject`;
