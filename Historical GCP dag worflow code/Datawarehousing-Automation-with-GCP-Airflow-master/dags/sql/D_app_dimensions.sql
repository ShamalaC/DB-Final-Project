
ALTER TABLE dbproject-406721.playstore_staging_dataset.dataset_dbproject
ADD COLUMN IF NOT EXISTS Content_Rating STRING;

-- Create or replace a table with a selected set of columns from the staging dataset
CREATE OR REPLACE TABLE `dbproject-406721.playstore_dataset.D_app_dimensions` AS
SELECT
  IFNULL(App_Name, 'Unknown') AS App_Name,
  App_Id,
  Category
FROM `dbproject-406721.playstore_staging_dataset.dataset_dbproject`;


