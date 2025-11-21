-- ================================
-- FINAL CLEANING SCRIPT (SQLite) - CORRECTED
-- Run as a single script
-- ================================

-- ============================
-- Step 0: Backup original table (in-DB copy)
-- ============================
DROP TABLE IF EXISTS Airbnb_Open_Data_backup;
CREATE TABLE Airbnb_Open_Data_backup AS SELECT * FROM Airbnb_Open_Data;

-- ============================
-- Step 2: Create Airbnb_Data_new with normalized column names
-- ============================
DROP TABLE IF EXISTS Airbnb_Data_new;
CREATE TABLE Airbnb_Data_new AS
SELECT 
    id,
    name,
    host_id,
    host_identity_verified,
    host_name,
    neighbourhood_group,
    neighbourhood,
    instant_bookable,
    cancellation_policy,
    room_type,
    Construction_year    AS construction_year,
    price,
    service_fee,
    minimum_nights,
    availability_365,
    number_of_reviews,
    last_review,
    review_rate_number,
    calculated_host_listings_count
FROM Airbnb_Open_Data;

-- ============================
-- Step 3: Normalize empty strings to NULL for key text columns
-- ============================
UPDATE Airbnb_Data_new SET name = NULL WHERE TRIM(name) = '';
UPDATE Airbnb_Data_new SET host_name = NULL WHERE TRIM(host_name) = '';
UPDATE Airbnb_Data_new SET host_identity_verified = NULL WHERE TRIM(host_identity_verified) = '';
UPDATE Airbnb_Data_new SET neighbourhood = NULL WHERE TRIM(neighbourhood) = '';
UPDATE Airbnb_Data_new SET neighbourhood_group = NULL WHERE TRIM(neighbourhood_group) = '';
UPDATE Airbnb_Data_new SET instant_bookable = NULL WHERE TRIM(instant_bookable) = '';
UPDATE Airbnb_Data_new SET cancellation_policy = NULL WHERE TRIM(cancellation_policy) = '';

-- ============================
-- Step 4: Remove duplicates (keep lowest rowid)
-- ============================
DELETE FROM Airbnb_Data_new
WHERE rowid NOT IN (
  SELECT MIN(rowid)
  FROM Airbnb_Data_new
  GROUP BY id, host_id
);

-- ============================
-- Step 5: Replace listing name NULLs with 'Unnamed Listing'
-- ============================
UPDATE Airbnb_Data_new SET name = 'Unnamed Listing' WHERE name IS NULL;

-- ============================
-- Step 6: Replace host_identity_verified & host_name NULLs
-- ============================
UPDATE Airbnb_Data_new SET host_identity_verified = 'Status not updated' WHERE host_identity_verified IS NULL;
UPDATE Airbnb_Data_new SET host_name = 'Unknown Name/Unknown host' WHERE host_name IS NULL;

-- ============================
-- Step 7: Repair neighbourhood / neighbourhood_group using mappings
-- ============================
-- (A) fill neighbourhood_group from known neighbourhood values
UPDATE Airbnb_Data_new AS t
SET neighbourhood_group = (
  SELECT neighbourhood_group
  FROM Airbnb_Data_new AS x
  WHERE x.neighbourhood = t.neighbourhood
    AND x.neighbourhood_group IS NOT NULL
  LIMIT 1
)
WHERE t.neighbourhood_group IS NULL AND t.neighbourhood IS NOT NULL;

-- (B) fill neighbourhood from most common neighbourhood in that neighbourhood_group
UPDATE Airbnb_Data_new AS t
SET neighbourhood = (
  SELECT neighbourhood FROM (
    SELECT neighbourhood, COUNT(*) AS cnt
    FROM Airbnb_Data_new
    WHERE neighbourhood_group = t.neighbourhood_group AND neighbourhood IS NOT NULL
    GROUP BY neighbourhood
    ORDER BY cnt DESC
    LIMIT 1
  )
)
WHERE t.neighbourhood IS NULL AND t.neighbourhood_group IS NOT NULL;

-- (C) Final fallback labels
UPDATE Airbnb_Data_new SET neighbourhood_group = 'Unknown Group' WHERE neighbourhood_group IS NULL;
UPDATE Airbnb_Data_new SET neighbourhood = 'Unknown Neighbourhood' WHERE neighbourhood IS NULL;

-- ============================
-- Step 8: Replace instant_bookable NULL -> 'Not known' & cancellation_policy NULL -> 'Unknown'
-- ============================
UPDATE Airbnb_Data_new SET instant_bookable = 'Not known' WHERE instant_bookable IS NULL;
UPDATE Airbnb_Data_new SET cancellation_policy = 'Unknown' WHERE cancellation_policy IS NULL;

-- ============================
-- Step 9: Impute construction_year cautiously
-- ============================
UPDATE Airbnb_Data_new
SET construction_year = (
  SELECT ROUND(AVG(construction_year))
  FROM Airbnb_Data_new AS x
  WHERE x.neighbourhood = Airbnb_Data_new.neighbourhood AND x.construction_year IS NOT NULL
)
WHERE construction_year IS NULL;

UPDATE Airbnb_Data_new SET construction_year = -1 WHERE construction_year IS NULL;

-- ============================
-- Step 10: Replace minimum_nights NULL -> 1
-- ============================
UPDATE Airbnb_Data_new SET minimum_nights = 1 WHERE minimum_nights IS NULL;

-- ============================
-- Step 11: availability_365 imputation
-- ============================
UPDATE Airbnb_Data_new
SET availability_365 = (
  SELECT ROUND(AVG(x.availability_365))
  FROM Airbnb_Data_new AS x
  WHERE x.neighbourhood = Airbnb_Data_new.neighbourhood
    AND x.room_type = Airbnb_Data_new.room_type
    AND x.availability_365 IS NOT NULL
)
WHERE availability_365 IS NULL;

UPDATE Airbnb_Data_new
SET availability_365 = (SELECT ROUND(AVG(availability_365)) FROM Airbnb_Data_new WHERE availability_365 IS NOT NULL)
WHERE availability_365 IS NULL;

-- ============================
-- Step 12: number_of_reviews NULL -> 0
-- ============================
UPDATE Airbnb_Data_new SET number_of_reviews = 0 WHERE number_of_reviews IS NULL;

-- ============================
-- Step 13: review_rate_number imputation
-- ============================
UPDATE Airbnb_Data_new
SET review_rate_number = (
  SELECT AVG(x.review_rate_number)
  FROM Airbnb_Data_new AS x
  WHERE x.room_type = Airbnb_Data_new.room_type
    AND x.neighbourhood = Airbnb_Data_new.neighbourhood
    AND x.review_rate_number IS NOT NULL
)
WHERE review_rate_number IS NULL;

UPDATE Airbnb_Data_new
SET review_rate_number = (SELECT AVG(review_rate_number) FROM Airbnb_Data_new WHERE review_rate_number IS NOT NULL)
WHERE review_rate_number IS NULL;

-- ============================
-- Step 14: calculated_host_listings_count NULL -> 1
-- ============================
UPDATE Airbnb_Data_new SET calculated_host_listings_count = 1 WHERE calculated_host_listings_count IS NULL;

-- ============================
-- Step 15: RECREATE TABLE with new numeric columns (FIXED APPROACH)
-- ============================
DROP TABLE IF EXISTS Airbnb_Data_temp;
CREATE TABLE Airbnb_Data_temp AS
SELECT 
    id, name, host_id, host_identity_verified, host_name,
    neighbourhood_group, neighbourhood, instant_bookable, cancellation_policy,
    room_type, construction_year, 
    price, service_fee,
    minimum_nights, availability_365, number_of_reviews, last_review, 
    review_rate_number, calculated_host_listings_count,
    -- Add new columns with initial values
    CAST(NULL AS REAL) AS price_num,
    CAST(NULL AS REAL) AS service_fee_num,
    0 AS price_imputed,
    0 AS service_fee_imputed,
    0 AS has_last_review
FROM Airbnb_Data_new;

-- Drop old and rename
DROP TABLE Airbnb_Data_new;
ALTER TABLE Airbnb_Data_temp RENAME TO Airbnb_Data_new;

-- ============================
-- Step 16: Populate price_num (cleaning)
-- ============================
UPDATE Airbnb_Data_new
SET price_num = (
  CASE
    WHEN price IS NULL OR TRIM(price) = '' THEN NULL
    WHEN INSTR(REPLACE(price,' ',''), '-') > 0 THEN
      CAST(
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
          SUBSTR(REPLACE(price,' ',''), 1, INSTR(REPLACE(price,' ',''), '-')-1)
        ,'$',''),',',''),'(',''),')',''),' ','') AS REAL
      )
    ELSE
      CAST(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TRIM(price), '$', ''), ',', ''), '(', ''), ')', ''), ' ', '') AS REAL)
  END
);

-- ============================
-- Step 17: Populate service_fee_num (cleaning)
-- ============================
UPDATE Airbnb_Data_new
SET service_fee_num = (
  CASE
    WHEN service_fee IS NULL OR TRIM(service_fee) = '' THEN NULL
    WHEN INSTR(REPLACE(service_fee,' ',''), '-') > 0 THEN
      CAST(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
        SUBSTR(REPLACE(service_fee,' ',''), 1, INSTR(REPLACE(service_fee,' ',''), '-')-1)
      ,'$',''),',',''),'(',''),')',''),' ','') AS REAL)
    ELSE
      CAST(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TRIM(service_fee), '$', ''), ',', ''), '(', ''), ')', ''), ' ', '') AS REAL)
  END
);

-- ============================
-- Step 18: Impute price_num
-- ============================
-- Group (room_type, neighbourhood)
UPDATE Airbnb_Data_new
SET price_num = (
  SELECT AVG(x.price_num)
  FROM Airbnb_Data_new AS x
  WHERE x.room_type = Airbnb_Data_new.room_type
    AND x.neighbourhood = Airbnb_Data_new.neighbourhood
    AND x.price_num IS NOT NULL AND x.price_num > 0
)
WHERE price_num IS NULL OR price_num <= 0;

-- Fallback by room_type
UPDATE Airbnb_Data_new
SET price_num = (
  SELECT AVG(x.price_num)
  FROM Airbnb_Data_new AS x
  WHERE x.room_type = Airbnb_Data_new.room_type
    AND x.price_num IS NOT NULL AND x.price_num > 0
)
WHERE price_num IS NULL OR price_num <= 0;

-- Global fallback
UPDATE Airbnb_Data_new
SET price_num = (SELECT AVG(price_num) FROM Airbnb_Data_new WHERE price_num IS NOT NULL AND price_num > 0)
WHERE price_num IS NULL OR price_num <= 0;

-- Mark imputed prices
UPDATE Airbnb_Data_new
SET price_imputed = 1
WHERE price IS NULL OR TRIM(price) = '';

-- ============================
-- Step 19: Impute service_fee_num
-- ============================
-- Group (room_type, neighbourhood)
UPDATE Airbnb_Data_new
SET service_fee_num = (
  SELECT AVG(x.service_fee_num)
  FROM Airbnb_Data_new AS x
  WHERE x.room_type = Airbnb_Data_new.room_type
    AND x.neighbourhood = Airbnb_Data_new.neighbourhood
    AND x.service_fee_num IS NOT NULL AND x.service_fee_num >= 0
)
WHERE service_fee_num IS NULL;

-- Fallback by room_type
UPDATE Airbnb_Data_new
SET service_fee_num = (
  SELECT AVG(x.service_fee_num)
  FROM Airbnb_Data_new AS x
  WHERE x.room_type = Airbnb_Data_new.room_type
    AND x.service_fee_num IS NOT NULL AND x.service_fee_num >= 0
)
WHERE service_fee_num IS NULL;

-- Global fallback
UPDATE Airbnb_Data_new
SET service_fee_num = (SELECT AVG(service_fee_num) FROM Airbnb_Data_new WHERE service_fee_num IS NOT NULL)
WHERE service_fee_num IS NULL;

-- Mark imputed service fees
UPDATE Airbnb_Data_new
SET service_fee_imputed = 1
WHERE service_fee IS NULL OR TRIM(service_fee) = '';

-- ============================
-- Step 20: Set has_last_review flag
-- ============================
UPDATE Airbnb_Data_new
SET has_last_review = 1
WHERE last_review IS NOT NULL AND TRIM(last_review) <> '';

-- ============================
-- VERIFICATION: Final null check
-- ============================
SELECT
    SUM(CASE WHEN id IS NULL THEN 1 ELSE 0 END) AS id_nulls,
    SUM(CASE WHEN name IS NULL THEN 1 ELSE 0 END) AS name_nulls,
    SUM(CASE WHEN host_id IS NULL THEN 1 ELSE 0 END) AS host_id_nulls,
    SUM(CASE WHEN price_num IS NULL THEN 1 ELSE 0 END) AS price_num_nulls,
    SUM(CASE WHEN service_fee_num IS NULL THEN 1 ELSE 0 END) AS service_fee_num_nulls,
    SUM(CASE WHEN price_imputed = 1 THEN 1 ELSE 0 END) AS price_imputed_count,
    SUM(CASE WHEN service_fee_imputed = 1 THEN 1 ELSE 0 END) AS service_fee_imputed_count,
    COUNT(*) AS total_rows
FROM Airbnb_Data_new;

-- ============================
-- END OF SCRIPT
-- ============================