-- ***************
-- CLEANING SCRIPT
-- ***************

-- Step 1: Inspect initial data
SELECT *
FROM Airbnb_Open_Data;

-- Step 2: Create a new table without unwanted columns
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
    Construction_year,
    price,
    service_fee,
    minimum_nights,
    availability_365,
    number_of_reviews,
    last_review,
    review_rate_number,
    calculated_host_listings_count
FROM Airbnb_Open_Data;

-- Step 3: Drop old table
DROP TABLE Airbnb_Open_Data;

-- Step 4: Inspect new table
SELECT *
FROM Airbnb_Data_new;
