
-- Create the project catalog
CREATE CATALOG IF NOT EXISTS customer_data_platform;

-- Create Bronze, Silver and Gold schemas
CREATE SCHEMA IF NOT EXISTS customer_data_platform.bronze;

CREATE SCHEMA IF NOT EXISTS customer_data_platform.silver;

CREATE SCHEMA IF NOT EXISTS customer_data_platform.gold;



-- Check whether the catalog exists
SHOW CATALOGS LIKE 'customer_data_platform';

-- Check whether the three schemas exist
SHOW SCHEMAS IN customer_data_platform;
