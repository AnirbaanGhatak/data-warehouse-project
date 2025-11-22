/* 

Create Database and Schemas

Script Purpose:
	This script creates a new database named DataWarehouse after checking if it already exists.
	If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas within the database:
	bronze, silver and gold

Warning:
	Running this script will drip the entire 'DataWarehouse' DB if it exists.
	All data in the DB will be permanently deleted. PRoceed with Caution.
	ensure backups before running this script.

*/





USE master;

--Drop and Recreate DataWarehouse DB --

IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;

GO

CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

CREATE SCHEMA bronze;

CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;

