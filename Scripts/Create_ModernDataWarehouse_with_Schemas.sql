/*
=================================================================
			CREATE DATABASE AND SCHEMAS
=================================================================
Script Overview:
The goal of this script is to create a fresh database named "ModernDataWarehouse". 
Before doing this, the script first checks whether a database with this name already exists. 
If it does, the existing database will be completely removed and then recreated from scratch.
After creating the new database, the script will also set up three distinct schemas within it, 
named "Bronze", "Silver", and "Gold".
Important Note:
Running this script will permanently delete the existing DataWarehouse database along with all the data it contains. 
This action cannot be undone. 
Make sure to create and save proper backups before executing the script. 
Use this script carefully, especially in live environments.
*/
USE master;
GO
-- Drop the 'ModernDataWarehouse' database if it already exists, to allow for recreation
IF EXISTS (SELECT 1 FROM SYS.DATABASES WHERE name= 'ModernDataWarehouse')
BEGIN
	ALTER DATABASE ModernDataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE ModernDataWarehouse;
END;
GO
-- Create 'ModernDataWarehouse' Database
CREATE DATABASE ModernDataWarehouse;
GO
--Create 'Bronze','Silver'& 'Gold' Schemas
CREATE SCHEMA Bronze;
GO
CREATE SCHEMA Silver;
GO
CREATE SCHEMA Gold;
GO


