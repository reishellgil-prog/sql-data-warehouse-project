/*
=================================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
Script Purpose:
This stored procedure loads data into the 'bronze' schema from external CSV files.
It performs the following actions:
- Truncates the bronze tables before loading data.
- Uses the 'BULK INSERT command to load data from csv Files to bronze tables.
Parameters:
None.
I
This stored plotedure does not accept any parameters or return any values.
Usage Example:
EXEC bronze. load_bronze;
==============================================================================
*/
Create or alter procedure bronze.load_bronze as
begin
declare @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
    begin try 

         set @batch_start_time= getdate (); 
 
        Print '=============================================';
        Print 'Loading the Bronze Layer' ;
        Print '=============================================';
         
        Print '----------------------------------------------';
        Print 'Loading CRM Tables';
        Print '----------------------------------------------';


        Set @start_time= getdate ();
        Print '>> Truncating Table: bronze.crm_cust_info';
        --Loading Bronze Layer
        --Loading and truncating CRM Tables
        Truncate table bronze.crm_cust_info;  -- Vacía la tabla completamente antes de recargar datos (evita duplicados).

        Print '>> Inserting Data Into: bronze.crm_cust_info';

        --BULKS FOR CRM 
        --Method of loading massive amount of data very quickly from files, like csv files (excel) or text file, directly into the database.
        Bulk insert bronze.crm_cust_info
        from 'C:\Users\rgste\Downloads\sql-data-warehouse-project\datasets\source_crm\cust_info.csv' --El location del csv file, al final debes especificarle el nopmbre del archivo (cust_info.csv) igual a como se llama 
        with (
            firstrow= 2,-- esto es desde que fila comienza la info, por ejemplo aqui es en la 2, porque en la 1 estan los nombres de las columnas y eso no nos interesa.
            fieldterminator=',',--como estan divididas las filas, en este caso con (,) abrelo con visual studio code 
            tablock-- esto es una option that as sql is loading the data to this table, it gonna go and lock the whole table. 

            ); 

           Set @end_time= getdate ();
           Print '>> Load Duration: ' + cast (datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';   
           Print '>>--------------';

        Set @start_time= getdate ();
        Print '>> Truncating Table: bronze.crm_prd_info';
        Truncate table bronze.crm_prd_info; 
        Print '>> Inserting Data Into: bronze.crm_prd_info';
        Bulk insert bronze. crm_prd_info
        from 'C:\Users\rgste\Downloads\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        with ( 
            firstrow= 2, 
            fieldterminator=',',
            tablock

            ); 
           Set @end_time= getdate ();
           Print '>> Load Duration: ' + cast (datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';   
           Print '>>--------------';

        Set @start_time= getdate ();
        Print '>> Truncating Table: bronze.crm_sls_info';
        Truncate table bronze. crm_sls_details; 
        Print '>> Inserting Data Into: bronze.crm_sls_info';
        Bulk insert bronze. crm_sls_details 
        from 'C:\Users\rgste\Downloads\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        with (
            firstrow=2, 
            fieldterminator=',', 
            tablock 

            ); 
        Set @end_time= getdate ();
        Print '>> Load Duration: ' + cast (datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';   
        Print '>>--------------';

        Print '----------------------------------------------';
        Print 'Loading ERP Tables';
        Print '----------------------------------------------';

        --BULKS WITH ERP DATA SOURCES 
        --Loading and truncating erp Tables

        Set @start_time= getdate ();
        Print '>> Truncating Table: bronze.erp_loc_a101';
        Truncate table bronze. erp_loc_a101; 
        Print '>> Inserting data Into: bronze. erp_loc_a101';
        Bulk insert bronze.erp_loc_a101
        from 'C:\Users\rgste\Downloads\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        with (
            firstrow= 2, 
            fieldterminator=',',
            tablock

            ); 
        Set @end_time= getdate ();
        Print '>> Load Duration: ' + cast (datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';   
        Print '>>--------------';

        Set @start_time= getdate ();
        Print '>> Truncating Table: bronze.erp_cust_az12';
        Truncate table bronze. erp_cust_az12; 
        Print '>> Inserting data Into: bronze.erp_cust_az12';
        Bulk insert bronze.erp_cust_az12 
        from 'C:\Users\rgste\Downloads\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv' 
        with ( 
             firstrow=2, 
             fieldterminator=',',
             tablock

             );
        Set @end_time= getdate ();
        Print '>> Load Duration: ' + cast (datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';   
        Print '>>--------------';

        Set @start_time= getdate ();
        Print '>> Truncating Table: bronze.erp_px_cat_g1v2';
        Truncate table bronze. erp_px_cat_g1v2; 
        Print '>> Truncating Table: bronze.erp_px_cat_g1v2';
        Bulk insert bronze.erp_px_cat_g1v2
        from 'C:\Users\rgste\Downloads\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv' 
        with ( 
           firstrow=2, 
           fieldterminator=',',
           tablock

           ); 
        Set @end_time= getdate ();
        Print '>> Load Duration: ' + cast (datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';   
        Print '>>--------------';


        -- Nuevo: fin del tiempo total 
        set @batch_end_time= getdate (); 
        
        PRINT '=============================================';
        PRINT 'Loading Bronze Layer is completed';  
        PRINT  '   -Total Load Duration : ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) as nvarchar) + ' seconds'; 
        PRINT '=============================================';
   end try 
   begin catch 
    PRINT '======================================';
    PRINT 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
    PRINT 'Error message: ' + ERROR_MESSAGE();
    PRINT 'Error number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
    PRINT 'Error state: '  + CAST(ERROR_STATE()  AS NVARCHAR);
    PRINT '======================================';
   end catch 
end 

exec bronze.load_bronze

