-- creating stored procedure to load silver layer

create or alter procedure silver.load_silver as

begin
    declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime -- setting variables
    -- loading data after cleaning and validating into silver.crm_cust_info
    print '====================='
    print 'Loading silver layer'
    print '====================='

    set @batch_start_time = getdate()

    begin try 

        print '====================='
        print 'Loading CRM tables'
        print '====================='

        set @start_time = GETDATE()

        print '>>Truncating table: silver.crm_cust_info'
        truncate table silver.crm_cust_info
        print '>> Inserting data into: silver.crm_cust_info'

        insert into silver.crm_cust_info (
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date)

        select 
        cst_id,
        cst_key,
        -- trimmed unwanted space in first and last names after validating query
        trim(cst_firstname) as cst_firstname,
        trim(cst_lastname) as cst_lastname,

        case when upper(trim(cst_marital_status)) = 'M' then 'Married'
             when upper(trim(cst_marital_status)) = 'S' then 'Single'
	         else 'n/a' -- normalize martital status values into readable format and replaced nulls with n/a
        end cst_marital_status,

        case when upper(trim(cst_gndr)) = 'F' then 'Female'
	         when upper(trim(cst_gndr)) = 'M' then 'Male'
	         else 'n/a' -- normalize gender values into readable format and replaced nulls with n/a
        end cst_gndr,

        cst_create_date
        from
	        (select *,
	        row_number() over (partition by cst_id order by cst_create_date desc) flaglast
	        from bronze.crm_cust_info
	        where cst_id is not null) t
        where flaglast = 1 -- select the most recent record per customer 

        set @end_time = GETDATE()

        print '>> Loading duration: ' +  cast(datediff(second,@start_time, @end_time) as nvarchar) + ' seconds'

        -- loading data after cleaning and validating into silver.crm_prd_info

         set @start_time = GETDATE()

        print '>>Truncating table: silver.crm_prd_info'
        truncate table silver.crm_prd_info;
        print '>> Inserting data into: silver.crm_prd_info'

        insert into silver.crm_prd_info (
        prd_id,
        cat_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt)

        SELECT 
               [prd_id]
              ,replace(SUBSTRING(prd_key,1,5), '-', '_') as cat_id -- extract category id
              ,SUBSTRING(prd_key,7, LEN(prd_key)) as prd_key   -- extract product key
              ,[prd_nm]
              ,coalesce([prd_cost], 0) prd_cost, -- is null used to remove nulls from cost replacing it with 0
               case when upper(trim(prd_line)) = 'M' then 'Mountain'
                     when upper(trim(prd_line)) = 'R' then 'Road'
                     when upper(trim(prd_line)) = 'S' then 'Other Sales'
                     when upper(trim(prd_line)) = 'T' then 'Touring'
                     else 'n/a'
                end as prd_line, -- map product line codes to descriptive values 
               cast([prd_start_dt] as date) as prd_start_dt
              ,cast(lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)-1 as date) as prd_end_dt
              -- calculate end date as one day before the next start date 
          FROM [DataWarehouse].[bronze].[crm_prd_info]

          set @end_time = GETDATE()

          print '>> Loading duration: ' +  cast(datediff(second,@start_time, @end_time) as nvarchar) + ' seconds'



        -- loading data from bronze to silver.crm_sales_details

        set @start_time = getdate()

        print '>>Truncating table: silver.crm_sales_details'
        truncate table silver.crm_sales_details
        print '>> Inserting data into: silver.crm_sales_details'

        insert into silver.crm_sales_details (
            [sls_ord_num],  
            [sls_prd_key], 
            [sls_cust_id],
            [sls_order_dt], 
            [sls_ship_dt],  
            [sls_due_dt],   
            [sls_sales],    
            [sls_quantity], 
            [sls_price])

        SELECT [sls_ord_num]
              ,[sls_prd_key]
              ,[sls_cust_id]
              ,
               case when sls_order_dt = 0 or len(sls_order_dt) != 8 then null
               else cast(cast(sls_order_dt as varchar) as date)
               end sls_order_dt
              ,
               case when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null
               else cast(cast(sls_ship_dt as varchar) as date)
               end sls_ship_dt
              ,-- handling invalid data for all dates coloumns and data type casting from integers to date (int to varchar to date)
               case when sls_due_dt = 0 or len(sls_due_dt) != 8 then null
               else cast(cast(sls_due_dt as varchar) as date)
               end sls_due_dt


              ,case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price) 
               then sls_quantity * abs(sls_price)
               else sls_sales
               end as sls_sales 
               , -- recalculating sales if original value is missing or incorrect 
               [sls_quantity]
      
              ,case when sls_price is null or sls_price <=0 
               then abs(sls_sales)/nullif(sls_quantity,0)
               else sls_price 
               end as sls_price --derive prices if original values are missing or invalid 

          FROM [DataWarehouse].[bronze].[crm_sales_details]

          set @end_time = getdate()

          print '>> Loading duration: ' +  cast(datediff(second,@start_time, @end_time) as nvarchar) + ' seconds'
  
  
          -- clean data load from bronze to silver.erp_cust_az12
      
        print '====================='
        print 'Loading ERP tables'
        print '====================='

        set @start_time = GETDATE()

        print '>>Truncating table: silver.erp_cust_az12'
        truncate table silver.erp_cust_az12
        print '>> Inserting data into: silver.erp_cust_az12'

        insert into silver.erp_cust_az12 (
        cid,
        bdate,
        gen)

        select
        case when cid like 'NAS%' then 
        substring(cid,4,LEN(cid))
        else cid
        end as cid, -- remove NAS prefix if present

        case when bdate > getdate() then null
        else bdate
        end bdate, -- set future birthdates to null

        case when upper(trim(gen)) in ('F','FEMALE') then 'Female'
	         when upper(trim(gen)) in ('M', 'MALE') then 'Male'
	        else 'n/a'
        end gen -- normalize gender values and handle unknown cases
        from bronze.erp_cust_az12

        set @start_time = GETDATE()

        print '>> Loading duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds'


        -- clean data insert from bronze to silver.erp_loc_a101 with data validation & standardization 

        set @start_time = GETDATE() 

        print '>>Truncating table: silver.erp_loc_a101'
        truncate table silver.erp_loc_a101;
        print '>> Inserting data into: silver.erp_loc_a101'

        insert into silver.erp_loc_a101(
        cid,
        cntry)

        select 
        replace(cid, '-', '') as cid, -- handled invalid values and converted cid in order to join with crm_cust_info

        case when trim(cntry) = 'DE' then 'Germany'
	         when trim(cntry) in ('US', 'USA') then 'United States'
	         when trim(cntry) is null or trim(cntry) = '' then 'n/a'
	         else trim(cntry)
        end cntry -- normalizing and handling missing or blank country codes 
        from bronze.erp_loc_a101

        set @end_time = GETDATE() 
    
        print '>> Loading duration ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds'

        -- TABLE bronze.erp_px_cat_g1v2 already had clean data so insert into silver.erp_px_cat_g1v2

        set @start_time = getdate()

        print '>>Truncating table: silver.erp_px_cat_g1v2'
        truncate table silver.erp_px_cat_g1v2
        print '>> Inserting data into: silver.erp_px_cat_g1v2'

        insert into silver.erp_px_cat_g1v2 (
        id,
        cat,
        subcat,
        maintenance)
        select
        id,
        cat,
        subcat,
        maintenance
        from bronze.erp_px_cat_g1v2

        set @end_time = getdate()

        print '>> Loading duration ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds'


        print '---------------------------'
        set @batch_end_time = getdate()
        print 'Total silver loading duration ' + cast(datediff(second, @batch_start_time, @batch_end_time) as nvarchar) + ' seconds'
        print '---------------------------'

    end try
    
    begin catch
            print '=================================='
            print 'Error occured loading silver layer'
            print 'Error Message' + error_message()
            print 'Error Message' + cast(error_number() as nvarchar)
            print 'Error Message' + cast(error_state() as nvarchar)
            print '=================================='
    end catch

end

--exec silver.load_silver
