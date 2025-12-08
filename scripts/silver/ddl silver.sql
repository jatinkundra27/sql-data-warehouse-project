-- silver layer ddl

if OBJECT_ID('[silver].[crm_cust_info]', 'U') is not null
drop table [silver].[crm_cust_info];

CREATE TABLE [silver].[crm_cust_info] (
    [cst_id]             INT           NULL,
    [cst_key]            NVARCHAR (50) NULL,
    [cst_firstname]      NVARCHAR (50) NULL,
    [cst_lastname]       NVARCHAR (50) NULL,
    [cst_marital_status] NVARCHAR (50) NULL,
    [cst_gndr]           NVARCHAR (50) NULL,
    [cst_create_date]    DATE          NULL,
    dwh_create_date      datetime2 default  getdate()
)

if OBJECT_ID('[silver].[crm_prd_info]', 'U') is not null
drop table [silver].[crm_prd_info];

CREATE TABLE [silver].[crm_prd_info] (
    [prd_id]       INT           NULL,
	[cat_id]      NVARCHAR (50) NULL
    [prd_key]      NVARCHAR (50) NULL,
    [prd_nm]       NVARCHAR (50) NULL,
    [prd_cost]     INT           NULL,
    [prd_line]     NVARCHAR (50) NULL,
    [prd_start_dt] date      NULL,
    [prd_end_dt]   date      NULL,
    dwh_create_date      datetime2 default  getdate()
)

if OBJECT_ID('[silver].[crm_sales_details]', 'U') is not null
drop table [silver].[crm_sales_details];

CREATE TABLE [silver].[crm_sales_details] (
    [sls_ord_num]  NVARCHAR (50) NULL,
    [sls_prd_key]  NVARCHAR (50) NULL,
    [sls_cust_id]  INT           NULL,
    [sls_order_dt] date           NULL,
    [sls_ship_dt]  date           NULL,
    [sls_due_dt]   date           NULL,
    [sls_sales]    INT           NULL,
    [sls_quantity] INT           NULL,
    [sls_price]    INT           NULL,
    dwh_create_date      datetime2 default  getdate()
)

if OBJECT_ID('[silver].[erp_cust_az12]', 'U') is not null
drop table [silver].[erp_cust_az12];

CREATE TABLE [silver].[erp_cust_az12] (
    [cid]   NVARCHAR (50) NULL,
    [bdate] DATE          NULL,
    [gen]   NVARCHAR (50) NULL,
     dwh_create_date      datetime2 default  getdate()
)

if OBJECT_ID('[silver].[erp_loc_a101]', 'U') is not null
drop table [silver].[erp_loc_a101];

CREATE TABLE [silver].[erp_loc_a101] (
    [cid]   NVARCHAR (50) NULL,
    [cntry] NVARCHAR (50) NULL,
     dwh_create_date      datetime2 default  getdate()
)

if OBJECT_ID('[silver].[erp_px_cat_g1v2]', 'U') is not null
drop table [silver].[erp_px_cat_g1v2];

CREATE TABLE [silver].[erp_px_cat_g1v2] (
    [id]          NVARCHAR (50) NULL,
    [cat]         NVARCHAR (50) NULL,
    [subcat]      NVARCHAR (50) NULL,
    [maintenance] NVARCHAR (50) NULL,
     dwh_create_date      datetime2 default  getdate()  

)

