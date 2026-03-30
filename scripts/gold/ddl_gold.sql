CREATE VIEW gold.dim_customers AS
SELECT 
ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key,
ci.cst_id AS customer_id,
ci.cst_key AS customer_number,
ci.cst_firstname AS first_name,
ci.cst_lastname AS last_name,
cl.cntry AS country,
ci.cst_marital_status AS marital_status,
CASE
	WHEN ci.cst_gndr != 'Unknown' THEN ci.cst_gndr -- CRM is the master, the source of truth
	ELSE COALESCE(cb.gen, 'Unknown')
END AS gender,

cb.bdate AS birthdate,
ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 cb
ON ci.cst_key = cb.cid
LEFT JOIN silver.erp_loc_a101 cl
ON ci.cst_key = cl.cid



CREATE VIEW gold.dim_products AS
SELECT 
	ROW_NUMBER() OVER( ORDER BY prd_start_dt, prd_key ) AS product_key,
	pri.prd_id AS product_id,
	pri.prd_key AS product_number,
	pri.prd_nm AS product_name,
	pri.cat_id AS category_id,
	prc.cat AS category,
	prc.subcat AS subcategory,
	prc.maintenance,
	pri.prd_cost AS cost,
	pri.prd_line AS product_line,
	pri.prd_start_dt AS start_date
FROM silver.crm_prd_info AS pri
LEFT JOIN silver.erp_px_cat_g1v2 AS prc
ON pri.cat_id = prc.id
WHERE pri.prd_end_dt IS NULL -- get current products that didn't end yet. to filter out historical data



CREATE VIEW gold.fact_sales AS
SELECT 
sd.sls_ord_num AS order_number,
dp.product_key,
dc.customer_key,
sd.sls_order_dt AS order_date,
sd.sls_ship_dt AS shipping_date,
sd.sls_due_dt AS due_date,
sd.sls_sales AS sales_amount,
sd.sls_quantity AS quantity,
sd.sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products dp
ON dp.product_number = sd.sls_prd_key
LEFT JOIN gold.dim_customers dc
ON dc.customer_id = sd.sls_cust_id

