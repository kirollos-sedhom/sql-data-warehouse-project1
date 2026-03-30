
-- data integration
SELECT DISTINCT
ci.cst_gndr,
cb.gen,
CASE
	WHEN ci.cst_gndr != 'Unknown' THEN ci.cst_gndr -- CRM is the master, the source of truth
	ELSE COALESCE(cb.gen, 'Unknown')
END AS corrected_gender
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 cb
ON ci.cst_key = cb.cid
LEFT JOIN silver.erp_loc_a101 cl
ON ci.cst_key = cl.cid
ORDER BY 1,2






---- check duplicates
SELECT prd_key, COUNT(*) FROM
(
SELECT 
	pri.prd_id,
	pri.cat_id,
	pri.prd_key,
	pri.prd_nm,
	pri.prd_cost,
	pri.prd_line,
	pri.prd_start_dt,
	prc.cat,
	prc.subcat,
	prc.maintenance
FROM silver.crm_prd_info AS pri
LEFT JOIN silver.erp_px_cat_g1v2 AS prc
ON pri.cat_id = prc.id
WHERE pri.prd_end_dt IS NULL -- get current products that didn't end yet. to filter out historical data
) t
GROUP BY prd_key
HAVING COUNT(*) > 1


-- check foreign key integrity (dimensions)
SELECT * FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON f.customer_key = c.customer_key
LEFT JOIN gold.dim_products p
ON f.product_key = p.product_key
WHERE c.customer_key IS NULL OR p.product_key IS NULL



