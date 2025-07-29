

----DATA DUPLICATION CHECK

SELECT cst_id,
count(*) FROM
(
SELECT 
	ci.cst_id,
	ci.cst_key,
	ci.cst_firstname,
	ci.cst_lastname,
	ci.cst_martial_status,
	ci.cst_gndr,
	ci.cst_create_date,
	ca.BDATE,
	ca.GEN,
	la.CNTRY
FROM Silver.CRM_Cust_Info AS ci
LEFT JOIN Silver.ERP_Cust_Az12 AS ca
ON ci.cst_key=ca.CID
LEFT JOIN Silver.ERP_Loc_A101 AS la
on ci.cst_key=la.CID)t
GROUP BY cst_id
HAVING count(*)>1
--Data Integration

SELECT DISTINCT
	ci.cst_gndr,
	ca.GEN,
	CASE WHEN ci.cst_gndr != 'N/A' THEN cst_gndr
	ELSE COALESCE(ca.GEN,'N/A')
	END AS gndr
FROM Silver.CRM_Cust_Info AS ci
LEFT JOIN Silver.ERP_Cust_Az12 AS ca
ON ci.cst_key=ca.CID
LEFT JOIN Silver.ERP_Loc_A101 AS la
on ci.cst_key=la.CID
---Views: Gender Data Quality Check
SELECT DISTINCT gender FROM Gold.DIM_CUSTOMER
---Data Quality Check: Historical Data Filtered OUT
SELECT prd_key, count(*)
FROM
(SELECT 
	pi.prd_id,
	pi.cat_id,
	pi.prd_key,
	pi.prd_nm,
	pi.prd_cost,
	pi.prd_line,
	pi.prd_start_dt,
	pc.CAT,
	pc.SUBCAT,
	pc.MAINTENANCE
FROM Silver.CRM_Prd_Info as pi
LEFT JOIN Silver.ERP_Px_Cat_G1v2 as pc
ON pi.cat_id=pc.ID
where pi.prd_end_dt is null)t---Historical Data is Filtered
GROUP BY prd_key
HAVING count(*) >1
----Foreign Key Inferential Integrity (Dimensions)
SELECT * FROM Gold.Fact_Sales as fs
LEFT JOIN gold.dim_customer as dc
ON fs.customer_number = dc.customer_number
where fs.customer_number is null
LEFT JOIN gold.dim_product as dm
on fs.product_number =dm.product_number 
where fs.product_number is null

