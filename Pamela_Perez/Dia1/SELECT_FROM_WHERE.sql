-- Databricks notebook source
SELECT *
FROM silver.olist.pedido
WHERE NOT date(dtEstimativaEntrega) >= date(dtEntregue)

-- COMMAND ----------

SELECT *, vlComprimentoCm*vlAlturaCm*vlLarguraCm as nrCM3
FROM silver.olist.produto
WHERE (descCategoria = 'bebes' 
OR descCategoria = 'perfumaria')
AND vlComprimentoCm*vlAlturaCm*vlLarguraCm < 1000

-- COMMAND ----------

SELECT *, vlComprimentoCm*vlAlturaCm*vlLarguraCm as nrCM3
FROM silver.olist.produto
WHERE descCategoria IN ('bebes', 'perfumaria')
AND vlComprimentoCm*vlAlturaCm*vlLarguraCm < 1000

-- COMMAND ----------

SELECT *
FROM silver.olist.produto
WHERE descCategoria LIKE '%ferr%'

-- COMMAND ----------

SELECT *
FROM silver.olist.produto
WHERE STARTSWITH(descCategoria, 'ferr') 
AND CONTAINS(descCategoria, 'ferr')
