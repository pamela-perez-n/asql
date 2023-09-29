-- Databricks notebook source
SELECT count(*), sum(2), count(idProduto), count(DISTINCT idProduto) --Cuentan lineas NO nulas
FROM silver.olist.produto

-- COMMAND ----------

SELECT count(*)
FROM silver.olist.pedido

-- COMMAND ----------

SELECT count(*), sum(2), count(idProduto), count(DISTINCT idProduto) 
FROM silver.olist.item_pedido

-- COMMAND ----------

SELECT avg(vlPesoGramas), 
min(vlPesoGramas), 
max(vlPesoGramas), 
std(vlPesoGramas),
percentile(vlPesoGramas, 0.25),
median(vlPesoGramas),
percentile(vlPesoGramas, 0.75)

FROM silver.olist.produto

-- COMMAND ----------

SELECT descUF, count(DISTINCT idVendedor)
FROM silver.olist.vendedor
GROUP BY descUF
ORDER BY descUF

-- COMMAND ----------

SELECT descCategoria,
  avg(vlPesoGramas) as avgPesoCategoria
FROM silver.olist.produto
GROUP BY descCategoria
ORDER BY avgPesoCategoria DESC

LIMIT 10

-- COMMAND ----------

SELECT date(dtPedido) AS diapedido,
  count(DISTINCT idPedido) as cantidadPedido

FROM silver.olist.pedido
GROUP BY diapedido
ORDER BY diapedido

-- COMMAND ----------

SELECT * 

FROM
(

  SELECT descCategoria,
    count(idProduto) AS cantidadProducto,
    avg(vlPesoGramas) AS avgPeso,
    avg(vlAlturaCM *vlComprimentoCm *vlLarguraCm) as avgVolumen

  FROM silver.olist.produto

  GROUP BY descCategoria
  HAVING cantidadProducto>500


  ORDER BY cantidadProducto DESC

)


-- COMMAND ----------

-- Que estado tiene mas vendedores?

SELECT
  descUF,
  count(distinct idVendedor) AS cantidad
FROM silver.olist.vendedor
WHERE descUF IS NOT NULL
GROUP BY descUF
HAVING cantidad >20
ORDER BY cantidad DESC



-- COMMAND ----------

SELECT *

FROM silver.olist.pedido

WHERE dtPedido <= '2017-12-01'
AND dtPedido >= date('2017-12-01') - INTERVAL 3 MONTH

-- WHERE dtPedido <= NOW()
-- AND dtPedido >= add_months(NOW(), -3)

-- WHERE dtPedido <= NOW()
-- AND dtPedido >= dateadd(month,-3, '2017-12-01')

-- COMMAND ----------

-- Que estado tiene mas vendedores?

SELECT
  descUF,
  count(distinct idVendedor) AS cantidad
FROM silver.olist.vendedor --1
WHERE descUF IS NOT NULL --2
GROUP BY descUF 
HAVING cantidad >20
ORDER BY cantidad DESC
