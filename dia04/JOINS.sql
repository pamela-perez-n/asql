-- Databricks notebook source
SELECT *

FROM silver.olist.pedido

RIGHT JOIN silver.olist.cliente
ON silver.olist.cliente.idCliente = silver.olist.pedido.idCliente

WHERE descCidade = 'presidente prudente'

-- COMMAND ----------

SELECT *

FROM silver.olist.pedido AS t1

LEFT JOIN silver.olist.cliente AS t2
ON t1.idCliente = t2.idCliente

WHERE YEAR(dtPedido) = 2017
AND MONTH (dtPedido) = 11

-- COMMAND ----------

SELECT
      t2.descUF,
      count(distinct t1.idPedido) AS qtdePedido

FROM silver.olist.pedido AS t1

LEFT JOIN silver.olist.cliente AS t2
ON t1.idCliente = t2.idCliente

WHERE YEAR(dtPedido) = 2017
AND MONTH (dtPedido) = 11

GROUP BY t2.descUF
ORDER BY qtdePedido DESC

-- COMMAND ----------

SELECT
      date_trunc('month', t1.dtPedido) AS dtMesPedido,
      t2.descUF,
      count(distinct t1.idPedido) AS qtdeCliente

FROM silver.olist.pedido AS t1

LEFT JOIN silver.olist.cliente AS t2
ON t1.idCliente = t2.idCliente

GROUP BY dtMesPedido, t2.descUF
ORDER BY dtMesPedido, qtdePedido DESC

-- COMMAND ----------

SELECT date_trunc('month', '2023-10-31')

-- COMMAND ----------

SELECT dtPedido,
       date_trunc('month', dtPedido)

FROM silver.olist.pedido AS t1

-- COMMAND ----------

SELECT 
      t4.descUF,
      count(distinct t1.idPedido) AS qtdePedido,
      count(*) AS qtdeItens

FROM silver.olist.item_pedido AS t1

LEFT JOIN silver.olist.produto AS t2
ON t2.idProduto = t1.idProduto

LEFT JOIN silver.olist.pedido AS t3
ON t1.idPedido = t3.idPedido

LEFT JOIN silver.olist.cliente AS t4
ON t3.idCliente = t4.idCliente

WHERE t2.descCategoria = 'bebes'

GROUP BY t4.descUF
HAVING count(distinct t1.idPedido) > 10

ORDER BY qtdePedido DESC

-- COMMAND ----------

SELECT 
      t4.descUF,
      count(distinct t1.idPedido) AS qtdePedido,
      count(*) AS qtdeItens

FROM silver.olist.item_pedido AS t1

INNER JOIN silver.olist.produto AS t2
ON t2.idProduto = t1.idProduto

INNER JOIN silver.olist.pedido AS t3
ON t1.idPedido = t3.idPedido

INNER JOIN silver.olist.cliente AS t4
ON t3.idCliente = t4.idCliente

WHERE t2.descCategoria = 'bebes'

GROUP BY t4.descUF
HAVING count(distinct t1.idPedido) > 10

ORDER BY qtdePedido DESC

-- COMMAND ----------

SELECT 
       date(t1.dtPedido) AS dataPedido,
       sum(t2.vlFrete + t2.vlPreco) AS vlTotalReceita

FROM silver.olist.pedido AS t1

LEFT JOIN silver.olist.item_pedido AS t2
ON t1.idPedido = t2.idPedido

WHERE t1.descSituacao = 'delivered'

GROUP BY dataPedido
ORDER BY dataPedido

-- COMMAND ----------

select * from silver.olist.item_pedido
