-- Databricks notebook source
SELECT *
FROM silver.olist.avaliacao_pedido

-- COMMAND ----------

SELECT 
  count(distinct idAvaliacao) as num_idAvaliacao,
  count(distinct idPedido) as num_idPedido

FROM silver.olist.avaliacao_pedido

-- COMMAND ----------

SELECT *

FROM silver.olist.produto

-- COMMAND ----------

SELECT *

FROM silver.olist.item_pedido

-- COMMAND ----------

SELECT idPedido, sum(vlPagamento)

FROM silver.olist.pagamento_pedido
where idPedido = 'fa65dad1b0e818e3ccc5cb0e39231352'
GROUP BY idPedido


-- COMMAND ----------

SELECT *, vlPreco+vlFrete

FROM silver.olist.item_pedido
where idPedido = 'fa65dad1b0e818e3ccc5cb0e39231352'

-- COMMAND ----------

SELECT *

FROM silver.olist.pagamento_pedido
where idPedido = '00018f77f2f0320c557190d7a144bdd3'


-- COMMAND ----------

SELECT *

FROM silver.olist.item_pedido
where idPedido = '00018f77f2f0320c557190d7a144bdd3'
--GROUP BY idPedido
