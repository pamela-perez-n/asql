-- Databricks notebook source
SELECT t1.idPedido,
        t2.descCategoria,
        t3.idCliente,
        t4.descUF

FROM silver.olist.item_pedido AS t1

LEFT JOIN silver.olist.produto AS t2
ON t1.idProduto = t2.idProduto

LEFT JOIN silver.olist.pedido AS t3
ON t1.idPedido=t3.idPedido

LEFT JOIN silver.olist.cliente AS t4
ON t3.idCliente=t4.idCliente

WHERE t2.descCategoria = 'bebes'

-- COMMAND ----------

SELECT        t4.descUF, 
        count(distinct t1.idPedido) AS qtdePedido,
        count(*) AS qtdeItems

FROM silver.olist.item_pedido AS t1

LEFT JOIN silver.olist.produto AS t2
ON t1.idProduto = t2.idProduto

LEFT JOIN silver.olist.pedido AS t3
ON t1.idPedido=t3.idPedido

LEFT JOIN silver.olist.cliente AS t4
ON t3.idCliente=t4.idCliente

WHERE t2.descCategoria = 'bebes'

GROUP BY t4.descUF
HAVING count(distinct t1.idPedido)>10

ORDER BY qtdePedido DESC

-- COMMAND ----------

SELECT t1.idPedido,
        t1.descSituacao,
        date(t1.dtPedido) AS datePedido,
        t2.vlPreco,
        t2.vlFrete,
        t2.vlFrete + t2.vlPreco AS vlTotalReceita

FROM silver.olist.pedido AS t1

LEFT JOIN silver.olist.item_pedido AS t2
ON t1.idPedido = t2.idPedido

WHERE t1.descSituacao = 'delivered'


-- COMMAND ----------

SELECT date(t1.dtPedido) as dataPedido,
                        sum(t2.vlFrete*t2.vlPreco) as TotalReceita

FROM silver.olist.pedido AS t1

LEFT JOIN silver.olist.item_pedido AS t2
ON t1.idPedido = t2.idPedido

WHERE t1.descSituacao = 'delivered'

GROUP BY dataPedido
ORDER BY dataPedido

