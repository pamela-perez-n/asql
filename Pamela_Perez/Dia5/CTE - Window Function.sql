-- Databricks notebook source
SELECT
  descSituacao,
  count(*) AS qtdePedido
FROM silver.olist.pedido
GROUP BY descSituacao
HAVING count(*) > 100
ORDER BY qtdePedido

-- COMMAND ----------

SELECT *
FROM
(
  SELECT
    descSituacao,
    count(*) AS qtdePedido
  FROM silver.olist.pedido
  GROUP BY descSituacao
  ORDER BY qtdePedido
)
WHERE qtdePedido > 100

-- COMMAND ----------

--Common table expression
WITH 

tb_pedidos_agregados AS 
(
  SELECT
    descSituacao,
    count(*) AS qtdePedido
  FROM silver.olist.pedido
  GROUP BY descSituacao
  ORDER BY qtdePedido
),

tb_status_pedidos_100 AS 
(
  SELECT *
  FROM tb_pedidos_agregados
  WHERE qtdePedido>100
)

SELECT * FROM tb_status_pedidos_100

-- COMMAND ----------

WITH

tb_pedidos AS 
(
  SELECT t1.idPedido, t2.idVendedor, t1.dtPedido
  FROM silver.olist.pedido as t1
  LEFT JOIN silver.olist.item_pedido as t2
  on t1.idPedido=t2.idPedido

  WHERE t1.dtPedido >= "2017-05-01"
  AND t1.dtPedido < "2017-07-01"
),

tb_vendedor AS 
(
select idVendedor,
  date(date_trunc("month", dtPedido)) as dtMes,
  count(distinct idPedido) AS qtdPedidos
from tb_pedidos
GROUP BY idVendedor, dtMes
),

tb_join AS 
(
select t1.*, t2.dtMes AS dtMesProx, t2.qtdPedidos AS qtdPedidosProx, t1.qtdPedidos / t2.qtdPedidos AS txPedidosM1M2
FROM tb_vendedor AS t1
LEFT JOIN tb_vendedor AS t2
ON t1.idVendedor = t2.idVendedor
AND t1.dtMes < t2.dtMes
AND t1.dtMes = add_months(t2.dtMes, -1 )
WHERE t2.qtdPedidos IS NOT NULL 
)


select * FROM tb_join
ORDER BY tb_join.idVendedor, tb_join.dtMes

-- COMMAND ----------

WITH

tb_pedidos AS 
(
  SELECT t1.idPedido, t2.idVendedor, t1.dtPedido
  FROM silver.olist.pedido as t1
  LEFT JOIN silver.olist.item_pedido as t2
  on t1.idPedido=t2.idPedido

  WHERE t1.dtPedido >= "2017-05-01"
  AND t1.dtPedido < "2017-07-01"
),

tb_vendedor AS 
(
select idVendedor,
  date(date_trunc("month", dtPedido)) as dtMes,
  count(distinct idPedido) AS qtdPedidos
from tb_pedidos
WHERE idVendedor IS NOT NULL
GROUP BY idVendedor, dtMes
)


SELECT *,
  LAG(qtdPedidos) OVER (PARTITION BY idVendedor ORDER BY dtMes) as lagPedidos,
  sum(qtdPedidos) OVER (PARTITION BY idVendedor ORDER BY dtMes) as sumPedidos
FROM tb_vendedor
ORDER BY idVendedor, dtMes

-- COMMAND ----------

WITH 
tb_vendedor AS
(
SELECT t1.idVendedor,
        t2.descUF,
        count(DISTINCT t1.idPedido) as qtPedido
FROM silver.olist.item_pedido AS t1
LEFT JOIN silver.olist.vendedor AS t2
ON t1.idVendedor = t2.idVendedor
GROUP BY t1.idVendedor, t2.descUF
),

tb_ranking AS 
(
SELECT *,
  row_number() OVER (PARTITION BY descUF ORDER BY qtPedido DESC) AS rankingVendedor
FROM tb_vendedor
ORDER BY descUF, qtPedido DESC
)

SELECT * FROM tb_ranking
WHERE rankingVendedor<=5

-- COMMAND ----------

WITH 
tb_vendedor AS
(
SELECT t1.idVendedor,
        t2.descUF,
        count(DISTINCT t1.idPedido) as qtPedido
FROM silver.olist.item_pedido AS t1
LEFT JOIN silver.olist.vendedor AS t2
ON t1.idVendedor = t2.idVendedor
GROUP BY t1.idVendedor, t2.descUF
),

tb_ranking AS 
(
SELECT *,
  row_number() OVER (PARTITION BY descUF ORDER BY qtPedido DESC) AS rankingVendedor
FROM tb_vendedor
QUALIFY rankingVendedor <= 5
ORDER BY descUF, qtPedido DESC
)

SELECT * FROM tb_ranking
