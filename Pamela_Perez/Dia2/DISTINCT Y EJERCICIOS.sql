-- Databricks notebook source
SELECT DISTINCT descUF, descCidade
FROM silver.olist.cliente
ORDER BY 1 ASC, 2 DESC

-- COMMAND ----------

--Selecione todos os clientes paulistanos
SELECT *
FROM silver.olist.cliente
WHERE descCidade = 'sao paulo'

-- COMMAND ----------

--Selecione todos os clientes paulistas
SELECT *
FROM silver.olist.cliente
WHERE descUF = 'SP'

-- COMMAND ----------

--Selecione todos os vendedores cariocas (ciudad) e paulistas (estado)
SELECT *
FROM silver.olist.vendedor
WHERE UPPER(descUF) in ('SP') OR (UPPER(descCidade) in (UPPER('rio de janeiro')) AND UPPER(descUF) in ('RJ'))

-- COMMAND ----------

--Selecione produtos de perfumaria e bebes com altura maior que 5cm
SELECT *
FROM silver.olist.produto
WHERE descCategoria in ('perfumaria', 'bebes') AND vlAlturaCm > 5

-- COMMAND ----------

--Ej 1 Lista de pedidos com mais de um item.
SELECT *
FROM silver.olist.item_pedido
WHERE idPedidoItem = 2

-- COMMAND ----------

--Lista de pedidos que o frete é mais caro que o item.
SELECT *
FROM silver.olist.item_pedido
WHERE vlFrete>vlPreco

-- COMMAND ----------

--Lista de pedidos que ainda não foram enviados.
SELECT *
FROM silver.olist.pedido
WHERE dtEnvio IS NULL

-- COMMAND ----------

--Lista de pedidos que foram entregues com atraso.
SELECT *
FROM silver.olist.pedido
WHERE dtEntregue>dtEstimativaEntrega

-- COMMAND ----------

--Lista de pedidos que foram entregues com 2 dias de antecedência.
SELECT *
FROM silver.olist.pedido
WHERE datediff( date(dtEstimativaEntrega),date(dtEntregue)) = 2

-- COMMAND ----------

--Lista de pedidos feitos em dezembro de 2017 e entregues com atraso
SELECT *
FROM silver.olist.pedido
WHERE (date(dtPedido)>= '2017-12-01' AND date(dtPedido)<= '2017-12-30')
AND date(dtEstimativaEntrega)<date(dtEntregue)

-- COMMAND ----------

--Lista de pedidos com avaliação maior ou igual que 4
SELECT *
FROM silver.olist.avaliacao_pedido
WHERE vlNota >= 4

-- COMMAND ----------

--Lista de pedidos com 2 ou mais parcelas menores que R$20,00
SELECT *
FROM silver.olist.pagamento_pedido
WHERE nrParcelas>= 2
AND vlPagamento/nrParcelas <= 20

-- COMMAND ----------

--Selecione todos os pedidos e marque se houve atraso ou não
SELECT *,
  CASE WHEN date(dtEstimativaEntrega)<date(dtEntregue) THEN 'CON ATRASO'
  ELSE 'SIN ATRASO'
  END AS Atraso
FROM silver.olist.pedido

-- COMMAND ----------

/*Selecione os pedidos e defina os grupos em uma nova coluna:
para frete inferior à 10%: ‘10%’
para frete entre 10% e 25%: ‘10% a 25%’
para frete entre 25% e 50%: ‘25% a 50%’
para frete maior que 50%: ‘+50%’ */
SELECT *,
  CASE WHEN vlFrete/vlPreco < 0.1 THEN '10%'
  WHEN vlFrete/vlPreco < 0.25 THEN '25%'
  WHEN vlFrete/vlPreco < 0.5 THEN '50%'
  ELSE '+50%'
  END AS Frete_Porcentaje
FROM silver.olist.item_pedido


-- COMMAND ----------


