-- Databricks notebook source
SELECT *
FROM silver.olist.pedido
-- Selecione todas las columnas de la tabla silver.olist.pedido

-- COMMAND ----------

SELECT
      idPedido,
      idCliente,
      dtEntregue
FROM silver.olist.pedido
--Seleccione las 3 columnas de la tabla silver.olist.pedido

-- COMMAND ----------

SELECT 
    idPedido,
    idCliente,
    dtEntregue,
    dtEstimativaEntrega,
    date(dtEntregue) AS dateEntregue,
    date(dtEstimativaEntrega) AS dateEstimativa,
    dtEntregue > dtEstimativaEntrega AS flAtraso,
    date(dtEntregue) > date(dtEstimativaEntrega) AS flDateAtraso,
    date_diff(dtEntregue, dtEstimativaEntrega) AS nrDiasEntrega
FROM silver.olist.pedido

-- COMMAND ----------

SELECT *,
      vlComprimentoCm * vlAlturaCm * vlLarguraCm AS volCM3
FROM silver.olist.produto

-- COMMAND ----------


