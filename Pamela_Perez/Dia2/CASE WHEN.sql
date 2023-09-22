-- Databricks notebook source
SELECT *,
        CASE WHEN descUF = 'SP' THEN 'Paulista' 
        WHEN descUF = 'PR' THEN 'Paranense'
        WHEN descUF = 'RJ' THEN 'Fluminense'
        ELSE 'Desconocido'
        END AS naturalidade,

        CASE WHEN descUF IN ('SP', 'RJ') THEN 'Sudeste' 
        WHEN descUF IN ('PR', 'RC') THEN 'Sur'
        ELSE 'Desconocido'
        END AS region

FROM silver.olist.cliente

-- COMMAND ----------

/*Selecione a tabela silver.olist.produto :
Crie uma coluna nova chamada ‘descNovaCategoria’ seguindo:
agrupe ‘alimentos’ e ‘alimentos_bebidas’ em ‘alimentos’
agrupe ‘artes’ e ‘artes_e_artesanato’ em ‘artes’
agrupe todas categorias de construção em uma única categoria chamada ‘construção’
Cria uma coluna nova chamada ‘descPeso’
para peso menor que 2kg: ‘leve’
para peso entre 2kg e 5kg: ‘médio’
para peso entre 5kg e 10kg: ‘pesado’
para peso maior que 10kg: ‘muito pesado’ */

SELECT *,
    CASE WHEN descCategoria in ('alimentos', 'alimentos_bebidas') THEN 'alimentos'
    WHEN descCategoria in ('artes', 'artes_e_artesanato') THEN 'artes'
    WHEN descCategoria LIKE '%constru%' THEN 'construção'
    ELSE descCategoria
    END AS descNovaCategoria,

    CASE WHEN vlPesoGramas <2000 THEN 'leve'
    WHEN vlPesoGramas <5000  THEN 'medio'
    WHEN vlPesoGramas <10000  THEN 'pesado'
    ELSE 'muy pesado'
    END AS descPeso
FROM silver.olist.produto
