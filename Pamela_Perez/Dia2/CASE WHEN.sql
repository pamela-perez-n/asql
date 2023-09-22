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
