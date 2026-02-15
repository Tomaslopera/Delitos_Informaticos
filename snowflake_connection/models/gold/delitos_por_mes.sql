{{ config(
    materialized='table',
    schema='gold'
) }}

SELECT
    anio,
    mes,
    DATE_TRUNC('MONTH', fecha_hecho) AS mes_fecha,
    categoria_delito,
    COUNT(*) AS total_incidentes,
    SUM(cantidad) AS total_cantidad,
    COUNT(DISTINCT codigo_departamento) AS departamentos_afectados
FROM {{ ref('stg_delitos_categorizado') }}
GROUP BY anio, mes, mes_fecha, categoria_delito
ORDER BY mes_fecha DESC, total_cantidad DESC