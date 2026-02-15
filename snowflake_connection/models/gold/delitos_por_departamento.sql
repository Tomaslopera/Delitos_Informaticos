{{ config(
    materialized='table',
    schema='gold'
) }}

SELECT
    departamento,
    anio,
    categoria_delito,
    COUNT(*) AS total_incidentes,
    SUM(cantidad) AS total_cantidad,
    COUNT(DISTINCT codigo_municipio) AS municipios_afectados,
    MIN(fecha_hecho) AS primera_fecha,
    MAX(fecha_hecho) AS ultima_fecha
FROM {{ ref('stg_delitos_categorizado') }}
GROUP BY departamento, anio, categoria_delito
ORDER BY anio DESC, total_cantidad DESC