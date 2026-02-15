{{ config(
    materialized='table',
    schema='gold'
) }}

WITH ranking AS (
    SELECT
        municipio,
        departamento,
        anio,
        COUNT(*) AS total_incidentes,
        SUM(cantidad) AS total_cantidad,
        ROW_NUMBER() OVER (PARTITION BY anio ORDER BY SUM(cantidad) DESC) AS ranking
    FROM {{ ref('stg_delitos_categorizado') }}
    GROUP BY municipio, departamento, anio
)

SELECT
    ranking,
    municipio,
    departamento,
    anio,
    total_incidentes,
    total_cantidad
FROM ranking
WHERE ranking <= 20 
ORDER BY anio DESC, ranking