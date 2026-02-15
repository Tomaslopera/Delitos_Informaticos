{{ config(
    materialized='table',
    schema='gold'
) }}

WITH delitos_base AS (
    SELECT
        DATE_TRUNC('MONTH', fecha_hecho) AS mes,
        es_ciberdelito,
        delito_id,
        cantidad
    FROM {{ ref('stg_delitos_categorizado') }}
),

monthly_cyber AS (
    SELECT
        mes,
        es_ciberdelito,
        COUNT(DISTINCT delito_id) AS incidentes,
        SUM(cantidad) AS cantidad_total
    FROM delitos_base
    GROUP BY mes, es_ciberdelito
),

totals AS (
    SELECT
        mes,
        SUM(CASE WHEN es_ciberdelito THEN incidentes ELSE 0 END) AS ciberdelitos,
        SUM(incidentes) AS total_delitos,
        SUM(CASE WHEN es_ciberdelito THEN cantidad_total ELSE 0 END) AS cantidad_ciberdelitos,
        SUM(cantidad_total) AS cantidad_total
    FROM monthly_cyber
    GROUP BY mes
)

SELECT
    mes,
    ciberdelitos,
    total_delitos,
    cantidad_ciberdelitos,
    cantidad_total,
    ROUND(100.0 * ciberdelitos / NULLIF(total_delitos, 0), 2) AS porcentaje_ciberdelitos,
    LAG(ciberdelitos) OVER (ORDER BY mes) AS ciberdelitos_mes_anterior,
    ciberdelitos - LAG(ciberdelitos) OVER (ORDER BY mes) AS cambio_mes_anterior
FROM totals
ORDER BY mes DESC