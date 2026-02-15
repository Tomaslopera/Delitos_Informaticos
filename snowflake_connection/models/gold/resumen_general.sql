{{ config(
    materialized='table',
    schema='gold'
) }}

SELECT
    COUNT(*) AS total_registros,
    SUM(cantidad) AS total_incidentes,
    COUNT(DISTINCT codigo_departamento) AS total_departamentos,
    COUNT(DISTINCT codigo_municipio) AS total_municipios,
    MIN(fecha_hecho) AS fecha_inicio,
    MAX(fecha_hecho) AS fecha_fin,
    COUNT(DISTINCT CASE WHEN es_ciberdelito THEN delito_id END) AS total_ciberdelitos,
    ROUND(100.0 * COUNT(DISTINCT CASE WHEN es_ciberdelito THEN delito_id END) / COUNT(*), 2) AS porcentaje_ciberdelitos
FROM {{ ref('stg_delitos_categorizado') }}