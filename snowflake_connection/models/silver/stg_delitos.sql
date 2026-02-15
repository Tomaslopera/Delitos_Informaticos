{{ config(
    materialized='view',
    schema='silver'
) }}

WITH source AS (
    SELECT * FROM {{ source('bronze', 'delitos_raw') }}
),

cleaned AS (
    SELECT
        CAST(cantidad AS INTEGER) AS cantidad,
        
        LPAD(cod_depto, 2, '0') AS codigo_departamento,
        LPAD(cod_muni, 5, '0') AS codigo_municipio,
        
        TRIM(UPPER(departamento)) AS departamento,
        TRIM(UPPER(municipio)) AS municipio,
        
        TRIM(descripcion_conducta) AS descripcion_conducta,
        
        fecha_hecho::DATE AS fecha_hecho,
        
        YEAR(fecha_hecho) AS anio,
        MONTH(fecha_hecho) AS mes,
        QUARTER(fecha_hecho) AS trimestre,
        DAYOFWEEK(fecha_hecho) AS dia_semana,
        
        ingestion_timestamp,
        
        MD5(CONCAT(
            COALESCE(cod_muni, ''),
            COALESCE(fecha_hecho::VARCHAR, ''),
            COALESCE(descripcion_conducta, '')
        )) AS delito_id
        
    FROM source
    WHERE fecha_hecho IS NOT NULL
      AND cantidad > 0
)

SELECT * FROM cleaned