{{ config(
    materialized='view',
    schema='silver'
) }}

WITH delitos AS (
    SELECT * FROM {{ ref('stg_delitos') }}
),

categorizado AS (
    SELECT
        *,
        
        CASE
            WHEN descripcion_conducta LIKE '%HURTO%' THEN 'HURTO'
            WHEN descripcion_conducta LIKE '%ESTAFA%' THEN 'ESTAFA'
            WHEN descripcion_conducta LIKE '%SUPLANTACION%' THEN 'SUPLANTACION'
            WHEN descripcion_conducta LIKE '%ACCESO ABUSIVO%' THEN 'ACCESO_ABUSIVO'
            WHEN descripcion_conducta LIKE '%TRANSFERENCIA%' THEN 'TRANSFERENCIA_FRAUDULENTA'
            WHEN descripcion_conducta LIKE '%VIOLACION%DATOS%' THEN 'VIOLACION_DATOS'
            ELSE 'OTRO'
        END AS categoria_delito,
        
        CASE
            WHEN descripcion_conducta LIKE '%AGRAVADO%' THEN 'ALTO'
            WHEN descripcion_conducta LIKE '%CALIFICADO%' THEN 'ALTO'
            ELSE 'MEDIO'
        END AS nivel_severidad,
        
        CASE
            WHEN descripcion_conducta LIKE '%INFORMATICO%' OR
                 descripcion_conducta LIKE '%MEDIOS INFORMATICOS%' OR
                 descripcion_conducta LIKE '%ELECTRONICO%' OR
                 descripcion_conducta LIKE '%INTERNET%'
            THEN TRUE
            ELSE FALSE
        END AS es_ciberdelito
        
    FROM delitos
)

SELECT * FROM categorizado