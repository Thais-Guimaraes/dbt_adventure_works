WITH 
    sales AS (
        SELECT
            facts.invoice_number AS salesorderid,
            facts.gross_value AS grossvalue  -- Mudamos para gross_value
        FROM {{ ref('fact_sales') }} AS facts
    ),
    sales_reasons AS (
        SELECT
            dsr.pk_fk_sales_order_id AS salesorderid,
            dsr.concatenated_reason_names AS reasonname,
            dsr.concatenated_reason_types AS reasontype  -- Adicionamos o tipo de motivo
        FROM {{ ref('dim_sales_reasons') }} AS dsr
    )

SELECT
    sr.reasontype AS tipomotivocompra,  -- Agora agrupamos pelo tipo de motivo
    SUM(s.grossvalue) AS valortotalvendas
FROM sales s
JOIN sales_reasons sr
    ON s.salesorderid = sr.salesorderid
GROUP BY sr.reasontype  -- Agrupando por tipo de motivo
ORDER BY valortotalvendas DESC