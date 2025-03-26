CREATE OR REPLACE TABLE `leo-data-d-data-warehouse.leoplan_gold.gold_FLeoPlan_partitioned`
PARTITION BY DataUltimaAtualizacao  -- Particionamento pela coluna DataUltimaAtualizacao
AS

WITH cte_pedidos_desconsiderar AS (
    SELECT DISTINCT 
        P.Id AS ProjetoId
    FROM `leo-data-d-data-warehouse.leoplan_silver.slv_Projeto` AS P
    INNER JOIN `leo-data-d-data-warehouse.leoplan_silver.slv_ProjetoProduto` AS PP
        ON P.Id = PP.ProjetoId
    WHERE LEFT(CAST(PP.codigo AS STRING), 1) <> '5'  
)

SELECT 
    P.DataCriacao AS DataCriacao,
    P.DataUltimaAtualizacao AS DataUltimaAtualizacao,
    P.Id,
    P.Nome,
    CL.CodERP,
    PedidoSap,
    P.LojaIdErp,
    V.Email,
    CASE 
        WHEN SUBSTRING(PP.Codigo, 8, 1) = '-' THEN LEFT(PP.Codigo, 7)
        ELSE PP.Codigo 
    END AS Codigo,
    P.OrigemProjeto AS IdLocalCriacao,
    IF(P.OrigemProjeto = 1, 'LOJA', 'E-COMMERCE') AS LocalCriacao,
    P.TipoCorte AS IdTipoCorte,
    CASE P.TipoCorte 
        WHEN 1 THEN 'Normal'
        WHEN 2 THEN 'TirasHorizontal'
        WHEN 3 THEN 'TirasVertical'
        WHEN 4 THEN 'Canaletado'
        WHEN 5 THEN 'CorteTransporte'
    END AS TipoCorte,
    SUM(PP.Quantidade) AS Quantidade
FROM `leo-data-d-data-warehouse.leoplan_silver.slv_Projeto` AS P
INNER JOIN `leo-data-d-data-warehouse.leoplan_silver.slv_ProjetoProduto` AS PP
    ON P.Id = PP.ProjetoId
INNER JOIN `leo-data-d-data-warehouse.leoplan_silver.slv_ClienteLEO` AS CL
    ON P.ClienteLEOId = CL.Id
INNER JOIN `leo-data-d-data-warehouse.leoplan_silver.slv_Vendedor` AS V
    ON P.VendedorId = V.Id
WHERE 1 = 1
    AND NOT EXISTS (
        SELECT 1 
        FROM cte_pedidos_desconsiderar AS CPD
        WHERE CPD.ProjetoId = P.Id
    )
GROUP BY 
    P.DataCriacao,
    P.DataUltimaAtualizacao,
    P.Id,
    P.Nome,
    CL.CodERP,
    PedidoSap,
    P.LojaIdErp,
    V.Email,
    PP.Codigo,
    P.OrigemProjeto,
    P.TipoCorte;