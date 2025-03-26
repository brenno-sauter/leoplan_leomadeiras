
------------------------------------------------------------------------------------------------------------------
---Create Table leoplanclientes------

CREATE OR REPLACE TABLE `leo-data-d-data-warehouse.leoplan_silver.slv_ClienteLEO` AS
SELECT DISTINCT
    _row_id,
    _batch_id,
  _ingestion_timestamp AS _processing_timestamp,
    Id, 
    CodERP, 
    UPPER(LTRIM(REGEXP_REPLACE(nome, '^[,.-]', ''))) AS nome 
FROM `leo-data-d-data-warehouse.leoplan_bronze.brz_ClienteLEO`
WHERE Id IS NOT NULL
  AND CodERP IS NOT NULL
  AND nome IS NOT NULL;

-------------------------------------------------------------------------------------------------------------------
-----------------Create Table  Vendedor----------------------------------------------------------------------------

CREATE OR REPLACE TABLE `leo-data-d-data-warehouse.leoplan_silver.slv_Vendedor` AS
SELECT DISTINCT
  _row_id,
  _batch_id,
  _ingestion_timestamp AS _processing_timestamp,
  Id,
  UPPER(LTRIM(Nome)) as Nome,
  Email FROM `leo-data-d-data-warehouse.leoplan_bronze.brz_Vendedor` where 
  Id is not null and 
  Nome is not null 

--------------------------------------------------------------------------------------------------------------------
-----------------Create Table Projeto-------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `leo-data-d-data-warehouse.leoplan_silver.slv_Projeto` AS
SELECT DISTINCT 
  _row_id,
  _batch_id,
  _ingestion_timestamp AS _processing_timestamp,
  Id,  
  UPPER(Nome) AS Nome,  
  ClienteFinalId,  
  DATE(DataCriacao) AS DataCriacao,  
  StatusId,  
  ClienteLEOId,  
  VendedorId,  
  OrcamentoNro,  
  OrcamentoDtCriacao,  
  Ativo,  
  DataInativacao,  
  UsuarioInativacao,  
  PedidoId,  
  DATE(DataUltimaAtualizacao) AS DataUltimaAtualizacao,  
  OrigemProjeto,  
  LojaIdErp,  
  PedidoSap,  
  TipoCorte,  
  Migrado,  
  MigradoDate,  
  SemServico 
FROM `leo-data-d-data-warehouse.leoplan_bronze.brz_Projeto`;
---------------------------------------------------------------------------------------------------------------------
------------------

CREATE OR REPLACE TABLE `leo-data-d-data-warehouse.leoplan_silver.slv_ProjetoProduto`  AS
SELECT DISTINCT
  _row_id,
  _batch_id,
  _ingestion_timestamp AS _processing_timestamp,
  Id,  
  TipoProdutoId,  
  Codigo,  
  Descricao,  
  Largura,  
  Espessura,  
  Veio,  
  Tipo,  
  CentroVenda,  
  CentroSaida,  
  ProjetoId,  
  Altura,  
  Quantidade,  
  ProjetoProdutoColorId,  
  CaminhoImagem,  
  OtimizacaoId,  
  OtimizacaoIndex,  
  DescricaoResumida,  
  Canaletado,  
  PermiteSerCanaletado
FROM `leo-data-d-data-warehouse.leoplan_bronze.brz_ProjetoProduto`;

---------------------------------------------------------------------------------------------------------
---------------Matarializando gold-----------------------------------------------------------------------

CREATE OR REPLACE TABLE `leo-data-d-data-warehouse.leoplan_gold.gold_FLeoPlan` AS

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
    P.TipoCorte
ORDER BY P.Id;

