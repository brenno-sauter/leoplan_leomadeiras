MERGE INTO `leo-data-d-data-warehouse.leoplan_silver.slv_ClienteLEO` AS silver
USING `leo-data-d-data-warehouse.leoplan_bronze.brz_ClienteLEO` AS bronze
ON silver.Id = bronze.Id
WHEN MATCHED THEN
  UPDATE SET
    silver.CodERP = bronze.CodERP,
    silver.nome = UPPER(LTRIM(REGEXP_REPLACE(bronze.nome, '^[,.-]', ''))),
    silver._processing_timestamp = CURRENT_TIMESTAMP()  -- Mantém hora e UTC
WHEN NOT MATCHED THEN
  INSERT (Id, CodERP, nome, _processing_timestamp)
  VALUES (bronze.Id, bronze.CodERP, UPPER(LTRIM(REGEXP_REPLACE(bronze.nome, '^[,.-]', ''))), CURRENT_TIMESTAMP());  -- Mantém hora e UTC
------------------------------------------------------------------------------------------------------------------------------------------
MERGE INTO `leo-data-d-data-warehouse.leoplan_silver.slv_Vendedor` AS silver
USING `leo-data-d-data-warehouse.leoplan_bronze.brz_Vendedor` AS bronze
ON silver.Id = bronze.Id
WHEN MATCHED THEN
  UPDATE SET
    silver.Nome = UPPER(LTRIM(bronze.Nome)),
    silver.Email = bronze.Email,
    silver._processing_timestamp = CURRENT_TIMESTAMP()  -- Mantém hora e UTC
WHEN NOT MATCHED THEN
  INSERT (Id, Nome, Email, _processing_timestamp)
  VALUES (bronze.Id, UPPER(LTRIM(bronze.Nome)), bronze.Email, CURRENT_TIMESTAMP());  -- Mantém hora e UTC
---------------------------------------------------------------------------------------------------------------------------------------------------
MERGE INTO `leo-data-d-data-warehouse.leoplan_silver.slv_Projeto` AS silver
USING `leo-data-d-data-warehouse.leoplan_bronze.brz_Projeto` AS bronze
ON silver.Id = bronze.Id
WHEN MATCHED THEN
  UPDATE SET
    silver.Nome = UPPER(bronze.Nome),
    silver.ClienteFinalId = bronze.ClienteFinalId,
    silver.DataCriacao = DATE(bronze.DataCriacao),
    silver.StatusId = bronze.StatusId,
    silver.ClienteLEOId = bronze.ClienteLEOId,
    silver.VendedorId = bronze.VendedorId,
    silver.OrcamentoNro = bronze.OrcamentoNro,
    silver.OrcamentoDtCriacao = DATE(bronze.OrcamentoDtCriacao),
    silver.Ativo = bronze.Ativo,
    silver.DataInativacao = DATE(bronze.DataInativacao),
    silver.UsuarioInativacao = bronze.UsuarioInativacao,
    silver.PedidoId = bronze.PedidoId,
    silver.DataUltimaAtualizacao = DATE(bronze.DataUltimaAtualizacao),
    silver.OrigemProjeto = bronze.OrigemProjeto,
    silver.LojaIdErp = bronze.LojaIdErp,
    silver.PedidoSap = bronze.PedidoSap,
    silver.TipoCorte = bronze.TipoCorte,
    silver.Migrado = bronze.Migrado,
    silver.MigradoDate = DATE(bronze.MigradoDate),
    silver.SemServico = bronze.SemServico,
    silver._processing_timestamp = CURRENT_TIMESTAMP()  -- Mantém hora e UTC
WHEN NOT MATCHED THEN
  INSERT (Id, Nome, ClienteFinalId, DataCriacao, StatusId, ClienteLEOId, VendedorId, OrcamentoNro, OrcamentoDtCriacao, Ativo, DataInativacao, UsuarioInativacao, PedidoId, DataUltimaAtualizacao, OrigemProjeto, LojaIdErp, PedidoSap, TipoCorte, Migrado, MigradoDate, SemServico, _processing_timestamp)
  VALUES (bronze.Id, UPPER(bronze.Nome), bronze.ClienteFinalId, DATE(bronze.DataCriacao), bronze.StatusId, bronze.ClienteLEOId, bronze.VendedorId, bronze.OrcamentoNro, DATE(bronze.OrcamentoDtCriacao), bronze.Ativo, DATE(bronze.DataInativacao), bronze.UsuarioInativacao, bronze.PedidoId, DATE(bronze.DataUltimaAtualizacao), bronze.OrigemProjeto, bronze.LojaIdErp, bronze.PedidoSap, bronze.TipoCorte, bronze.Migrado, DATE(bronze.MigradoDate), bronze.SemServico, CURRENT_TIMESTAMP());  -- Mantém hora e UTC
---------------------------------------------------------------------------------------------------------------------------------------------
MERGE INTO `leo-data-d-data-warehouse.leoplan_silver.slv_ProjetoProduto` AS silver
USING `leo-data-d-data-warehouse.leoplan_bronze.brz_ProjetoProduto` AS bronze
ON silver.Id = bronze.Id
WHEN MATCHED THEN
  UPDATE SET
    silver.TipoProdutoId = bronze.TipoProdutoId,
    silver.Codigo = bronze.Codigo,
    silver.Descricao = bronze.Descricao,
    silver.Largura = bronze.Largura,
    silver.Espessura = bronze.Espessura,
    silver.Veio = bronze.Veio,
    silver.Tipo = bronze.Tipo,
    silver.CentroVenda = bronze.CentroVenda,
    silver.CentroSaida = bronze.CentroSaida,
    silver.ProjetoId = bronze.ProjetoId,
    silver.Altura = bronze.Altura,
    silver.Quantidade = bronze.Quantidade,
    silver.ProjetoProdutoColorId = bronze.ProjetoProdutoColorId,
    silver.CaminhoImagem = bronze.CaminhoImagem,
    silver.OtimizacaoId = bronze.OtimizacaoId,
    silver.OtimizacaoIndex = bronze.OtimizacaoIndex,
    silver.DescricaoResumida = bronze.DescricaoResumida,
    silver.Canaletado = bronze.Canaletado,
    silver.PermiteSerCanaletado = bronze.PermiteSerCanaletado,
    silver._processing_timestamp = CURRENT_TIMESTAMP()  -- Mantém hora e UTC
WHEN NOT MATCHED THEN
  INSERT (Id, TipoProdutoId, Codigo, Descricao, Largura, Espessura, Veio, Tipo, CentroVenda, CentroSaida, ProjetoId, Altura, Quantidade, ProjetoProdutoColorId, CaminhoImagem, OtimizacaoId, OtimizacaoIndex, DescricaoResumida, Canaletado, PermiteSerCanaletado, _processing_timestamp)
  VALUES (bronze.Id, bronze.TipoProdutoId, bronze.Codigo, bronze.Descricao, bronze.Largura, bronze.Espessura, bronze.Veio, bronze.Tipo, bronze.CentroVenda, bronze.CentroSaida, bronze.ProjetoId, bronze.Altura, bronze.Quantidade, bronze.ProjetoProdutoColorId, bronze.CaminhoImagem, bronze.OtimizacaoId, bronze.OtimizacaoIndex, bronze.DescricaoResumida, bronze.Canaletado, bronze.PermiteSerCanaletado, CURRENT_TIMESTAMP());  -- Mantém hora e UTC