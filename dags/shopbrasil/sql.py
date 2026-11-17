"""SQL da base analítica."""

# Criação das tabelas
CREATE_TABLES = """
CREATE TABLE IF NOT EXISTS metricas_categoria (
    data_referencia  DATE          NOT NULL,
    categoria        TEXT          NOT NULL,
    preco_medio      NUMERIC(12,2) NOT NULL,
    preco_minimo     NUMERIC(12,2) NOT NULL,
    preco_maximo     NUMERIC(12,2) NOT NULL,
    qtd_produtos     INTEGER       NOT NULL,
    atualizado_em    TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (data_referencia, categoria)
);

CREATE TABLE IF NOT EXISTS metricas_categoria_historico (
    id               BIGSERIAL     PRIMARY KEY,
    data_referencia  DATE          NOT NULL,
    run_id           TEXT          NOT NULL,
    categoria        TEXT          NOT NULL,
    preco_medio      NUMERIC(12,2) NOT NULL,
    preco_minimo     NUMERIC(12,2) NOT NULL,
    preco_maximo     NUMERIC(12,2) NOT NULL,
    qtd_produtos     INTEGER       NOT NULL,
    inserido_em      TIMESTAMPTZ   NOT NULL DEFAULT now()
);
"""

# Upsert do snapshot
UPSERT_SNAPSHOT = """
INSERT INTO metricas_categoria
    (data_referencia, categoria, preco_medio, preco_minimo, preco_maximo, qtd_produtos)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (data_referencia, categoria) DO UPDATE SET
    preco_medio   = EXCLUDED.preco_medio,
    preco_minimo  = EXCLUDED.preco_minimo,
    preco_maximo  = EXCLUDED.preco_maximo,
    qtd_produtos  = EXCLUDED.qtd_produtos,
    atualizado_em = now();
"""

# Insert do histórico
INSERT_HISTORICO = """
INSERT INTO metricas_categoria_historico
    (data_referencia, run_id, categoria, preco_medio, preco_minimo, preco_maximo, qtd_produtos)
VALUES (%s, %s, %s, %s, %s, %s, %s);
"""
