# ShopBrasil — Pipeline de preços por categoria (Apache Airflow)

Substitui o script via cron por uma DAG no Airflow que coleta o catálogo da
[FakeStore API](https://fakestoreapi.com/docs), calcula preço médio, mínimo, máximo e
quantidade de produtos por categoria e grava em PostgreSQL.

## Como rodar

Pré-requisito: Docker Desktop (WSL 2) com pelo menos 4 GB de memória.

```bash
docker compose up -d
```

- Airflow UI: http://localhost:8080 (usuário `airflow`, senha `airflow`)
- Ative a DAG `shopbrasil_precos_por_categoria` e clique em **Trigger DAG**.
- Base analítica: `postgres://analytics:analytics@localhost:5432/analytics`

O serviço `airflow-init` migra o banco, cria o usuário, o **pool `ecommerce_pool` (2 slots)**,
e a **Connection `postgres_analytics`** vem por variável de ambiente (`AIRFLOW_CONN_POSTGRES_ANALYTICS`).

Consultar os resultados:

```bash
docker compose exec postgres psql -U analytics -d analytics -c "SELECT * FROM metricas_categoria ORDER BY data_referencia DESC, categoria;"
```

## Estrutura

```
docker-compose.yaml            Airflow 2.10 (LocalExecutor) + Postgres 16
sql/init-analytics.sql         cria a base/usuário analytics
dags/shopbrasil_precos.py      DAG (TaskFlow API)
dags/shopbrasil/callbacks.py   callbacks de sucesso/retry/falha + alerta simulado e SLA
dags/shopbrasil/sql.py         DDL, upsert do snapshot e insert do histórico
plugins/operators/validar_produtos.py   ValidarProdutosOperator (BaseOperator)
```

## Arquitetura da DAG

```
ingestao:      buscar_produtos ─► validar_produtos ─► listar_categorias        (linear)
analise:       calcular_metricas[categoria 1..N]  (expand, pool=ecommerce_pool) (fan-out)
                    └──────────► consolidar_metricas                          (fan-in)
persistencia:  criar_tabelas ─► salvar_snapshot (upsert)
                             └► salvar_historico (append)
```

![Graph View da DAG com os TaskGroups expandidos](docs/graph-view.png)

## Como cada requisito foi atendido

| Requisito | Implementação |
|---|---|
| TaskFlow API | `@dag` / `@task`; dependências pela chamada das funções |
| XComs via `return` | só listas pequenas (id, preço, categoria) e métricas agregadas |
| Linear / fan-out / fan-in | grupos `ingestao` / `calcular_metricas.expand` / `consolidar_metricas` |
| 06:00 Brasília | `schedule="0 6 * * *"`, `start_date` com `pendulum` em `America/Sao_Paulo`, `catchup=False` |
| Retry + backoff | `buscar_produtos`: `retries=5`, `retry_exponential_backoff=True`, `max_retry_delay=10min` |
| try/except + raise | erros de rede/JSON relançados como `AirflowException` para acionar o retry |
| Callbacks | `on_failure_callback`, `on_retry_callback`, `on_success_callback` na task crítica |
| Dynamic Task Mapping | `calcular_metricas.partial(produtos=...).expand(categoria=...)` — novas categorias entram sozinhas |
| Pool | `ecommerce_pool` com 2 slots, criado no `airflow-init` |
| TaskGroups | `ingestao`, `analise`, `persistencia` |
| PostgresHook + Connection | `PostgresHook(postgres_conn_id="postgres_analytics")` |
| Idempotência | PK `(data_referencia, categoria)` + `INSERT ... ON CONFLICT DO UPDATE`, numa única transação |
| *Opcional* operador customizado | `ValidarProdutosOperator` valida tipos/campos e falha se >20% inválidos |
| *Opcional* histórico | `metricas_categoria_historico` em modo append com `data_referencia` e `run_id` |
| *Opcional* SLA / alerta | `sla=30min` em `salvar_snapshot` + `sla_miss_callback`; `on_failure_callback` simula alerta no log |

## Testar a idempotência

Rode a DAG, depois use **Clear** na mesma run e rode de novo: `metricas_categoria` continua com
uma linha por categoria por dia, enquanto o histórico ganha novas linhas.
