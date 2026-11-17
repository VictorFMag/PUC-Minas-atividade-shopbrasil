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

## Requisitos

Cada requisito do enunciado está mapeado para a implementação e o arquivo onde pode ser verificado.

### Modelagem e agendamento

| Requisito | Implementação | Onde |
|---|---|---|
| TaskFlow API | Tasks declaradas com `@dag` / `@task`; as dependências são inferidas a partir da passagem de retorno entre funções, sem uso de `>>` entre tasks de dados. | `dags/shopbrasil_precos.py` |
| XComs via `return` | Trafegam apenas payloads enxutos: produtos reduzidos a `id`, `price` e `category` após a validação, e um dicionário de métricas por categoria. | `dags/shopbrasil_precos.py` |
| Topologias | **Linear** em `ingestao` (buscar → validar → listar categorias); **fan-out** em `calcular_metricas` (uma instância por categoria); **fan-in** em `consolidar_metricas`. | Graph View acima |
| Agendamento | `schedule="0 6 * * *"` com `start_date` timezone-aware (`pendulum`, `America/Sao_Paulo`) e `catchup=False`, evitando backfill de execuções passadas. | `dags/shopbrasil_precos.py` |

### Resiliência da ingestão

| Requisito | Implementação | Onde |
|---|---|---|
| Retry com backoff exponencial | `buscar_produtos` com `retries=5`, `retry_delay=20s`, `retry_exponential_backoff=True` e teto `max_retry_delay=10min`; `execution_timeout=2min` impede execuções presas. | `dags/shopbrasil_precos.py` |
| Tratamento de erros | `try/except` captura falhas de rede, HTTP e JSON inválido e relança como `AirflowException`, delegando a nova tentativa ao scheduler. Resposta vazia também é tratada como falha. | `dags/shopbrasil_precos.py` |
| Callbacks de ciclo de vida | `on_success_callback`, `on_retry_callback` e `on_failure_callback` na task crítica, registrando DAG, task, run e número da tentativa. | `dags/shopbrasil/callbacks.py` |

### Processamento paralelo e organização

| Requisito | Implementação | Onde |
|---|---|---|
| Dynamic Task Mapping | `calcular_metricas.partial(produtos=...).expand(categoria=...)`: as categorias são descobertas em tempo de execução, portanto novas categorias são processadas sem alteração de código. | `dags/shopbrasil_precos.py` |
| Pool | `ecommerce_pool` com 2 slots limita a concorrência das instâncias mapeadas; é provisionado automaticamente pelo serviço `airflow-init`. | `docker-compose.yaml` |
| TaskGroups | Três grupos com responsabilidades distintas: `ingestao`, `analise` e `persistencia`. | `dags/shopbrasil_precos.py` |

### Persistência

| Requisito | Implementação | Onde |
|---|---|---|
| PostgresHook + Connection | `PostgresHook(postgres_conn_id="postgres_analytics")`; a Connection é injetada via variável de ambiente `AIRFLOW_CONN_POSTGRES_ANALYTICS`, sem credenciais no código da DAG. | `docker-compose.yaml` |
| Idempotência | Chave primária `(data_referencia, categoria)` com `INSERT ... ON CONFLICT DO UPDATE`, executado em uma única transação: reprocessar a mesma run atualiza as linhas existentes em vez de duplicá-las. | `dags/shopbrasil/sql.py` |

### Requisitos opcionais

| Requisito | Implementação | Onde |
|---|---|---|
| Operador customizado | `ValidarProdutosOperator(BaseOperator)` valida presença e tipo dos campos obrigatórios e rejeita preços negativos; registros inválidos são descartados com log, e a task falha se ultrapassarem 20% do lote. | `plugins/operators/validar_produtos.py` |
| Tabela de histórico | `metricas_categoria_historico` em modo append, com `data_referencia` e `run_id`, para acompanhar a evolução dos preços ao longo do tempo. | `dags/shopbrasil/sql.py` |
| SLA / alerta | SLA de 30 minutos em `salvar_snapshot` com `sla_miss_callback`, e `on_failure_callback` que simula o envio de alerta ao canal do time. | `dags/shopbrasil/callbacks.py` |

## Testar a idempotência

Rode a DAG, depois use **Clear** na mesma run e rode de novo: `metricas_categoria` continua com
uma linha por categoria por dia, enquanto o histórico ganha novas linhas.
