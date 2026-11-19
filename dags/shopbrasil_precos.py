"""
ShopBrasil - Panorama diário de preços por categoria.

Topologia:
    [ingestao]  buscar_produtos -> validar_produtos -> listar_categorias      (linear)
    [analise]   calcular_metricas.expand(categoria=...)                         (fan-out)
                -> consolidar_metricas                                          (fan-in)
    [persistencia] criar_tabelas -> salvar_snapshot (upsert idempotente)
                                 -> salvar_historico (append)
"""
# Imports
from datetime import timedelta

import pendulum
import requests
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook

from operators.validar_produtos import ValidarProdutosOperator
from shopbrasil import callbacks, sql

# Configurações
API_URL = "https://fakestoreapi.com/products"
POSTGRES_CONN_ID = "postgres_lab"
POOL = "ecommerce_pool"

# Argumentos padrão
default_args = {
    "owner": "time-dados",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    "on_failure_callback": callbacks.on_failure,
}


# DAG
@dag(
    dag_id="shopbrasil_precos_por_categoria",
    description="Coleta o catálogo, calcula métricas de preço por categoria e grava no Postgres.",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 11, 1, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    sla_miss_callback=callbacks.on_sla_miss,
    tags=["shopbrasil", "pricing"],
    doc_md=__doc__,
)
def shopbrasil_precos():

    # Ingestão
    with TaskGroup(group_id="ingestao"):

        # Buscar produtos
        @task(
            task_id="buscar_produtos",
            retries=5,
            retry_delay=timedelta(seconds=20),
            retry_exponential_backoff=True,
            max_retry_delay=timedelta(minutes=10),
            execution_timeout=timedelta(minutes=2),
            on_failure_callback=callbacks.on_failure,
            on_retry_callback=callbacks.on_retry,
            on_success_callback=callbacks.on_success,
        )
        def buscar_produtos() -> list[dict]:
            """Busca Produtos na API de catálogo (FakeStore)."""
            try:
                resp = requests.get(API_URL, timeout=15, headers={"User-Agent": "shopbrasil-airflow/1.0"})
                resp.raise_for_status()
                produtos = resp.json()
            except (requests.RequestException, ValueError) as exc:
                raise AirflowException(f"Falha ao consultar a API de catálogo: {exc}") from exc

            if not isinstance(produtos, list) or not produtos:
                raise AirflowException("API retornou uma lista de produtos vazia ou inválida.")
            return produtos

        # Validar produtos
        validar = ValidarProdutosOperator(task_id="validar_produtos", produtos=buscar_produtos())

        # Listar categorias
        @task
        def listar_categorias(produtos: list[dict]) -> list[str]:
            """Categorias descobertas dinamicamente: novas categorias entram sozinhas."""
            return sorted({p["category"] for p in produtos})

        produtos = validar.output
        categorias = listar_categorias(produtos)

    # Análise
    with TaskGroup(group_id="analise"):

        # Calcular métricas
        @task(pool=POOL)
        def calcular_metricas(categoria: str, produtos: list[dict]) -> dict:
            precos = [float(p["price"]) for p in produtos if p["category"] == categoria]
            return {
                "categoria": categoria,
                "preco_medio": round(sum(precos) / len(precos), 2),
                "preco_minimo": round(min(precos), 2),
                "preco_maximo": round(max(precos), 2),
                "qtd_produtos": len(precos),
            }

        # Consolidar métricas
        @task
        def consolidar_metricas(metricas: list[dict]) -> list[dict]:
            consolidado = sorted(metricas, key=lambda m: m["categoria"])
            total = sum(m["qtd_produtos"] for m in consolidado)
            print(f"{len(consolidado)} categorias consolidadas, {total} produtos.")
            return consolidado

        # Fan-out e fan-in
        por_categoria = calcular_metricas.partial(produtos=produtos).expand(categoria=categorias)
        metricas = consolidar_metricas(por_categoria)

    # Persistência
    with TaskGroup(group_id="persistencia"):

        # Criar tabelas
        @task
        def criar_tabelas():
            PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).run(sql.CREATE_TABLES)

        # Salvar snapshot
        @task(sla=timedelta(minutes=30))
        def salvar_snapshot(metricas: list[dict], ds=None):
            """Upsert por (data_referencia, categoria): re-rodar a mesma run não duplica linhas."""
            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            linhas = [
                (ds, m["categoria"], m["preco_medio"], m["preco_minimo"], m["preco_maximo"], m["qtd_produtos"])
                for m in metricas
            ]
            with hook.get_conn() as conn, conn.cursor() as cur:
                cur.executemany(sql.UPSERT_SNAPSHOT, linhas)
            print(f"Snapshot {ds}: {len(linhas)} categorias gravadas (upsert).")

        # Salvar histórico
        @task
        def salvar_historico(metricas: list[dict], ds=None, run_id=None):
            """Append: uma linha por categoria a cada execução, para ver a evolução dos preços."""
            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            linhas = [
                (ds, run_id, m["categoria"], m["preco_medio"], m["preco_minimo"], m["preco_maximo"], m["qtd_produtos"])
                for m in metricas
            ]
            with hook.get_conn() as conn, conn.cursor() as cur:
                cur.executemany(sql.INSERT_HISTORICO, linhas)
            print(f"Histórico {ds}: {len(linhas)} linhas adicionadas.")

        # Dependências
        tabelas = criar_tabelas()
        tabelas >> [salvar_snapshot(metricas), salvar_historico(metricas)]


shopbrasil_precos()
