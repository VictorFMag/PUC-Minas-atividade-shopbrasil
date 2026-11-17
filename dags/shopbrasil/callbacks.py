"""Callbacks de ciclo de vida e alertas (simulados) do pipeline."""
import logging

log = logging.getLogger("shopbrasil.alertas")


def _info(context):
    ti = context["task_instance"]
    return ti.dag_id, ti.task_id, ti.run_id, ti.try_number


def on_success(context):
    dag_id, task_id, run_id, tentativa = _info(context)
    log.info("[SUCESSO] %s.%s (run=%s) concluída na tentativa %s", dag_id, task_id, run_id, tentativa)


def on_retry(context):
    dag_id, task_id, run_id, tentativa = _info(context)
    erro = context.get("exception")
    log.warning("[RETRY] %s.%s (run=%s) falhou na tentativa %s: %s — nova tentativa agendada com backoff.",
                dag_id, task_id, run_id, tentativa, erro)


def on_failure(context):
    """Simula o envio de alerta (ex.: Slack/e-mail) quando a task falha de vez."""
    dag_id, task_id, run_id, tentativa = _info(context)
    erro = context.get("exception")
    mensagem = (
        f":rotating_light: *Pipeline de preços falhou*\n"
        f"DAG: {dag_id} | Task: {task_id} | Run: {run_id} | Tentativas: {tentativa}\n"
        f"Erro: {erro}\n"
        f"Log: {context['task_instance'].log_url}"
    )
    log.error("[ALERTA SIMULADO] Enviando para #dados-alertas:\n%s", mensagem)


def on_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis):
    log.error("[ALERTA SIMULADO] SLA estourado na DAG %s. Tasks: %s", dag.dag_id, task_list)
