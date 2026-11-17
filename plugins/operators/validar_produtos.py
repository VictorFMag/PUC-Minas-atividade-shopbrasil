"""Operador customizado que valida o schema dos produtos da API de catálogo."""

# Imports
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator

# Schema
SCHEMA = {
    "id": (int,),
    "title": (str,),
    "price": (int, float),
    "category": (str,),
}


# Operador
class ValidarProdutosOperator(BaseOperator):
    """Valida cada produto e devolve apenas os campos usados no processamento.

    Produtos inválidos são descartados e registrados em log. Se a proporção
    de inválidos passar de ``max_invalidos_pct``, a task falha.
    """

    template_fields = ("produtos",)
    ui_color = "#ffe9a8"

    # Inicialização
    def __init__(self, *, produtos, max_invalidos_pct: float = 0.2, **kwargs):
        super().__init__(**kwargs)
        self.produtos = produtos
        self.max_invalidos_pct = max_invalidos_pct

    # Erros do produto
    @staticmethod
    def _erros(produto) -> list[str]:
        if not isinstance(produto, dict):
            return ["registro não é um objeto"]
        erros = []
        for campo, tipos in SCHEMA.items():
            valor = produto.get(campo)
            if valor is None:
                erros.append(f"campo '{campo}' ausente")
            elif isinstance(valor, bool) or not isinstance(valor, tipos):
                erros.append(f"campo '{campo}' com tipo inválido ({type(valor).__name__})")
        if not erros and produto["price"] < 0:
            erros.append("preço negativo")
        return erros

    # Execução
    def execute(self, context):
        if not self.produtos:
            raise AirflowException("Nenhum produto recebido para validação.")

        validos, invalidos = [], 0
        for produto in self.produtos:
            erros = self._erros(produto)
            if erros:
                invalidos += 1
                self.log.warning("Produto descartado %s: %s", getattr(produto, "get", lambda _: None)("id"), erros)
                continue
            validos.append({k: produto[k] for k in ("id", "price", "category")})

        pct = invalidos / len(self.produtos)
        self.log.info("Validação: %d válidos, %d inválidos (%.0f%%).", len(validos), invalidos, pct * 100)
        if pct > self.max_invalidos_pct:
            raise AirflowException(f"{pct:.0%} dos produtos são inválidos (limite {self.max_invalidos_pct:.0%}).")
        return validos
