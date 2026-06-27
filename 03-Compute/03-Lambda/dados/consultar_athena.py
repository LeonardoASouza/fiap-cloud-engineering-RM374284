#!/usr/bin/env python3
"""Consulta o faturamento por cidade no Athena (Fase 3) e imprime o resultado.

Faz tudo num comando: dispara a query no workgroup 'pedeja', espera terminar
e mostra o resultado numa tabela. Sem copiar/colar ID de execucao.

Uso:
    python3 consultar_athena.py
"""
import subprocess
import sys
import time

WORKGROUP = "pedeja"
DATABASE = "pedeja"
QUERY = (
    "SELECT cidade, COUNT(*) AS pedidos, ROUND(SUM(valor),2) AS faturamento "
    "FROM pedeja.pedidos GROUP BY cidade ORDER BY faturamento DESC"
)


def aws(*args):
    return subprocess.run(
        ["aws", *args], capture_output=True, text=True
    )


def main():
    print("Consultando o Athena (workgroup 'pedeja')...")
    r = aws(
        "athena", "start-query-execution",
        "--query-string", QUERY,
        "--query-execution-context", f"Database={DATABASE}",
        "--work-group", WORKGROUP,
        "--query", "QueryExecutionId", "--output", "text",
    )
    if r.returncode != 0:
        print("Erro ao iniciar a query:\n" + r.stderr.strip())
        sys.exit(1)
    qid = r.stdout.strip()

    # Espera a query terminar (ate ~30s)
    estado = ""
    for _ in range(15):
        s = aws(
            "athena", "get-query-execution", "--query-execution-id", qid,
            "--query", "QueryExecution.Status.State", "--output", "text",
        )
        estado = s.stdout.strip()
        if estado in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(2)

    if estado != "SUCCEEDED":
        motivo = aws(
            "athena", "get-query-execution", "--query-execution-id", qid,
            "--query", "QueryExecution.Status.StateChangeReason", "--output", "text",
        ).stdout.strip()
        print(f"Query nao concluiu (estado: {estado}). {motivo}")
        print("Se o Firehose ainda nao entregou (buffer de 60s), espere e rode de novo.")
        sys.exit(2)

    # Pega o resultado e imprime como tabela
    res = aws(
        "athena", "get-query-results", "--query-execution-id", qid,
        "--query", "ResultSet.Rows[].Data[].VarCharValue", "--output", "text",
    )
    valores = res.stdout.split("\t")
    # 3 colunas: cidade, pedidos, faturamento (a 1a linha e o cabecalho)
    linhas = [valores[i:i + 3] for i in range(0, len(valores), 3)]
    if len(linhas) <= 1:
        print("Tabela vazia. O Firehose ainda nao entregou o Parquet? Espere ~30s e rode de novo.")
        sys.exit(2)

    larguras = [max(len(str(l[c])) for l in linhas) for c in range(3)]
    total_ped = total_fat = 0.0
    for i, l in enumerate(linhas):
        print("  ".join(str(l[c]).ljust(larguras[c]) for c in range(3)))
        if i == 0:
            print("  ".join("-" * larguras[c] for c in range(3)))
        else:
            total_ped += int(l[1])
            total_fat += float(l[2])
    print("  ".join("-" * larguras[c] for c in range(3)))
    print(f"TOTAL: {int(total_ped)} pedidos, faturamento {round(total_fat, 2)}")


if __name__ == "__main__":
    main()
