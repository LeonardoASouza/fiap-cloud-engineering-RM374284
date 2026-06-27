import os
import json
import boto3

from aws_lambda_powertools import Logger, Metrics, Tracer
from aws_lambda_powertools.metrics import MetricUnit

logger = Logger(service="pedeja-consumidor")
metrics = Metrics(namespace="PedeJa", service="pedeja-consumidor")
tracer = Tracer(service="pedeja-consumidor")

s3 = boto3.client("s3")
BUCKET = os.environ["BUCKET_DATA_LAKE"]


@tracer.capture_method
def grava_no_datalake(pedido):
    dt = pedido["event_time"][:10]
    key = f"pedidos/dt={dt}/{pedido['pedido_id']}.json"
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(pedido, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )
    return key


@logger.inject_lambda_context
@metrics.log_metrics
@tracer.capture_lambda_handler
def handler(event, context):
    # Consumidor e disparado pelo SQS com um LOTE de mensagens (event["Records"]).
    # Gatilho 100% event-driven: a Lambda nem sabe quem enfileirou.
    registros = event.get("Records", [])
    logger.info("lote recebido", tamanho_lote=len(registros))

    for r in registros:
        pedido = json.loads(r["body"])
        key = grava_no_datalake(pedido)

        metrics.add_dimension(name="cidade", value=pedido["cidade"])
        metrics.add_metric(name="pedidos_processados", unit=MetricUnit.Count, value=1)
        metrics.add_metric(name="valor_pedido", unit="None", value=pedido["valor"])
        logger.info("pedido gravado", pedido_id=pedido["pedido_id"], s3_key=key)

    # Sem excecao = SQS apaga as mensagens do lote. Com excecao = volta para a fila
    # e, apos maxReceiveCount tentativas, vai para a DLQ (nada se perde).
    return {"processados": len(registros)}
