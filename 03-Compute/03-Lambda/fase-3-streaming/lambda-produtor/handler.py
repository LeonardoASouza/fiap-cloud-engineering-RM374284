import os
import json
import boto3

from aws_lambda_powertools import Logger, Metrics, Tracer
from aws_lambda_powertools.metrics import MetricUnit

logger = Logger(service="pedeja-produtor-stream")
metrics = Metrics(namespace="PedeJa", service="pedeja-produtor-stream")
tracer = Tracer(service="pedeja-produtor-stream")

kinesis = boto3.client("kinesis")
STREAM_NAME = os.environ["STREAM_NAME"]


@logger.inject_lambda_context
@metrics.log_metrics
@tracer.capture_lambda_handler
def handler(event, context):
    # Produtor publica o pedido no STREAM. Diferente da fila, o registro NAO some
    # ao ser lido: fica retido e pode ser lido por varios consumidores e reprocessado.
    pedido = json.loads(event.get("body") or "{}")

    kinesis.put_record(
        StreamName=STREAM_NAME,
        Data=json.dumps(pedido, ensure_ascii=False).encode("utf-8"),
        PartitionKey=pedido["cidade"],  # mesma cidade -> mesmo shard -> ordem preservada
    )

    metrics.add_metric(name="pedidos_publicados", unit=MetricUnit.Count, value=1)
    logger.info("pedido publicado no stream", pedido_id=pedido.get("pedido_id"))

    return {
        "statusCode": 202,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({"status": "publicado", "pedido_id": pedido.get("pedido_id")}, ensure_ascii=False),
    }
