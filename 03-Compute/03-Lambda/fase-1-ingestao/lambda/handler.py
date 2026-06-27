import os
import json
import boto3

from aws_lambda_powertools import Logger, Metrics, Tracer
from aws_lambda_powertools.metrics import MetricUnit

# Powertools: padrao recomendado pela AWS para observabilidade em Lambda.
# - Logger : log estruturado em JSON (pesquisavel no CloudWatch Logs Insights)
# - Metrics: metricas via EMF (sem precisar de permissao PutMetricData)
# - Tracer : trace distribuido no X-Ray (ve o salto Lambda -> S3)
logger = Logger(service="pedeja-ingestao")
metrics = Metrics(namespace="PedeJa", service="pedeja-ingestao")
tracer = Tracer(service="pedeja-ingestao")

# O Tracer do Powertools instrumenta o boto3 automaticamente: a chamada ao S3
# vira um subsegmento no trace, mostrando quanto tempo a Lambda gastou gravando.
s3 = boto3.client("s3")
BUCKET = os.environ["BUCKET_DATA_LAKE"]


@tracer.capture_method
def grava_no_datalake(pedido):
    dt = pedido["event_time"][:10]  # particiona pela data DENTRO do evento
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
    # API Gateway (proxy) entrega a requisicao como EVENTO em event["body"].
    # A Lambda nao escuta porta: ela e invocada com o evento ja montado.
    pedido = json.loads(event.get("body") or "{}")

    key = grava_no_datalake(pedido)

    # Metrica de NEGOCIO: quantos pedidos e qual faturamento, por cidade.
    metrics.add_dimension(name="cidade", value=pedido["cidade"])
    metrics.add_metric(name="pedidos_processados", unit=MetricUnit.Count, value=1)
    metrics.add_metric(name="valor_pedido", unit="None", value=pedido["valor"])

    logger.info("pedido gravado", pedido_id=pedido["pedido_id"], s3_key=key,
                cidade=pedido["cidade"], valor=pedido["valor"])

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({"status": "gravado", "s3_key": key}, ensure_ascii=False),
    }
