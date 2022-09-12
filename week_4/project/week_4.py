from typing import List
from datetime import datetime as dt
from dagster import Nothing, asset, with_resources,get_dagster_logger,AssetIn
from project.resources import redis_resource, s3_resource
from project.types import Aggregation, Stock

logger = get_dagster_logger()


@asset(required_resource_keys={"s3"},
    config_schema={"s3_key": str},
    op_tags={"kind": "s3"},
    description="Get a list of stocks from an S3 file",
    group_name="corise"
)
def get_s3_data(context):
    output = list()
    data = context.resources.s3.get_data(context.op_config["s3_key"])
    for row in data:
        stock = Stock.from_list(row)
        output.append(stock)
    return output

@asset(
    description="Aggregate stock data",
    group_name="corise"
)
def process_data(get_s3_data: List[Stock]) -> Aggregation:
    top_high = sorted(get_s3_data, key=lambda x: x.high, reverse=True)[0]
    return Aggregation(date=top_high.date, high=top_high.high)


@asset(required_resource_keys={"redis"},
    op_tags={"kind": "redis"},
    description="Upload aggregation to Redis",
    group_name="corise"
)
def put_redis_data(context,process_data: Aggregation):
    agg_date = dt.strftime(process_data.date, '%m/%d/%Y')
    agg_high = str(process_data.high)
    context.resources.redis.put_data(agg_date, agg_high)
    logger.info(f"Date: {agg_date} with daily high of ${agg_high} stored on Redis")



get_s3_data_docker, process_data_docker, put_redis_data_docker = with_resources(
    definitions=[get_s3_data, process_data, put_redis_data],
    resource_defs={"s3": s3_resource, "redis": redis_resource},
    resource_config_by_key={
        "s3": {
            "config": {
                "bucket": "dagster",
                "access_key": "test",
                "secret_key": "test",
                "endpoint_url": "http://localstack:4566",
            }
        },
        "redis": {
            "config": {
                "host": "redis",
                "port": 6379,
            }
        },
        },
)