import asyncio
from random import randint
from prefect import task, flow
from prefect import get_run_logger

from prefect.artifacts import create_link_artifact, create_table_artifact


# 8b:0e:53:70:84:75:52:a6:73:f4:6f:6e:d8:9f:30:c7
@task(persist_result=True, result_storage_key="func1-{parameters[param]}.json")
# @task(persist_result=True, result_storage_key="{flow_run.flow_name}_{flow_run.name}_func1.json")
async def func1(param, data_table: dict):
    logger = get_run_logger()

    request_await_time = randint(2, 5)
    logger.info("< Идет запрос на сервер >")
    await asyncio.sleep(request_await_time)
    logger.info("\n>> Запрос выполнен...")
    res_data = f"Done... - <{param}>"

    await create_link_artifact(key=f"t-{param}", link='', description=f"Запрос выполнен func1_{param}...")

    highest_churn_possibility = [data_table]

    await create_table_artifact(
        key=f"personalized-{param}",
        table=highest_churn_possibility,
        description="# Marvin, please reach out to these customers today!"
    )

    return res_data


@flow(name='Main_Async')
async def main_async():
    task_list = []
    data_list = []
    error_list = []

    input_data = [
        {'customer_id': '12345', 'name': 'John Smith', 'churn_probability': 0.85},
        {'customer_id': '56789', 'name': 'Jane Jones', 'churn_probability': 0.65},
        {'customer_id': '56783', 'name': 'Pavel Nebrat', 'churn_probability': 0.95},
        {'customer_id': '56733', 'name': 'Masha Popova', 'churn_probability': 1.35},
        {'customer_id': '53233', 'name': 'Gregory Goshin', 'churn_probability': 0.32},
    ]

    for t, data in enumerate(input_data, start=1):
        task_list.append(func1(param=t, data_table=data))

    res = await asyncio.gather(*task_list)

    for r in res:
        if 'Done' not in r:
            error_list.append(r)
        else:
            data_list.append(r)
    if error_list:
        raise TypeError("Ошибка при получении данных...")

    for data in data_list:
        with open('text_data_test.txt', 'a', encoding='utf-8') as file:
            file.write(f"{data}\n")

    msg = string_block.value
    print(msg)


if __name__ == '__main__':
    from prefect.deployments import Deployment
    from prefect.blocks.system import String

    string_block = String.load("my-block-atom-text")

    def deploy():
        deployment = Deployment.build_from_flow(
            flow=main_async,
            name="prefect-example-deployment"
        )
        deployment.apply()

    deploy()

    # main_async.with_options(result_storage=LocalFileSystem())
    loop = asyncio.new_event_loop()
    loop.run_until_complete(main_async())