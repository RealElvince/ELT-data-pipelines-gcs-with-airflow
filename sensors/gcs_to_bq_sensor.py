
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor

def get_gcs_to_bq_sensor(task_id,bucket_name,object_path,poke_interval=30,timeout=300,mode='poke'):
    return GCSObjectExistenceSensor(
        task_id=task_id,
        bucket=bucket_name,
        object=object_path,
        poke_interval=poke_interval,
        timeout=timeout,
        mode=mode
    )