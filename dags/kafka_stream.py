from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator



default_args = {
    'owner': 'dang yen nhi',
    'start_date': datetime(2024, 8, 12, 10, 00)
}

def get_data():
    import json
    import requests 
    
    response = requests.get("https://random-data-api.com/api/v2/addresses")
    response = response.json()
    return response


def format_data(response):
    data = {}
    data['iD'] = response['id']
    data['uid'] = response['uid']
    data['city'] = response['city']
    data['street_name'] = response['street_name']
    data['street_address'] = response['street_address']
    data['secondary_address'] = response['secondary_address']
    data['building_number'] = response['building_number']
    data['mail_box'] = response['mail_box']
    data['community'] = response['community']
    data['zip_code'] = response['zip_code']
    data['ziP'] = response['zip']
    data['postcode'] = response['postcode']
    data['time_zone'] = response['time_zone']
    data['street_suffix'] = response['street_suffix']
    data['city_suffix'] = response['city_suffix']
    data['city_prefix'] = response['city_prefix']
    data['state'] = response['state']
    data['state_abbr'] = response['state_abbr']
    data['country'] = response['country']
    data['country_code'] = response['country_code']
    data['latitude'] = response['latitude']
    data['longitude'] = response['longitude']
    data['full_address'] = response['full_address']
    
    return data
    
    
def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    import logging
    
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    current_time = time.time()
    
    while True:
        if time.time() > current_time + 60: #1 minute
            break
        try:
            response = get_data()
            response = format_data(response)

            producer.send('address', json.dumps(response).encode('utf-8'))
        except Exception as e:
            logging.error(f'An error occured: {e}')
            continue
            
    
with DAG(
    'user_automation',
         default_args=default_args,
         schedule='@hourly'
    ) as dag:
    
    
    streaming_task = PythonOperator(
        task_id='stream_data_from_API',
        python_callable=stream_data  
    )
    
    
streaming_task
    
    
    



