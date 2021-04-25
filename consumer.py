import json

from kafka import KafkaConsumer

if __name__ == '__main__':
    parsed_topic_name = 'parsed_recipes'
    # Notifica se uma receita tem mais que ${calories_max} calorias
    calories_max = 300

    consumer = KafkaConsumer(parsed_topic_name, auto_offset_reset='earliest',
                             bootstrap_servers=['localhost:9092'], api_version=(0, 10), consumer_timeout_ms=1000)

    print('CONSUMER: Procurando se alguma receita tem mais que {} calorias'.format(calories_max))

    for msg in consumer:
        record = json.loads(msg.value)
        calories = int(record['calories'])
        title = record['title']

        if calories > calories_max:
            print('ATENÇÃO: {} tem {} calorias'.format(title, calories))

    if consumer is not None:
        consumer.close()