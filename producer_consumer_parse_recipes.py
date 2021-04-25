import json

from bs4 import BeautifulSoup
from kafka import KafkaConsumer, KafkaProducer


def publish_message(producer_instance, topic_name, key, value):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print('PARSER: Mensagem publicada com sucesso.')
    except Exception as ex:
        print('PARSER: Exception na publicação de mensagem.')
        print(str(ex))


def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10))
    except Exception as ex:
        print('PARSER: Exception durante a conexão com o kafka.')
        print(str(ex))
    finally:
        return _producer


def parse(markup):
    description = '-'
    calories = 0
    protein = 0
    carbohydrates = 0
    ingredients = []
    rec = {}
    title = '-'

    try:
        soup = BeautifulSoup(markup, 'lxml')

        # title
        title_section = soup.select('.main-header .heading-content')
        if title_section:
            title = title_section[0].text

        # description
        description_section = soup.select('.recipe-summary p')
        if description_section:
            description = description_section[0].text

        # ingredientes
        ingredients_section = soup.select('.ingredients-item-name')
        if ingredients_section:
            for ingredient in ingredients_section:
                ingredients.append({'name': ingredient.text.strip()})

        # nutrição
        info_section = soup.select('.recipe-nutrition-section .section-body')
        if(info_section):
            infos = info_section[0].text.strip().split("; ")
            calories = infos[0].replace(' calories', '')
            protein = infos[1].replace('protein ', '')
            carbohydrates = infos[2].replace('carbohydrates ', '')

        rec = {'title': title, 'description': description, 'calories': calories,
               'protein': protein, 'carbohydrates': carbohydrates,'ingredients': ingredients}

    except Exception as ex:
        print('PARSER: Exception durante o parse')
        print(str(ex))
    finally:
        return json.dumps(rec)


if __name__ == '__main__':
    print('PARSER: Iniciando consumidor...')
    parsed_records = []
    topic_name = 'raw_recipes'
    parsed_topic_name = 'parsed_recipes'

    consumer = KafkaConsumer(topic_name, auto_offset_reset='earliest',
                             bootstrap_servers=['localhost:9092'], api_version=(0, 10), consumer_timeout_ms=1000)
    for msg in consumer:
        html = msg.value
        result = parse(html)
        parsed_records.append(result)
    consumer.close()

    if len(parsed_records) > 0:
        print('PARSER: Publicando dados...')
        producer = connect_kafka_producer()
        for rec in parsed_records:
            publish_message(producer, parsed_topic_name, 'parsed', rec)
