
import requests
from bs4 import BeautifulSoup
from kafka import KafkaProducer


# bin/windows/zookeeper-server-start.bat config/zookeeper.properties
# bin/windows/kafka-server-start.bat config/server.properties

# kafka-consumer-groups.bat --bootstrap-server kafka-host:9092 --group
# my-group --reset-offsets --to-earliest --all-topics --execute
# bin/windows/kafka-topics.bat --list --zookeeper localhost:2181

# bin/windows/kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test
# Created topic "test"

# bin/windows/kafka-console-producer.bat --broker-list localhost:9092 --topic test
# bin/windows/kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic test --from-beginning

# pip install kafka-python
# pip install beautifulsoup4
# pip install lxml

def publish_message(producer_instance, topic_name, key, value):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print('PRODUCER: Mensagem publicada com sucesso.')
    except Exception as ex:
        print('PRODUCER: Exception na publicação de mensagem.')
        print(str(ex))


def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10))
    except Exception as ex:
        print('PRODUCER: Exception durante a conexão com o kafka.')
        print(str(ex))
    finally:
        return _producer


def fetch_raw(recipe_url):
    html = None
    print('PRODUCER: Processing...{}'.format(recipe_url))
    try:
        r = requests.get(recipe_url, headers=headers)
        if r.status_code == 200:
            html = r.text
    except Exception as ex:
        print('PRODUCER: Exception durante o acesso ao html')
        print(str(ex))
    finally:
        return html.strip()


def get_recipes():
    recipes = []
    url = 'https://www.allrecipes.com/recipes/95/salad/'
    print('PRODUCER: Acessando lista')

    try:
        r = requests.get(url, headers=headers)
        if r.status_code == 200:
            html = r.text
            soup = BeautifulSoup(html, 'lxml')
            links = soup.select('.card__imageContainer a')
            idx = 0
            for link in links:
                recipe = fetch_raw(link['href'])
                recipes.append(recipe)
                idx += 1
    except Exception as ex:
        print('PRODUCER: Exception em get_recipes')
        print(str(ex))
    finally:
        return recipes


if __name__ == '__main__':
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36',
        'Pragma': 'no-cache'
    }

    all_recipes = get_recipes()
    if len(all_recipes) > 0:
        kafka_producer = connect_kafka_producer()
        for recipe in all_recipes:
            publish_message(kafka_producer, 'raw_recipes', 'raw', recipe.strip())
        if kafka_producer is not None:
            kafka_producer.close()
