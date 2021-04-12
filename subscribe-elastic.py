from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
from elasticsearch import Elasticsearch

project_id = "dwh-siloam"
subscription_id = "ax-dwh-log-sub"
timeout = 5.0

subscriber = pubsub_v1.SubscriberClient()

subscription_path = subscriber.subscription_path(project_id, subscription_id)

es = Elasticsearch("http://34.101.246.247:9200")

def callback(message):
    print(f"Received {message.data}.")
    doc = {
            'log': message.data
        }
    res = es.index(index = 'dwh-siloam', body = doc)
    print(" data insert to elastic .")
    message.ack()

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}..\n")

with subscriber:
    try:
        streaming_pull_future.result(timeout=timeout)
    except TimeoutError:
        streaming_pull_future.cancel()