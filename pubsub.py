from google.cloud import pubsub_v1
# import logging

project_id = "dwh-siloam"
topic_id = "ax-dwh-log"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

# FORMAT = '%(asctime)s:%(name)s:%(levelname)s - %(message)s'

message_data = 'tes loging dwh 1'
 
# data = logging.basicConfig(format=FORMAT, 
#                     level=logging.INFO, 
#                     datefmt='%Y-%b-%d %X%z')
# logging.info(message_data)

future = publisher.publish(
        topic_path, message_data.encode("utf-8"))
print(future.result())