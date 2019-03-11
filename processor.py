import pika, logging, yaml

from consumer import Consumer
from publisher import Publisher

logging.basicConfig(level=logging.DEBUG)

logger = logging.getLogger()

with open('config.yaml') as f:
	config_file = f.read()

config = yaml.load(config_file)

configuration = config['rabbitmq']
amqp_url = configuration['url']
consumer_config = configuration['consumers']
publisher_config = configuration['publishers']

consumers = dict()
for consumer in consumer_config:
	logger.debug("Consumer:\t{}::{}".format(type(consumer),consumer))
	consumers[consumer['name']] = Consumer(amqp_url, **consumer)

publishers = dict()
for publisher in publisher_config:
	logger.debug("Publisher:\t{}::{}".format(type(publisher),publisher))
	publishers[publisher['name']] = Publisher(amqp_url, **publisher)

try:
	for consumer in consumers.values():
		consumer.start()
	for publisher in publishers.values():
		publisher.start()

	while True:
		tasking_requests = list()
		tasking_respones = list()
		agent_connects = list()

		tasking_requests = consumers['tasking.request.from.controller'].dequeue()
		tasking_responses = consumers['tasking.respone.from.handler'].dequeue()
		agent_connects = consumers['connect.from.handler'].dequeue()

		logger.debug("Found {} tasking requests.".format(len(tasking_requests)))
		if len(tasking_requests) > 0:
			logger.debug("Pushing tasking requests to publisher.")
			publishers['tasking.requst.to.handler'].enqueue(tasking_requests)
			logger.debug("Tasking requests pushed.")
		logger.debug("Found {} tasking responses.".format(len(tasking_respones)))
		if len(tasking_respones) > 0:
			logger.debug("Pushing tasking responses to controllers.")
			publishers['tasking.response.to.controller'].enqueue(tasking_responses)
			logger.debug("Tasking responses pushed...")
		logger.debug("Found {} agent connects.".format(len(agent_connects)))
		if len(agent_connects) > 0:
			logger.debug("Pushing agent conect.")
			publishers['connect.to.controller'].enqueue(agent_connects)
			logger.debug("Agent connects pushed...")

except KeyboardInterrupt:
	for consumer in consumers:
		consumer.stop()
		consumer.join()
	for publisher in publishers:
		publisher.stop()
		publisher.join()
