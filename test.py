from consumer import Consumer
from test_publisher import Publisher

import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()

url= 'amqp://guest:guest@localhost:5672/%2F?connection_attempts=3&heartbeat_interval=3600'
#name= 'tasking.request.from.controller'
#exchange= 'incoming'
#exchange_type= 'direct'
#queue= 'tasking.request.from.controller'
#routing_key= 'tasking.request.from.controller'
#name= 'tasking.respone.from.handler'
#exchange= 'incoming'
#exchange_type= 'direct'
#queue= 'tasking.respone.from.handler'
#routing_key= 'tasking.respone.from.handler'
name= 'connect.from.handler'
exchange= 'incoming'
exchange_type= 'direct'
queue= 'connect.from.handler'
routing_key= 'connect.from.handler'
publisher = Publisher(url, name=name, exchange=exchange, exchange_type=exchange_type, queue=queue, routing_key=routing_key)

try:
	publisher.start()

except KeyboardInterrupt:

	publisher.stop()

publisher.join()
