import zmq


def closeZmq(context, *args):
    try:
        if context:
            for connection in args:
                connection.close()
            context.term()
    except Exception: raise


def getProducer(context):
    try:
        # prepare ZMQ queue
        producer = context.socket(zmq.PUSH)
        producer.connect("tcp://127.0.0.1:5558")
        return producer
    except Exception: raise
def getConsumerServer(context):
    try:
        # Set up a channel to receive results
        consumer = context.socket(zmq.PULL)
        consumer.bind("tcp://127.0.0.1:5558")
        return consumer
    except Exception: raise

# use diff chan to communicate with recorder
def getRecorderServer(context):
    try:
        producer = context.socket(zmq.PUSH)
        producer.bind("tcp://127.0.0.1:5559")
        return producer
    except Exception: raise
def getRecorderConsumer(context):
    try:
        consumer = context.socket(zmq.PULL)
        consumer.connect("tcp://127.0.0.1:5559")
        return consumer
    except Exception: raise

# use diff chan to communicate with recorder
def getSyncServer(context):
    try:
        producer = context.socket(zmq.PUSH)
        producer.bind("tcp://127.0.0.1:5560")
        return producer
    except Exception: raise
def getSyncListener(context):
    try:
        consumer = context.socket(zmq.PULL)
        consumer.connect("tcp://127.0.0.1:5560")
        return consumer
    except Exception: raise