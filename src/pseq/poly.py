import logging
from pseq.core import ParallelSequenceProcessor, Producer, Processor, Consumer


LOG = logging.getLogger(__name__)


class Shutdown(object):
    pass


class JobProducer(object):
    def shutdown(self):
        pass

    def produce(self, job):
        raise NotImplementedError

    def get_job_class(self):
        raise NotImplementedError

    def get_data_class(self):
        raise NotImplementedError


class PolymorphicProducer(Producer):
    def __init__(self, job_queue):
        self.job_queue = job_queue
        self.producers = {}

    def register(self, producer):
        if not isinstance(producer, JobProducer):
            raise TypeError("producer not instance of JobProducer")
        self.producers[producer.get_job_class()] = producer

    def init(self):
        for producer in self.producers.values():
            producer.init()

    def produce(self):
        job = self.job_queue.get()
        while not isinstance(job, Shutdown):
            producer = self.producers[job.__class__]
            yield from producer.produce(job)
            job = self.job_queue.get()
        for producer in self.producers.values():
            producer.shutdown()


class PolymorphicProcessor(Processor):
    def __init__(self):
        self.processors = {}

    def register(self, data_cls, processor):
        self.processors[data_cls] = processor

    def init(self):
        for processor in self.processors.values():
            processor.init()

    def process(self, data):
        processor = self.processors[data.__class__]
        return processor.process(data)

    def shutdown(self):
        for processor in self.processors.values():
            processor.shutdown()


class PolymorphicConsumer(Consumer):
    def __init__(self):
        self.consumers = {}

    def register(self, data_cls, consumer):
        self.consumers[data_cls] = consumer

    def init(self):
        for consumer in self.consumers.values():
            consumer.init()

    def consume(self, data, result, exception):
        consumer = self.consumers[data.__class__]
        consumer.consume(data, result, exception)

    def shutdown(self):
        for consumer in self.consumers.values():
            consumer.shutdown()


class PolymorphicParallelSequenceProcessor(ParallelSequenceProcessor):
    def __init__(self, job_queue, n_processors=None, require_in_order=None):
        self.job_queue = job_queue
        self.poly_producer = PolymorphicProducer(job_queue)
        self.poly_processor = PolymorphicProcessor()
        self.poly_consumer = PolymorphicConsumer()
        super().__init__(self.poly_producer, self.poly_processor, self.poly_consumer, n_processors, require_in_order)

    def register(self, producer, processor, consumer):
        if not isinstance(producer, JobProducer):
            raise TypeError("producer not instance of JobProducer")
        self.poly_producer.register(producer)
        self.poly_processor.register(producer.get_data_class(), processor)
        self.poly_consumer.register(producer.get_data_class(), consumer)

    def join(self):
        self.job_queue.put(Shutdown())
        super().join()
