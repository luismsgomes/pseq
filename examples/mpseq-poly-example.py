from multiprocessing import Queue
from pseq.poly import PolymorphicParallelSequenceProcessor, JobProducer
from pseq.core import Processor, Consumer
from random import random, randint, choice
from time import sleep
import logging


QUEUE = Queue()


class SumJob(object):
    def __init__(self, max, n):
        self.max = max
        self.n = n


class SumData(object):
    def __init__(self, a, b):
        self.a, self.b = a, b


class SumProducer(JobProducer):
    def produce(self, sum_job):
        for _ in range(sum_job.n):
            yield SumData(randint(0, sum_job.max), randint(0, sum_job.max))

    def shutdown(self):
        print("SumProducer shutting down")

    def get_job_class(self):
        return SumJob

    def get_data_class(self):
        return SumData


class SumProcessor(Processor):
    def init(self):
        print("SumProcessor.init(): this is where we start sub-processes if needed")

    def process(self, data):
        print(f"Summing {data.a} with {data.b}")
        return data.a + data.b

    def shutdown(self):
        print("SumProcessor.shutdown(): this is where we cleanup allocated resources")


class SumConsumer(Consumer):
    def consume(self, data, result, exception):
        print(f"Computed {data.a} + {data.b} = {result}; exception={repr(exception)}.")

    def shutdown(self):
        print("SumConsumer shutting down")


class SleepJob(object):
    def __init__(self, max, n):
        self.max = max
        self.n = n


class SleepData(object):
    def __init__(self, secs):
        self.secs = secs


class SleepProducer(JobProducer):
    def produce(self, sleep_job):
        for _ in range(sleep_job.n):
            yield SleepData(random() * sleep_job.max)

    def shutdown(self):
        print("SleepProducer shutting down")

    def get_job_class(self):
        return SleepJob

    def get_data_class(self):
        return SleepData


class SleepProcessor(Processor):
    def process(self, sleep_data):
        print(f"Sleeping {sleep_data.secs} secs")
        sleep(sleep_data.secs)
        return sleep_data.secs

    def shutdown(self):
        print("SleepProcessor shutting down")


class SleepConsumer(Consumer):
    def consume(self, data, result, exception):
        print(f"Slept {result} secs exception={repr(exception)}.")

    def shutdown(self):
        print("SleepConsumer shutting down")


def main():
    logging.basicConfig(level=logging.DEBUG)
    processor = PolymorphicParallelSequenceProcessor(QUEUE, 10)
    processor.register(SleepProducer(), SleepProcessor(), SleepConsumer())
    processor.register(SumProducer(), SumProcessor(), SumConsumer())
    processor.start()

    print("Hello from main process.")
    QUEUE.put(SleepJob(3, 10))
    QUEUE.put(SumJob(100, 10))

    # could do other things here

    processor.join()  # wait for all sub-procs to finish


if __name__ == "__main__":
    main()
