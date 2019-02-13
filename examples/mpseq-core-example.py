from pseq.core import ParallelSequenceProcessor, Producer, Processor, Consumer
from random import random, choice
from time import sleep
import logging


class TimeProducer(Producer):
    def produce(self):
        for _ in range(20):
            yield random() * 3

    def shutdown(self):
        print("Producer shutting down")

class TimeProcessor(Processor):
    def process(self, secs):
        print(f"Sleeping {secs} secs")
        slept_secs = choice([secs, secs, secs / 2])
        sleep(slept_secs)
        if choice([True, False, False, False]):
            raise Exception
        return slept_secs

    def shutdown(self):
        print("Processor shutting down")

class TimeConsumer(Consumer):
    def consume(self, given_secs, slept_secs, exception):
        print(f"Tried to sleep {given_secs}, slept {slept_secs}; exception {repr(exception)}")

    def shutdown(self):
        print("Consumer shutting down")


def main():
    logging.basicConfig(level=logging.DEBUG)
    processor = ParallelSequenceProcessor(TimeProducer(), TimeProcessor(), TimeConsumer(), 2)
    processor.start()

    print("Hello from main process.")
    # could do other things here

    processor.join() # wait for all sub-procs to finish


if __name__ == "__main__":
    main()
