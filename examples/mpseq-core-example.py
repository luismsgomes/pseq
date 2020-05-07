from os import getpid
from pseq.core import ParallelSequenceProcessor, Producer, Processor, Consumer
from random import random, choice
from time import sleep


class TimeProducer(Producer):
    def __init__(self):
        print(f"TimeProducer.__init__() @pid={getpid()}")

    def init(self):
        print(f"TimeProducer.init() @pid={getpid()}")

    def produce(self):
        print(f"TimeProducer.produce() @pid={getpid()}")
        for _ in range(10):
            yield random() * 3

    def shutdown(self):
        print(f"TimeProducer.shutdown() @pid={getpid()}")


class TimeProcessor(Processor):
    def __init__(self):
        print(f"TimeProcessor.__init__() @pid={getpid()}")

    def init(self):
        print(f"TimeProcessor.init() @pid={getpid()}")

    def process(self, secs):
        print(f"TimeProcessor.process(secs={secs:.2f}) @pid={getpid()}")
        slept_secs = choice([secs, secs, secs / 2])
        sleep(slept_secs)
        if choice([True, False, False, False]):
            raise Exception("example exception")
        return slept_secs

    def shutdown(self):
        print(f"TimeProcessor.shutdown() @pid={getpid()}")


class TimeConsumer(Consumer):
    def __init__(self):
        print(f"TimeConsumer.__init__() @pid={getpid()}")

    def init(self):
        print(f"TimeConsumer.init() @pid={getpid()}")

    def consume(self, given_secs, slept_secs, exception):
        print(
            f"TimeConsumer.consume(given_secs={given_secs:.2f},"
            + " slept_secs=" + ("None" if slept_secs is None else f"{slept_secs:.2f}")
            + f" exception={exception!r}) @pid={getpid()}"
        )

    def shutdown(self):
        print(f"TimeConsumer.shutdown() @pid={getpid()}")


def main():
    print(f"main() @pid={getpid()}")
    processor = ParallelSequenceProcessor(
        TimeProducer(), TimeProcessor(), TimeConsumer(), n_processors=2
    )
    processor.start()

    # could do other things here

    processor.join()  # wait for all sub-procs to finish


if __name__ == "__main__":
    main()
