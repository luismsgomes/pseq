from os import getpid
from pseq.core import ParallelSequencePipeline, Producer, Processor, Consumer
from random import random, choice
from time import sleep


class TimeProducer(Producer):
    def __init__(self):
        print(f"  TimeProducer.__init__() @pid={getpid()}")

    def init(self):
        print(f"  TimeProducer.init() @pid={getpid()}")

    def produce(self, n):
        print(f"  TimeProducer.produce() @pid={getpid()}")
        for _ in range(n):
            yield random() * 3

    def shutdown(self):
        print(f"  TimeProducer.shutdown() @pid={getpid()}")


class TimeProcessor(Processor):
    def __init__(self):
        print(f"  TimeProcessor.__init__() @pid={getpid()}")

    def init(self):
        print(f"  TimeProcessor.init() @pid={getpid()}")

    def process(self, secs):
        print(f"  TimeProcessor.process(secs={secs:.2f}) @pid={getpid()}")
        slept_secs = choice([secs, secs, secs / 2])
        sleep(slept_secs)
        if choice([True, False, False, False]):
            raise Exception("example exception")
        return slept_secs

    def shutdown(self):
        print(f"  TimeProcessor.shutdown() @pid={getpid()}")


class TimeConsumer(Consumer):
    def __init__(self):
        print(f"  TimeConsumer.__init__() @pid={getpid()}")

    def init(self):
        print(f"  TimeConsumer.init() @pid={getpid()}")

    def consume(self, given_secs, slept_secs, exception):
        print(
            f"  TimeConsumer.consume(given_secs={given_secs:.2f},"
            + " slept_secs="
            + ("None" if slept_secs is None else f"{slept_secs:.2f}")
            + f" exception={exception!r}) @pid={getpid()}"
        )

    def shutdown(self):
        print(f"  TimeConsumer.shutdown() @pid={getpid()}")


def main():
    pipeline = ParallelSequencePipeline(
        TimeProducer(), TimeProcessor(), TimeConsumer(), n_processors=2
    )
    print(f"calling pipeline.start() @pid={getpid()}")
    pipeline.start()
    
    sleep(1) # simulate other things being done

    jobs_data = [3, 5, 4]
    jobs = []

    for data in jobs_data:
        print(f"calling pipeline.submit({data}) @pid={getpid()}")
        job = pipeline.submit(data)
        jobs.append(job)
        print(f"submitted job {job.serial} @pid={getpid()}")

    while True:
        
        sleep(1) # simulate other things being done

        print("finished jobs:")
        finished = pipeline.fetch()
        while finished is not None:
            print("  ", finished)
            jobs = [job for job in jobs if job != finished]
            finished = pipeline.fetch()
        # print other job status
        print("other jobs:")
        for job in jobs:
            print("  ", job)
        if not any(job.status.running for job in jobs):
            break



    print(f"calling pipeline.shutdown() @pid={getpid()}")
    pipeline.shutdown()


if __name__ == "__main__":
    main()
