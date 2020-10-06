from os import getpid
from pseq import ParallelSequencePipeline, Producer, Processor, Consumer
from random import random, choice
from time import sleep
from collections import namedtuple


JobData = namedtuple("JobData", "n_units")

ChunkData = namedtuple("ChunkData", "secs_to_sleep")

ProcessedChunkData = namedtuple("ProcessedChunkData", "secs_slept")


class TimeProducer(Producer):
    def __init__(self):
        print(f"  TimeProducer.__init__() @pid={getpid()}")

    def init(self):
        print(f"  TimeProducer.init() @pid={getpid()}")

    def produce(self, job_data):
        print(f"  TimeProducer.produce() @pid={getpid()}")
        for _ in range(job_data.n_units):
            yield ChunkData(random() * 3)


class TimeProcessor(Processor):
    def __init__(self):
        print(f"  TimeProcessor.__init__() @pid={getpid()}")

    def init(self):
        print(f"  TimeProcessor.init() @pid={getpid()}")

    def process(self, job_data, chunk_data):
        secs = chunk_data.secs_to_sleep
        print(
            f"  TimeProcessor.process(chunk_data.segs_to_sleep={secs:.2f}) "
            f"@pid={getpid()}"
        )
        secs_slept = choice([secs, secs, secs / 2])
        sleep(secs_slept)
        if choice([True, False, False, False]):
            raise Exception("example exception")
        return ProcessedChunkData(secs_slept)


class TimeConsumer(Consumer):
    def __init__(self):
        print(f"  TimeConsumer.__init__() @pid={getpid()}")

    def init(self):
        print(f"  TimeConsumer.init() @pid={getpid()}")

    def consume(self, job_data, chunk_data, processed_chunk_data, exception):
        print(
            "  TimeConsumer.consume(\n"
            + f"    chunk_data.secs_to_sleep={chunk_data.secs_to_sleep:.2f},\n"
            + "    chunk_data.secs_slept="
            + (
                "None,\n"
                if processed_chunk_data is None
                else f"{processed_chunk_data.secs_slept:.2f},\n"
            )
            + f"    exception={exception!r}\n"
            + f"  ) @pid={getpid()}"
        )


def main():
    pipeline = ParallelSequencePipeline(
        TimeProducer(), TimeProcessor(), TimeConsumer(), n_processors=2
    )
    print(f"calling pipeline.start() @pid={getpid()}")
    pipeline.start()

    sleep(1)  # simulate other things being done

    jobs_data = [JobData(3), JobData(5), JobData(4)]
    jobs = []

    for data in jobs_data:
        print(f"calling pipeline.submit({data}) @pid={getpid()}")
        job = pipeline.submit(data)
        jobs.append(job)
        print(f"submitted job {job.serial} @pid={getpid()}")

    while True:

        sleep(1)  # simulate other things being done

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
    print("calling pipeline.join()")
    pipeline.join()
    print("main() has finished")


if __name__ == "__main__":
    main()
