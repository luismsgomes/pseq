import logging
from os import getpid
from pseq import ParallelSequencePipeline, Producer, Processor, Consumer
from random import random, choice
from time import sleep
from collections import namedtuple


LOG = logging.getLogger("example.py")


JobData = namedtuple("JobData", "n_units")

ChunkData = namedtuple("ChunkData", "secs_to_sleep")

ProcessedChunkData = namedtuple("ProcessedChunkData", "secs_slept")


class TimeProducer(Producer):
    def __init__(self):
        LOG.info(f"TimeProducer.__init__() @pid={getpid()}")

    def init(self):
        logging.basicConfig(level=logging.DEBUG)
        LOG.info(f"TimeProducer.init() @pid={getpid()}")

    def produce(self, job_data):
        LOG.info(f"TimeProducer.produce() @pid={getpid()}")
        for _ in range(job_data.n_units):
            yield ChunkData(random() * 3)


class TimeProcessor(Processor):
    def __init__(self):
        LOG.info(f"TimeProcessor.__init__() @pid={getpid()}")

    def init(self):
        logging.basicConfig(level=logging.DEBUG)
        LOG.info(f"TimeProcessor.init() @pid={getpid()}")

    def process(self, job_data, chunk_data):
        secs = chunk_data.secs_to_sleep
        LOG.info(
            f"TimeProcessor.process(chunk_data.segs_to_sleep={secs:.2f}) "
            f"@pid={getpid()}"
        )
        secs_slept = choice([secs, secs, secs / 2])
        sleep(secs_slept)
        if choice([True, False, False, False]):
            raise Exception("example exception")
        return ProcessedChunkData(secs_slept)


class TimeConsumer(Consumer):
    def __init__(self):
        LOG.info(f"TimeConsumer.__init__() @pid={getpid()}")

    def init(self):
        logging.basicConfig(level=logging.DEBUG)
        LOG.info(f"TimeConsumer.init() @pid={getpid()}")

    def consume(self, job_data, chunk_data, processed_chunk_data, exception):
        LOG.info(
            "TimeConsumer.consume("
            + f"chunk_data.secs_to_sleep={chunk_data.secs_to_sleep:.2f},"
            + " chunk_data.secs_slept="
            + (
                "None,"
                if processed_chunk_data is None
                else f"{processed_chunk_data.secs_slept:.2f},"
            )
            + f" exception={exception!r}"
            + f") @pid={getpid()}"
        )


def main():
    logging.basicConfig(level=logging.DEBUG)
    pipeline = ParallelSequencePipeline(
        TimeProducer(), TimeProcessor(), TimeConsumer(), n_processors=2
    )
    LOG.info(f"calling pipeline.start() @pid={getpid()}")
    pipeline.start()

    sleep(1)  # simulate other things being done

    jobs_data = [JobData(3), JobData(5), JobData(4)]
    jobs = []

    for data in jobs_data:
        LOG.info(f"calling pipeline.submit({data}) @pid={getpid()}")
        job = pipeline.submit(data)
        jobs.append(job)
        LOG.info(f"submitted job {job.serial} @pid={getpid()}")

    while True:

        sleep(1)  # simulate other things being done

        LOG.info("finished jobs:")
        finished = pipeline.fetch()
        while finished is not None:
            LOG.info(f"{finished}")
            jobs = [job for job in jobs if job != finished]
            finished = pipeline.fetch()
        # print other job status
        LOG.info("other jobs:")
        for job in jobs:
            LOG.info(f"{job}")
        if not any(job.status.running for job in jobs):
            break
    LOG.info("calling pipeline.close()")
    pipeline.close()
    LOG.info("main() has finished")


if __name__ == "__main__":
    main()
