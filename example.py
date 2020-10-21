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

    def shutdown(self):
        LOG.info(f"TimeProducer.shutdown() @pid={getpid()}")


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

    def shutdown(self):
        LOG.info(f"TimeProcessor.shutdown() @pid={getpid()}")


class TimeConsumer(Consumer):
    def __init__(self):
        LOG.info(f"TimeConsumer.__init__() @pid={getpid()}")

    def init(self):
        logging.basicConfig(level=logging.DEBUG)
        LOG.info(f"TimeConsumer.init() @pid={getpid()}")

    def consume(self, job_data, chunk_data, processed_chunk_data, exception):
        LOG.info(
            "TimeConsumer.consume("
            + f"job_data.n_units={job_data.n_units},"
            + f" chunk_data.secs_to_sleep={chunk_data.secs_to_sleep:.2f},"
            + " chunk_data.secs_slept="
            + (
                "None,"
                if processed_chunk_data is None
                else f"{processed_chunk_data.secs_slept:.2f},"
            )
            + f" exception={exception!r}"
            + f") @pid={getpid()}"
        )

    def shutdown(self):
        LOG.info(f"TimeConsumer.shutdown() @pid={getpid()}")


def example(jobs_data, priority_lanes, example_name):
    LOG.info(f"{example_name}: starting @pid={getpid()}")
    pipeline = ParallelSequencePipeline(
        TimeProducer(),
        TimeProcessor(),
        TimeConsumer(),
        n_processors=2,
        priority_lanes=priority_lanes,
    )
    LOG.info(f"{example_name}: calling pipeline.start() @pid={getpid()}")
    pipeline.start()

    sleep(1)  # simulate other things being done

    jobs = []

    for priority, data in jobs_data:
        LOG.info(
            f"{example_name}: calling pipeline.submit({data}, priority={priority}) @pid={getpid()}"
        )
        job = pipeline.submit(data, priority=priority)
        jobs.append(job)
        LOG.info(f"{example_name}: submitted job {job.serial} @pid={getpid()}")

    while True:

        sleep(1)  # simulate other things being done

        LOG.info(f"{example_name}: finished jobs:")
        finished = pipeline.fetch()
        while finished is not None:
            LOG.info(f"{finished}")
            jobs = [job for job in jobs if job != finished]
            finished = pipeline.fetch()
        # print other job status
        LOG.info(f"{example_name}: other jobs:")
        for job in jobs:
            LOG.info(f"{job}")
        if not any(job.status.running for job in jobs):
            break
    LOG.info(f"{example_name}: calling pipeline.shutdown() @pid={getpid()}")
    pipeline.shutdown()
    LOG.info(f"{example_name}: pipeline has been shutdown @pid={getpid()}")


def main():
    logging.basicConfig(level=logging.DEBUG)

    # (priority, jobdata)
    jobs_data = [(0, JobData(3)), (0, JobData(5)), (0, JobData(4))]
    example(jobs_data, None, "[1]")

    # (priority, jobdata)
    jobs_data = [
        (1, JobData(3)),
        (0, JobData(5)),
        (1, JobData(4)),
        (1, JobData(5)),
        (0, JobData(6)),
        (1, JobData(2)),
    ]
    example(jobs_data, [2, 1], "[2,1]")

    LOG.info("main() has finished")


if __name__ == "__main__":
    main()
