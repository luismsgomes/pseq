import itertools
import logging
import multiprocessing
import multiprocessing.managers
import queue
import os
import traceback

__version__ = "1.1.0"

__author__ = "Luís Gomes"


LOG = logging.getLogger(__name__)


class Job(object):
    def __init__(self, lane, priority, serial, data, status):
        self.lane = lane
        self.priority = priority
        self.serial = serial
        self.data = data
        self.status = status

    def __str__(self):
        return f"Job {self.priority}!{self.lane}:{self.serial}, {self.status}"

    def __eq__(self, other):
        return (
            other.lane == self.lane
            and other.priority == self.priority
            and other.serial == self.serial
        )

    def __ne__(self, other):
        return not (self == other)

    def __lt__(self, other):
        return (
            self.priority < other.priority
            or self.priority == other.priority
            and self.serial < other.serial
        )

    def __le__(self, other):
        return (self < other) or (self == other)

    def __gt__(self, other):
        return not (self <= other)

    def __ge__(self, other):
        return not (self < other)


class Status(object):
    def __init__(self, manager):
        self.d = manager.dict(
            producing=False,
            produced=0,
            processed=0,
            failed=0,
            consumed=0,
        )

    @property
    def producing(self):
        return self.d["producing"]

    def started_producing(self):
        self.d["producing"] = True

    def stopped_producing(self):
        self.d["producing"] = False

    @property
    def produced(self):
        return self.d["produced"]

    def incr_produced(self):
        self.d["produced"] += 1

    @property
    def processed(self):
        return self.d["processed"]

    def incr_processed(self):
        self.d["processed"] += 1

    @property
    def failed(self):
        return self.d["failed"]

    def incr_failed(self):
        self.d["failed"] += 1

    @property
    def consumed(self):
        return self.d["consumed"]

    def incr_consumed(self):
        self.d["consumed"] += 1

    @property
    def standing(self):
        return self.produced - self.consumed

    @property
    def running(self):
        return self.producing or self.standing > 0

    def __str__(self):
        return (
            f"{'running' if self.running else 'finished'}"
            f" ({self.produced} produced,"
            f" {self.processed} processed,"
            f" {self.failed} failed,"
            f" {self.consumed} consumed)"
        )


class Component(object):
    def init(self):
        pass

    def __str__(self):
        return f"{self.__class__.__name__} pid={os.getpid()} ppid={os.getppid()}"


class Producer(Component):
    def produce(self, data):
        raise NotImplementedError


class Processor(Component):
    def process(self, data):
        raise NotImplementedError


class Consumer(Component):
    def consume(self, data, result, exception):
        raise NotImplementedError


class WorkUnit(object):
    def __init__(self, lane, priority, serial, job_serial, data):
        self.lane = lane
        self.priority = priority
        self.serial = serial
        self.job_serial = job_serial
        self.data = data
        self.result = None
        self.exception = None

    def __str__(self):
        data = "None" if self.data is None else "<...>"
        result = "None" if self.result is None else "<...>"
        exception = "None" if self.exception is None else "<...>"
        return (
            f"WorkUnit {self.priority}!{self.lane}:{self.serial} "
            f"job_serial={self.job_serial} data={data} result={result} "
            f"exception={exception}"
        )


class PipelineSyncManager(multiprocessing.managers.SyncManager):
    pass


PipelineSyncManager.register("PriorityQueue", queue.PriorityQueue)


class ParallelSequencePipeline(object):
    def __init__(
        self,
        producer,
        processor,
        consumer,
        priority_lanes=None,
        n_processors=None,
        require_in_order=None,
        mp_context=None,
    ):
        """
        Processes a sequence in parallel.

        Argument mp_context must be obtained by calling
        multiprocessing.get_context().
        See https://docs.python.org/3/library/multiprocessing.html

        """
        if mp_context is None:
            self.mp_context = multiprocessing.get_context(method="forkserver")
        else:
            self.mp_context = mp_context
        self.mp_manager = PipelineSyncManager(ctx=self.mp_context)
        self.mp_manager.start()
        if not isinstance(producer, Producer):
            raise TypeError("producer not an instance of Producer")
        if not isinstance(processor, Processor):
            raise TypeError("processor not an instance of Processor")
        if not isinstance(consumer, Consumer):
            raise TypeError("consumer not an instance of Consumer")
        if priority_lanes is None:
            priority_lanes = [1]  # compatible with old API
        elif not isinstance(priority_lanes, list):
            priority_lanes = list(priority_lanes)
        self.max_priority = len(priority_lanes)
        lane_priorities = []
        for priority, priority_n_lanes in enumerate(priority_lanes):
            if not isinstance(priority_n_lanes, int):
                raise TypeError(f"invalid number of lanes: {priority_n_lanes!r}")
            if priority_n_lanes < 1:
                raise ValueError(
                    f"number of lanes for priority {priority} must be greater "
                    "than or equal to one"
                )
            lane_priorities.extend([priority for i in range(priority_n_lanes)])
        self.n_lanes = len(lane_priorities)
        if n_processors is None:
            self.n_processors = self.mp_context.cpu_count()
        else:
            if not isinstance(n_processors, int):
                raise TypeError("n_processors is not an integer")
            if n_processors < 1:
                raise ValueError("n_processors must be greater than or equal to one")
            self.n_processors = n_processors
        self.require_in_order = True if require_in_order is None else require_in_order
        self.job_serials = [itertools.count(start=1) for _ in range(self.n_lanes)]
        self.job_input_queues = [
            self.mp_context.Queue() for _ in range(self.max_priority)
        ]
        self.job_output_queue = self.mp_context.PriorityQueue()
        self.work_unit_input_queue = self.mp_context.PriorityQueue()
        self.work_unit_output_queues = [
            self.mp_context.Queue() for _ in range(self.n_lanes)
        ]
        self.active_jobs = self.mp_manager.dict()
        self.producers = [
            self.mp_context.Process(
                target=produce,
                args=(
                    producer,
                    lane,
                    self.job_input_queues[priority],
                    self.active_jobs,
                    self.work_unit_input_queue,
                ),
            )
            for lane, priority in enumerate(lane_priorities)
        ]
        self.processors = [
            self.mp_context.Process(
                target=process,
                args=(
                    processor,
                    self.work_unit_input_queue,
                    self.active_jobs,
                    self.work_unit_output_queues,
                ),
            )
            for _ in range(self.n_processors)
        ]
        self.consumers = [
            self.mp_context.Process(
                target=consume,
                args=(
                    consumer,
                    work_unit_output_queue,
                    self.active_jobs,
                    self.job_output_queue,
                    self.require_in_order,
                ),
            )
            for work_unit_output_queue in self.work_unit_output_queues
        ]

    def start(self):
        for proc in itertools.chain(self.producers, self.processors, self.consumers):
            proc.start()

    def __str__(self):
        return f"{self.__class__.__name__} pid={os.getpid()} ppid={os.getppid()}"

    def submit(self, priority, job_data):
        if not 0 <= priority < self.max_priority:
            raise ValueError(
                f"invalid priority value ({priority}); expected 0 <= priority "
                f"< {self.max_priority}"
            )
        job = Job(
            lane=None,  # will be assigned immediately before given to a producer
            priority=priority,
            serial=next(self.job_serial),
            data=job_data,
            status=Status(self.mp_manager),
        )
        self.active_jobs[job.serial] = job
        self.job_input_queues[priority].put(job.serial)
        return job

    def fetch(self, wait=False):
        if self.job_output_queue.empty() and not wait:
            return None
        serial = self.job_output_queue.get()
        return self.active_jobs.pop(serial)


def _log_gen_exc(gen, logmsg):
    try:
        yield from gen
    except:  # noqa E722
        LOG.exception(logmsg)


def produce(producer, lane, job_input_queue, active_jobs, work_unit_input_queue):
    producer.init()
    work_unit_serial = itertools.count(start=1)
    job_serial = job_input_queue.get()
    while job_serial is not None:
        job = active_jobs[job_serial]
        job.lane = lane
        job.status.started_producing()
        logmsg = f"[{producer}] raised exception while producing work units for [{job}]"
        for work_unit_data in _log_gen_exc(producer.produce(job.data), logmsg):
            work_unit = WorkUnit(
                lane=lane,
                priority=job.priority,
                serial=next(work_unit_serial),
                job_serial=job_serial,
                data=work_unit_data,
            )
            job.status.incr_produced()
            work_unit_input_queue.put(work_unit)
        job.status.stopped_producing()
        job_input_queue.task_done()
        job_serial = job_input_queue.get()


def process(processor, work_unit_input_queue, active_jobs, work_unit_output_queues):
    processor.init()
    work_unit = work_unit_input_queue.get()
    while work_unit is not None:
        job = active_jobs[work_unit.job_serial]
        try:
            work_unit.result = processor.process(job.data, work_unit.data)
            job.status.incr_processed()
        except:  # noqa E722
            work_unit.exception = traceback.format_exc()
            LOG.exception(f"[{processor}] failed to process [{work_unit}]")
            job.status.incr_failed()
        work_unit_output_queues[work_unit.lane].put(work_unit)
        work_unit_input_queue.task_done()
        work_unit = work_unit_input_queue.get()


def consume(
    consumer,
    work_unit_output_queue,
    active_jobs,
    job_output_queue,
    require_in_order,
):
    consumer.init()
    work_units = get_done_work_units(work_unit_output_queue)
    if require_in_order:
        work_units = arrange_work_units_in_order(work_units)
    for work_unit in work_units:
        job = active_jobs[work_unit.job_serial]
        try:
            consumer.consume(
                job.data, work_unit.data, work_unit.result, work_unit.exception
            )
        except:  # noqa E722
            LOG.exception(f"[{consumer}] failed to consume [{work_unit}] of [{job}]")
        job.status.incr_consumed()
        if not job.status.running:
            job_output_queue.put(job.serial)


def get_done_work_units(processed):
    work_unit = processed.get()
    while work_unit is not None:
        yield work_unit
        processed.task_done()
        work_unit = processed.get()


def arrange_work_units_in_order(work_units):
    serial = itertools.count(start=1)
    next_serial = next(serial)
    on_hold = dict()
    for work_unit in work_units:
        if work_unit.serial == next_serial:
            yield work_unit
            next_serial = next(serial)
            while next_serial in on_hold:
                held_unit = on_hold.pop(next_serial)
                yield held_unit
                next_serial = next(serial)
        else:
            on_hold[work_unit.serial] = work_unit
    assert not on_hold


__all__ = [
    "Status",
    "Component",
    "Producer",
    "Processor",
    "Consumer",
    "ParallelSequencePipeline",
]
