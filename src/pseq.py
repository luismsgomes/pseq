import itertools
import multiprocessing
import traceback


__version__ = "1.0.0"

__author__ = "Luís Gomes"


class Job(object):
    def __init__(self, serial, data, status):
        self.serial = serial
        self.data = data
        self.status = status

    def __str__(self):
        return f"Job {self.serial} {self.status}"

    def __eq__(self, other):
        return isinstance(other, Job) and other.serial == self.serial


class Status(object):
    def __init__(self, manager):
        self.d = manager.dict(
            producing=False, produced=0, processed=0, failed=0, consumed=0,
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

    def shutdown(self):
        pass


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
    def __init__(self, job_serial, serial, data):
        self.job_serial = job_serial
        self.serial = serial
        self.data = data
        self.result = None
        self.exception = None


class Shutdown(WorkUnit):
    def __init__(self, serial):
        super().__init__(None, serial, None)


class ParallelSequencePipeline(object):
    def __init__(
        self,
        producer,
        processor,
        consumer,
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
        self.mp_manager = self.mp_context.Manager()
        if not isinstance(producer, Producer):
            raise TypeError("producer not an instance of Producer")
        if not isinstance(processor, Processor):
            raise TypeError("processor not an instance of Processor")
        if not isinstance(consumer, Consumer):
            raise TypeError("consumer not an instance of Consumer")
        if n_processors is None:
            self.n_processors = self.mp_context.cpu_count()
        else:
            self.n_processors = n_processors
        self.require_in_order = True if require_in_order is None else require_in_order
        self.job_serial = itertools.count(start=1)
        self.job_input_queue = self.mp_context.Queue()
        self.job_output_queue = self.mp_context.Queue()
        self.work_unit_input_queue = self.mp_context.Queue(
            maxsize=2 * self.n_processors
        )
        self.work_unit_output_queue = self.mp_context.Queue(
            maxsize=2 * self.n_processors
        )
        self.active_jobs = self.mp_manager.dict()
        self.procs = [
            self.mp_context.Process(
                target=produce,
                args=(
                    producer,
                    self.job_input_queue,
                    self.active_jobs,
                    self.work_unit_input_queue,
                    self.n_processors,
                ),
            ),
            self.mp_context.Process(
                target=consume,
                args=(
                    consumer,
                    self.work_unit_output_queue,
                    self.active_jobs,
                    self.job_output_queue,
                    self.n_processors,
                    self.require_in_order,
                ),
            ),
        ]
        self.procs.extend(
            self.mp_context.Process(
                target=process,
                args=(
                    processor,
                    self.work_unit_input_queue,
                    self.active_jobs,
                    self.work_unit_output_queue,
                ),
            )
            for _ in range(self.n_processors)
        )

    def start(self):
        for proc in self.procs:
            proc.start()

    def shutdown(self):
        self.job_input_queue.put(None)
        for proc in self.procs:
            proc.join()

    def __str__(self):
        return f"{self.__class__.__name__}()"

    def submit(self, job_data):
        job = Job(
            serial=next(self.job_serial), data=job_data, status=Status(self.mp_manager),
        )
        self.active_jobs[job.serial] = job
        self.job_input_queue.put(job.serial)
        return job

    def fetch(self, wait=False):
        if self.job_output_queue.empty() and not wait:
            return None
        serial = self.job_output_queue.get()
        return self.active_jobs.pop(serial)


def produce(
    producer, job_input_queue, active_jobs, work_unit_input_queue, n_processors
):
    producer.init()
    work_unit_serial = itertools.count(start=1)
    job_serial = job_input_queue.get()
    while job_serial is not None:
        job = active_jobs[job_serial]
        job.status.started_producing()
        try:
            for work_unit_data in producer.produce(job.data):
                work_unit = WorkUnit(
                    job_serial=job_serial,
                    serial=next(work_unit_serial),
                    data=work_unit_data,
                )
                job.status.incr_produced()
                work_unit_input_queue.put(work_unit)
        except:  # noqa E722
            traceback.print_exc()
        job.status.stopped_producing()
        job_serial = job_input_queue.get()
    try:
        producer.shutdown()
    except:  # noqa E722
        traceback.print_exc()
    for _ in range(n_processors):
        shutdown_unit = Shutdown(next(work_unit_serial))
        work_unit_input_queue.put(shutdown_unit)


def process(processor, work_unit_input_queue, active_jobs, work_unit_output_queue):
    processor.init()
    work_unit = None
    while not isinstance(work_unit, Shutdown):
        work_unit = work_unit_input_queue.get()
        if isinstance(work_unit, Shutdown):
            try:
                processor.shutdown()
            except:  # noqa E722
                traceback.print_exc()
                pass
        else:
            job = active_jobs[work_unit.job_serial]
            try:
                work_unit.result = processor.process(job.data, work_unit.data)
                job.status.incr_processed()
            except Exception as exception:
                work_unit.exception = exception
                job.status.incr_failed()
        work_unit_output_queue.put(work_unit)


def consume(
    consumer,
    work_unit_output_queue,
    active_jobs,
    job_output_queue,
    n_processors,
    require_in_order,
):
    consumer.init()
    work_units = get_done_work_units(work_unit_output_queue, n_processors)
    if require_in_order:
        work_units = arrange_work_units_in_order(work_units)
    for work_unit in work_units:
        if not isinstance(work_unit, Shutdown):
            job = active_jobs[work_unit.job_serial]
            try:
                consumer.consume(
                    job.data, work_unit.data, work_unit.result, work_unit.exception
                )
            except:  # noqa E722
                traceback.print_exc()
            job.status.incr_consumed()
            if not job.status.running:
                job_output_queue.put(job.serial)
    consumer.shutdown()


def get_done_work_units(processed, n_processors):
    n_running = n_processors
    while n_running:
        work_unit = processed.get()
        if isinstance(work_unit, Shutdown):
            n_running -= 1
        else:
            assert isinstance(work_unit, WorkUnit)
            yield work_unit


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
