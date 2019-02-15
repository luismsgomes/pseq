from datetime import datetime
from enum import Enum
from itertools import count, chain
from multiprocessing import cpu_count, Process, Queue
from os import getpid
import logging


LOG = logging.getLogger(__name__)


class PipelineComponent(object):
    def init(self):
        pass

    def shutdown(self):
        pass


class Producer(PipelineComponent):
    def produce(self):
        raise NotImplementedError


class Processor(PipelineComponent):
    def process(self, data):
        raise NotImplementedError


class Consumer(PipelineComponent):
    def consume(self, data, result, exception):
        raise NotImplementedError


class WorkUnit(object):
    def __init__(self, serial, data):
        self.serial = serial
        self.data = data
        self.result = None
        self.exception = None

    def log_event(self, event):
        LOG.debug(f"{self} {event}")

    def __str__(self):
        return f"{self.__class__.__name__}-{self.serial}"

    class Event(object):
        def __init__(self, *args, **kwargs):
            self.ts = datetime.now()
            self.args = args
            self.kwargs = kwargs

        def __str__(self):
            s = [f"{self.__class__.__name__} {self.ts}"]
            s.extend(map(str, self.args))
            for k, v in sorted(self.kwargs.items()):
                s.append(f"{k}={repr(v)}")
            return " ".join(s)

    class Produced(Event): pass
    class Queued(Event): pass
    class Executing(Event): pass
    class Executed(Event): pass
    class OnHold(Event): pass
    class Consuming(Event): pass
    class Consumed(Event): pass


class Shutdown(WorkUnit):
    def __init__(self, serial):
        super().__init__(serial, None)


def process(processor, todo_queue, done_queue):
    pid = getpid()
    processor.init()
    work_unit = None
    while not isinstance(work_unit, Shutdown):
        work_unit = todo_queue.get()
        work_unit.log_event(WorkUnit.Executing(pid=pid))
        if isinstance(work_unit, Shutdown):
            try:
                processor.shutdown()
            except:
                pass
        else:
            try:
                work_unit.result = processor.process(work_unit.data)
            except Exception as exception:
                work_unit.exception = exception
        work_unit.log_event(WorkUnit.Executed(pid=pid))
        done_queue.put(work_unit)


def produce(producer, todo_queue, n_processors):
    producer.init()
    serial = count(start=1)
    for data in producer.produce():
        work_unit = WorkUnit(next(serial), data)
        work_unit.log_event(WorkUnit.Produced())
        todo_queue.put(work_unit)
        work_unit.log_event(WorkUnit.Queued())
    producer.shutdown()
    for _ in range(n_processors):
        shutdown_unit = Shutdown(next(serial))
        shutdown_unit.log_event(WorkUnit.Produced())
        todo_queue.put(shutdown_unit)
        shutdown_unit.log_event(WorkUnit.Queued())


def get_done_work_units(done_queue, n_processors):
    n_running = n_processors
    while n_running:
        work_unit = done_queue.get()
        if isinstance(work_unit, Shutdown):
            n_running -= 1
        else:
            assert isinstance(work_unit, WorkUnit)
            yield work_unit


def arrange_work_units_in_order(work_units):
    serial = count(start=1)
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
            work_unit.log_event(WorkUnit.OnHold())
            on_hold[work_unit.serial] = work_unit
    assert not on_hold


def consume(consumer, done_queue, n_processors, require_in_order):
    consumer.init()
    work_units = get_done_work_units(done_queue, n_processors)
    if require_in_order:
        work_units = arrange_work_units_in_order(work_units)
    for work_unit in work_units:
        work_unit.log_event(WorkUnit.Consuming())
        if not isinstance(work_unit, Shutdown):
            consumer.consume(work_unit.data, work_unit.result, work_unit.exception)
        work_unit.log_event(WorkUnit.Consumed())
    consumer.shutdown()


class ParallelSequenceProcessor(object):
    def __init__(self, producer, processor, consumer, n_processors=None, require_in_order=None):
        """
        Creates sub-processes to generate, process and consume a sequence of WorkUnits in parallel.
        """
        if require_in_order is None:
            require_in_order = True
        if n_processors is None:
            n_processors = cpu_count()
        if not isinstance(producer, Producer):
            raise TypeError("producer not an instance of Producer")
        if not isinstance(processor, Processor):
            raise TypeError("processor not an instance of Processor")
        if not isinstance(consumer, Consumer):
            raise TypeError("consumer not an instance of Consumer")
        self.n_processors = n_processors
        self.todo_queue = Queue(maxsize=2*n_processors)
        self.done_queue = Queue(maxsize=2*n_processors)
        LOG.info(f"{self}: creating producer and consumer processes")
        self.procs = [
            Process(
                target=produce,
                args=(producer, self.todo_queue, n_processors),
            ),
            Process(
                target=consume,
                args=(consumer, self.done_queue, n_processors, require_in_order),
            ),
        ]
        LOG.info(f"{self}: creating {n_processors} processor processes")
        self.procs.extend(
            Process(
                target=process,
                args=(processor, self.todo_queue, self.done_queue)
            )
            for _ in range(n_processors)
        )

    def start(self):
        LOG.info(f"{self}: starting {len(self.procs)} sub-processes")
        for proc in self.procs:
            proc.start()
        LOG.info(f"{self}: started {len(self.procs)} sub-processes")

    def join(self):
        LOG.info(f"{self}: waiting for {len(self.procs)} sub-processes")
        for proc in self.procs:
            proc.join()
        LOG.info(f"{self}: all {len(self.procs)} sub-processes terminated")

    def __str__(self):
        return f"{self.__class__.__name__}()"


__all__ = ["ParallelSequenceProcessor", "Producer", "Processor", "Consumer"]
