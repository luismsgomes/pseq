import datetime
import itertools
import os
import multiprocessing
import traceback


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


class Shutdown(WorkUnit):
    def __init__(self, serial):
        super().__init__(serial, None)


def process(processor, todo_queue, done_queue):
    pid = os.getpid()
    processor.init()
    work_unit = None
    while not isinstance(work_unit, Shutdown):
        work_unit = todo_queue.get()
        if isinstance(work_unit, Shutdown):
            try:
                processor.shutdown()
            except:
                traceback.print_exc()
                pass
        else:
            try:
                work_unit.result = processor.process(work_unit.data)
            except Exception as exception:
               work_unit.exception = exception
        done_queue.put(work_unit)


def produce(producer, todo_queue, n_processors):
    producer.init()
    serial = itertools.count(start=1)
    try:
        for data in producer.produce():
            work_unit = WorkUnit(next(serial), data)
            todo_queue.put(work_unit)
    except:  # if producer.produce() crashes
        traceback.print_exc()
    try:
        producer.shutdown()
    except:
        traceback.print_exc()
    for _ in range(n_processors):
        shutdown_unit = Shutdown(next(serial))
        todo_queue.put(shutdown_unit)

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


def consume(consumer, done_queue, n_processors, require_in_order):
    consumer.init()
    work_units = get_done_work_units(done_queue, n_processors)
    if require_in_order:
        work_units = arrange_work_units_in_order(work_units)
    for work_unit in work_units:
        if not isinstance(work_unit, Shutdown):
            try:
                consumer.consume(work_unit.data, work_unit.result, work_unit.exception)
            except:
                traceback.print_exc()
    consumer.shutdown()


class ParallelSequenceProcessor(object):
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
        Creates sub-processes to generate, process and consume a sequence of WorkUnits in parallel.

        Argument mp_context must be obtained by calling multiprocessing.get_context().
        See https://docs.python.org/3/library/multiprocessing.html?highlight=multiprocessing#multiprocessing.get_context

        """
        if mp_context is None:
            mp_context = multiprocessing.get_context(method="forkserver")
        if require_in_order is None:
            require_in_order = True
        if n_processors is None:
            n_processors = mp_context.cpu_count()
        if not isinstance(producer, Producer):
            raise TypeError("producer not an instance of Producer")
        if not isinstance(processor, Processor):
            raise TypeError("processor not an instance of Processor")
        if not isinstance(consumer, Consumer):
            raise TypeError("consumer not an instance of Consumer")
        self.n_processors = n_processors
        self.todo_queue = mp_context.Queue(maxsize=2 * n_processors)
        self.done_queue = mp_context.Queue(maxsize=2 * n_processors)
        self.procs = [
            mp_context.Process(
                target=produce, args=(producer, self.todo_queue, n_processors),
            ),
            mp_context.Process(
                target=consume,
                args=(consumer, self.done_queue, n_processors, require_in_order),
            ),
        ]
        self.procs.extend(
            mp_context.Process(
                target=process, args=(processor, self.todo_queue, self.done_queue)
            )
            for _ in range(n_processors)
        )

    def start(self):
        for proc in self.procs:
            proc.start()

    def join(self):
        for proc in self.procs:
            proc.join()

    def __str__(self):
        return f"{self.__class__.__name__}()"


__all__ = ["ParallelSequenceProcessor", "Producer", "Processor", "Consumer"]
