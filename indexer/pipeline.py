"""
indexer.pipeline utility for describing and initializing queues
for a processing pipeline using indexer.worker.QApp/Worker/...

Every consumer has an an input queue WORKERNAME-in, Every producer has
an output exchange WORKERNAME-out with links to downstream input
queues.
"""

from typing import Dict, List, Set, Union


class PipelineError(RuntimeError):
    """Pipeline exception"""


class Process:
    """Virtual class describing a pipeline process (producer, worker, consumer)"""

    def __init__(self, pipeline: "Pipeline", name: str, producer: bool):
        if name in pipeline.workers:
            raise PipelineError(f"{name} is already defined")

        self.name: str = name
        self.pipeline: "Pipeline" = pipeline
        self.producer: bool = producer

        pipeline.workers[name] = self
        pipeline.queues.add(name)  # XXX f"{name}-in" ?


class Producer(Process):
    """Process with no inputs"""

    def __init__(self, pipeline: "Pipeline", name: str):
        super().__init__(pipeline, name, True)


Inputs = List[Union[Producer, "Worker"]]


class Worker(Process):
    """Process with inputs and output"""

    def __init__(
        self, pipeline: "Pipeline", name: str, inputs: Inputs, _producer: bool = True
    ):
        if len(inputs) == 0:
            raise PipelineError(f"consumer {name} has no inputs")

        for input in inputs:
            if not input.producer:
                raise PipelineError(f"{name} input {input.name} not a producer!")

        super().__init__(pipeline, name, _producer)


class Consumer(Worker):
    """Process with inputs but no output"""

    def __init__(self, pipeline: "Pipeline", name: str, inputs: Inputs):
        super().__init__(pipeline, name, inputs, False)


class Pipeline:
    def __init__(self, name: str):
        self.name = name
        self.queues: Set = set()
        self.workers: Dict[str, Process] = {}

    def add_producer(self, name: str) -> Producer:
        """Add a Process with no inputs"""
        return Producer(self, name)

    def add_worker(self, name: str, inputs: Inputs) -> Worker:
        return Worker(self, name, inputs)

    def add_consumer(self, name: str, inputs: Inputs) -> Consumer:
        """Add a Process with no output"""
        return Consumer(self, name, inputs)


if __name__ == "__main__":
    p = Pipeline("test")
    p.add_consumer("c", [p.add_worker("b", [p.add_producer("a")])])

    print(p.queues)
    print(p.workers)
