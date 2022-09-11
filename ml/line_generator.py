import abc
from typing import Dict
import numpy

default_rng = numpy.random.default_rng()

class LineGeneratorBase(abc.ABC):

    def __init__(self, rng=None):
        self.rng = rng if rng is not None else default_rng

    @abc.abstractmethod
    def generate(self, line_data : Dict[str, str]) -> str:
        pass
