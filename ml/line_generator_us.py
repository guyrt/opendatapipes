

from typing import Dict
from line_generator import LineGeneratorBase

us_address_patterns = [
    ('number', 'street', 'unit', 'city', 'region', 'postcode'),
    ('unit', 'number', 'street', 'city', 'region', 'postcode'),
]
us_address_frequency = [0.9, 0.1]  # i made these up.

class USLineGenerator(LineGeneratorBase):

    def generate(self, line_data : Dict[str, str]) -> str:
        format = self.rng.choice(us_address_patterns, p=us_address_frequency)
        line = []
        labels = []
        for pattern in format:
            data = line_data.get(format)
            if not data:
                continue
            tokens = data.split(' ')
            line.extend(tokens)
            labels.extend([pattern] * len(tokens))

        assert len(line) == len(labels)
        return line, labels
