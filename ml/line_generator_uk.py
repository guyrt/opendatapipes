from typing import Dict

from line_generator import LineGeneratorBase

uk_format_stats = {('city', 'country'): 72852,
    ('city', 'country', 'postcode'): 272330,
    ('city', 'postcode'): 12078,
    ('city', 'postcode', 'country'): 18285,
    ('house_number', 'road'): 303513,
    ('house_number', 'road', 'city', 'country', 'postcode'): 545992,
    ('house_number', 'road', 'city', 'postcode'): 24218,
    ('house_number', 'road', 'city', 'postcode', 'country'): 36710,
    ('house_number', 'road', 'postcode', 'city'): 6127,
    ('house_number', 'road', 'postcode', 'city', 'country'): 145694,
    ('postcode', 'city'): 2962,
    ('postcode', 'city', 'country'): 72920,
    ('road',): 65253,
    ('road', 'city', 'country', 'postcode'): 117204,
    ('road', 'city', 'postcode'): 5405,
    ('road', 'city', 'postcode', 'country'): 7905,
    ('road', 'postcode', 'city'): 1325,
    ('road', 'postcode', 'city', 'country'): 31250
}

total_lines = sum(uk_format_stats.values())
stat_arrays = [[k, v / total_lines] for k, v in uk_format_stats.items()]
uk_formats, uk_format_stats = zip(*stat_arrays)

class UkLineGenerator(LineGeneratorBase):

    def generate(self, line_data : Dict[str, str]) -> str:
        format = self.rng.choice(uk_formats, p=uk_format_stats)
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
