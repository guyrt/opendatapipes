import pprint

fh = open("../data/uk_openaddresses_formatted_addresses_tagged.random.tsv", 'r')
patterns = {}

for line in fh:
    line = line.strip().split('\t')[2]
    line = line.replace('|/FSEP', '')

    parts = []

    for part in line.split(' '):
        if not part:
            continue
        
        if part.startswith('//'):
            data, code = '/', part[2:]
        else:
            try:
                data, code = part.split('/')
            except ValueError:
                print(line)
                raise 

        if code == 'SEP':
            continue

        if len(parts) == 0 or parts[-1] != code:
            parts.append(code)

    key = tuple(parts)
    if key not in patterns:
        patterns[key] = 0
    patterns[key] += 1

pp = pprint.PrettyPrinter(indent=4)
pp.pprint(patterns)


"""
produces 

{   ('city', 'country'): 72852,
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
    ('road', 'postcode', 'city', 'country'): 31250}
    """