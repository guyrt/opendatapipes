import glob
import json
from tokenizers import Tokenizer
import torch
import random

from line_generator_uk import UkLineGenerator
from line_generator_us import USLineGenerator

tokenizer = Tokenizer.from_file("tokenizers/uk_us.json")


class DataSequence(torch.utils.data.IterableDataset):
    """For now, ignoring the TorchData project, but taking their idea of iteration."""

    # todo - generate text from line
    # todo - also return attention mask and labels.
    
    def __init__(self, folder, labels, labels_to_ids):
        
        # here is where you need to build your lines!
        self._filehandles = {}  # filename => filehandle
        self._files = self._get_files(folder)  # cumsum => extended metadata
        self.total_lines = max(self._files.keys())
        
        self._raw_labels = labels
        self._labels_to_ids = labels_to_ids

        self._us_line_generator = USLineGenerator()
        self._uk_line_generator = UkLineGenerator()
        #self.texts = [tokenizer.encode(line, is_pretokenized=True) for line in lines]
        #self.labels = [create_label_array(self.texts[j].word_ids, labels[j], labels_to_ids) for j in range(len(lines))]    

    def __iter__(self):
        # todo change to numpy choice.
        randint = random.randint(0, self.total_lines)
        key = max([k for k in self._files.keys() if k < randint])

        fh = self._get_fh(key)
        line = fh.readline()
        if not line:
            fh.seek(0)
            line = fh.readline()
        
        line_tokens, labels = self._convert_json_to_line(line)
        line_texts = tokenizer.encode(line_tokens, is_pretoknized=True)
        labels = create_label_array(line_texts.word_ids, labels, self._labels_to_ids)
        labels_t = torch.tensor(labels)

        return {
            'input_ids': torch.tensor(line_texts.ids).flatten(),
            'attention_mask': torch.tensor(line_texts.attention_mask),
            'labels': labels_t
        }
    
    def _convert_json_to_line(self, line):
        # Return line and labels
        line_data = json.loads(line.strip())
        if line_data.get('country') == 'United Kingdom':
            line, labels = self._uk_line_generator.generate(line_data)
        else:
            line, labels = self._us_line_generator.generate(line_data)

        return line, labels

    def _get_fh(self, file_key):
        if file_key in self._filehandles:
            return self._filehandles[file_key]
        fh_metadata = self._files[file_key]
        filename = fh_metadata['fullfilename']
        fh = open(filename, 'r')
        self._filehandles[file_key] = fh
        return fh

    def _get_files(self, folder):
        # return cumsum_breakpoint -> {extended metadata}
        all_files = {}
        files = glob.glob(f"{folder}/**/*.geojson", recursive=True)
        cumsum = 0
        for filename in files:
            metadata_filename = filename.replace(".geojson", ".meta.json")
            metadata = json.load(open(metadata_filename))
            cumsum += metadata['numlines']
            metadata['cumsum'] = cumsum
            metadata['fullfilename'] = filename
            all_files[cumsum] = metadata
        return all_files


def create_label_array(word_ids, original_labels, labels_to_ids):
    try:
        t = [original_labels[i] if i is not None else -100 for i in word_ids]
        return [labels_to_ids[tt] if tt in labels_to_ids else tt for tt in t]
    except IndexError:
        print(f"Error for index {idx}")
        raise
        

def get_data_sequences(fh, num_lines, train_percent, seed=0):
    # todo - kill this. make it part of data prep.
    random.seed(seed)
    unique_labels = set()
    
    train_lines = []
    train_labels = []
    test_lines = []
    test_labels = []
    for i in range(num_lines):
        clean_lines, clean_labels = clean_line(fh.readline(), unique_labels)
        if random.random() < train_percent:
            target_lines = train_lines
            target_labels = train_labels
        else:
            target_lines = test_lines
            target_labels = test_labels
            
        target_lines.append(clean_lines)
        target_labels.append(clean_labels)

        
    labels_to_ids = {k: v for v, k in enumerate(sorted(unique_labels))}

    return len(unique_labels), labels_to_ids, DataSequence(train_lines, train_labels, labels_to_ids), DataSequence(test_lines, test_labels, labels_to_ids)