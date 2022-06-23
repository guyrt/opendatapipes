import datetime
from json import loads, dumps
import tempfile
import logging

from .blob_helpers import get_blob_client, get_service_client


class FecFileParser(object):
    """
    Given a file from the FEC, apply correct definitions.
    """

    def __init__(self, definitions, upload_date):
        self.feclookup = definitions
        self.upload_date = upload_date

    def getschema(self, version, linetype):
        logging.info(f'Operating on version {version}')
        versioned_formdata = self.feclookup['v' + version]
        i = len(linetype)
        while i >= 0 and linetype[:i] not in versioned_formdata:
            i -= 1
        if not linetype:
            raise Exception("Could not match linetype {0} on version {1}".format(linetype, version))
        final_key = linetype[:i]
        return final_key, versioned_formdata.get(final_key, dict())

    def processfile(self, filehandle, filename):
        """
        Process all lines of a file and list of dictionaries, one per line.
        """
        first_line = filehandle.readline().decode('latin-1')
        first_line = first_line.replace('"', '').strip().split(chr(28))
        if first_line[0] != "HDR":
            raise Exception("Failed to parse: HDR expected on first line")

        fileversion = first_line[2].strip()

        in_comment = False

        for line in filehandle:
            line = line.decode('latin-1').strip()
            line = line.replace('"', '')

            if not line:
                continue
            
            if line == '[BEGINTEXT]':
                in_comment = True
                continue
            elif in_comment:
                if line == '[ENDTEXT]':
                    in_comment = False
                continue

            line = line.split(chr(28))
            linetype = line[0]
            clean_linetype, schema = self.getschema(fileversion, linetype)
            if schema:
                line_dict = {k: v for k, v in zip(schema, line)}
                line_dict['filer'] = line[1]
            else:
                line_dict = {'contents': ','.join(line), 'clean_linetype': clean_linetype, 'FORM': linetype, "error": "NoSchema"}
                line_dict['filename'] = filename

            line_dict["clean_linetype"] = clean_linetype
            line_dict['upload_date'] = self.upload_date

            yield line_dict


def build_parser():
    utc_timestamp = str(datetime.datetime.utcnow())
    definitions = loads(open("./dataloadlib/rawparserdata.json", "r").read())
    return FecFileParser(definitions, utc_timestamp)


class DailyFileWriter(object):

    def __init__(self, fec_file_parser : FecFileParser) -> None:
        self.fec_file_parser = fec_file_parser
        self.output_file_types = {}  # simplified line => tmp file handle

    def parse(self, parse_msg : dict):
        
        q_msgs = []

        input_blob_file_path = parse_msg['blobpath']
        datepattern = parse_msg['datepattern']

        service_client = get_service_client()
        blob = get_blob_client(service_client, "rawunzips", input_blob_file_path)
        tmp_input = tempfile.TemporaryFile()
        tmp_input.write(blob.download_blob().readall())
        tmp_input.flush()
        tmp_input.seek(0)

        for line in self.fec_file_parser.processfile(tmp_input, input_blob_file_path):
            if 'error' in line:
                simple_linetype = 'error'
            else:
                simple_linetype = line['clean_linetype']

            tmp_file = self.get_tmpfile(simple_linetype)
            txt_line = f"{dumps(line)}\n".encode()
            tmp_file.writelines([txt_line])
        
        # upload
        for line_type, fp in self.output_file_types.items():
            fp.flush()
            fp.seek(0)
            out_blob_name = self._get_output_blob_name(input_blob_file_path, line_type, datepattern)
            out_blob = get_blob_client(service_client, "rawparsed", out_blob_name)
            out_blob.upload_blob(fp, overwrite=True)

            q_msgs.append(dumps({
                'input_filename': input_blob_file_path,
                'output_filename': out_blob_name,
                'container': 'rawparsed'
            }))

        return q_msgs

    def _get_output_blob_name(self, input_blob_file_path : str, line_type : str, datepattern : str):
        split_path = input_blob_file_path.split("/")
        t = "/".join(split_path[:-1])
        orig_file = split_path[-1]
        orig_file = orig_file.replace(".fec", ".json")
        return "/".join([t, line_type, datepattern, orig_file])
    
    def get_tmpfile(self, line_type : str):
        if line_type in self.output_file_types:
            return self.output_file_types[line_type]

        fp = tempfile.TemporaryFile()
        self.output_file_types[line_type] = fp
        return fp


if __name__ == "__main__":
    file_parser = build_parser()
    uploader = DailyFileWriter(file_parser)
    ret = uploader.parse({'datepattern': "20220301", "blobpath": "electronic/1577433.fec"})
    print(ret)
