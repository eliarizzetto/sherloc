import re
from datetime import datetime
import csv
from os import makedirs, listdir
from os.path import join, isdir
from csv import DictReader, field_size_limit, DictWriter
from zipfile import ZipFile
import tarfile
from tqdm import tqdm
from io import TextIOWrapper
import logging
import json



"""
La funzione read_compressed_meta_dump serve solo per leggere il dump compresso di Meta CSV.
 La classe MultiFileWriter è un context manager per scrivere l'output del processo riga per riga su diversi 
file: in questo modo non c'è più un unico grande file lunghissimo, ma una cartella il cui percorso è specificato dall'utente,
all'interno della quale vengono salvati file nominati con numeri in modo incrementale (es. 0.json, 1.json, 2.json, ecc) e 
lunghi ciascuno 10.000 righe. I file sono in formato JSON-Lines, quindi ogni riga è un JSON object: ciascun file
deve essere letto in streaming riga per riga. In ogni caso, la funzione get_orcids() ritorna comunque un grande dizionario,
che praticamente racchiude il contenuto di tutti i file JSON-L in un unico oggetto.
Per avere direttamente la cartella con i file, puoi mandare lo script così com'è, dopo aver specificato i percorsi di input e 
output sotto if __name__ == '__main__'.
"""

def read_compressed_meta_dump(archive_path: str):
    """
    Reads the ZIP or TAR.GZ archive storing the CSV files of OC Meta CSV dump.
    :param archive_path: the path to the archive file.
    :return: Yields rows from the CSV files as dictionaries.
    """
    field_size_limit(131072 * 12)  # raise default field_size_limit for csv operations

    if archive_path.endswith('.zip'):
        with ZipFile(archive_path) as archive:
            for csv_file in tqdm(archive.namelist()):
                if csv_file.endswith('.csv'):
                    logging.debug(f'Processing file {csv_file}')
                    with archive.open(csv_file, 'r') as f:
                        reader = DictReader(TextIOWrapper(f, encoding='utf-8'), dialect='unix')
                        for row in reader:
                            yield row

    elif archive_path.endswith('.tar.gz'):
        with tarfile.open(archive_path, 'r:gz') as archive:
            print('Reading .tar file (this may take a while)...')
            for member in tqdm(archive.getmembers()):
                if member.isfile() and member.name.endswith('.csv'):
                    logging.debug(f'Processing file {member.name}')
                    f = archive.extractfile(member)
                    if f:
                        reader = DictReader(TextIOWrapper(f, encoding='utf-8'), dialect='unix')
                        for row in reader:
                            yield row

    else:
        raise ValueError("Unsupported archive format. Only .zip and .tar.gz are supported.")


class MultiFileWriter:
    """
    A context manager for writing rows to CSV files with automatic file splitting.

    :param out_dir: The directory for storing CSV files.
    :param max_rows_per_file: Max rows before creating a new file (default: 10,000).
    :type max_rows_per_file: int, optional
    :param fieldnames: Field names for the CSV file.
    :type fieldnames: List[str]
    :param file_extension: File extension for the CSV files (default: 'csv').
    :type file_extension: str, optional
    :param encoding: Encoding for writing CSV files (default: 'utf-8').
    :type encoding: str, optional
    :param dialect: CSV dialect to use (default: 'unix').
    :type dialect: str, optional

    Example::

        fieldnames = ['omid', 'type', 'omid_only']
        with MultiFileWriter('output_data', fieldnames, max_rows_per_file=5000, file_extension='csv') as file_writer:
            for data_row in dataset:
                processed_row = process_data(data_row)
                file_writer.write_row(processed_row)
    """
    def __init__(self, out_dir, nrows=10000, **kwargs):
        self.out_dir = out_dir
        self.max_rows_per_file = nrows  # maximum number of rows per file
        self.file_name = 0
        self.rows_written = 0
        self.current_file = None
        self.kwargs = kwargs
        makedirs(out_dir, exist_ok=True)
        csv.field_size_limit(131072 * 12)  # increase the default field size limit

    def __enter__(self):
        self._open_new_file()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def _open_new_file(self):
        if self.current_file:
            self.current_file.close()
        file_extension = self.kwargs.get('file_extension', 'csv')
        file_path = join(self.out_dir, f'{self.file_name}.{file_extension}')
        encoding = self.kwargs.get('encoding', 'utf-8')
        self.current_file = open(file_path, 'w', encoding=encoding, newline='')

        if file_extension == 'csv':
            fieldnames = self.kwargs.get('fieldnames', None)
            dialect = self.kwargs.get('dialect', 'unix')
            self.writer = DictWriter(self.current_file, fieldnames=fieldnames, dialect=dialect, quotechar='"')
            self.writer.writeheader()
            self.write_line = self._write_csv_row
        elif file_extension == 'json':
            self.write_line = self._write_jsonl_row
        else:
            raise ValueError("File extension must be either 'csv' or 'json'.")

    def _write_csv_row(self, row):
        self.writer.writerow(row)

    def _write_jsonl_row(self, row):
        json_line = json.dumps(row, ensure_ascii=False)
        self.current_file.write(json_line + '\n')

    def write_row(self, row):
        self.write_line(row)
        self.rows_written += 1
        if self.rows_written >= self.max_rows_per_file:
            self.file_name += 1
            self.rows_written = 0
            self._open_new_file()

    def close(self):
        if self.current_file:
            self.current_file.close()


def check_orcids_consistency(row:dict):
    full_authors = row['author'].split('; ') # list of all authors in their full representation (last name, first name and IDs)
    result = dict()
    if full_authors:
        for a in full_authors:
            match = re.search(r'\[.*]', a)

            if match:
                ids = match.group().replace('[', '', 1).replace(']', '', 1).strip().split()
                name = a[:a.index('[')].strip()
                orcids = {o for o in ids if o.startswith('orcid')}
                try:
                    omid = [i for i in ids if i.startswith('omid')][0]
                except IndexError:
                    # if no OMID is found inside square brackets it's because suare brackets are used in the author's name string
                    logging.warning(f'Square brackets inside author name string! {row}')
                    continue

                # # todo: tieni registro di autori con più di un orcid
                # if len(orcids) > 1:
                #     logging.warning('Multiple ORCIDs for the same OMID.')
                #     print(row, '\n')

                if name and orcids:
                    for orcid in orcids:
                        if result.get(orcid):
                            result[orcid]['name'].update({name})
                            result[orcid]['omid'].update({omid})
                        else:
                            result[orcid] = {'name': {name}, 'omid': {omid}}
    return result


def get_orcids(zip_path, out_dir):
    orcid_dict = dict()

    for row in read_compressed_meta_dump(zip_path):

        authors_orcids_in_row = check_orcids_consistency(row).items()
        for orcid, value in authors_orcids_in_row:
            if orcid_dict.get(orcid):
                orcid_dict[orcid]['name'].update(value['name'])
                orcid_dict[orcid]['omid'].update(value['omid'])
                # if len(orcid_dict[orcid]['name']) > 1:
                #     print(f"Stesso ORCID ({orcid}) per nomi diversi: {orcid_dict[orcid]['name']}.")
                # if len(orcid_dict[orcid]['omid']) > 1:
                #     print(f"Stesso ORCID ({orcid}) per OMID diversi: {orcid_dict[orcid]['omid']}.")
            else:
                orcid_dict[orcid] = {'name': value['name'], 'omid': value['omid']}

    with MultiFileWriter(out_dir, file_extension='json') as writer:
        for k, v in orcid_dict.items():
            writer.write_row({k:v})

    return orcid_dict



if __name__ == '__main__':
    log_file = f'get_orcids_{datetime.now().strftime("%Y-%m-%d")}.log'
    logging.basicConfig(filename=log_file, encoding='utf-8', level=logging.WARNING)

    path_meta_csv = ''  # percorso al file .zip o .tar.gz del dump CSV di Meta
    out = ''  # percorso alla cartella dove vuoi salvare i file JSON di output
    result = get_orcids(path_meta_csv, out)

    # to work only with the results for which there are multiple OMIDs for the same ORCID:
    for fn in listdir(out):
        fp = join(out, fn)
        with open(fp, 'r', encoding='utf-8') as f:
            for l in f:
                line = json.loads(l)

                if len(list(line.values())[0]['omid']) > 1:
                    print(line)
                    ...  # do stuff