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

