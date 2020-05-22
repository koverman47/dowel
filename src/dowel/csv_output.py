"""A `dowel.logger.LogOutput` for CSV files."""
import csv
import os
import warnings
from copy import deepcopy

from dowel import TabularInput
from dowel.simple_outputs import FileOutput
from dowel.utils import colorize


class CsvOutput(FileOutput):
    """CSV file output for logger.

    :param file_name: The file this output should log to.
    """

    def __init__(self, file_name):
        super().__init__(file_name)
        self._writer = None
        self._fieldnames = None
        self._disable_warnings = False

    @property
    def types_accepted(self):
        """Accept TabularInput objects only."""
        return (TabularInput, )

    def record(self, data, prefix=''):
        """Log tabular data to CSV."""
        if isinstance(data, TabularInput):
            to_csv = data.as_primitive_dict

            if not to_csv.keys() and not self._writer:
                return

            if not self._writer:
                self._fieldnames = set(to_csv.keys())
                self._writer = self._make_writer(actions='ignore')
                self._writer.writeheader()

            if len(set(to_csv.keys()).difference(set(self._fieldnames))) > 0:
                self._augment_csv(deepcopy(data))

            self._writer.writerow(to_csv)

            for k in to_csv.keys():
                data.mark(k)
        else:
            raise ValueError('Unacceptable type.')

    def _augment_csv(self, data):
        """Augment tabular data with new column(s)
           
           Old CSV file is renamed to a temporary file. Its
           data is written line-by-line to the new file (of 
           the original name) with the new keys.

           The initial data.reset() call is to ensure that there
           is no data spillage from the new key-value pair into
           the old CSV rows.
        """
        data.reset()
        self._log_file.close()

        temp_file = os.path.splitext(self._file_name)
        temp_file = temp_file[0] + "_temp" + temp_file[1]
        os.rename(self._file_name, temp_file)
        old_file = open(temp_file, 'r')
        self._log_file = open(self._file_name, 'w')

        self._fieldnames = data.as_primitive_dict.keys()
        self._writer = self._make_writer()
        self._writer.writeheader()

        reader = csv.DictReader(old_file)
        old_fields = reader.fieldnames
        augmented_elements = set(self._fieldnames).difference(set(old_fields))
        for row in reader:
            for k, v in row.items():
                data.record(k, v)
            self._writer.writerow(data.as_primitive_dict)
            data.reset()
        old_file.close()
        os.remove(temp_file)

    def _make_writer(self, log_file=None, fields=None, actions='raise'):
        """Simplified method for creating new DictWriter object"""
        if not log_file:
            log_file = self._log_file
        if not fields:
            fields = self._fieldnames
        return csv.DictWriter(
                log_file,
                fieldnames=fields,
                extrasaction=actions)

    def disable_warnings(self):
        """Disable logger warnings for testing."""
        self._disable_warnings = True


class CsvOutputWarning(UserWarning):
    """Warning class for CsvOutput."""

    pass
