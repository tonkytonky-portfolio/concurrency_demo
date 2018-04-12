import csv
import itertools
import os
import random
import string
import threading
import zipfile

from glob import glob
from lxml import etree as et


class StringsGenerator(object):
    def __init__(self):
        self.strings = []

    def generate_unique_string(
        self,
        size=32,
        chars=string.ascii_letters + string.digits
    ):
        new_string = self.generate_string(size=size, chars=chars)
        while new_string in self.strings:
            new_string = self.generate_string(size=size, chars=chars)
        self.strings.append(new_string)
        return new_string

    @staticmethod
    def generate_string(size=32, chars=string.ascii_letters + string.digits):
        return ''.join(random.choice(chars) for _ in range(size))


class FilesGenerator(object):
    @classmethod
    def build_archives(cls, config):
        strings_generator = StringsGenerator()

        threads = []
        for archive_number in range(config['archives_number']):
            archive_name = '{}.zip'.format(archive_number)
            thread = threading.Thread(
                target=cls.build_archive,
                args=(archive_name, strings_generator, config)
            )
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

    @classmethod
    def build_archive(cls, archive_name, strings_generator, config):
        prepare_working_dir(config['working_dir'])

        threads = []
        for file_numbers_chunk in chunks(
                range(config['files_number']),
                config['files_open_in_parallel']
        ):
            for file_number in file_numbers_chunk:
                filename = '{}-{}.xml'.format(
                    os.path.splitext(archive_name)[0],
                    file_number
                )
                thread = threading.Thread(
                    target=cls.build_xml_file,
                    args=(filename, strings_generator, config['working_dir'])
                )
                thread.start()
                threads.append(thread)

            for thread in threads:
                thread.join()

        filenames = glob('{}-*.xml'.format(os.path.splitext(archive_name)[0]))
        with zipfile.ZipFile(
                archive_name,
                'w',
                zipfile.ZIP_DEFLATED
        ) as archive:
            for filename in filenames:
                archive.write(filename)
                os.remove(filename)

    @classmethod
    def build_xml_file(cls, filename, strings_generator, working_dir='.'):
        prepare_working_dir(working_dir)

        with open(filename, 'w') as f:
            f.write(cls.build_xml_string(strings_generator))

    @classmethod
    def build_xml_string(cls, string_generator):
        root = et.Element('root')
        root.append(et.Element(
            'var',
            attrib={
                'name': 'id',
                'value': string_generator.generate_unique_string()
            }
        ))
        root.append(et.Element(
            'var',
            attrib={
                'name': 'level',
                'value': str(random.randint(1, 100))
            }
        ))
        root.append(et.Element('objects'))
        objects = root.find('objects')
        objects.extend(
            et.Element(
                'object',
                attrib={
                    'name': string_generator.generate_string(
                        size=16,
                        chars=string.ascii_uppercase
                    )
                })
            for _ in range(random.randint(1, 10))
        )

        return et.tostring(root, pretty_print=True, encoding=str)


class FilesProceeder(object):
    mutex = threading.RLock()

    @classmethod
    def proceed_archives(cls, config):
        prepare_working_dir(config['working_dir'])
        threads = []
        for archive_name in glob('*.zip'):
            thread = threading.Thread(
                target=cls.proceed_archive,
                args=(archive_name, config)
            )
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

    @classmethod
    def proceed_archive(cls, archive_name, config):
        with zipfile.ZipFile(archive_name) as archive:
            threads = []
            for filenames_chunk in chunks(
                    archive.namelist(),
                    config['files_open_in_parallel']
            ):
                for filename in filenames_chunk:
                    thread = threading.Thread(
                        target=cls.proceed_file,
                        args=(archive, filename)
                    )
                    thread.start()
                    threads.append(thread)

                for thread in threads:
                    thread.join()

    @classmethod
    def proceed_file(cls, archive, filename):
        with archive.open(filename) as file:
            root = et.fromstring(file.read().decode('utf-8'))
            id = root.xpath('//var[@name="id"]/@value')[0]
            level = root.xpath('//var[@name="level"]/@value')[0]

            with cls.mutex:
                with open('first.csv', 'a', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f, quoting=csv.QUOTE_NONNUMERIC)
                    writer.writerow((id, level))
            with cls.mutex:
                with open('second.csv', 'a', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f, quoting=csv.QUOTE_NONNUMERIC)
                    writer.writerows([
                        (id, object.attrib['name'])
                        for id, object in zip(
                            itertools.repeat(id),
                            root.find('objects').findall('object')
                        )
                    ])


def _main():
    config = {
        'archives_number': 50,
        'files_number': 100,
        'files_open_in_parallel': 25,
        'working_dir': os.path.join(
            os.path.expanduser('~'),
            'concurrency_demo'
        )
    }

    FilesGenerator.build_archives(config)
    FilesProceeder.proceed_archives(config)


def prepare_working_dir(working_dir):
    if not os.path.exists(working_dir):
        os.mkdir(working_dir)
    os.chdir(working_dir)


def chunks(lst, n):
    for index in range(0, len(lst), n):
        yield lst[index:index + n]


if __name__ == '__main__':
    _main()
