from ocds.storage.backends.fs import FSStorage
from ocds.storage.backends.main import MainStorage
from ocds.export.record import Record, Record_Package
import argparse
import yaml
import os
import time


def read_config(path):
    with open(path) as cfg:
        config = yaml.load(cfg)
    return config


def parse_args():
    parser = argparse.ArgumentParser('Release Packages')
    parser.add_argument('-c', '--config', required=True, help="Path to configuration file")
    parser.add_argument('-p', '--record_pack', action='store_true', help="Choose for record pack generator")
    parser.add_argument('-r', '--record', help="Choose for records generator")
    return parser.parse_args()


def dump_to_file(config, record=None, package=None):
    if record:
        for records in gen_records(config):
            path = os.path.join(config.get('path_for_record'), 'record-{}.json'.format(time.time()))
            with open(path, 'w') as outfile:
                outfile.write(records.to_json())
    if package:
        for pack in gen_pack(config, config.get('release')):
            if not os.path.exists(config.get('path_for_record_package')):
                os.makedirs(config.get('path_for_record_package'))
            path = os.path.join(config.get('path_for_record_package'), 'record-{}.json'.format(time.time()))
            with open(path, 'w') as outfile:
                outfile.write(pack.to_json())


def gen_records(config):
    mstorage = MainStorage(config, config['path_for_release'])
    for releases in mstorage.get_rel_for_record():
        rec = Record(releases, releases[0]['ocid'])
        yield rec


def gen_pack(config, info):
    records = []
    count = 0
    for record in gen_records(config):
        if count == 1000:
            count = 0
            pack = Record_Package(records,
                                  info['publisher'],
                                  info['publicationPolicy'],
                                  info['license']
                                  )
            records = []
            yield pack
        else:
            records.append(record)
            count += 1


def run():
    args = parse_args()
    config = read_config(args.config)
    if args.record:
        dump_to_file(config, record=True)
    if args.record_pack:
        dump_to_file(config, package=True)
