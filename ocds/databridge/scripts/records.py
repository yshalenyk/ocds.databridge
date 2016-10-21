from ocds.storage.backends.fs import FSStorage
from ocds.export.record import Record, Record_Package
from ocds.export.helpers import check_releases
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
    parser.add_argument('-p', '--record_pack', help="Choose for record pack generator")
    parser.add_argument('-r', '--record', help="Choose for records generator")
    return parser.parse_args()


def dump_to_file(dir_path, info, record=None, package=None):
    if record:
        for records in gen_records():
            path = os.path.join(dir_path, 'record-{}.json'.format(time.time()))
            with open(path, 'w') as outfile:
                outfile.write(records.to_json())
    if package:
        path = os.path.join(dir_path, 'record-{}.json'.format(time.time()))
        with open(path, 'w') as outfile:
            outfile.write(gen_pack(info).to_json())


def gen_records():
    fstorage = FSStorage('var/releases')
    for releases in fstorage.ocid_walk():
            if check_releases(releases):
                yield Record(releases, releases[0]['ocid'])


def gen_pack(info):
    package = Record_Package(list(gen_records()),
                             info['publisher'],
                             info['publicationPolicy'],
                             info['license'])
    return package


def run():
    args = parse_args()
    config = read_config(args.config)
    info = config.get('release')
    if args.record:
        path = config.get('path_for_record')
        dump_to_file(path, info, record=True)
    if args.record_pack:
        path = config.get('path_for_record_package')
        dump_to_file(path, info, package=True)
