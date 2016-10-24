# -*- coding: utf-8 -*-
from ocds.storage import (
    TendersStorage,
    FSStorage
)
import argparse
import yaml
import os
from ocds.export.release import get_release_from_tender
import sys


def read_config(path):
    with open(path) as cfg:
        config = yaml.load(cfg)
    return config


def parse_args():
    parser = argparse.ArgumentParser('Release Packages')
    parser.add_argument('-c', '--config', required=True, help="Path to configuration file")
    return parser.parse_args()


def run():
    args = parse_args()
    config = read_config(args.config)
    info = config.get('release')

    tenders = TendersStorage(config.get('tenders_db'))
    path = os.path.join(config.get('path_for_release'), 'releases')
    releases = FSStorage(path)

    count = 0
    counter = 0
    fold = 1
    for tender in tenders.get_all():
        sys.stdout.write('Parsed {} tenders\r'.format(count))
        sys.stdout.flush()
        try:
            if counter == 1000:
                fold += 1
                counter = 0
            if 'ТЕСТУВАННЯ'.decode('utf-8') not in tender['title']:
                release = get_release_from_tender(tender, info['prefix'])
                releases.save(release, fold)
                count += 1
                counter += 1
        except KeyError as e:
            print e.message
