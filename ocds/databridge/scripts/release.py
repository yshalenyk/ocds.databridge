# -*- coding: utf-8 -*-
import argparse
import yaml
import os
import sys
import logging
from ocds.export.release import release_tender
from ocds.export.helpers import mode_test
from ocds.storage import (
    TendersStorage,
    FSStorage
)
from ocds.storage.helpers import date_path


Logger = logging.getLogger(__name__)


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
    releases = FSStorage(config.get('path'), date_path)

    count = 0
    for tender in tenders:
        Logger.info('Parsed {} docs'.format(count))
        try:
            if mode_test(tender):
                release = release_tender(tender, info['prefix'])
                releases.save(release)
                count += 1
        except KeyError as e:
            Logger.fatal('Error: {}'.format(e))
