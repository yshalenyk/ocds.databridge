# -*- coding: utf-8 -*-
import argparse
import yaml
import iso8601
import os
import time
import sys
import logging

from ocds.storage.backends.couch import TendersStorage 
from ocds.export.package import Package
from ocds.export.helpers import mode_test
from ocds.export.release import release_tender
from uuid import uuid4

URI = 'https://fake-url/tenders-{}'.format(uuid4().hex)
Logger = logging.getLogger(__name__)


def read_config(path):
    with open(path) as cfg:
        config = yaml.load(cfg)
        return config


def parse_args():
    parser = argparse.ArgumentParser('Release Packages')
    parser.add_argument('-c', '--config', required=True, help="Path to configuration file")
    parser.add_argument('-d', action='append', dest='dates', default=[], help='Start-end dates to generate package')
    parser.add_argument('-n', '--number')
    return parser.parse_args()


def parse_date(date):
    return iso8601.parse_date(date).isoformat()


def get_releases(gen, info):
    for row in gen:
        try:
            if mode_test(tender):
                release = release_tender(row, info['prefix'])
                yield release
        except KeyError as e:
            Logger.fatal(e)
            yield None

def dump_package(releases, config):
    info = config['release']
    Logger.info('Packaging {} releases'.format(len(releases)))
    try:
        package = Package(
            releases,
            info['publisher'],
            info['license'],
            info['publicationPolicy'],
            URI
        )
        path = os.path.join(config['path'], 'release-{}.json'.format(time.time()))
        with open(path, 'w') as outfile:
            outfile.write(package.to_json())
        Logger.debug('Successfully dumped {}'.format(path))
    except (OSError, IOError) as e:
        Logger.fatal('Falied to dump package to {}'.format(path))
    except Exception as e:
        Logger.fatal('Fatal error during release. Error {}'.format(e))


def run():
    args = parse_args()
    releases = []
    config = read_config(args.config)
    storage = TendersStorage(config.get('tenders_db'))
    info = config.get('release')

    if not os.path.exists(config.get('path')):
        os.makedirs(config.get('path'))

    if args.dates:
        datestart, datefinish  = [parse_date(dt) for dt in args.dates[:2]]
        Logger.info('Releasing from {} to {}'.format(datestart, datefinish))
        for release in get_releases(storage.get_tenders_between_dates(datestart, datefinish), info):
            if release:
                releases.append(release)
        dump_package(releases, config)
    else:
        count = 0
        total = int(args.number) if args.number else 16384
        for release in get_releases(storage, info):
            Logger.info('Parsed {} docs'.format(count))
            if release:
                releases.append(release)
                count += 1
            if count == total:
                dump_package(releases, config)
                count = 0
                releases = []
