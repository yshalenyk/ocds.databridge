# -*- coding: utf-8 -*-
from ocds.storage.backends.couch import TendersStorage
import argparse
import yaml
import iso8601
import os
from ocds.export.package import Package
from ocds.export.release import get_release_from_tender
from uuid import uuid4
import time
import sys
import tarfile


URI = 'https://fake-url/tenders-{}'.format(uuid4().hex)


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
            if 'ТЕСТУВАННЯ'.decode('utf-8') not in row['title']:
                release = get_release_from_tender(row, info['prefix'])
                yield release
        except KeyError as e:
            print e
            yield None


def dump_package(releases, config, dates=None):
    info = config['release']
    package = Package(
        releases,
        info['publisher'],
        info['license'],
        info['publicationPolicy'],
        URI
    )
    if dates:
        path = os.path.join(config['path_for_release_package_btw_dates'], 'release{}-{}.json'.format(dates[0], dates[1]))
        with open(path, 'w') as outfile:
            outfile.write(package.to_json())
    else:
        path = os.path.join(config['path_for_release_package'], 'release-{}.json'.format(time.time()))
        with open(path, 'w') as outfile:
            outfile.write(package.to_json())


def compress_and_delete(source_dir):
    path = os.path.join(source_dir, "releases{}".format(time.strftime("%Y-%m-%d")))
    with tarfile.open(path, "w:gz") as tar:
        tar.add(source_dir, arcname=os.path.basename(source_dir))
    for file in os.listdir(source_dir):
        if file.endswith(".json"):
            os.remove(os.path.join(source_dir, file))


def run():
    args = parse_args()
    releases = []
    config = read_config(args.config)
    storage = TendersStorage(config.get('tenders_db'))
    info = config.get('release')
    if not os.path.exists(config.get('path_for_release_package')):
            os.makedirs(config.get('path_for_release_package'))
    if not os.path.exists(config.get('path_for_release_package_btw_dates')):
        os.makedirs(config.get('path_for_release_package_btw_dates'))
    if args.dates:
        datestart = parse_date(args.dates[0])
        datefinish = parse_date(args.dates[1])
        for release in get_releases(storage.get_tenders_between_dates(datestart, datefinish), info):
            if release:
                releases.append(release)
        dump_package(releases, config, [datestart, datefinish])
    else:
        count = 0
        if not args.number:
            total = 16384
        else:
            total = int(args.number)
        for release in get_releases(storage.get_all(), info):
            sys.stdout.write('{}\r'.format(count))
            sys.stdout.flush()
            if release:
                releases.append(release)
                count += 1
            if count == total:
                dump_package(releases, config)
                count = 0
                releases = []
    compress_and_delete(config.get('path_for_release_package'))
