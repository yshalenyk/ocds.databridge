# -*- coding: utf-8 -*-
from ocds.storage import (
    MainStorage
)
import argparse
import yaml
import os


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
    path = os.path.join(config.get('path_for_release'), 'releases')
    storage = MainStorage(config, path)
    storage.save()
