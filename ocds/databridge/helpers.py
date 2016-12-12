import gevent
import logging
from .exceptions import LBMismatchError
from ocds.export.release import release_tender


logger = logging.getLogger(__name__)


def get_start_point(forward, backward, cookie, queue,
                    callback=lambda x: x, extra={}):
    forward_params = {'feed': 'changes'}
    backward_params = {'feed': 'changes', 'descending': '1'}
    if extra:
        [x.update(extra) for x in [forward_params, backward_params]]
    r = backward.get_tenders(backward_params)
    if backward.session.cookies != cookie:
        raise LBMismatchError
    backward_params['offset'] = r['next_page']['offset']
    forward_params['offset'] = r['prev_page']['offset']
    queue.put(filter(callback, r['data']))
    return forward_params, backward_params


def fetch_tenders(client, src, dest):
    logger.info('Starting downloading tenders')
    while True:
        for feed in src:
            if not feed:
                continue
            logger.info('Uploading {} tenders'.format(len(feed)))
            resp = client.fetch(feed)
            if resp:
                logger.info('fetched {} tenders'.format(len(resp)))
            dest.put(resp)
        gevent.sleep(0.5)


def create_releases(prefix, src, dest):
        logger.info('Starting generating releases')
        while True:
            for batch in src:
                logger.info('Got {} tenders'.format(len(batch)))
                for tender in batch:
                    try:
                        release = release_tender(tender, prefix)
                        logger.info("generated release for tender "
                                    "{}".format(tender['id']))
                        dest.put(release)
                    except Exception as e:
                        logger.fatal('Error {} during'
                                     ' generation release'.format(e))
                gevent.sleep(0.5)
            gevent.sleep(2)


def save_items(storage, src, dest):
    logger.info('Start saving')
    while True:
        for item in src:
            for obj in item:
                if obj['id'] not in storage:
                    storage.save(obj)
                    logger.info('Save doc {}'.format(obj['id']))
                else:
                    doc = storage.get(obj['id'])
                    doc.update(obj)
                    storage.save(doc)
                    logger.info('Updated doc {}'.format(obj['id']))


def exists_or_modified(db, doc):
    if doc['id'] not in db:
        return True
    else:
        if db.get(doc['id'])['dateModified'] < doc['dateModified']:
            return True
    return False
