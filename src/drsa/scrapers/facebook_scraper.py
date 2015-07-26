# Facebook Page Post/Comment Scraper
#
# Author: Izhar Firdaus <izhar@kagesenshi.org>
# License: MIT/BSD
# Deps:
#     facebook-sdk
#     requests[security]
#     argh
# 
# Config File:
#
# [facebook]
# app-id = <FACEBOOK-APP-ID>
# app-secret = <FACEBOOK-APP-SECRET>
#
# Usage:
#
#    Download posts of page into posts.json
#         ./facebook-scraper.py config.cfg <pageid>
#
#    Download comments of posts into comments.json
#         ./facebook-scraper.py config.cfg posts.json
#

import facebook 
import argh
import requests
from ConfigParser import ConfigParser
from pprint import pprint
import time
import json
import logging
import traceback
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('Facebook Crawler')

LIMIT = 250
SLEEP = 1.5

class DataStore(object):

    def __init__(self, filename):
        self._fp = open(filename, 'aw')

    def write(self, data):
        self._fp.write(json.dumps(data) + '\n')

class BaseScraper(object):

    graph_url = 'https://graph.facebook.com'
    _api = None

    @property
    def _storefile(self):
        raise NotImplementedError

    def __init__(self, config, objid, resume_url=None):
        self.config = config
        self._objid = objid
        self._resume_url = resume_url
        self.store = DataStore(self._storefile)

    def query(self):
        raise NotImplementedError

    def extend_data(self, data):
        return data

    @property
    def api(self):
        if self._api is None:
            appid = self.config.get('facebook', 'app-id')
            appsecret = self.config.get('facebook', 'app-secret')
            api = facebook.GraphAPI('%s|%s' % (appid, appsecret))
            self._api = api
        return self._api

    def dump_resumefile(self, objid, url=None):
        faildata = json.dumps({'objid': objid, 'url': url})
        logger.error('Failure at %s. Resumefile dumped' % faildata)
        open('resumefile.json', 'w').write(faildata)
       

    def walk(self, result):
        for d in result['data']:
            yield d
        time.sleep(SLEEP)
        if result['data']:
            url = result['paging'].get('next', None)
            if url is None:
                return
            logger.info('%s: Next page: %s' % (self.__class__.__name__, url))
            try:
                data = requests.get(url).json()
            except:
                self.dump_resumefile(self._objid, url)
                traceback.print_exc()
                raise
            if data.has_key('error'):
                logger.error('%s: Error encountered at %s' % (
                            (self.__class__.__name__, url)))
                self.dump_resumefile(self._objid, url)
                raise Exception('Unknown Error : %s' % json.dumps(data))
            for d in self.walk(data):
                yield d

    def run(self):
        logger.info("%s: Crawling %s/%s" % (self.__class__.__name__, 
                        self.graph_url, self._objid))
        resume_url = None
        if self._resume_url:
            result = requests.get(self._resume_url).json()
            resume_url = self._resume_url
            self._resume_url = None
        else:
            try:
                result = self.query()
            except:
                self.dump_resumefile(self._objid)
                traceback.print_exc()
                raise 

        if result.has_key('error'):
            if resume_url:
                logger.error('%s: Error encountered at %s' % (
                                self.__class__.__name__, resume_url))
                self.dump_resumefile(self._objid, resume_url)
            else:
                logger.error('%s: Error encountered at %s/%s' % (
                            (self.__class__.__name__, self.graph_url, self._objid)))
                self.dump_resumefile(self._objid)
            raise Exception('Unknown Error : %s' % json.dumps(result))

        for d in self.walk(result):
            extended_data = self.extend_data(d)
            self.store.write(extended_data)

class PostScraper(BaseScraper):

    _fields = [
        'message',
        'from',
        'story',
        'story_tags',
        'link',
        'status_type',
        'updated_time',
        'created_time',
        'shares',
    ]

    _storefile = 'posts.ldjson'

    def query(self):
        return self.api.get_connections(self._objid, 'posts',
                fields=self._fields,limit=LIMIT)

    def extend_data(self, data):
        data['page_id'] = self._objid
        return data

class CommentScraper(BaseScraper):

    _storefile = 'comments.ldjson'

    _fields = [
        'message',
        'from',
        'created_time',
        'like_count',
    ]

    def query(self):
        return self.api.get_connections(self._objid, 'comments', fields=self._fields,
                limit=LIMIT)

    def extend_data(self, data):
        data['post_id'] = self._objid
        return data

class LikeScraper(BaseScraper):

    _storefile = 'likes.ldjson'

    def query(self):
        return self.api.get_connections(self._objid, 'likes', limit=LIMIT)

    def extend_data(self, data):
        data['post_id'] = self._objid
        return data


class PostIterator(object):

    def __init__(self, config, postsdb, resumefile, scraper, type):
        conf = ConfigParser()
        conf.readfp(open(config))
        self._conf = conf
        self._postsdb = postsdb
        self._resumefile = resumefile
        self._scraper = scraper
        self._type = type

    def run(self):
        resumefile = self._resumefile
        postsdb = self._postsdb

        if resumefile is not None:
            resumedata = json.loads(open(resumefile).read())
            resume_url = resumedata['url']
            resume_post = resumedata['objid']
            start = False
            for data in open(postsdb):
                post = json.loads(data)
                if (post['id'] == resume_post) and not start:
                    start = True
                    if resume_url:
                        logger.info('Resuming: %s' % resume_url)
                    else:
                        logger.info('Resuming fom PostID: %s' % post['id'])
                if not start:
                    logger.info('Skipping PostID: %s' % post['id'])
                    continue
                logger.info('Scraping %s for PostID: %s' % (self._type,
                                                            post['id']))
                scraper = self._scraper(self._conf, post['id'], resume_url)
                resume_post = None
                resume_url = None
                scraper.run()
        else:
            for data in open(postsdb):
                post = json.loads(data)
                logger.info('Scraping %s for PostID: %s' % (self._type,
                                                            post['id']))
                scraper = self._scraper(self._conf, post['id'])
                scraper.run()


@argh.arg('config', help='Config File')
@argh.arg('pageid', help='Page ID')
def posts(config, pageid):
    conf = ConfigParser()
    conf.readfp(open(config))

    scraper = PostScraper(conf, pageid)
    scraper.run()

@argh.arg('config', help='Config File')
@argh.arg('postsdb', help='Posts database')
@argh.arg('-r', '--resumefile', help='Resume file', default=None)
def comments(config, postsdb, resumefile=None):
    iterator = PostIterator(config, postsdb, resumefile, 
                            CommentScraper, 'comments')
    iterator.run()


@argh.arg('config', help='Config File')
@argh.arg('postsdb', help='Posts database')
@argh.arg('-r', '--resumefile', help='Resume file', default=None)
def likes(config, postsdb, resumefile=None):
    iterator = PostIterator(config, postsdb, resumefile, 
                            LikeScraper, 'likes')
    iterator.run()

parser = argh.ArghParser()
parser.add_commands([posts, comments, likes])

def main():
    parser.dispatch()

if __name__ == '__main__':
    main()
