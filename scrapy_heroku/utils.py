# coding=utf-8
from scrapy_heroku.spiderqueue import Psycopg2SpiderQueue
from scrapyd.utils import get_project_list


def get_spider_queues(config):
    queues = {}
    for project in get_project_list(config):
        table = 'scrapy_{}_queue'.format(project)
        queues[project] = Psycopg2SpiderQueue(config, table=table)
    return queues
