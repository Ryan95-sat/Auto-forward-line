import os

from loguru import logger
from tornado.httpserver import HTTPServer
from tornado.wsgi import WSGIContainer
from tornado import ioloop
from api.index import app


def init_logger():
    log_dir = os.path.expanduser('logs')
    log_file = os.path.join(log_dir, 'auto_forward_log_{time}.log')
    if not os.path.exists(log_dir):
        os.mkdir(log_dir)
    logger.add(log_file, rotation='10MB', compression='zip', retention="72h")


def main():
    init_logger()
    application = HTTPServer(WSGIContainer(app))
    server = HTTPServer(application)
    server.listen(8090)
    ioloop.IOLoop.instance().start()


if __name__ == "__main__":
    main()
