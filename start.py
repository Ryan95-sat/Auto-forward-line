from tornado.httpserver import HTTPServer
from tornado.wsgi import WSGIContainer
from tornado import ioloop
from api.index import app


def main():
    application = HTTPServer(WSGIContainer(app))
    server = HTTPServer(application)
    server.listen(8090)
    ioloop.IOLoop.instance().start()


if __name__ == "__main__":
    main()
