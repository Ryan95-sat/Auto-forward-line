from tornado.httpserver import HTTPServer
from tornado.wsgi import WSGIContainer
from tornado import ioloop
from api.index import app

cert_path = '/etc/nginx/ssl/'


def main():
    application = HTTPServer(WSGIContainer(app))
    https_cert_file = cert_path + 'fullchain.cer'
    https_key_file = cert_path + 'bnine.wallter1.xyz.key'
    # https服务
    server = HTTPServer(application, ssl_options={"certfile": https_cert_file, "keyfile": https_key_file})
    server.listen(8090)
    ioloop.IOLoop.instance().start()


if __name__ == "__main__":
    main()
