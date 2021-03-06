import os
import datetime as dt
import json

from multiprocessing import Process

from urllib.parse import urlparse
from werkzeug.wrappers import Response, Request
from werkzeug.routing import Map, Rule
from werkzeug.exceptions import HTTPException, NotFound
from werkzeug.wsgi import SharedDataMiddleware
from werkzeug.utils import redirect
from jinja2 import Environment, FileSystemLoader

from podismo.spider import Crawler as podismo
from runedia.spider import Crawler as runedia

from singer_morning import SingerMorning


appConfig = {
    'aux_host': None,
    'aux_port': None,
    'spiders': [
        {
            "name": "podismo",
            "crawler": podismo,
            "running": False,
            "results": {
                "end": False,
                "start": False,
                "date": None,
                "file": False
            }
        },
        {
            "name": "runedia",
            "crawler": runedia,
            "running": False,
            "results": {
                "end": False,
                "start": False,
                "date": None
            }
        }
    ]
}


class App(object):

    def __init__(self):
        config = dict(appConfig)
        self.aux_host = config["aux_host"]
        self.aux_port = config["aux_port"]
        self.spiders = config["spiders"]
        template_path = os.path.join(os.path.dirname(__file__), 'templates')
        self.jinja_env = Environment(loader=FileSystemLoader(template_path),
                                     autoescape=True)

        self.url_map = Map([
            Rule('/', endpoint='index'),
            Rule('/spiders/<spider_name>/<action>', endpoint='spider_caller'),
        ])

    def __call__(self, environ, start_response):
        return self.wsgi_app(environ, start_response)

    def render_template(self, template_name, **context):
        t = self.jinja_env.get_template(template_name)
        return Response(t.render(context), mimetype='text/html')

    def dispatch_request(self, request):
        adapter = self.url_map.bind_to_environ(request.environ)
        try:
            endpoint, values = adapter.match()
            return getattr(self, 'on_' + endpoint)(request, **values)
        except (HTTPException):
            return HTTPException

    def wsgi_app(self, environ, start_response):
        request = Request(environ)
        response = self.dispatch_request(request)
        return response(environ, start_response)

    def is_valid_url(self, url):
        parts = urlparse(url)
        return parts.scheme in ('http', 'https')

    def on_index(self, request):
        if request.method == 'GET':
            return self.render_template('index.html', spiders=self.spiders)
        else:
            return redirect(NotFound())

    def on_spider_caller(self, request, **arguments):
        if request.method != "GET":
            return

        for spider_state in self.spiders:
            if spider_state["name"] == arguments["spider_name"]:
                spider = spider_state

        file_path = os.path.join(os.path.dirname(__file__), 'results', spider["name"] + ".csv")
        if arguments['action'] == 'start':
            p = None
            def process(): spider["crawler"](file_path=file_path, on_end=on_end, run=True)
            def on_end():
                spider["running"] = False
                p.join()

            p = Process(target=process)
            p.start()

            spider["running"] = True
            spider["process"] = p
            date = dt.datetime.now()
            spider["results"] = {
                "date": str(date.year) + '/' + str(date.month) + '/' + str(date.day) + ' ' + str(date.hour)
                        + ':' + str(date.minute) + ':' + str(date.second),
                "start": True,
                "end": False,
                "file": True
            }
            self.singer_morning.start_singing(spider["name"])
            return Response(json.dumps({"success": True}))

        elif arguments['action'] == 'stop':
            spider["process"].terminate()
            spider["running"] = False
            self.singer_morning.end_singing(spider["name"])
            return Response(json.dumps({"success": True}))

        elif arguments['action'] == 'get_file':
            response = open(file_path).read()
            open(file_path, 'w').close()
            return Response(response, headers={
                "Content-Disposition": "attachment; filename='{!s}.csv'".format(spider["name"]),
                "Content-Type": "text/plain",
                "Access-Controll-Allow-Origin": "*"
            })

        def end_callback():
            spider["running"] = False
            date = dt.datetime.now()
            spider["results"] = {
                "date": str(date.year) + '/' + str(date.month) + '/' + str(date.day) + ' ' + str(date.hour)
                        + ':' + str(date.minute) + ':' + str(date.second),
                "end": True,
                "start": False,
                "file": True
            }

        spider.on_end(end_callback)


def create_app(with_static=True):
    app = App()

    if with_static:
        app.wsgi_app = SharedDataMiddleware(app.wsgi_app, {
            '/statics':  os.path.join(os.path.dirname(__file__), 'statics')
        })

    if not os.path.isdir('results'):
        os.mkdir('results')

    SingerMorning(app)

    return app


if __name__ == '__main__':
    from werkzeug.serving import run_simple
    app = create_app()
    run_simple('127.0.0.1', 5001, app, use_debugger=True, use_reloader=True)
else:
    application = create_app()