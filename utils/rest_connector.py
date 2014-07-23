
from wsgi_intercept.httplib2_intercept import install
install()
import httplib2, json
import wsgi_intercept
from socket import error as SocketError
import logging
import time
import urllib


def retry_decorator(fct):
    def _handler(self, *args, **kwargs):
        delay = 5
        forever = True if self.max_request_tries == -1 else False
        tries = self.max_request_tries

        while forever or tries > 0:
            try:
                return fct(self, *args, **kwargs)
            except SocketError as e:
                logger = logging.getLogger(__name__)
                if len(logger.handlers) == 0:
                    logger.addHandler(logging.StreamHandler())
                if not forever:
                    tries -= 1
                if forever or tries > 0:
                    msg = "%s: Retrying in %d seconds (tries: %d)" % (e, delay, tries)
                    logger.error(msg)
                    time.sleep(delay)
                else:
                    raise
    return _handler


def kwargs_to_query_str(**kwargs):
    parts = []
    for k, v in kwargs.iteritems():
        parts.append("{:}={:}".format(urllib.quote(k), urllib.quote(v)))
    if parts:
        return "?" + "&".join(parts)
    return ""


class RestConnector(object):
    def __init__(self, host, max_request_tries=1):
        self.h = httplib2.Http(disable_ssl_certificate_validation=True)
        self.host = host
        self.cookie = ""
        self.max_request_tries = max_request_tries

    def checkStatusCode(self, action, path, response, content, data=None):
        if "set-cookie" in response:
            self.cookie = response["set-cookie"] #store the auth token
        dataStr = ""
        if data is not None:
            dataStr = " Posted data: %s" % json.dumps(data)
        if response["status"][0] == '5':
            raise Exception("Invalid status code {:} on {:} to endpoint {:}. "
                            "Content was '{:}'.{:}".format(response["status"],
                                                           action,
                                                           path,
                                                           content,
                                                           dataStr))

    @retry_decorator
    def get(self, path, params=[]):
        uri = self.host + path
        if params:
            if type(params) is dict:
                pieces = []
                for k in params:
                    pieces.append(k + "=" + params[k])
            else:
                pieces = params

            uri += '?' + '&'.join(pieces)

        self.headers = {"Cookie": self.cookie}
        res, content = self.h.request(uri, headers=self.headers)
        self.checkStatusCode('GET', path, res, content)
        return self.manageReply(res, content)

    @retry_decorator
    def post(self, path, data):
        self.headers = \
            {'Content-Type' : 'application/json', "Cookie" : self.cookie}
        res, content = self.h.request(
            self.host + path, "POST", body=json.dumps(data),
            headers=self.headers)
        self.checkStatusCode('POST', path, res, content, data)

        return self.manageReply(res, content)

    @retry_decorator
    def put(self, path, data):
        self.headers = \
            {'Content-Type' : 'application/json', "Cookie" : self.cookie}
        res, content = self.h.request(
            self.host + path, "PUT", body=json.dumps(data),
            headers=self.headers)
        self.checkStatusCode('PUT', path, res, content, data)

        return self.manageReply(res, content)

    @retry_decorator
    def delete(self, path):
        self.headers = {"Cookie": self.cookie}
        res, content = self.h.request(
            self.host + path, "DELETE", headers=self.headers)
        self.checkStatusCode('DELETE', path, res, content)

        return self.manageReply(res, content)

    def manageReply(self, res, content):
        if res['status'] == '204':
            return res, None
        if res['status'][0] == '2':
            if 'content-type' in res and \
                    res['content-type'] == 'application/json':
                try:
                    return res, json.loads(content)
                except:
                    msg = "The result was not json as expected. " \
                        "Received [{:}] under status code [{:}]" \
                        .format(content, res['status'])
                    raise Exception(msg)

        return res, content


class WSGIInterceptRestConnector(RestConnector):
    def __init__(self, app):
        wsgi_intercept.add_wsgi_intercept('intercepted', 80, lambda: app)
        super(WSGIInterceptRestConnector, self) \
            .__init__("http://intercepted:80")

    def log(self, method, path, call):
        try:
            h, r = call()
            print method, path, "...", h['status']
            return h, r
        except:
            print ">", method, path, "... 500 <"
            raise

    def get(self, path, params=[]):
        return self.log(
            "GET", path,
            lambda: super(WSGIInterceptRestConnector, self).get(path, params))

    def put(self, path, data):
        return self.log(
            "PUT", path,
            lambda: super(WSGIInterceptRestConnector, self).put(path, data))

    def post(self, path, data):
        return self.log(
            "POST", path,
            lambda: super(WSGIInterceptRestConnector, self).post(path, data))

    def delete(self, path):
        return self.log(
            "DELETE", path,
            lambda: super(WSGIInterceptRestConnector, self).delete(path))

    def remove_wsgi_intercept(self):
        wsgi_intercept.remove_wsgi_intercept('intercepted', 80)
        print ""

class TestRestConnector(WSGIInterceptRestConnector):
    def __init__(self, app, testCase):
        super(TestRestConnector, self).__init__(app)
        self.testCase = testCase

    def adapt(self, res, expectStatus=None):
        h,r = res
        if expectStatus:
            self.testCase.assertEqual(int(h["status"]), int(expectStatus),
                msg="\nResponse status {} != expected {} \nResponse body was: \n{}"
                .format(int(h["status"]), int(expectStatus), r)
                )
        return h,r

    def get(self, path, params=[], **kwargs):
        return self.adapt(
            super(TestRestConnector, self).get(path, params), **kwargs)

    def put(self, path, data, **kwargs):
        return self.adapt(
            super(TestRestConnector, self).put(path, data), **kwargs)

    def post(self, path, data, **kwargs):
        return self.adapt(
            super(TestRestConnector, self).post(path, data), **kwargs)

    def delete(self, path, **kwargs):
        return self.adapt(
            super(TestRestConnector, self).delete(path), **kwargs)

