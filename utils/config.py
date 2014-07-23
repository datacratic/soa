import os, json


class Config():

    _config = None

    def __init__(self):
        path = os.getenv('CONFIG')
        if not path:
            raise Exception("Environment variable CONFIG must be defined")
        if not os.path.exists(path):
            raise Exception("Environment variable CONFIG must point toward a "
                            "local file: %s" % path)

        class RaiiFile():
            def __init__(self, path):
                self.f = open(path, 'r')

            def __del__(self):
                self.f.close()

        f = RaiiFile(path)
        try:
            Config._config = json.load(f.f)
        except ValueError:
            raise Exception("Failed to parse JSON from file pointed by CONFIG")

    def __getattr__(self, name):
        parts = name.split(".")
        pointer = Config._config
        while len(parts) > 0:
            part = parts.pop(0)
            if part not in pointer:
                raise AttributeError(name + " not found in config")
            pointer = pointer[part]
        return pointer

    def __setattr__(self, name, value):
        self._config[name] = value

    def __contains__(self, key):
        return key in self._config


config = Config()
