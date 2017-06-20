from nameko.extensions import DependencyProvider, SharedExtension, ProviderCollector

from redis import StrictRedis

__all__ = ['SharedRedis', 'Redis']

REDIS_URIS_KEY = 'REDIS_URIS'


class SharedRedis(ProviderCollector, SharedExtension):

    def __init__(self, uri_key='default', **redis_opts):
        super(SharedRedis, self).__init__()
        self.uri_key = uri_key
        self.redis_opts = redis_opts

        self._client = None

    def get_client(self) -> StrictRedis:
        return self._client

    @property
    def redis_uri(self):
        return self.container.config[REDIS_URIS_KEY][self.uri_key]

    def start(self):
        self._client = StrictRedis.from_url(self.redis_uri, **self.redis_opts)

    def stop(self):
        self._client = None


class Redis(DependencyProvider):
    shared_redis = SharedRedis()

    def setup(self):
        self.shared_redis.register_provider(self)

    def stop(self):
        self.shared_redis.unregister_provider(self)
        super(Redis, self).stop()

    def get_dependency(self, worker_ctx):
        return self.shared_redis.get_client()
