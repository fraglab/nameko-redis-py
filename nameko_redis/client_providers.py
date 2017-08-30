from nameko.extensions import DependencyProvider, SharedExtension, ProviderCollector

from redis import StrictRedis

__all__ = ['SharedRedis', 'Redis']

REDIS_CONFIG_KEY = 'REDIS_CONFIG'
DEFAULT_URI_KEY = 'DEFAULT'


class SharedRedis(ProviderCollector, SharedExtension):

    def __init__(self, uri_key=DEFAULT_URI_KEY):
        super(SharedRedis, self).__init__()
        self.uri_key = uri_key

        self._client = None

    def get_client(self) -> StrictRedis:
        return self._client

    @property
    def redis_uri(self):
        return self.container.config[REDIS_CONFIG_KEY][self.uri_key]['url']

    @property
    def redis_options(self):
        return self.container.config[REDIS_CONFIG_KEY][self.uri_key].get('options') or {}

    def start(self):
        if not self._client:
            self._client = StrictRedis.from_url(self.redis_uri, **self.redis_options)

    def stop(self):
        self._client = None


class Redis(DependencyProvider):

    def __init__(self, uri_key=DEFAULT_URI_KEY):
        self.shared_redis = SharedRedis(uri_key)

    def setup(self):
        self.shared_redis.register_provider(self)

    def stop(self):
        self.shared_redis.unregister_provider(self)
        super(Redis, self).stop()

    def get_dependency(self, worker_ctx):
        return self.shared_redis.get_client()
