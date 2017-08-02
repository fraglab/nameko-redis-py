from traceback import format_exc
from logging import getLogger
from typing import Dict
from uuid import uuid4

from nameko.extensions import DependencyProvider
from nameko.exceptions import deserialize_to_instance
from eventlet.event import Event
import eventlet

from nameko_redis.client_providers import SharedRedis

__all__ = ['PubSubResponsesListener', 'MessageData', 'WaitTimeout', 'KeyNotFound']

logger = getLogger(__name__)


@deserialize_to_instance
class RedisListenerException(Exception):
    pass


@deserialize_to_instance
class WaitTimeout(RedisListenerException):
    pass


@deserialize_to_instance
class KeyNotFound(RedisListenerException):
    pass


class MessageData:
    def __init__(self, response_key):
        self.response_key = response_key
        self.event = Event()
        self.data = None


class PubSubResponsesListener(DependencyProvider):
    shared_redis = SharedRedis(retry_on_timeout=True)

    def __init__(self, deserializer, event_validation=lambda deserialized_obj: True):
        super(PubSubResponsesListener, self).__init__()

        self.deserializer = deserializer
        self.event_validation = event_validation

        self._redis = None
        self._pubsub = None
        self._stop_listen = False

        self._responses = {}  # type: Dict['str', MessageData]

    def is_healthy(self) -> bool:
        return not self._stop_listen

    def setup(self):
        self.shared_redis.register_provider(self)

    def start(self):
        self.shared_redis.start()
        self._redis = self.shared_redis.get_client()
        self._pubsub = self._redis.pubsub(ignore_subscribe_messages=True)
        self.container.spawn_managed_thread(self._listener)

    def _listener(self):
        try:
            logger.debug("PubSub Responses Listener", extra={'state': 'UP'})
            while not self._stop_listen:
                if self._pubsub.subscribed:
                    message = self._pubsub.get_message(timeout=0.2)
                    if message:
                        self._update_data(message['data'])
                    else:
                        eventlet.sleep(0.001)
                        continue

                eventlet.sleep(1.0)
        except Exception as error:
            logger.error("PubSub Responses Listener", extra={
                'state': 'DOWN', 'error': str(error), 'traceback': format_exc()})
        finally:
            self._stop_listen = True

    def _update_data(self, raw_data: str) -> bool:
        response_key, deserialized_obj = self.deserializer(raw_data)

        data_updated = False
        for _, message in self._responses.items():
            if message.response_key == response_key:
                message.data = deserialized_obj

                log_extra = dict(response_key=response_key, message_data=vars(deserialized_obj))
                if self.event_validation(deserialized_obj):
                    logger.info('Message event send', extra=log_extra)
                    message.event.send()
                    data_updated = True
                else:
                    logger.debug('Update message data', extra=log_extra)

        return data_updated

    def stop(self):
        self.shared_redis.unregister_provider(self)
        self._stop_listen = True
        self._pubsub.reset()
        self._pubsub = None
        self._redis = None
        super(PubSubResponsesListener, self).stop()

    def wait_for_response(self, response_key: str, timeout: int, wait_not_exists_key: bool=True):
        logger.debug("Waiting for response", extra={'response_key': response_key})
        message_id = uuid4()

        try:
            self._pubsub.subscribe(response_key)
            self._responses[message_id] = MessageData(response_key)

            if not self._check_key_exists(response_key, wait_not_exists_key):
                with eventlet.Timeout(timeout):
                    self._responses[message_id].event.wait()

            return self._responses[message_id].data
        except (eventlet.TimeoutError, eventlet.Timeout):
            logger.error("Response timeout", extra={'response_key': response_key, 'timeout': timeout})
            raise WaitTimeout("Timeout error")
        finally:
            self._pubsub.unsubscribe(response_key)
            del self._responses[message_id]

    def _check_key_exists(self, response_key, wait_not_exists_key):
        response_raw = self._redis.get(response_key)
        if response_raw:
            return self._update_data(response_raw)

        if wait_not_exists_key is False:
            raise KeyNotFound("Waiting for not exists key", extra={'response_key': response_key})

    def get_dependency(self, worker_ctx):

        class Api:
            wait_for_response = self.wait_for_response
            is_healthy = self.is_healthy

        return Api()
