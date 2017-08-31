from logging import getLogger
from typing import Dict, NewType
from uuid import uuid4

import eventlet
from eventlet.event import Event
from redis.client import StrictRedis, PubSub
from nameko.extensions import DependencyProvider, SharedExtension, ProviderCollector
from nameko.exceptions import deserialize_to_instance

from nameko_redis.client_providers import SharedRedis, DEFAULT_URI_KEY

__all__ = ['PubSubResponsesListener', 'SharedResponsesListener', 'IResponsesListener',
           'MessageData', 'WaitTimeout', 'KeyNotFound']

logger = getLogger(__name__)
MessageID = NewType('MessageID', str)


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


class IResponsesListener:

    def is_healthy(self):
        raise NotImplementedError()

    def wait_for_response(self, response_key: str, timeout: int, wait_not_exists_key: bool=True):
        raise NotImplementedError()

    def publish_response(self, channel: str, message: str, max_retries: int=3, retry_timeout_sec: int=1):
        raise NotImplementedError()


class SharedResponsesListener(ProviderCollector, SharedExtension, IResponsesListener):

    def __init__(self, deserializer,
                 event_validation=lambda deserialized_obj: True,
                 uri_key=DEFAULT_URI_KEY):
        super(SharedResponsesListener, self).__init__()

        self.deserializer = deserializer
        self.event_validation = event_validation

        self.shared_redis = SharedRedis(uri_key)
        self.redis = None  # type: StrictRedis
        self.pubsub = None  # type: PubSub

        self._responses = {}  # type: Dict[MessageID, MessageData]
        self._stop_listen = False

    def start(self):
        self.shared_redis.start()
        self.redis = self.shared_redis.get_client()
        self.pubsub = self.redis.pubsub(ignore_subscribe_messages=True)
        self.container.spawn_managed_thread(self._listener)

    def _listener(self):
        try:
            logger.info("PubSub Responses Listener is starting", extra={'state': 'UP'})
            while not self._stop_listen:
                if self.pubsub.subscribed:
                    message = self.pubsub.get_message(timeout=0.2)
                    if message:
                        self._update_data(message['data'])
                    else:
                        eventlet.sleep(0.001)
                        continue

                eventlet.sleep(1.0)
        except:
            logger.exception("PubSub Responses Listener has crashed", extra={'state': 'DOWN'})
            raise
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
                    logger.debug('Message event was sent', extra=log_extra)
                    message.event.send()
                    data_updated = True
                else:
                    logger.debug('Update message data', extra=log_extra)

        return data_updated

    def stop(self):
        self.shared_redis.stop()
        self._stop_listen = True
        self.pubsub.close()
        self.pubsub = None
        self.redis = None

    def is_healthy(self) -> bool:
        return not self._stop_listen

    def wait_for_response(self, response_key: str, timeout: int, wait_not_exists_key: bool=True):
        logger.debug("Waiting for response", extra={'response_key': response_key})
        message_id = uuid4()

        try:
            self.pubsub.subscribe(response_key)
            self._responses[message_id] = MessageData(response_key)

            if not self._check_key_exists(response_key, wait_not_exists_key):
                with eventlet.Timeout(timeout):
                    self._responses[message_id].event.wait()

            return self._responses[message_id].data
        except (eventlet.TimeoutError, eventlet.Timeout):
            logger.error("Response timeout", extra={'response_key': response_key, 'timeout': timeout})
            raise WaitTimeout("Timeout error")
        finally:
            self.pubsub.unsubscribe(response_key)
            del self._responses[message_id]

    def _check_key_exists(self, response_key, wait_not_exists_key):
        response_raw = self.redis.get(response_key)
        if response_raw:
            return self._update_data(response_raw)

        if wait_not_exists_key is False:
            raise KeyNotFound("Waiting for not exists key")

    def publish_response(self, channel: str, message: str, max_retries: int=3, retry_timeout_sec: int=1):
        retry = 0
        while retry <= max_retries:
            subscribers = self.redis.publish(channel, message)
            if subscribers:
                logger.debug('Response delivered successful', extra={
                    'channel': channel, 'retry': retry, 'subscribers': subscribers})
                return True

            retry += 1
            eventlet.sleep(retry_timeout_sec)

        logger.debug('Response was not delivered', extra={'channel': channel})


class PubSubResponsesListener(DependencyProvider):

    def __init__(self, deserializer,
                 event_validation=lambda deserialized_obj: True,
                 uri_key=DEFAULT_URI_KEY):
        self.responses_listener = SharedResponsesListener(deserializer, event_validation, uri_key)

    def setup(self):
        self.responses_listener.register_provider(self)

    def stop(self):
        self.responses_listener.unregister_provider(self)
        super(PubSubResponsesListener, self).stop()

    def get_dependency(self, worker_ctx):
        return self.responses_listener
