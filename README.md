# nameko-redis-utils
Redis dependency and utils for Nameko


# Installation

Install
```bash
pip3 install nameko-redis-py
```

Install latest version from Git:
```bash
pip install git+https://github.com/fraglab/nameko-redis-py.git
```

# Usage

## Redis client

```python
from redis import StrictRedis
from nameko.rpc import rpc
from nameko.extensions import DependencyProvider
from nameko_redis import Redis, SharedRedis


class MyService:
    name = "my_service"

    redis = Redis()  # type: StrictRedis

    @rpc
    def get_data(self, key):
        return self.redis.get(key)
    
    
class MyDependencyProvider(DependencyProvider):

    shared_redis = SharedRedis()
    redis = None
    
    def setup(self):
        self.shared_redis.register_provider(self)

    def start(self):
        self.shared_redis.start()
        self.redis = self.shared_redis.get_client()

    def stop(self):
        self.shared_redis.unregister_provider(self)    
    
    def my_logic(self):
        data = self.redis.get("key")
        # Some logic
        return data
    
    def get_dependency(self, worker_ctx):
        return self
    
```

## Utils

### PubSub Response Listener

Problem: Waiting for key or value attribute in Redis without blocking connection <br>
Solution: Use single pubsub connection for waiting keys

```python
import json
from typing import Optional

from nameko.rpc import rpc
from nameko_redis.pubsub_response_listener import PubSubResponsesListener, IResponsesListener


# Must return tuple with response_key and deserialized object 
def message_deserializer(raw_data) -> (str, Optional[object, dict]):
    data = json.loads(raw_data)
    return data['response_key'], data


class MyObj:
    def __init__(self, name, state):
        self.name = name
        self.state = state


def event_validation(deserialized_obj: MyObj):
    return deserialized_obj.state == 'done'


# WAIT NOT EXISTS KEY
class MyService:

    listener = PubSubResponsesListener(message_deserializer)  # type: IResponsesListener

    @rpc
    def get(self):
        return self.listener.wait_for_response("RESPONSE_KEY", timeout=10, wait_not_exists_key=True)


# WAIT FOR CHANGE IN VALUE
class MyService2:

    listener = PubSubResponsesListener(
        message_deserializer, event_validation=event_validation)  # type: IResponsesListener

    @rpc
    def get(self):
        return self.listener.wait_for_response("RESPONSE_KEY", timeout=10)

```


# Configuration

Nameko configuration example:

```
REDIS_CONFIG:
  DEFAULT:
    url: 'redis://localhost:6379/0'
    options:
      retry_on_timeout: True
      decode_responses: True
```


Redis, SharedRedis arguments:
* **url** - specify configuration redis key, like "DEFAULT"
* ****options** - specify any StrictRedis optional arguments, like encoding, retry_on_timeout, etc.


```python
from nameko_redis import Redis

class MyService:
    name = "my_service"

    redis = Redis()  # type: StrictRedis
```
