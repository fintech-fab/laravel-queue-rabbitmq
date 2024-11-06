RabbitMQ Queue driver for Laravel
======================
[![Latest Stable Version](https://poser.pugx.org/vladimir-yuldashev/laravel-queue-rabbitmq/v/stable?format=flat-square)](https://packagist.org/packages/vladimir-yuldashev/laravel-queue-rabbitmq)
[![Build Status](https://github.com/vyuldashev/laravel-queue-rabbitmq/actions/workflows/tests.yml/badge.svg?branch=master)](https://github.com/vyuldashev/laravel-queue-rabbitmq/actions/workflows/tests.yml)
[![Total Downloads](https://poser.pugx.org/vladimir-yuldashev/laravel-queue-rabbitmq/downloads?format=flat-square)](https://packagist.org/packages/vladimir-yuldashev/laravel-queue-rabbitmq)
[![License](https://poser.pugx.org/vladimir-yuldashev/laravel-queue-rabbitmq/license?format=flat-square)](https://packagist.org/packages/vladimir-yuldashev/laravel-queue-rabbitmq)

## Support Policy

Only the latest version will get new features. Bug fixes will be provided using the following scheme:

| Package Version | Laravel Version | Bug Fixes Until  |                                                                                             |
|-----------------|-----------------|------------------|---------------------------------------------------------------------------------------------|
| 13              | 9               | August 8th, 2023 | [Documentation](https://github.com/vyuldashev/laravel-queue-rabbitmq/blob/master/README.md) |

## Installation

You can install this package via composer using this command:

```
composer require vladimir-yuldashev/laravel-queue-rabbitmq
```

The package will automatically register itself.

### Configuration

Add connection to `config/queue.php`:

> This is the minimal config for the rabbitMQ connection/driver to work.

```php
'connections' => [
    // ...

    'rabbitmq' => [
    
       'driver' => 'rabbitmq',
       'hosts' => [
           [
               'host' => env('RABBITMQ_HOST', '127.0.0.1'),
               'port' => env('RABBITMQ_PORT', 5672),
               'user' => env('RABBITMQ_USER', 'guest'),
               'password' => env('RABBITMQ_PASSWORD', 'guest'),
               'vhost' => env('RABBITMQ_VHOST', '/'),
           ],
           // ...
       ],

       // ...
    ],

    // ...    
],
```

### Optional Queue Config

Optionally add queue options to the config of a connection.
Every queue created for this connection, gets the properties.

When you want to prioritize messages when they were delayed, then this is possible by adding extra options.

- When max-priority is omitted, the max priority is set with 2 when used.

```php
'connections' => [
    // ...

    'rabbitmq' => [
        // ...

        'options' => [
            'queue' => [
                // ...

                'prioritize_delayed' =>  false,
                'queue_max_priority' => 10,
            ],
        ],
    ],

    // ...    
],
```

When you want to publish messages against an exchange with routing-keys, then this is possible by adding extra options.

- When the exchange is omitted, RabbitMQ will use the `amq.direct` exchange for the routing-key
- When routing-key is omitted the routing-key by default is the `queue` name.
- When using `%s` in the routing-key the queue_name will be substituted.

> Note: when using an exchange with routing-key, you probably create your queues with bindings yourself.

```php
'connections' => [
    // ...

    'rabbitmq' => [
        // ...

        'options' => [
            'queue' => [
                // ...

                'exchange' => 'application-x',
                'exchange_type' => 'topic',
                'exchange_routing_key' => '',
            ],
        ],
    ],

    // ...    
],
```

In Laravel failed jobs are stored into the database. But maybe you want to instruct some other process to also do
something with the message.
When you want to instruct RabbitMQ to reroute failed messages to a exchange or a specific queue, then this is possible
by adding extra options.

- When the exchange is omitted, RabbitMQ will use the `amq.direct` exchange for the routing-key
- When routing-key is omitted, the routing-key by default the `queue` name is substituted with `'.failed'`.
- When using `%s` in the routing-key the queue_name will be substituted.

> Note: When using failed_job exchange with routing-key, you probably need to create your exchange/queue with bindings
> yourself.

```php
'connections' => [
    // ...

    'rabbitmq' => [
        // ...

        'options' => [
            'queue' => [
                // ...

                'reroute_failed' => true,
                'failed_exchange' => 'failed-exchange',
                'failed_routing_key' => 'application-x.%s',
            ],
        ],
    ],

    // ...    
],
```

When you want to disable x-expires from a delayed queue, then this is possible by adding extra option on queue `expire_delay_queue`.
- When the `expire_delay_queue` option is omitted, it is considered to be true.
- Useful for when you're using quorum queues, as they risk to delete the queue before the messages are moved to their destination queue.

```php
'connections' => [
    // ...

    'rabbitmq' => [
        // ...

        'options' => [
            'queue' => [
                // ...

                'expire_delay_queue' => false,
            ],
        ],
    ],

    // ...    
],
```

### Horizon support

Starting with 8.0, this package supports [Laravel Horizon](https://laravel.com/docs/horizon) out of the box. Firstly,
install Horizon and then set `RABBITMQ_WORKER` to `horizon`.

Horizon is depending on events dispatched by the worker.
These events inform Horizon what was done with the message/job.

This Library supports Horizon, but in the config you have to inform Laravel to use the QueueApi compatible with horizon.

```php
'connections' => [
    // ...

    'rabbitmq' => [
        // ...

        /* Set to "horizon" if you wish to use Laravel Horizon. */
       'worker' => env('RABBITMQ_WORKER', 'default'),
    ],

    // ...    
],
```

### Use your own RabbitMQJob class

Sometimes you have to work with messages published by another application.  
Those messages probably won't respect Laravel's job payload schema.
The problem with these messages is that, Laravel workers won't be able to determine the actual job or class to execute.

You can extend the build-in `RabbitMQJob::class` and within the queue connection config, you can define your own class.
When you specify a `job` key in the config, with your own class name, every message retrieved from the broker will get
wrapped by your own class.

An example for the config:

```php
'connections' => [
    // ...

    'rabbitmq' => [
        // ...

        'options' => [
            'queue' => [
                // ...

                'job' => \App\Queue\Jobs\RabbitMQJob::class,
            ],
        ],
    ],

    // ...    
],
```

An example of your own job class:

```php
<?php

namespace App\Queue\Jobs;

use VladimirYuldashev\LaravelQueueRabbitMQ\Queue\Jobs\RabbitMQJob as BaseJob;

class RabbitMQJob extends BaseJob
{

    /**
     * Fire the job.
     *
     * @return void
     */
    public function fire()
    {
        $payload = $this->payload();

        $class = WhatheverClassNameToExecute::class;
        $method = 'handle';

        ($this->instance = $this->resolve($class))->{$method}($this, $payload);

        $this->delete();
    }
}

```

Or maybe you want to add extra properties to the payload:

```php
<?php

namespace App\Queue\Jobs;

use VladimirYuldashev\LaravelQueueRabbitMQ\Queue\Jobs\RabbitMQJob as BaseJob;

class RabbitMQJob extends BaseJob
{
   /**
     * Get the decoded body of the job.
     *
     * @return array
     */
    public function payload()
    {
        return [
            'job'  => 'WhatheverFullyQualifiedClassNameToExecute@handle',
            'data' => json_decode($this->getRawBody(), true)
        ];
    }
}
```

If you want to handle raw message, not in JSON format or without 'job' key in JSON,
you should add stub for `getName` method:

```php
<?php

namespace App\Queue\Jobs;

use Illuminate\Support\Facades\Log;
use VladimirYuldashev\LaravelQueueRabbitMQ\Queue\Jobs\RabbitMQJob as BaseJob;

class RabbitMQJob extends BaseJob
{
    public function fire()
    {
        $anyMessage = $this->getRawBody();
        Log::info($anyMessage);

        $this->delete();
    }

    public function getName()
    {
        return '';
    }
}
```

### Use your own Connection

You can extend the built-in `PhpAmqpLib\Connection\AMQPStreamConnection::class`
or `PhpAmqpLib\Connection\AMQPSLLConnection::class` and within the connection config, you can define your own class.
When you specify a `connection` key in the config, with your own class name, every connection will use your own class.

An example for the config:

```php
'connections' => [
    // ...

    'rabbitmq' => [
        // ...

        'connection' = > \App\Queue\Connection\MyRabbitMQConnection::class,
    ],

    // ...    
],
```

### Use your own Worker class

If you want to use your own `RabbitMQQueue::class` this is possible by
extending `VladimirYuldashev\LaravelQueueRabbitMQ\Queue\RabbitMQQueue`.
and inform laravel to use your class by setting `RABBITMQ_WORKER` to `\App\Queue\RabbitMQQueue::class`.

> Note: Worker classes **must** extend `VladimirYuldashev\LaravelQueueRabbitMQ\Queue\RabbitMQQueue`

```php
'connections' => [
    // ...

    'rabbitmq' => [
        // ...

        /* Set to a class if you wish to use your own. */
       'worker' => \App\Queue\RabbitMQQueue::class,
    ],

    // ...    
],
```

```php
<?php

namespace App\Queue;

use VladimirYuldashev\LaravelQueueRabbitMQ\Queue\RabbitMQQueue as BaseRabbitMQQueue;

class RabbitMQQueue extends BaseRabbitMQQueue
{
    // ...
}
```

**For Example: A reconnect implementation.**

If you want to reconnect to RabbitMQ, if the connection is dead.
You can override the publishing and the createChannel methods.

> Note: this is not best practice, it is an example.

```php
<?php

namespace App\Queue;

use PhpAmqpLib\Exception\AMQPChannelClosedException;
use PhpAmqpLib\Exception\AMQPConnectionClosedException;
use VladimirYuldashev\LaravelQueueRabbitMQ\Queue\RabbitMQQueue as BaseRabbitMQQueue;

class RabbitMQQueue extends BaseRabbitMQQueue
{

    protected function publishBasic($msg, $exchange = '', $destination = '', $mandatory = false, $immediate = false, $ticket = null): void
    {
        try {
            parent::publishBasic($msg, $exchange, $destination, $mandatory, $immediate, $ticket);
        } catch (AMQPConnectionClosedException|AMQPChannelClosedException) {
            $this->reconnect();
            parent::publishBasic($msg, $exchange, $destination, $mandatory, $immediate, $ticket);
        }
    }

    protected function publishBatch($jobs, $data = '', $queue = null): void
    {
        try {
            parent::publishBatch($jobs, $data, $queue);
        } catch (AMQPConnectionClosedException|AMQPChannelClosedException) {
            $this->reconnect();
            parent::publishBatch($jobs, $data, $queue);
        }
    }

    protected function createChannel(): AMQPChannel
    {
        try {
            return parent::createChannel();
        } catch (AMQPConnectionClosedException) {
            $this->reconnect();
            return parent::createChannel();
        }
    }
}
```

### Default Queue

The connection does use a default queue with value 'default', when no queue is provided by laravel.
It is possible to change te default queue by adding an extra parameter in the connection config.

```php
'connections' => [
    // ...

    'rabbitmq' => [
        // ...
            
        'queue' => env('RABBITMQ_QUEUE', 'default'),
    ],

    // ...    
],
```

### Heartbeat

By default, your connection will be created with a heartbeat setting of `0`.
You can alter the heartbeat settings by changing the config.

```php

'connections' => [
    // ...

    'rabbitmq' => [
        // ...

        'options' => [
            // ...

            'heartbeat' => 10,
        ],
    ],

    // ...    
],
```

### SSL Secure

If you need a secure connection to rabbitMQ server(s), you will need to add these extra config options.

```php
'connections' => [
    // ...

    'rabbitmq' => [
        // ...

        'secure' = > true,
        'options' => [
            // ...

            'ssl_options' => [
                'cafile' => env('RABBITMQ_SSL_CAFILE', null),
                'local_cert' => env('RABBITMQ_SSL_LOCALCERT', null),
                'local_key' => env('RABBITMQ_SSL_LOCALKEY', null),
                'verify_peer' => env('RABBITMQ_SSL_VERIFY_PEER', true),
                'passphrase' => env('RABBITMQ_SSL_PASSPHRASE', null),
            ],
        ],
    ],

    // ...    
],
```

### Events after Database commits

To instruct Laravel workers to dispatch events after all database commits are completed.

```php
'connections' => [
    // ...

    'rabbitmq' => [
        // ...

        'after_commit' => true,
    ],

    // ...    
],
```

### Lazy Connection

By default, your connection will be created as a lazy connection.
If for some reason you don't want the connection lazy you can turn it off by setting the following config.

```php
'connections' => [
    // ...

    'rabbitmq' => [
        // ...

        'lazy' = > false,
    ],

    // ...    
],
```

### Network Protocol

By default, the network protocol used for connection is tcp.
If for some reason you want to use another network protocol, you can add the extra value in your config options.
Available protocols : `tcp`, `ssl`, `tls`

```php
'connections' => [
    // ...

    'rabbitmq' => [
        // ...

        'network_protocol' => 'tcp',
    ],

    // ...    
],
```

### Octane support

Starting with 13.3.0, this package supports [Laravel Octane](https://laravel.com/docs/octane) out of the box.
Firstly, install Octane and don't forget to warm 'rabbitmq' connection in the octane config.
> See: https://github.com/vyuldashev/laravel-queue-rabbitmq/issues/460#issuecomment-1469851667

## Laravel Usage

Once you completed the configuration you can use the Laravel Queue API. If you used other queue drivers you do not
need to change anything else. If you do not know how to use the Queue API, please refer to the official Laravel
documentation: http://laravel.com/docs/queues

## Lumen Usage

For Lumen usage the service provider should be registered manually as follow in `bootstrap/app.php`:

```php
$app->register(VladimirYuldashev\LaravelQueueRabbitMQ\LaravelQueueRabbitMQServiceProvider::class);
```

## Consuming Messages

There are two ways of consuming messages.

1. `queue:work` command which is Laravel's built-in command. This command utilizes `basic_get`. Use this if you want to consume multiple queues.

2. `rabbitmq:consume` command which is provided by this package. This command utilizes `basic_consume` and is more performant than `basic_get` by ~2x, but does not support multiple queues.

## Testing

Setup RabbitMQ using `docker-compose`:

```bash
docker compose up -d
```

To run the test suite you can use the following commands:

```bash
# To run both style and unit tests.
composer test

# To run only style tests.
composer test:style

# To run only unit tests.
composer test:unit
```

If you receive any errors from the style tests, you can automatically fix most,
if not all the issues with the following command:

```bash
composer fix:style
```

## Contribution

You can contribute to this package by discovering bugs and opening issues. Please, add to which version of package you
create pull request or issue. (e.g. [5.2] Fatal error on delayed job)
