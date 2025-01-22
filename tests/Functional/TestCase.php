<?php

namespace VladimirYuldashev\LaravelQueueRabbitMQ\Tests\Functional;

use Exception;
use PhpAmqpLib\Channel\AMQPChannel;
use ReflectionClass;
use ReflectionException;
use VladimirYuldashev\LaravelQueueRabbitMQ\Tests\TestCase as BaseTestCase;

abstract class TestCase extends BaseTestCase
{
    protected function getEnvironmentSetUp($app): void
    {
        $app['config']->set('queue.default', 'rabbitmq');
        $app['config']->set('queue.connections.rabbitmq', [
            'driver' => 'rabbitmq',
            'queue' => 'order',
            'connection' => 'default',

            'hosts' => [
                [
                    'host' => getenv('HOST'),
                    'port' => getenv('PORT'),
                    'vhost' => '/',
                    'user' => 'guest',
                    'password' => 'guest',
                ],
            ],

            'options' => [
                'ssl_options' => [
                    'cafile' => null,
                    'local_cert' => null,
                    'local_key' => null,
                    'verify_peer' => true,
                    'passphrase' => null,
                ],
            ],

            'worker' => 'default',

        ]);
        $app['config']->set('queue.connections.rabbitmq-with-options', [
            'driver' => 'rabbitmq',
            'queue' => 'order',
            'connection' => 'default',

            'hosts' => [
                [
                    'host' => getenv('HOST'),
                    'port' => getenv('PORT'),
                    'vhost' => '/',
                    'user' => 'guest',
                    'password' => 'guest',
                ],
            ],

            'options' => [
                'ssl_options' => [
                    'cafile' => null,
                    'local_cert' => null,
                    'local_key' => null,
                    'verify_peer' => true,
                    'passphrase' => null,
                ],

                'queue' => [
                    'prioritize_delayed' => true,
                    'queue_max_priority' => 20,
                    'exchange' => 'application-x',
                    'exchange_type' => 'topic',
                    'exchange_routing_key' => 'process.%s',
                    'reroute_failed' => true,
                    'failed_exchange' => 'failed-exchange',
                    'failed_routing_key' => 'application-x.%s.failed',
                ],
            ],

            'worker' => 'default',

        ]);
        $app['config']->set('queue.connections.rabbitmq-with-options-empty', [
            'driver' => 'rabbitmq',
            'queue' => 'order',
            'connection' => 'default',

            'hosts' => [
                [
                    'host' => getenv('HOST'),
                    'port' => getenv('PORT'),
                    'vhost' => '/',
                    'user' => 'guest',
                    'password' => 'guest',
                ],
            ],

            'options' => [
                'ssl_options' => [
                    'cafile' => null,
                    'local_cert' => null,
                    'local_key' => null,
                    'verify_peer' => true,
                    'passphrase' => null,
                ],

                'queue' => [
                    'prioritize_delayed' => '',
                    'queue_max_priority' => '',
                    'exchange' => '',
                    'exchange_type' => '',
                    'exchange_routing_key' => '',
                    'reroute_failed' => '',
                    'failed_exchange' => '',
                    'failed_routing_key' => '',
                    'quorum' => '',
                ],
            ],

            'worker' => 'default',

        ]);
        $app['config']->set('queue.connections.rabbitmq-with-options-null', [
            'driver' => 'rabbitmq',
            'queue' => 'order',
            'connection' => 'default',

            'hosts' => [
                [
                    'host' => null,
                    'port' => null,
                    'vhost' => null,
                    'user' => null,
                    'password' => null,
                ],
            ],

            'options' => [
                'ssl_options' => [
                    'cafile' => null,
                    'local_cert' => null,
                    'local_key' => null,
                    'verify_peer' => null,
                    'passphrase' => null,
                ],

                'queue' => [
                    'prioritize_delayed' => null,
                    'queue_max_priority' => null,
                    'exchange' => null,
                    'exchange_type' => null,
                    'exchange_routing_key' => null,
                    'reroute_failed' => null,
                    'failed_exchange' => null,
                    'failed_routing_key' => null,
                    'quorum' => null,
                ],
            ],

            'worker' => 'default',

        ]);
        $app['config']->set('queue.connections.rabbitmq-with-quorum-options', [
            'driver' => 'rabbitmq',
            'queue' => 'order',
            'connection' => 'default',

            'hosts' => [
                [
                    'host' => getenv('HOST'),
                    'port' => getenv('PORT'),
                    'vhost' => '/',
                    'user' => 'guest',
                    'password' => 'guest',
                ],
            ],

            'options' => [
                'ssl_options' => [
                    'cafile' => null,
                    'local_cert' => null,
                    'local_key' => null,
                    'verify_peer' => true,
                    'passphrase' => null,
                ],

                'queue' => [
                    'exchange' => 'application-x',
                    'exchange_type' => 'topic',
                    'exchange_routing_key' => 'process.%s',
                    'reroute_failed' => true,
                    'failed_exchange' => 'failed-exchange',
                    'failed_routing_key' => 'application-x.%s.failed',
                    'quorum' => true,
                ],
            ],

            'worker' => 'default',

        ]);
    }

    /**
     * @throws Exception
     */
    protected function callMethod($object, string $method, array $parameters = []): mixed
    {
        try {
            $className = get_class($object);
            $reflection = new ReflectionClass($className);
        } catch (ReflectionException $e) {
            throw new Exception($e->getMessage());
        }

        $method = $reflection->getMethod($method);
        $method->setAccessible(true);

        return $method->invokeArgs($object, $parameters);
    }

    /**
     * @throws Exception
     */
    protected function callProperty($object, string $property): mixed
    {
        try {
            $className = get_class($object);
            $reflection = new ReflectionClass($className);
        } catch (ReflectionException $e) {
            throw new Exception($e->getMessage());
        }

        $property = $reflection->getProperty($property);
        $property->setAccessible(true);

        return $property->getValue($object);
    }

    public function test_connect_channel(): void
    {
        $queue = $this->connection();
        $this->assertFalse($queue->getConnection()->isConnected());

        /** @var AMQPChannel $channel */
        $channel = $this->callMethod($queue, 'getChannel');
        $this->assertTrue($queue->getConnection()->isConnected());
        $this->assertSame($channel, $this->callProperty($queue, 'channel'));
        $this->assertTrue($channel->is_open());
    }

    public function test_reconnect(): void
    {
        $queue = $this->connection();
        $this->assertFalse($queue->getConnection()->isConnected());

        // connect
        $channel = $this->callMethod($queue, 'getChannel');
        $this->assertTrue($queue->getConnection()->isConnected());
        $this->assertSame($channel, $this->callProperty($queue, 'channel'));

        // close
        $queue->getConnection()->close();
        $this->assertFalse($queue->getConnection()->isConnected());

        // reconnect
        $this->callMethod($queue, 'reconnect');
        $this->assertTrue($queue->getConnection()->isConnected());
        $this->assertTrue($queue->getChannel()->is_open());
    }
}
