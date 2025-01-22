<?php

namespace VladimirYuldashev\LaravelQueueRabbitMQ\Tests\Feature;

use Illuminate\Queue\QueueManager;
use PhpAmqpLib\Connection\AMQPConnectionConfig;
use PhpAmqpLib\Connection\AMQPLazyConnection;
use PhpAmqpLib\Connection\AMQPSSLConnection;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use VladimirYuldashev\LaravelQueueRabbitMQ\Queue\RabbitMQQueue;
use VladimirYuldashev\LaravelQueueRabbitMQ\Tests\Mocks\TestSSLConnection;

class ConnectorTest extends \VladimirYuldashev\LaravelQueueRabbitMQ\Tests\TestCase
{
    public function test_lazy_connection(): void
    {
        $this->app['config']->set('queue.connections.rabbitmq', [
            'driver' => 'rabbitmq',
            'queue' => env('RABBITMQ_QUEUE', 'default'),
            'connection' => AMQPLazyConnection::class,

            'hosts' => [
                [
                    'host' => getenv('HOST'),
                    'port' => getenv('PORT'),
                    'user' => 'guest',
                    'password' => 'guest',
                    'vhost' => '/',
                ],
            ],

            'options' => [
                'ssl_options' => [
                    'cafile' => env('RABBITMQ_SSL_CAFILE', null),
                    'local_cert' => env('RABBITMQ_SSL_LOCALCERT', null),
                    'local_key' => env('RABBITMQ_SSL_LOCALKEY', null),
                    'verify_peer' => env('RABBITMQ_SSL_VERIFY_PEER', true),
                    'passphrase' => env('RABBITMQ_SSL_PASSPHRASE', null),
                ],
            ],

            'worker' => env('RABBITMQ_WORKER', 'default'),
        ]);

        /** @var QueueManager $queue */
        $queue = $this->app['queue'];

        /** @var RabbitMQQueue $connection */
        $connection = $queue->connection('rabbitmq');

        $this->assertInstanceOf(RabbitMQQueue::class, $connection);
        $this->assertInstanceOf(AMQPLazyConnection::class, $connection->getConnection());
        $this->assertFalse($connection->getConnection()->isConnected());
        $this->assertTrue($connection->getChannel()->is_open());
        $this->assertTrue($connection->getConnection()->isConnected());
    }

    public function test_lazy_stream_connection(): void
    {
        $this->app['config']->set('queue.connections.rabbitmq', [
            'driver' => 'rabbitmq',
            'queue' => env('RABBITMQ_QUEUE', 'default'),
            'connection' => 'default',

            'hosts' => [
                [
                    'host' => getenv('HOST'),
                    'port' => getenv('PORT'),
                    'user' => 'guest',
                    'password' => 'guest',
                    'vhost' => '/',
                ],
            ],

            'options' => [
                'ssl_options' => [
                    'cafile' => env('RABBITMQ_SSL_CAFILE', null),
                    'local_cert' => env('RABBITMQ_SSL_LOCALCERT', null),
                    'local_key' => env('RABBITMQ_SSL_LOCALKEY', null),
                    'verify_peer' => env('RABBITMQ_SSL_VERIFY_PEER', true),
                    'passphrase' => env('RABBITMQ_SSL_PASSPHRASE', null),
                ],
            ],

            'worker' => env('RABBITMQ_WORKER', 'default'),
        ]);

        /** @var QueueManager $queue */
        $queue = $this->app['queue'];

        /** @var RabbitMQQueue $connection */
        $connection = $queue->connection('rabbitmq');

        $this->assertInstanceOf(RabbitMQQueue::class, $connection);
        $this->assertInstanceOf(AMQPStreamConnection::class, $connection->getConnection());
        $this->assertFalse($connection->getConnection()->isConnected());
        $this->assertTrue($connection->getChannel()->is_open());
        $this->assertTrue($connection->getConnection()->isConnected());
    }

    public function test_ssl_connection(): void
    {
        $this->markTestSkipped();

        $this->app['config']->set('queue.connections.rabbitmq', [
            'driver' => 'rabbitmq',
            'queue' => env('RABBITMQ_QUEUE', 'default'),
            'connection' => AMQPSSLConnection::class,

            'hosts' => [
                [
                    'host' => getenv('HOST'),
                    'port' => getenv('PORT_SSL'),
                    'user' => 'guest',
                    'password' => 'guest',
                    'vhost' => '/',
                ],
            ],

            'options' => [
                'ssl_options' => [
                    'cafile' => getenv('RABBITMQ_SSL_CAFILE'),
                    'local_cert' => null,
                    'local_key' => null,
                    'verify_peer' => true,
                    'passphrase' => null,
                ],
            ],

            'worker' => env('RABBITMQ_WORKER', 'default'),
        ]);

        /** @var QueueManager $queue */
        $queue = $this->app['queue'];

        /** @var RabbitMQQueue $connection */
        $connection = $queue->connection('rabbitmq');
        $this->assertInstanceOf(RabbitMQQueue::class, $connection);
        $this->assertInstanceOf(AMQPSSLConnection::class, $connection->getConnection());
        $this->assertTrue($connection->getConnection()->isConnected());
        $this->assertTrue($connection->getChannel()->is_open());
    }

    // Test to validate ssl connection params
    public function test_no_verification_ssl_connection(): void
    {
        $this->app['config']->set('queue.connections.rabbitmq', [
            'driver' => 'rabbitmq',
            'queue' => env('RABBITMQ_QUEUE', 'default'),
            'connection' => TestSSLConnection::class,
            'secure' => true,

            'hosts' => [
                [
                    'host' => getenv('HOST'),
                    'port' => getenv('PORT_SSL'),
                    'user' => 'guest',
                    'password' => 'guest',
                    'vhost' => '/',
                ],
            ],

            'options' => [
                'ssl_options' => [
                    'cafile' => getenv('RABBITMQ_SSL_CAFILE'),
                    'local_cert' => null,
                    'local_key' => null,
                    'verify_peer' => false,
                    'passphrase' => null,
                ],
            ],

            'worker' => env('RABBITMQ_WORKER', 'default'),
        ]);

        /** @var QueueManager $queue */
        $queue = $this->app['queue'];

        /** @var RabbitMQQueue $connection */
        $connection = $queue->connection('rabbitmq');
        $this->assertInstanceOf(RabbitMQQueue::class, $connection);
        $this->assertInstanceOf(AMQPSSLConnection::class, $connection->getConnection());
        /** @var AMQPConnectionConfig */
        $config = $connection->getConnection()->getConfig();
        $this->assertFalse($config->getSslVerify());
    }
}
