<?php

namespace VladimirYuldashev\LaravelQueueRabbitMQ\Tests\Feature;

use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Exception\AMQPChannelClosedException;
use PhpAmqpLib\Exception\AMQPConnectionClosedException;
use PhpAmqpLib\Exception\AMQPProtocolChannelException;
use VladimirYuldashev\LaravelQueueRabbitMQ\Tests\Mocks\TestJob;

class QueueTest extends TestCase
{
    protected function setUp(): void
    {
        parent::setUp();

        $this->withoutExceptionHandling([
            AMQPChannelClosedException::class, AMQPConnectionClosedException::class,
            AMQPProtocolChannelException::class,
        ]);
    }

    public function testConnection(): void
    {
        $this->assertInstanceOf(AMQPStreamConnection::class, $this->connection()->getChannel()->getConnection());
    }

    public function testWithoutReconnect(): void
    {
        $queue = $this->connection('rabbitmq');

        $queue->push(new TestJob);
        sleep(1);
        $this->assertSame(1, $queue->size());

        // close connection
        $queue->getConnection()->close();
        $this->assertFalse($queue->getConnection()->isConnected());

        $this->expectException(AMQPChannelClosedException::class);
        $queue->push(new TestJob);
    }
}
