<?php

namespace VladimirYuldashev\LaravelQueueRabbitMQ\Tests\Functional;

use Illuminate\Support\Str;
use PhpAmqpLib\Exchange\AMQPExchangeType;
use VladimirYuldashev\LaravelQueueRabbitMQ\Queue\RabbitMQQueue;
use VladimirYuldashev\LaravelQueueRabbitMQ\Tests\Functional\TestCase as BaseTestCase;

class RabbitMQQueueTest extends BaseTestCase
{
    public function test_connection(): void
    {
        $queue = $this->connection();
        $this->assertInstanceOf(RabbitMQQueue::class, $queue);

        $queue = $this->connection('rabbitmq-with-options');
        $this->assertInstanceOf(RabbitMQQueue::class, $queue);

        $queue = $this->connection('rabbitmq-with-options-empty');
        $this->assertInstanceOf(RabbitMQQueue::class, $queue);
    }

    public function test_config_reroute_failed(): void
    {
        $queue = $this->connection();
        $this->assertFalse($this->callProperty($queue, 'config')->isRerouteFailed());

        $queue = $this->connection('rabbitmq-with-options');
        $this->assertTrue($this->callProperty($queue, 'config')->isRerouteFailed());

        $queue = $this->connection('rabbitmq-with-options-empty');
        $this->assertFalse($this->callProperty($queue, 'config')->isRerouteFailed());

        $queue = $this->connection('rabbitmq-with-options-null');
        $this->assertFalse($this->callProperty($queue, 'config')->isRerouteFailed());
    }

    public function test_config_prioritize_delayed(): void
    {
        $queue = $this->connection();
        $this->assertFalse($this->callProperty($queue, 'config')->isPrioritizeDelayed());

        $queue = $this->connection('rabbitmq-with-options');
        $this->assertTrue($this->callProperty($queue, 'config')->isPrioritizeDelayed());

        $queue = $this->connection('rabbitmq-with-options-empty');
        $this->assertFalse($this->callProperty($queue, 'config')->isPrioritizeDelayed());

        $queue = $this->connection('rabbitmq-with-options-null');
        $this->assertFalse($this->callProperty($queue, 'config')->isPrioritizeDelayed());
    }

    public function test_queue_max_priority(): void
    {
        $queue = $this->connection();
        $this->assertIsInt($this->callProperty($queue, 'config')->getQueueMaxPriority());
        $this->assertSame(2, $this->callProperty($queue, 'config')->getQueueMaxPriority());

        $queue = $this->connection('rabbitmq-with-options');
        $this->assertIsInt($this->callProperty($queue, 'config')->getQueueMaxPriority());
        $this->assertSame(20, $this->callProperty($queue, 'config')->getQueueMaxPriority());

        $queue = $this->connection('rabbitmq-with-options-empty');
        $this->assertIsInt($this->callProperty($queue, 'config')->getQueueMaxPriority());
        $this->assertSame(2, $this->callProperty($queue, 'config')->getQueueMaxPriority());

        $queue = $this->connection('rabbitmq-with-options-null');
        $this->assertIsInt($this->callProperty($queue, 'config')->getQueueMaxPriority());
        $this->assertSame(2, $this->callProperty($queue, 'config')->getQueueMaxPriority());
    }

    public function test_config_exchange_type(): void
    {
        $queue = $this->connection();
        $this->assertSame(AMQPExchangeType::DIRECT, $this->callMethod($queue, 'getExchangeType'));
        $this->assertSame(AMQPExchangeType::DIRECT, $this->callMethod($queue, 'getExchangeType', ['']));
        $this->assertSame(AMQPExchangeType::TOPIC, $this->callMethod($queue, 'getExchangeType', ['topic']));

        $queue = $this->connection('rabbitmq-with-options');
        $this->assertSame(AMQPExchangeType::TOPIC, $this->callMethod($queue, 'getExchangeType'));
        $this->assertSame(AMQPExchangeType::DIRECT, $this->callMethod($queue, 'getExchangeType', ['direct']));

        $queue = $this->connection('rabbitmq-with-options-empty');
        $this->assertSame(AMQPExchangeType::DIRECT, $this->callMethod($queue, 'getExchangeType'));

        $queue = $this->connection('rabbitmq-with-options-null');
        $this->assertSame(AMQPExchangeType::DIRECT, $this->callMethod($queue, 'getExchangeType'));

        // testing an unkown type with a default
        $this->callProperty($queue, 'config')->setExchangeType('unknown');
        $this->assertSame(AMQPExchangeType::DIRECT, $this->callMethod($queue, 'getExchangeType'));
    }

    public function test_exchange(): void
    {
        $queue = $this->connection();
        $this->assertSame('test', $this->callMethod($queue, 'getExchange', ['test']));
        $this->assertSame('', $this->callMethod($queue, 'getExchange', ['']));
        $this->assertSame('', $this->callMethod($queue, 'getExchange', [null]));
        $this->assertSame('', $this->callMethod($queue, 'getExchange'));

        $queue = $this->connection('rabbitmq-with-options');
        $this->assertSame('application-x', $this->callMethod($queue, 'getExchange'));
        $this->assertSame('application-x', $this->callMethod($queue, 'getExchange', [null]));
        $this->assertSame('test', $this->callMethod($queue, 'getExchange', ['test']));
        $this->assertSame('', $this->callMethod($queue, 'getExchange', ['']));

        $queue = $this->connection('rabbitmq-with-options-empty');
        $this->assertSame('', $this->callMethod($queue, 'getExchange'));
        $this->assertSame('', $this->callMethod($queue, 'getExchange', [null]));
        $this->assertSame('test', $this->callMethod($queue, 'getExchange', ['test']));
        $this->assertSame('', $this->callMethod($queue, 'getExchange', ['']));

        $queue = $this->connection('rabbitmq-with-options-null');
        $this->assertSame('', $this->callMethod($queue, 'getExchange'));
        $this->assertSame('', $this->callMethod($queue, 'getExchange', [null]));
        $this->assertSame('test', $this->callMethod($queue, 'getExchange', ['test']));
        $this->assertSame('', $this->callMethod($queue, 'getExchange', ['']));
    }

    public function test_failed_exchange(): void
    {
        $queue = $this->connection();
        $this->assertSame('test', $this->callMethod($queue, 'getFailedExchange', ['test']));
        $this->assertSame('', $this->callMethod($queue, 'getFailedExchange', ['']));
        $this->assertSame('', $this->callMethod($queue, 'getFailedExchange', [null]));
        $this->assertSame('', $this->callMethod($queue, 'getFailedExchange'));

        $queue = $this->connection('rabbitmq-with-options');
        $this->assertSame('failed-exchange', $this->callMethod($queue, 'getFailedExchange'));
        $this->assertSame('failed-exchange', $this->callMethod($queue, 'getFailedExchange', [null]));
        $this->assertSame('test', $this->callMethod($queue, 'getFailedExchange', ['test']));
        $this->assertSame('', $this->callMethod($queue, 'getFailedExchange', ['']));

        $queue = $this->connection('rabbitmq-with-options-empty');
        $this->assertSame('', $this->callMethod($queue, 'getFailedExchange'));
        $this->assertSame('', $this->callMethod($queue, 'getFailedExchange', [null]));
        $this->assertSame('test', $this->callMethod($queue, 'getFailedExchange', ['test']));
        $this->assertSame('', $this->callMethod($queue, 'getFailedExchange', ['']));

        $queue = $this->connection('rabbitmq-with-options-null');
        $this->assertSame('', $this->callMethod($queue, 'getFailedExchange'));
        $this->assertSame('', $this->callMethod($queue, 'getFailedExchange', [null]));
        $this->assertSame('test', $this->callMethod($queue, 'getFailedExchange', ['test']));
        $this->assertSame('', $this->callMethod($queue, 'getFailedExchange', ['']));
    }

    public function test_routing_key(): void
    {
        $queue = $this->connection();
        $this->assertSame('test', $this->callMethod($queue, 'getRoutingKey', ['test']));
        $this->assertSame('test', $this->callMethod($queue, 'getRoutingKey', ['.test']));
        $this->assertSame('', $this->callMethod($queue, 'getRoutingKey', ['']));

        $queue = $this->connection('rabbitmq-with-options');
        $this->assertSame('process.test', $this->callMethod($queue, 'getRoutingKey', ['test']));

        $queue = $this->connection('rabbitmq-with-options-empty');
        $this->assertSame('test', $this->callMethod($queue, 'getRoutingKey', ['test']));

        $queue = $this->connection('rabbitmq-with-options-null');
        $this->assertSame('test', $this->callMethod($queue, 'getRoutingKey', ['test']));
        $this->callProperty($queue, 'config')->setExchangeRoutingKey('.an.alternate.routing-key');
        $this->assertSame('an.alternate.routing-key', $this->callMethod($queue, 'getRoutingKey', ['test']));
    }

    public function test_failed_routing_key(): void
    {
        $queue = $this->connection();
        $this->assertSame('test.failed', $this->callMethod($queue, 'getFailedRoutingKey', ['test']));
        $this->assertSame('test.failed', $this->callMethod($queue, 'getFailedRoutingKey', ['.test']));
        $this->assertSame('failed', $this->callMethod($queue, 'getFailedRoutingKey', ['']));

        $queue = $this->connection('rabbitmq-with-options');
        $this->assertSame('application-x.test.failed', $this->callMethod($queue, 'getFailedRoutingKey', ['test']));

        $queue = $this->connection('rabbitmq-with-options-empty');
        $this->assertSame('test.failed', $this->callMethod($queue, 'getFailedRoutingKey', ['test']));

        $queue = $this->connection('rabbitmq-with-options-null');
        $this->assertSame('test.failed', $this->callMethod($queue, 'getFailedRoutingKey', ['test']));
        $this->callProperty($queue, 'config')->setFailedRoutingKey('.an.alternate.routing-key');
        $this->assertSame('an.alternate.routing-key', $this->callMethod($queue, 'getFailedRoutingKey', ['test']));
    }

    public function test_config_quorum(): void
    {
        $queue = $this->connection();
        $this->assertFalse($this->callProperty($queue, 'config')->isQuorum());

        $queue = $this->connection('rabbitmq-with-options');
        $this->assertFalse($this->callProperty($queue, 'config')->isQuorum());

        $queue = $this->connection('rabbitmq-with-options-empty');
        $this->assertFalse($this->callProperty($queue, 'config')->isQuorum());

        $queue = $this->connection('rabbitmq-with-options-null');
        $this->assertFalse($this->callProperty($queue, 'config')->isQuorum());

        $queue = $this->connection('rabbitmq-with-quorum-options');
        $this->assertTrue($this->callProperty($queue, 'config')->isQuorum());
    }

    public function test_declare_delete_exchange(): void
    {
        $queue = $this->connection();

        $name = Str::random();

        $this->assertFalse($queue->isExchangeExists($name));

        $queue->declareExchange($name);
        $this->assertTrue($queue->isExchangeExists($name));

        $queue->deleteExchange($name);
        $this->assertFalse($queue->isExchangeExists($name));
    }

    public function test_declare_delete_queue(): void
    {
        $queue = $this->connection();

        $name = Str::random();

        $this->assertFalse($queue->isQueueExists($name));

        $queue->declareQueue($name);
        $this->assertTrue($queue->isQueueExists($name));

        $queue->deleteQueue($name);
        $this->assertFalse($queue->isQueueExists($name));
    }

    public function test_queue_arguments(): void
    {
        $name = Str::random();

        $queue = $this->connection();
        $actual = $this->callMethod($queue, 'getQueueArguments', [$name]);
        $expected = [];
        $this->assertEquals(array_keys($expected), array_keys($actual));
        $this->assertEquals(array_values($expected), array_values($actual));

        $queue = $this->connection('rabbitmq-with-options');
        $actual = $this->callMethod($queue, 'getQueueArguments', [$name]);
        $expected = [
            'x-max-priority' => 20,
            'x-dead-letter-exchange' => 'failed-exchange',
            'x-dead-letter-routing-key' => sprintf('application-x.%s.failed', $name),
        ];

        $this->assertEquals(array_keys($expected), array_keys($actual));
        $this->assertEquals(array_values($expected), array_values($actual));

        $queue = $this->connection('rabbitmq-with-quorum-options');
        $actual = $this->callMethod($queue, 'getQueueArguments', [$name]);
        $expected = [
            'x-dead-letter-exchange' => 'failed-exchange',
            'x-dead-letter-routing-key' => sprintf('application-x.%s.failed', $name),
            'x-queue-type' => 'quorum',
        ];

        $this->assertEquals(array_keys($expected), array_keys($actual));
        $this->assertEquals(array_values($expected), array_values($actual));

        $queue = $this->connection('rabbitmq-with-options-empty');
        $actual = $this->callMethod($queue, 'getQueueArguments', [$name]);
        $expected = [];

        $this->assertEquals(array_keys($expected), array_keys($actual));
        $this->assertEquals(array_values($expected), array_values($actual));
    }

    public function test_delay_queue_arguments(): void
    {
        $name = Str::random();
        $ttl = 12000;

        $queue = $this->connection();
        $actual = $this->callMethod($queue, 'getDelayQueueArguments', [$name, $ttl]);
        $expected = [
            'x-dead-letter-exchange' => '',
            'x-dead-letter-routing-key' => $name,
            'x-message-ttl' => $ttl,
            'x-expires' => $ttl * 2,
        ];
        $this->assertEquals(array_keys($expected), array_keys($actual));
        $this->assertEquals(array_values($expected), array_values($actual));

        $queue = $this->connection('rabbitmq-with-options');
        $actual = $this->callMethod($queue, 'getDelayQueueArguments', [$name, $ttl]);
        $expected = [
            'x-dead-letter-exchange' => 'application-x',
            'x-dead-letter-routing-key' => sprintf('process.%s', $name),
            'x-message-ttl' => $ttl,
            'x-expires' => $ttl * 2,
        ];
        $this->assertEquals(array_keys($expected), array_keys($actual));
        $this->assertEquals(array_values($expected), array_values($actual));

        $queue = $this->connection('rabbitmq-with-options-empty');
        $actual = $this->callMethod($queue, 'getDelayQueueArguments', [$name, $ttl]);
        $expected = [
            'x-dead-letter-exchange' => '',
            'x-dead-letter-routing-key' => $name,
            'x-message-ttl' => $ttl,
            'x-expires' => $ttl * 2,
        ];
        $this->assertEquals(array_keys($expected), array_keys($actual));
        $this->assertEquals(array_values($expected), array_values($actual));
    }
}
