<?php

namespace VladimirYuldashev\LaravelQueueRabbitMQ\Tests\Feature;

use Illuminate\Database\DatabaseTransactionsManager;
use Illuminate\Support\Facades\Queue;
use Illuminate\Support\Str;
use PhpAmqpLib\Exception\AMQPProtocolChannelException;
use RuntimeException;
use VladimirYuldashev\LaravelQueueRabbitMQ\Queue\Jobs\RabbitMQJob;
use VladimirYuldashev\LaravelQueueRabbitMQ\Tests\Mocks\TestEncryptedJob;
use VladimirYuldashev\LaravelQueueRabbitMQ\Tests\Mocks\TestJob;
use VladimirYuldashev\LaravelQueueRabbitMQ\Tests\TestCase as BaseTestCase;

abstract class TestCase extends BaseTestCase
{
    /**
     * @throws AMQPProtocolChannelException
     */
    protected function setUp(): void
    {
        parent::setUp();

        if ($this->connection()->isQueueExists()) {
            $this->connection()->purge();
        }
    }

    /**
     * @throws AMQPProtocolChannelException
     */
    protected function tearDown(): void
    {
        if ($this->connection()->isQueueExists()) {
            $this->connection()->purge();
        }

        self::assertSame(0, Queue::size());

        parent::tearDown();
    }

    public function test_size_does_not_throw_exception_on_unknown_queue(): void
    {
        $this->assertEmpty(0, Queue::size(Str::random()));
    }

    public function test_pop_nothing(): void
    {
        $this->assertNull(Queue::pop('foo'));
    }

    public function test_push_raw(): void
    {
        Queue::pushRaw($payload = Str::random());

        sleep(1);

        $this->assertSame(1, Queue::size());
        $this->assertNotNull($job = Queue::pop());
        $this->assertSame(1, $job->attempts());
        $this->assertInstanceOf(RabbitMQJob::class, $job);
        $this->assertSame($payload, $job->getRawBody());

        $this->assertNull($job->getJobId());

        $job->delete();
        $this->assertSame(0, Queue::size());
    }

    public function test_push(): void
    {
        Queue::push(new TestJob);

        sleep(1);

        $this->assertSame(1, Queue::size());
        $this->assertNotNull($job = Queue::pop());
        $this->assertSame(1, $job->attempts());
        $this->assertInstanceOf(RabbitMQJob::class, $job);
        $this->assertSame(TestJob::class, $job->resolveName());
        $this->assertNotNull($job->getJobId());

        $payload = $job->payload();

        $this->assertSame(TestJob::class, $payload['displayName']);
        $this->assertSame('Illuminate\Queue\CallQueuedHandler@call', $payload['job']);
        $this->assertNull($payload['maxTries']);
        $this->assertNull($payload['backoff']);
        $this->assertNull($payload['timeout']);
        $this->assertNull($payload['retryUntil']);
        $this->assertSame($job->getJobId(), $payload['id']);

        $job->delete();
        $this->assertSame(0, Queue::size());
    }

    public function test_push_after_commit(): void
    {
        $transaction = new DatabaseTransactionsManager;

        $this->app->singleton('db.transactions', function ($app) use ($transaction) {
            $transaction->begin('FakeDBConnection', 1);

            return $transaction;
        });

        TestJob::dispatch()->afterCommit();

        sleep(1);
        $this->assertSame(0, Queue::size());
        $this->assertNull(Queue::pop());

        $transaction->commit('FakeDBConnection', 1, 0);

        sleep(1);

        $this->assertSame(1, Queue::size());
        $this->assertNotNull($job = Queue::pop());

        $job->delete();
        $this->assertSame(0, Queue::size());
    }

    public function test_later_raw(): void
    {
        $payload = Str::random();
        $data = [Str::random() => Str::random()];

        Queue::later(3, $payload, $data);

        sleep(1);

        $this->assertSame(0, Queue::size());
        $this->assertNull(Queue::pop());

        sleep(3);

        $this->assertSame(1, Queue::size());
        $this->assertNotNull($job = Queue::pop());

        $this->assertInstanceOf(RabbitMQJob::class, $job);
        $this->assertSame($payload, $job->getName());

        $body = json_decode($job->getRawBody(), true);

        $this->assertSame($payload, $body['displayName']);
        $this->assertSame($payload, $body['job']);
        $this->assertSame($data, $body['data']);

        $job->delete();
        $this->assertSame(0, Queue::size());
    }

    public function test_later(): void
    {
        Queue::later(3, new TestJob);

        sleep(1);

        $this->assertSame(0, Queue::size());
        $this->assertNull(Queue::pop());

        sleep(3);

        $this->assertSame(1, Queue::size());
        $this->assertNotNull($job = Queue::pop());

        $this->assertInstanceOf(RabbitMQJob::class, $job);

        $body = json_decode($job->getRawBody(), true);

        $this->assertSame(TestJob::class, $body['displayName']);
        $this->assertSame('Illuminate\Queue\CallQueuedHandler@call', $body['job']);
        $this->assertSame(TestJob::class, $body['data']['commandName']);
        $this->assertNotNull($job->getJobId());

        $job->delete();
        $this->assertSame(0, Queue::size());
    }

    public function test_bulk(): void
    {
        $count = 100;
        $jobs = [];

        for ($i = 0; $i < $count; $i++) {
            $jobs[$i] = new TestJob($i);
        }

        Queue::bulk($jobs);

        sleep(1);

        $this->assertSame($count, Queue::size());
    }

    public function test_push_encrypted(): void
    {
        Queue::push(new TestEncryptedJob);

        sleep(1);

        $this->assertSame(1, Queue::size());
        $this->assertNotNull($job = Queue::pop());
        $this->assertSame(1, $job->attempts());
        $this->assertInstanceOf(RabbitMQJob::class, $job);
        $this->assertSame(TestEncryptedJob::class, $job->resolveName());
        $this->assertNotNull($job->getJobId());

        $payload = $job->payload();

        $this->assertSame(TestEncryptedJob::class, $payload['displayName']);
        $this->assertSame('Illuminate\Queue\CallQueuedHandler@call', $payload['job']);
        $this->assertNull($payload['maxTries']);
        $this->assertNull($payload['backoff']);
        $this->assertNull($payload['timeout']);
        $this->assertNull($payload['retryUntil']);
        $this->assertSame($job->getJobId(), $payload['id']);

        $job->delete();
        $this->assertSame(0, Queue::size());
    }

    public function test_push_encrypted_after_commit(): void
    {
        $transaction = new DatabaseTransactionsManager;

        $this->app->singleton('db.transactions', function ($app) use ($transaction) {
            $transaction->begin('FakeDBConnection', 1);

            return $transaction;
        });

        TestEncryptedJob::dispatch()->afterCommit();

        sleep(1);
        $this->assertSame(0, Queue::size());
        $this->assertNull(Queue::pop());

        $transaction->commit('FakeDBConnection', 1, 0);

        sleep(1);

        $this->assertSame(1, Queue::size());
        $this->assertNotNull($job = Queue::pop());

        $job->delete();
        $this->assertSame(0, Queue::size());
    }

    public function test_encrypted_later(): void
    {
        Queue::later(3, new TestEncryptedJob);

        sleep(1);

        $this->assertSame(0, Queue::size());
        $this->assertNull(Queue::pop());

        sleep(3);

        $this->assertSame(1, Queue::size());
        $this->assertNotNull($job = Queue::pop());

        $this->assertInstanceOf(RabbitMQJob::class, $job);

        $body = json_decode($job->getRawBody(), true);

        $this->assertSame(TestEncryptedJob::class, $body['displayName']);
        $this->assertSame('Illuminate\Queue\CallQueuedHandler@call', $body['job']);
        $this->assertSame(TestEncryptedJob::class, $body['data']['commandName']);
        $this->assertNotNull($job->getJobId());

        $job->delete();
        $this->assertSame(0, Queue::size());
    }

    public function test_encrypted_bulk(): void
    {
        $count = 100;
        $jobs = [];

        for ($i = 0; $i < $count; $i++) {
            $jobs[$i] = new TestEncryptedJob($i);
        }

        Queue::bulk($jobs);

        sleep(1);

        $this->assertSame($count, Queue::size());
    }

    public function test_release_raw(): void
    {
        Queue::pushRaw($payload = Str::random());

        sleep(1);

        $this->assertSame(1, Queue::size());
        $this->assertNotNull($job = Queue::pop());
        $this->assertSame(1, $job->attempts());

        for ($attempt = 2; $attempt <= 4; $attempt++) {
            $job->release();

            sleep(1);

            $this->assertSame(1, Queue::size());

            $job = Queue::pop();

            $this->assertSame($attempt, $job->attempts());
        }

        $job->delete();
        $this->assertSame(0, Queue::size());
    }

    public function test_release(): void
    {
        Queue::push(new TestJob);

        sleep(1);

        $this->assertSame(1, Queue::size());
        $this->assertNotNull($job = Queue::pop());
        $this->assertSame(1, $job->attempts());

        for ($attempt = 2; $attempt <= 4; $attempt++) {
            $job->release();

            sleep(1);

            $this->assertSame(1, Queue::size());

            $job = Queue::pop();

            $this->assertSame($attempt, $job->attempts());
        }

        $job->delete();
        $this->assertSame(0, Queue::size());
    }

    public function test_release_with_delay_raw(): void
    {
        Queue::pushRaw($payload = Str::random());

        sleep(1);

        $this->assertSame(1, Queue::size());
        $this->assertNotNull($job = Queue::pop());
        $this->assertSame(1, $job->attempts());

        for ($attempt = 2; $attempt <= 4; $attempt++) {
            $job->release(4);

            sleep(1);

            $this->assertSame(0, Queue::size());
            $this->assertNull(Queue::pop());

            sleep(4);

            $this->assertSame(1, Queue::size());

            $job = Queue::pop();

            $this->assertSame($attempt, $job->attempts());
        }

        $job->delete();
        $this->assertSame(0, Queue::size());
    }

    public function test_release_in_the_past(): void
    {
        Queue::push(new TestJob);

        $job = Queue::pop();
        $job->release(-3);

        sleep(1);

        $this->assertInstanceOf(RabbitMQJob::class, $job = Queue::pop());

        $job->delete();
        $this->assertSame(0, Queue::size());
    }

    public function test_release_and_release_with_delay_attempts(): void
    {
        Queue::push(new TestJob);

        sleep(1);

        $this->assertSame(1, Queue::size());
        $this->assertNotNull($job = Queue::pop());

        $job->release();

        sleep(1);

        $this->assertNotNull($job = Queue::pop());
        $this->assertSame(2, $job->attempts());

        $job->release(3);

        sleep(4);

        $this->assertNotNull($job = Queue::pop());
        $this->assertSame(3, $job->attempts());

        $job->delete();
        $this->assertSame(0, Queue::size());
    }

    public function test_delete(): void
    {
        Queue::push(new TestJob);

        $job = Queue::pop();

        $job->delete();

        sleep(1);

        $this->assertSame(0, Queue::size());
        $this->assertNull(Queue::pop());
    }

    public function test_failed(): void
    {
        Queue::push(new TestJob);

        $job = Queue::pop();

        $job->fail(new RuntimeException($job->resolveName().' has an exception.'));

        sleep(1);

        $this->assertSame(true, $job->hasFailed());
        $this->assertSame(true, $job->isDeleted());
        $this->assertSame(0, Queue::size());
        $this->assertNull(Queue::pop());
    }
}
