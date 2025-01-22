<?php

/** @noinspection PhpRedundantCatchClauseInspection */

namespace VladimirYuldashev\LaravelQueueRabbitMQ\Queue;

use ErrorException;
use Exception;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Contracts\Queue\ShouldBeEncrypted;
use Illuminate\Queue\Queue;
use Illuminate\Support\Arr;
use Illuminate\Support\Facades\Crypt;
use Illuminate\Support\Str;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Exception\AMQPChannelClosedException;
use PhpAmqpLib\Exception\AMQPConnectionBlockedException;
use PhpAmqpLib\Exception\AMQPConnectionClosedException;
use PhpAmqpLib\Exception\AMQPProtocolChannelException;
use PhpAmqpLib\Exception\AMQPRuntimeException;
use PhpAmqpLib\Exchange\AMQPExchangeType;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;
use RuntimeException;
use Throwable;
use VladimirYuldashev\LaravelQueueRabbitMQ\Contracts\RabbitMQQueueContract;
use VladimirYuldashev\LaravelQueueRabbitMQ\Queue\Jobs\RabbitMQJob;

class RabbitMQQueue extends Queue implements QueueContract, RabbitMQQueueContract
{
    /**
     * The RabbitMQ connection instance.
     */
    protected ?AbstractConnection $connection = null;

    /**
     * The RabbitMQ channel instance.
     */
    protected ?AMQPChannel $channel = null;

    /**
     * List of already declared exchanges.
     */
    protected array $exchanges = [];

    /**
     * List of already declared queues.
     */
    protected array $queues = [];

    /**
     * List of already bound queues to exchanges.
     */
    protected array $boundQueues = [];

    /**
     * Current job being processed.
     */
    protected ?RabbitMQJob $currentJob = null;

    /**
     * Holds the Configuration
     */
    protected QueueConfig $config;

    /**
     * RabbitMQQueue constructor.
     */
    public function __construct(QueueConfig $config)
    {
        $this->config = $config;
        $this->dispatchAfterCommit = $config->isDispatchAfterCommit();
    }

    /**
     * {@inheritdoc}
     *
     * @throws AMQPProtocolChannelException
     */
    public function size($queue = null): int
    {
        $queue = $this->getQueue($queue);

        if (! $this->isQueueExists($queue)) {
            return 0;
        }

        // create a temporary channel, so the main channel will not be closed on exception
        $channel = $this->createChannel();
        [, $size] = $channel->queue_declare($queue, true);
        $channel->close();

        return $size;
    }

    /**
     * {@inheritdoc}
     *
     * @throws AMQPProtocolChannelException
     */
    public function push($job, $data = '', $queue = null)
    {
        return $this->enqueueUsing(
            $job,
            $this->createPayload($job, $this->getQueue($queue), $data),
            $queue,
            null,
            function ($payload, $queue) {
                return $this->pushRaw($payload, $queue);
            }
        );
    }

    /**
     * {@inheritdoc}
     *
     * @throws AMQPProtocolChannelException
     */
    public function pushRaw($payload, $queue = null, array $options = []): int|string|null
    {
        [$destination, $exchange, $exchangeType, $attempts] = $this->publishProperties($queue, $options);

        $this->declareDestination($destination, $exchange, $exchangeType);

        [$message, $correlationId] = $this->createMessage($payload, $attempts);

        $this->publishBasic($message, $exchange, $destination, true);

        return $correlationId;
    }

    /**
     * {@inheritdoc}
     *
     * @throws AMQPProtocolChannelException
     */
    public function later($delay, $job, $data = '', $queue = null): mixed
    {
        return $this->enqueueUsing(
            $job,
            $this->createPayload($job, $this->getQueue($queue), $data),
            $queue,
            $delay,
            function ($payload, $queue, $delay) {
                return $this->laterRaw($delay, $payload, $queue);
            }
        );
    }

    /**
     * @throws AMQPProtocolChannelException
     */
    public function laterRaw($delay, string $payload, $queue = null, int $attempts = 0): int|string|null
    {
        $ttl = $this->secondsUntil($delay) * 1000;

        // default options
        $options = ['delay' => $delay, 'attempts' => $attempts];

        // When no ttl just publish a new message to the exchange or queue
        if ($ttl <= 0) {
            return $this->pushRaw($payload, $queue, $options);
        }

        // Create a main queue to handle delayed messages
        [$mainDestination, $exchange, $exchangeType, $attempts] = $this->publishProperties($queue, $options);
        $this->declareDestination($mainDestination, $exchange, $exchangeType);

        $destination = $this->getQueue($queue).'.delay.'.$ttl;

        $this->declareQueue($destination, true, false, $this->getDelayQueueArguments($this->getQueue($queue), $ttl));

        [$message, $correlationId] = $this->createMessage($payload, $attempts);

        // Publish directly on the delayQueue, no need to publish through an exchange.
        $this->publishBasic($message, null, $destination, true);

        return $correlationId;
    }

    /**
     * {@inheritdoc}
     *
     * @throws AMQPProtocolChannelException
     */
    public function bulk($jobs, $data = '', $queue = null): void
    {
        $this->publishBatch($jobs, $data, $queue);
    }

    /**
     * @throws AMQPProtocolChannelException
     */
    protected function publishBatch($jobs, $data = '', $queue = null): void
    {
        foreach ($jobs as $job) {
            $this->bulkRaw($this->createPayload($job, $queue, $data), $queue, ['job' => $job]);
        }

        $this->batchPublish();
    }

    /**
     * @throws AMQPProtocolChannelException
     */
    public function bulkRaw(string $payload, ?string $queue = null, array $options = []): int|string|null
    {
        [$destination, $exchange, $exchangeType, $attempts] = $this->publishProperties($queue, $options);

        $this->declareDestination($destination, $exchange, $exchangeType);

        [$message, $correlationId] = $this->createMessage($payload, $attempts);

        $this->getChannel()->batch_basic_publish($message, $exchange, $destination);

        return $correlationId;
    }

    /**
     * {@inheritdoc}
     *
     * @throws Throwable
     */
    public function pop($queue = null)
    {
        try {
            $queue = $this->getQueue($queue);

            $job = $this->getJobClass();

            /** @var AMQPMessage|null $message */
            if ($message = $this->getChannel()->basic_get($queue)) {
                return $this->currentJob = new $job(
                    $this->container,
                    $this,
                    $message,
                    $this->connectionName,
                    $queue
                );
            }
        } catch (AMQPProtocolChannelException $exception) {
            // If there is no exchange or queue AMQP will throw exception with code 404
            // We need to catch it and return null
            if ($exception->amqp_reply_code === 404) {
                // Because of the channel exception the channel was closed and removed.
                // We have to open a new channel. Because else the worker(s) are stuck in a loop, without processing.
                $this->getChannel(true);

                return null;
            }

            throw $exception;
        } catch (AMQPChannelClosedException|AMQPConnectionClosedException $exception) {
            // Queue::pop used by worker to receive new job
            // Thrown exception is checked by Illuminate\Database\DetectsLostConnections::causedByLostConnection
            // Is has to contain one of the several phrases in exception message in order to restart worker
            // Otherwise worker continues to work with broken connection
            throw new AMQPRuntimeException(
                'Lost connection: '.$exception->getMessage(),
                $exception->getCode(),
                $exception
            );
        }

        return null;
    }

    /**
     * @throws RuntimeException
     */
    public function getConnection(): AbstractConnection
    {
        if (! $this->connection) {
            throw new RuntimeException('Queue has no AMQPConnection set.');
        }

        return $this->connection;
    }

    public function setConnection(AbstractConnection $connection): RabbitMQQueue
    {
        $this->connection = $connection;

        return $this;
    }

    /**
     * Job class to use.
     *
     *
     * @throws Throwable
     */
    public function getJobClass(): string
    {
        $job = $this->getConfig()->getAbstractJob();

        throw_if(
            ! is_a($job, RabbitMQJob::class, true),
            Exception::class,
            sprintf('Class %s must extend: %s', $job, RabbitMQJob::class)
        );

        return $job;
    }

    /**
     * Gets a queue/destination, by default the queue option set on the connection.
     */
    public function getQueue($queue = null): string
    {
        return $queue ?: $this->getConfig()->getQueue();
    }

    /**
     * Checks if the given exchange already present/defined in RabbitMQ.
     * Returns false when the exchange is missing.
     *
     *
     * @throws AMQPProtocolChannelException
     */
    public function isExchangeExists(string $exchange): bool
    {
        if ($this->isExchangeDeclared($exchange)) {
            return true;
        }

        try {
            // create a temporary channel, so the main channel will not be closed on exception
            $channel = $this->createChannel();
            $channel->exchange_declare($exchange, '', true);
            $channel->close();

            $this->exchanges[] = $exchange;

            return true;
        } catch (AMQPProtocolChannelException $exception) {
            if ($exception->amqp_reply_code === 404) {
                return false;
            }

            throw $exception;
        }
    }

    /**
     * Declare an exchange in rabbitMQ, when not already declared.
     */
    public function declareExchange(
        string $name,
        string $type = AMQPExchangeType::DIRECT,
        bool $durable = true,
        bool $autoDelete = false,
        array $arguments = []
    ): void {
        if ($this->isExchangeDeclared($name)) {
            return;
        }

        $this->getChannel()->exchange_declare(
            $name,
            $type,
            false,
            $durable,
            $autoDelete,
            false,
            true,
            new AMQPTable($arguments)
        );
    }

    /**
     * Delete an exchange from rabbitMQ, only when present in RabbitMQ.
     *
     *
     * @throws AMQPProtocolChannelException
     */
    public function deleteExchange(string $name, bool $unused = false): void
    {
        if (! $this->isExchangeExists($name)) {
            return;
        }

        $idx = array_search($name, $this->exchanges);
        unset($this->exchanges[$idx]);

        $this->getChannel()->exchange_delete(
            $name,
            $unused
        );
    }

    /**
     * Checks if the given queue already present/defined in RabbitMQ.
     * Returns false when the queue is missing.
     *
     *
     * @throws AMQPProtocolChannelException
     */
    public function isQueueExists(?string $name = null): bool
    {
        $queueName = $this->getQueue($name);

        if ($this->isQueueDeclared($queueName)) {
            return true;
        }

        try {
            // create a temporary channel, so the main channel will not be closed on exception
            $channel = $this->createChannel();
            $channel->queue_declare($queueName, true);
            $channel->close();

            $this->queues[] = $queueName;

            return true;
        } catch (AMQPProtocolChannelException $exception) {
            if ($exception->amqp_reply_code === 404) {
                return false;
            }

            throw $exception;
        }
    }

    /**
     * Declare a queue in rabbitMQ, when not already declared.
     */
    public function declareQueue(
        string $name,
        bool $durable = true,
        bool $autoDelete = false,
        array $arguments = []
    ): void {
        if ($this->isQueueDeclared($name)) {
            return;
        }

        $this->getChannel()->queue_declare(
            $name,
            false,
            $durable,
            false,
            $autoDelete,
            false,
            new AMQPTable($arguments)
        );
    }

    /**
     * Delete a queue from rabbitMQ, only when present in RabbitMQ.
     *
     *
     * @throws AMQPProtocolChannelException
     */
    public function deleteQueue(string $name, bool $if_unused = false, bool $if_empty = false): void
    {
        if (! $this->isQueueExists($name)) {
            return;
        }

        $idx = array_search($name, $this->queues);
        unset($this->queues[$idx]);

        $this->getChannel()->queue_delete($name, $if_unused, $if_empty);
    }

    /**
     * Bind a queue to an exchange.
     */
    public function bindQueue(string $queue, string $exchange, string $routingKey = ''): void
    {
        if (in_array(
            implode('', compact('queue', 'exchange', 'routingKey')),
            $this->boundQueues,
            true
        )) {
            return;
        }

        $this->getChannel()->queue_bind($queue, $exchange, $routingKey);
    }

    /**
     * Purge the queue of messages.
     */
    public function purge(?string $queue = null): void
    {
        // create a temporary channel, so the main channel will not be closed on exception
        $channel = $this->createChannel();
        $channel->queue_purge($this->getQueue($queue));
        $channel->close();
    }

    /**
     * Acknowledge the message.
     */
    public function ack(RabbitMQJob $job): void
    {
        $this->getChannel()->basic_ack($job->getRabbitMQMessage()->getDeliveryTag());
    }

    /**
     * Reject the message.
     */
    public function reject(RabbitMQJob $job, bool $requeue = false): void
    {
        $this->getChannel()->basic_reject($job->getRabbitMQMessage()->getDeliveryTag(), $requeue);
    }

    /**
     * Create a AMQP message.
     */
    protected function createMessage($payload, int $attempts = 0): array
    {
        $properties = [
            'content_type' => 'application/json',
            'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT,
        ];

        $currentPayload = json_decode($payload, true);
        if ($correlationId = $currentPayload['id'] ?? null) {
            $properties['correlation_id'] = $correlationId;
        }

        if ($this->getConfig()->isPrioritizeDelayed()) {
            $properties['priority'] = $attempts;
        }

        if (isset($currentPayload['data']['command'])) {
            // If the command data is encrypted, decrypt it first before attempting to unserialize
            if (is_subclass_of($currentPayload['data']['commandName'], ShouldBeEncrypted::class)) {
                $currentPayload['data']['command'] = Crypt::decrypt($currentPayload['data']['command']);
            }

            $commandData = unserialize($currentPayload['data']['command']);
            if (property_exists($commandData, 'priority')) {
                $properties['priority'] = $commandData->priority;
            }
        }

        $message = new AMQPMessage($payload, $properties);

        $message->set('application_headers', new AMQPTable([
            'laravel' => [
                'attempts' => $attempts,
            ],
        ]));

        return [
            $message,
            $correlationId,
        ];
    }

    /**
     * Create a payload array from the given job and data.
     *
     * @param  string|object  $job
     * @param  string  $queue
     * @param  mixed  $data
     */
    protected function createPayloadArray($job, $queue, $data = ''): array
    {
        return array_merge(parent::createPayloadArray($job, $queue, $data), [
            'id' => $this->getRandomId(),
        ]);
    }

    /**
     * Get a random ID string.
     */
    protected function getRandomId(): string
    {
        return Str::uuid();
    }

    /**
     * Close the connection to RabbitMQ.
     *
     *
     * @throws Exception
     */
    public function close(): void
    {
        if (isset($this->currentJob) && ! $this->currentJob->isDeletedOrReleased()) {
            $this->reject($this->currentJob, true);
        }

        try {
            $this->getConnection()->close();
        } catch (ErrorException) {
            // Ignore the exception
        }
    }

    /**
     * Get the Queue arguments.
     */
    protected function getQueueArguments(string $destination): array
    {
        $arguments = [];

        // Messages without a priority property are treated as if their priority were 0.
        // Messages with a priority which is higher than the queue's maximum, are treated as if they were
        // published with the maximum priority.
        // Quorum queues does not support priority.
        if ($this->getConfig()->isPrioritizeDelayed() && ! $this->getConfig()->isQuorum()) {
            $arguments['x-max-priority'] = $this->getConfig()->getQueueMaxPriority();
        }

        if ($this->getConfig()->isRerouteFailed()) {
            $arguments['x-dead-letter-exchange'] = $this->getFailedExchange();
            $arguments['x-dead-letter-routing-key'] = $this->getFailedRoutingKey($destination);
        }

        if ($this->getConfig()->isQuorum()) {
            $arguments['x-queue-type'] = 'quorum';
        }

        return $arguments;
    }

    /**
     * Get the Delay queue arguments.
     */
    protected function getDelayQueueArguments(string $destination, int $ttl): array
    {
        return [
            'x-dead-letter-exchange' => $this->getExchange(),
            'x-dead-letter-routing-key' => $this->getRoutingKey($destination),
            'x-message-ttl' => $ttl,
            'x-expires' => $ttl * 2,
        ];
    }

    /**
     * Get the exchange name, or empty string; as default value.
     */
    protected function getExchange(?string $exchange = null): string
    {
        return $exchange ?? $this->getConfig()->getExchange();
    }

    /**
     * Get the routing-key for when you use exchanges
     * The default routing-key is the given destination.
     */
    protected function getRoutingKey(string $destination): string
    {
        return ltrim(sprintf($this->getConfig()->getExchangeRoutingKey(), $destination), '.');
    }

    /**
     * Get the exchangeType, or AMQPExchangeType::DIRECT as default.
     */
    protected function getExchangeType(?string $type = null): string
    {
        $constant = AMQPExchangeType::class.'::'.Str::upper($type ?: $this->getConfig()->getExchangeType());

        return defined($constant) ? constant($constant) : AMQPExchangeType::DIRECT;
    }

    /**
     * Get the exchange for failed messages.
     */
    protected function getFailedExchange(?string $exchange = null): string
    {
        return $exchange ?? $this->getConfig()->getFailedExchange();
    }

    /**
     * Get the routing-key for failed messages
     * The default routing-key is the given destination substituted by '.failed'.
     */
    protected function getFailedRoutingKey(string $destination): string
    {
        return ltrim(sprintf($this->getConfig()->getFailedRoutingKey(), $destination), '.');
    }

    /**
     * Checks if the exchange was already declared.
     */
    protected function isExchangeDeclared(string $name): bool
    {
        return in_array($name, $this->exchanges, true);
    }

    /**
     * Checks if the queue was already declared.
     */
    protected function isQueueDeclared(string $name): bool
    {
        return in_array($name, $this->queues, true);
    }

    /**
     * Declare the destination when necessary.
     *
     * @throws AMQPProtocolChannelException
     */
    protected function declareDestination(string $destination, ?string $exchange = null, string $exchangeType = AMQPExchangeType::DIRECT): void
    {
        // When an exchange is provided and no exchange is present in RabbitMQ, create an exchange.
        if ($exchange && ! $this->isExchangeExists($exchange)) {
            $this->declareExchange($exchange, $exchangeType);
        }

        // When an exchange is provided, just return.
        if ($exchange) {
            return;
        }

        // When the queue already exists, just return.
        if ($this->isQueueExists($destination)) {
            return;
        }

        // Create a queue for amq.direct publishing.
        $this->declareQueue($destination, true, false, $this->getQueueArguments($destination));
    }

    /**
     * Determine all publish properties.
     */
    protected function publishProperties($queue, array $options = []): array
    {
        $queue = $this->getQueue($queue);
        $attempts = Arr::get($options, 'attempts') ?: 0;

        $destination = $this->getRoutingKey($queue);
        $exchange = $this->getExchange(Arr::get($options, 'exchange'));
        $exchangeType = $this->getExchangeType(Arr::get($options, 'exchange_type'));

        return [$destination, $exchange, $exchangeType, $attempts];
    }

    protected function getConfig(): QueueConfig
    {
        return $this->config;
    }

    /**
     * @throws AMQPChannelClosedException
     * @throws AMQPConnectionClosedException
     * @throws AMQPConnectionBlockedException
     */
    protected function publishBasic($msg, $exchange = '', $destination = '', $mandatory = false, $immediate = false, $ticket = null): void
    {
        $this->getChannel()->basic_publish($msg, $exchange, $destination, $mandatory, $immediate, $ticket);
    }

    protected function batchPublish(): void
    {
        $this->getChannel()->publish_batch();
    }

    public function getChannel($forceNew = false): AMQPChannel
    {
        if (! $this->channel || $forceNew) {
            $this->channel = $this->createChannel();
        }

        return $this->channel;
    }

    protected function createChannel(): AMQPChannel
    {
        return $this->getConnection()->channel();
    }

    /**
     * @throws Exception
     */
    protected function reconnect(): void
    {
        // Reconnects using the original connection settings.
        $this->getConnection()->reconnect();
        // Create a new main channel because all old channels are removed.
        $this->getChannel(true);
    }
}
