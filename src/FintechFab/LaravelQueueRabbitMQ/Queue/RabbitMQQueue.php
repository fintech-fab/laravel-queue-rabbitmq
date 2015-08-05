<?php namespace FintechFab\LaravelQueueRabbitMQ\Queue;

use DateTime;
use FintechFab\LaravelQueueRabbitMQ\Queue\Jobs\RabbitMQJob;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Queue\Queue;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPConnection;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;

class RabbitMQQueue extends Queue implements QueueContract
{

	protected $connection;
	protected $channel;

	protected $defaultQueue;
	protected $configQueue;
	protected $configQueues;
	protected $configExchange;
	protected $prefix;

	protected $messageCount;

	/**
	 * @param AMQPConnection $amqpConnection
	 * @param array          $config
	 */
	public function __construct(AMQPConnection $amqpConnection, $config)
	{
		$this->connection = $amqpConnection;
		$this->defaultQueue = $config['queue'];
		$this->configQueue = $config['queue_params'];
		$this->configQueues = isset($config['queues_params']) ? $config['queues_params'] : [];
		$this->configExchange = $config['exchange_params'];
		$this->prefix = isset($config['prefix']) ? $config['prefix'] : '';

		$this->channel = $this->getChannel();
	}

	/**
	 * Purge queue.
	 *
	 * @param  string $queue
	 *
	 * @return void
	 */
	public function purge($queue, $nowait = false, $ticket = null)
	{
		$queue = $this->getQueueName($queue);
		$this->channel->queue_purge($queue, $nowait, $ticket);
	}

	/**
	 * Delete queue.
	 *
	 * @param  string $queue
	 *
	 * @return void
	 */
	public function delete($queue, $if_unused = false, $if_empty = false, $nowait = false, $ticket = null)
	{
		$queue = $this->getQueueName($queue);
		$this->channel->queue_delete($queue, $if_unused, $if_empty, $nowait, $ticket);
	}

	/**
	 * Get message count (only for queues with nowait == false).
	 *
	 * @param  string $queue
	 *
	 * @return integer
	 */
	public function getMessageCount($queue)
	{
		$queue = $this->getQueueName($queue);
		$this->declareQueue($queue);
		return $this->messageCount;
	}

	/**
	 * Push a new job onto the queue.
	 *
	 * @param  string $job
	 * @param  mixed  $data
	 * @param  string $queue
	 *
	 * @return bool
	 */
	public function push($job, $data = '', $queue = null, array $properties = [])
	{
		return $this->pushRaw($this->createPayload($job, $data), $queue, [], $properties);
	}

	/**
	 * Push a raw payload onto the queue.
	 *
	 * @param  string $payload
	 * @param  string $queue
	 * @param  array  $options
	 *
	 * @return mixed
	 */
	public function pushRaw($payload, $queue = null, array $options = [], array $properties = [])
	{
		$queue = $this->getQueueName($queue);
		$this->declareQueue($queue);
		if (isset($options['delay'])) {
			$queue = $this->declareDelayedQueue($queue, $options['delay']);
		}

		// push job to a queue
		$message = new AMQPMessage($payload, $properties + [
			'Content-Type'  => 'application/json',
			'delivery_mode' => 2,
		]);

		// push task to a queue
		$this->channel->basic_publish($message, $queue, $queue);

		return true;
	}

	/**
	 * Push a new job onto the queue after a delay.
	 *
	 * @param  \DateTime|int $delay
	 * @param  string        $job
	 * @param  mixed         $data
	 * @param  string        $queue
	 *
	 * @return mixed
	 */
	public function later($delay, $job, $data = '', $queue = null, array $properties = [])
	{
		return $this->pushRaw($this->createPayload($job, $data), $queue, ['delay' => $delay], $properties);
	}

	/**
	 * Pop the next job off of the queue.
	 *
	 * @param string|null $queue
	 *
	 * @return \Illuminate\Queue\Jobs\Job|null
	 */
	public function pop($queue = null)
	{
		$queue = $this->getQueueName($queue);

		// declare queue if not exists
		$this->declareQueue($queue);

		// get envelope
		$message = $this->channel->basic_get($queue);

		if ($message instanceof AMQPMessage) {
			return new RabbitMQJob($this->container, $this, $this->channel, $queue, $message);
		}

		return null;
	}

	/**
	 * Subscribe.
	 *
	 * @param string|null $queue
	 * @param string $tag
	 * @param Closure callback
	 *
	 * @return true
	 */

	public function subscribe($queue, $tag, \Closure $callback)
	{
		$queue = $this->getQueueName($queue);

		// declare queue if not exists
		$this->declareQueue($queue);

		$callbackEnvelope = function($message) use ($callback, $queue) {
			return $callback(new RabbitMQJob($this->container, $this, $this->channel, $queue, $message));
		};

		$this->channel->basic_consume(
			$queue,    // queue
			$tag,      // consumer tag
			false,     // no local
			false,     // no ack
			false,     // exclusive
			false,     // no wait
			$callbackEnvelope // callback
		);

		while(count($this->channel->callbacks)) {
			$this->channel->wait();
		}

		return true;
	}

	/**
	 * Unsubscribe.
	 *
	 * @param string $tag
	 *
	 * @return void
	 */

	public function unsubscribe($tag)
	{
		$this->channel->basic_cancel($tag);
	}

	/**
	 * @param string $queue
	 *
	 * @return string
	 */
	private function getQueueName($queue, $prefix = true)
	{
		$queue = $queue ? : $this->defaultQueue;

		if ($this->prefix and strpos($queue, $this->prefix) === 0) {
			// prefix already present at the beginning
			$prefix = '';
		}

		return ($prefix ? $this->prefix : '') . $queue;
	}

	/**
	 * @return AMQPChannel
	 */
	private function getChannel()
	{
		return $this->connection->channel();
	}

	/**
	 * @param string $name
	 */
	private function declareQueue($name)
	{
		$name = $this->getQueueName($name, false); // $name is already with prefix

		$arguments = isset($this->configQueues[$name]['arguments']) ? new AMQPTable($this->configQueues[$name]['arguments']) : null;

		$prefetch_count = isset($this->configQueues[$name]['prefetch_count'])
							? $this->configQueues[$name]['prefetch_count']
							: null;

		// declare queue
		$declare_result = $this->channel->queue_declare(
			$name,
			$this->configQueue['passive'],
			$this->configQueue['durable'],
			$this->configQueue['exclusive'],
			$this->configQueue['auto_delete'],
			false,
			$arguments
		);

		if ($declare_result !== null) {
			// if and only if nowait == false
			$this->messageCount = $declare_result[1];
		}

		if ($prefetch_count !== null) {
			// see http://www.rabbitmq.com/consumer-prefetch.html
			$this->channel->basic_qos(0, $prefetch_count, false);
		}

		// declare exchange
		$this->channel->exchange_declare(
			$name,
			$this->configExchange['type'],
			$this->configExchange['passive'],
			$this->configExchange['durable'],
			$this->configExchange['auto_delete']
		);

		// bind queue to the exchange
		$this->channel->queue_bind($name, $name, $name);
	}

	/**
	 * @param string       $destination
	 * @param DateTime|int $delay
	 *
	 * @return string
	 */
	private function declareDelayedQueue($destination, $delay)
	{
		$delay = $this->getSeconds($delay);
		$destination = $this->getQueueName($destination, false); // $destination is already with prefix
		$name = $destination . '_deferred_' . $delay;

		// arguments from normal (non-delayed == destination) queue
		$arguments = isset($this->configQueues[$destination]['arguments']) ? $this->configQueues[$destination]['arguments'] : [];

		// declare exchange
		$this->channel->exchange_declare(
			$name,
			$this->configExchange['type'],
			$this->configExchange['passive'],
			$this->configExchange['durable'],
			$this->configExchange['auto_delete']
		);

		// declare queue
		$this->channel->queue_declare(
			$name,
			$this->configQueue['passive'],
			$this->configQueue['durable'],
			$this->configQueue['exclusive'],
			$this->configQueue['auto_delete'],
			false,
			new AMQPTable([
				'x-dead-letter-exchange'    => $destination,
				'x-dead-letter-routing-key' => $destination,
				'x-message-ttl'             => $delay * 1000,
			] + $arguments)
		);

		// bind queue to the exchange
		$this->channel->queue_bind($name, $name, $name);

		return $name;
	}

}
