<?php

namespace VladimirYuldashev\LaravelQueueRabbitMQ\Queue;

use Illuminate\Support\Arr;

class QueueConfigFactory
{
    protected const CONFIG_OPTIONS = 'options';

    /**
     * Create a config object from config array
     */
    public static function make(array $config = []): QueueConfig
    {
        return tap(new QueueConfig, function (QueueConfig $queueConfig) use ($config) {
            if (! empty($queue = Arr::get($config, 'queue'))) {
                $queueConfig->setQueue($queue);
            }
            if (! empty($afterCommit = Arr::get($config, 'after_commit'))) {
                $queueConfig->setDispatchAfterCommit($afterCommit);
            }

            self::getOptionsFromConfig($queueConfig, $config);
        });
    }

    protected static function getOptionsFromConfig(QueueConfig $queueConfig, array $config): void
    {
        $queueOptions = Arr::get($config, self::CONFIG_OPTIONS.'.queue', []) ?: [];

        if ($job = Arr::pull($queueOptions, 'job')) {
            $queueConfig->setAbstractJob($job);
        }

        // Feature: Prioritize delayed messages.
        if ($prioritizeDelayed = Arr::pull($queueOptions, 'prioritize_delayed')) {
            $queueConfig->setPrioritizeDelayed($prioritizeDelayed);
        }
        if ($maxPriority = Arr::pull($queueOptions, 'queue_max_priority')) {
            $queueConfig->setQueueMaxPriority($maxPriority);
        }

        // Feature: Working with Exchange and routing-keys
        if ($exchange = Arr::pull($queueOptions, 'exchange')) {
            $queueConfig->setExchange($exchange);
        }
        if ($exchangeType = Arr::pull($queueOptions, 'exchange_type')) {
            $queueConfig->setExchangeType($exchangeType);
        }
        if ($exchangeRoutingKey = Arr::pull($queueOptions, 'exchange_routing_key')) {
            $queueConfig->setExchangeRoutingKey($exchangeRoutingKey);
        }

        // Feature: Reroute failed messages
        if ($rerouteFailed = Arr::pull($queueOptions, 'reroute_failed')) {
            $queueConfig->setRerouteFailed($rerouteFailed);
        }
        if ($failedExchange = Arr::pull($queueOptions, 'failed_exchange')) {
            $queueConfig->setFailedExchange($failedExchange);
        }
        if ($failedRoutingKey = Arr::pull($queueOptions, 'failed_routing_key')) {
            $queueConfig->setFailedRoutingKey($failedRoutingKey);
        }

        // Feature: Mark queue as quorum
        if ($quorum = Arr::pull($queueOptions, 'quorum')) {
            $queueConfig->setQuorum($quorum);
        }

        // All extra options not defined
        $queueConfig->setOptions($queueOptions);
    }
}
