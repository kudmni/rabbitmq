<?php

namespace PrCy\RabbitMQ;

use \PhpAmqpLib\Connection\AMQPConnection;
use \PhpAmqpLib\Message\AMQPMessage;
use \PhpAmqpLib\Wire\AMQPTable;
use \PhpAmqpLib\Exception\AMQPTimeoutException;
use \PrCy\RabbitMQ\Exception\TimeoutException;

class Producer
{
    const PRIORITY_LOW       = 0;
    const PRIORITY_NORMAL    = 1;
    const PRIORITY_HIGH      = 2;
    const PRIORITY_MAX       = 3;

    const MESSAGE_TTL        = 86400; // TTL сообщения по умолчанию = 1 сутки

    protected $connection;
    protected $messagePrefix;
    protected $exchangePrefix;

    /**
     * Конструктор класса
     * @param string $host
     * @param integer $port
     * @param string $user
     * @param string $password
     * @param string $messagePrefix    Используется для формирования ID сообщений
     * @param string $exchangePrefix   Используется для формирования уникального названия обменника (для разных проектов)
     */
    public function __construct($host = '127.0.0.1', $port = 5672, $user = 'guest', $password = 'guest', $messagePrefix = '', $exchangePrefix = '')
    {
        $this->connection     = $this->createConnection($host, $port, $user, $password);
        $this->messagePrefix  = $messagePrefix;
        $this->exchangePrefix = $exchangePrefix;
    }

    /**
     * Создаёт экземпляр соединения с RabbitMQ
     * @param string $host
     * @param integer $port
     * @param string $user
     * @param string $password
     * @return AMQPConnection
     * @codeCoverageIgnore
     */
    protected function createConnection($host, $port, $user, $password)
    {
        return new AMQPConnection($host, $port, $user, $password);
    }

    /**
     * Создаёт экземпляр сообщения
     * @param mixed $body
     * @param array $options
     * @return AMQPMessage
     * @codeCoverageIgnore
     */
    protected function createMessage($body, $options)
    {
        return new AMQPMessage(json_encode($body), $options);
    }

    /**
     * Создаёт экземпляр таблицы параметров сообщения
     * @param array $data
     * @return AMQPTable
     * @codeCoverageIgnore
     */
    protected function createTable($data)
    {
        return new AMQPTable($data);
    }

    /**
     * Создаёт уникальный идентификатор сообщения
     * @return string
     * @codeCoverageIgnore
     */
    protected function createUniqid()
    {
        return uniqid($this->messagePrefix, true);
    }

    /**
     * Отправка сообщения без подтверждения (отправили и забыли)
     * @link http://www.rabbitmq.com/tutorials/tutorial-one-php.html Simple message
     * @param string $routingKey
     * @param string $body
     * @param int $ttl
     * @return void
     */
    public function addMessage($routingKey, $body, $ttl = self::MESSAGE_TTL)
    {
        $channel = $this->connection->channel();
        $channel->basic_qos(null, 1, null);
        $msg = $this->createMessage(
            $body,
            ['delivery_mode' => 2, 'expiration' => 1000 * (int) $ttl]
        );
        $channel->basic_publish($msg, $this->exchangePrefix . 'bg', $routingKey);
        $channel->close();
    }

    /**
     * Отправка сообщения с подтверждением
     * @link http://www.rabbitmq.com/tutorials/tutorial-two-php.html Message acknowledgment
     * @param string $routingKey
     * @param string $body
     * @param int $priority
     * @param int $ttl
     * @return void
     */
    public function addAckMessage($routingKey, $body, $priority = self::PRIORITY_NORMAL, $ttl = self::MESSAGE_TTL)
    {
        $channel = $this->connection->channel();
        $channel->basic_qos(null, 1, null);
        $correlationId = $this->createUniqid();
        $msg = $this->createMessage(
            $body,
            [
                'delivery_mode'  => 2,
                'correlation_id' => $correlationId,
                'priority'       => $priority,
                'expiration'     => 1000 * (int) $ttl
            ]
        );
        $channel->basic_publish($msg, $this->exchangePrefix . 'ack', $routingKey);
        $channel->close();
    }

    /**
     * Создание канала для отправки сообщений
     * @return object channel
     */
    public function  createChannel()
    {
        $channel = $this->connection->channel();
        $channel->basic_qos(null, 1, null);
        return $channel;
    }

    /**
     * Создание канала для отправки ack-сообщений в параллельном режиме
     * @return object channel
     */
    public function createAckChannel($prefetchCount = 1)
    {
        $channel = $this->connection->channel();
        $channel->basic_qos(null, $prefetchCount, null);
        $channel->exchange_declare($this->exchangePrefix . 'ack', 'topic', false, false, false);
        return $channel;
    }

    /**
     * Создание канала для отправки rpc-сообщений в параллельном режиме
     * @return object channel
     */
    public function createRpcChannel($prefetchCount = 1)
    {
        $channel = $this->connection->channel();
        $channel->basic_qos(null, $prefetchCount, null);
        $channel->exchange_declare($this->exchangePrefix . 'rpc', 'topic', false, false, false);
        return $channel;
    }
    
    /**
     * Добавление ack-сообщения в канал
     * @param object $channel
     * @param string $routingKey
     * @param string $body
     * @param int $priority
     * @param int $ttl
     * @return void
     */
    public function appendAckMessage($channel, $routingKey, $body, $priority = self::PRIORITY_NORMAL, $ttl = self::MESSAGE_TTL)
    {
        $msg = $this->createMessage(
            $body,
            [
                'delivery_mode'  => 2,
                'correlation_id' => $this->createUniqid(),
                'priority'       => $priority,
                'expiration'     => 1000 * (int) $ttl
            ]
        );
        $channel->basic_publish($msg, $this->exchangePrefix . 'ack', $routingKey);
    }

    /**
     * Отправка сообщения с подтверждением в отложенную очередь
     * @link https://www.rabbitmq.com/dlx.html Dead Letter Exchanges
     * @param string $queue
     * @param string $exchange
     * @param string $body
     * @param int    $delay
     * @param int    $time
     */
    public function addDelayedMessage($queue, $exchange, $body, $delay, $time)
    {
        $exchange = $this->exchangePrefix . $exchange;
        $channel = $this->connection->channel();
        // Обменник
        $channel->exchange_declare($exchange, 'topic', false, false, false);
        // Обменник dlx
        $exchangeDlx = $exchange . ".dlx";
        $channel->exchange_declare($exchangeDlx, 'topic', false, false, false);
        // Очередь dlx
        $endtime     = $time + $delay * 1000;
        $queueFull   = "$exchange.$queue.$endtime";
        $arguments   = $this->createTable([
            "x-dead-letter-exchange" => $exchange,
            "x-message-ttl"          => $delay * 1000,
            "x-expires"              => $delay * 1000 + 10000
        ]);
        $channel->queue_declare($queueFull, false, true, false, true, false, $arguments);
        // Переплет очереди и обменника
        $routingKey = "$queue.$endtime";
        $channel->queue_bind($queueFull, $exchangeDlx, $routingKey);
        // Публикуем сообщение
        $msg = $this->createMessage($body, ['delivery_mode' => 2]);
        $channel->basic_qos(null, 1, null);
        $channel->basic_publish($msg, $exchangeDlx, $routingKey);
        $channel->close();
    }

    /**
     * Отправка сообщения с получением результата
     * @link http://www.rabbitmq.com/tutorials/tutorial-six-php.html Remote procedure call (RPC)
     * @param string $routingKey
     * @param string $body
     * @param int $priority
     * @param int $ttl
     * @return string
     */
    public function addRpcMessage($routingKey, $body, $priority = self::PRIORITY_NORMAL, $ttl = self::MESSAGE_TTL)
    {
        $channel = $this->connection->channel();
        $channel->basic_qos(null, 1, null);
        $arguments               = $this->createTable(["x-max-priority" => self::PRIORITY_HIGH]);
        list($callbackQueueName) = $channel->queue_declare("", false, true, true, true, false, $arguments);
        $response                = null;
        $correlationId           = $this->createUniqid();
        $callback                = function ($msg) use ($channel, &$response, $correlationId) {
            if ($msg->get('correlation_id') == $correlationId) {
                $response = $msg->body;
                // Перестаём слушать канал, так как ответ получен
                $channel->basic_cancel($msg->delivery_info['consumer_tag']);
            }
        };
        $channel->basic_consume(
            $callbackQueueName,
            '',
            false,
            false,
            false,
            false,
            $callback
        );
        $msg = $this->createMessage(
            $body,
            [
                'correlation_id' => $correlationId,
                'reply_to'       => $callbackQueueName,
                'priority'       => $priority,
                'expiration'     => 1000 * (int) $ttl
            ]
        );
        $channel->basic_publish($msg, $this->exchangePrefix . 'rpc', $routingKey);
        while ($response === null) {
            try {
                $channel->wait(null, false, $ttl);
            } catch (AMQPTimeoutException $e) {
                $channel->close();
                throw new TimeoutException(
                    __FUNCTION__ . " превышено максимальное время"
                    . " ($ttl сек.) выполнения RPC-задачи."
                );
            }
        }
        $channel->close();
        return json_decode($response, true);
    }

    /**
     * Добавление rpc-сообщения в канал
     * @param object $channel
     * @param string $routingKey
     * @param string $body
     * @param callback $outerCallback
     * @param int $priority
     * @param int $ttl
     * @return void
     */
    public function appendRpcMessage($channel, $routingKey, $body, $outerCallback, $priority = self::PRIORITY_NORMAL, $ttl = self::MESSAGE_TTL, $exchange = '')
    {
        $correlationId = $this->createUniqid();
        $callback = function ($msg) use ($channel, $correlationId, $outerCallback) {
            if ($msg->get('correlation_id') == $correlationId) {
                // Выполняем внешний callback
                $outerCallback(json_decode($msg->body, true));
                // Перестаём слушать канал, так как ответ получен
                $channel->basic_cancel($msg->delivery_info['consumer_tag']);
            }
        };
        $arguments = $this->createTable(["x-max-priority" => self::PRIORITY_HIGH]);
        list($callbackQueueName) = $channel->queue_declare(
            '',
            false,
            true,
            true,
            true,
            false,
            $arguments
        );
        $channel->basic_consume(
            $callbackQueueName,
            '',
            false,
            false,
            false,
            false,
            $callback
        );
        $msg = $this->createMessage(
            $body,
            [
                'correlation_id' => $correlationId,
                'reply_to'       => $callbackQueueName,
                'priority'       => $priority,
                'expiration'     => 1000 * (int) $ttl
            ]
        );
        if (!$exchange) {
            $exchange = $this->exchangePrefix . 'rpc';
        }
        $channel->basic_publish($msg, $exchange, $routingKey);
    }

    /**
     * Ожидание выполнения всех добавленных rpc-сообщений в канале
     * @param object $channel
     * @param int $ttl
     * @return void
     */
    public function waitRpcCallbacks($channel, $ttl = self::MESSAGE_TTL)
    {
        $startTime = time();
        // Ждём выполнения всех задач
        while (count($channel->callbacks)) {
            $timeLeft = $startTime + $ttl - time();
            if ($timeLeft <= 0) {
                break;
            }
            try {
                $channel->wait(null, false, $timeLeft);
            } catch (AMQPTimeoutException $e) {
                break;
            }
        }
        $tasksLeft = count($channel->callbacks);
        $channel->close();
        if ($tasksLeft > 0) {
            throw new TimeoutException(
                __FUNCTION__ . " превышено максимальное время ($ttl сек.)"
                . " параллельного выполнения RPC-задач."
                . " Не выполнено: " . $tasksLeft . " шт."
            );
        }
    }
    
    /**
     * Блокирующее выполнение задачи в произвольной очереди
     * @param string $queueName
     * @param array $messageBody
     * @param integer $priority
     * @param int $timeLimit
     * @return mix
     * @throws \Exception
     */
    public function doTask($queueName, $messageBody, $priority = self::PRIORITY_NORMAL, $timeLimit = self::MESSAGE_TTL)
    {
        $channel = $this->connection->channel();
        $channel->basic_qos(null, 1, null);
        $arguments   = $this->createTable([
            "x-message-ttl"  => self::MESSAGE_TTL * 1000,
            "x-max-priority" => self::PRIORITY_MAX
        ]);
        list($callbackQueueName) = $channel->queue_declare("", false, true, true, true, false, $arguments);
        $response                = null;
        $correlationId           = $this->createUniqid();
        $callback                = function ($msg) use ($channel, &$response, $correlationId) {
            if ($msg->get('correlation_id') == $correlationId) {
                $response = $msg->body;
                // Перестаём слушать канал, так как ответ получен
                $channel->basic_cancel($msg->delivery_info['consumer_tag']);
            }
        };
        $channel->basic_consume(
            $callbackQueueName,
            '',
            false,
            false,
            false,
            false,
            $callback
        );

        $msg = $this->createMessage(
            $messageBody,
            [
                'correlation_id' => $correlationId,
                'reply_to'       => $callbackQueueName,
                'priority'       => $priority
            ]
        );

        $queueName = $this->exchangePrefix . $queueName;
        $channel->queue_declare($queueName, false, false, false, false);
        $channel->basic_publish($msg, '', $queueName);
        while ($response === null) {
            try {
                $channel->wait(null, false, $timeLimit);
            } catch (\PhpAmqpLib\Exception\AMQPTimeoutException $e) {
                $channel->close();
                throw new \Exception(__FUNCTION__ . " превышено максимальное время ($timeLimit сек.) выполнения задачи.");
            }
        }
        $channel->close();
        return json_decode($response, true);
    }
}
