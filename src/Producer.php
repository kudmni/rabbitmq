<?php

namespace PrCy\RabbitMQ;

use \PhpAmqpLib\Channel\AMQPChannel;
use \PhpAmqpLib\Connection\AMQPConnection;
use \PhpAmqpLib\Exception\AMQPTimeoutException;
use \PhpAmqpLib\Message\AMQPMessage;
use \PhpAmqpLib\Wire\AMQPTable;
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
    protected $appPrefix;

    /**
     * Конструктор класса
     * @param string $host
     * @param int    $port
     * @param string $user
     * @param string $password
     * @param string $messagePrefix    Используется для формирования ID сообщений
     * @param string $appPrefix        Используется для формирования уникального названия обменника, очереди; полезно для бесконфликтного сосуществования нескольких проектов на одном RabbitMQ-сервере
     */
    public function __construct($host = '127.0.0.1', $port = 5672, $user = 'guest', $password = 'guest', $messagePrefix = '', $appPrefix = '')
    {
        $this->connection    = $this->createConnection($host, $port, $user, $password);
        $this->messagePrefix = $messagePrefix;
        $this->appPrefix     = $appPrefix;
    }

    /**
     * Создаёт экземпляр соединения с RabbitMQ
     * @param string $host
     * @param int    $port
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
     * Создаёт экземпляр таблицы параметров сообщения
     * @param array $data
     * @return AMQPTable
     * @codeCoverageIgnore
     */
    protected function createAMQPTable($data)
    {
        return new AMQPTable($data);
    }

    /**
     * Создаёт экземпляр сообщения
     * @param mixed $body
     * @param array $options
     * @return AMQPMessage
     * @codeCoverageIgnore
     */
    protected function createAMQPMessage($body, $options)
    {
        return new AMQPMessage(json_encode($body), $options);
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
     * Создаёт канал для отправки сообщений и обменник, если необходимо
     * @param int    $prefetchCount
     * @param string $exchange
     * @return AMQPChannel
     */
    public function createChannel($prefetchCount = 1, $exchange = null)
    {
        $channel = $this->connection->channel();
        $channel->basic_qos(0, $prefetchCount, false);
        if ($exchange) {
            $channel->exchange_declare($exchange, 'topic', false, false, false);
        }
        return $channel;
    }

    /**
     * Создаёт канал и обменник для отправки bg-сообщений
     * @param int $prefetchCount
     * @return AMQPChannel
     */
    public function createBgChannel($prefetchCount = 1)
    {
        return $this->createChannel($prefetchCount, $this->appPrefix . 'bg');
    }

    /**
     * Создаёт канал и обменник для отправки ack-сообщений
     * @param int $prefetchCount
     * @return AMQPChannel
     */
    public function createAckChannel($prefetchCount = 1)
    {
        return $this->createChannel($prefetchCount, $this->appPrefix . 'ack');
    }

    /**
     * Создаёт канал и обменник для отправки rpc-сообщений
     * @param int $prefetchCount
     * @return AMQPChannel
     */
    public function createRpcChannel($prefetchCount = 1)
    {
        return $this->createChannel($prefetchCount, $this->appPrefix . 'rpc');
    }

    /**
     * Отправка сообщения без подтверждения (bg)
     * @link http://www.rabbitmq.com/tutorials/tutorial-one-php.html Simple message
     * @param string $routingKey
     * @param string $body
     * @param int    $priority
     * @param int    $ttl
     * @return void
     */
    public function addMessage($routingKey, $body, $priority = self::PRIORITY_NORMAL, $ttl = self::MESSAGE_TTL)
    {
        $exchange = $this->appPrefix . 'bg';
        $channel  = $this->createBgChannel();
        $msg      = $this->createAMQPMessage(
            $body,
            [
                'delivery_mode' => 2,
                'priority'      => $priority,
                'expiration'    => 1000 * (int) $ttl
            ]
        );
        $channel->basic_publish($msg, $exchange, $routingKey);
        $channel->close();
    }

    /**
     * Отправка сообщения с подтверждением (ack)
     * @link http://www.rabbitmq.com/tutorials/tutorial-two-php.html Message acknowledgment
     * @param string $routingKey
     * @param string $body
     * @param int    $priority
     * @param int    $ttl
     * @return void
     */
    public function addAckMessage($routingKey, $body, $priority = self::PRIORITY_NORMAL, $ttl = self::MESSAGE_TTL)
    {
        $channel = $this->createAckChannel();
        $this->appendAckMessage($channel, $routingKey, $body, $priority, $ttl);
        $channel->close();
    }

    /**
     * Добавление ack-сообщения в существующий канал
     * @param AMQPChannel $channel
     * @param string      $routingKey
     * @param string      $body
     * @param int         $priority
     * @param int         $ttl
     * @return void
     */
    public function appendAckMessage($channel, $routingKey, $body, $priority = self::PRIORITY_NORMAL, $ttl = self::MESSAGE_TTL)
    {
        $exchange = $this->appPrefix . 'ack';
        $this->publishAckMessage($channel, $exchange, $routingKey, $body, $priority, $ttl);
    }

    /**
     * Создает и отправляет ack-сообщение
     * @param AMQPChannel $channel
     * @param string      $exchange
     * @param string      $routingKey
     * @param string      $body
     * @param int         $priority
     * @param int         $ttl
     * @return void
     */
    public function publishAckMessage($channel, $exchange, $routingKey, $body, $priority = self::PRIORITY_NORMAL, $ttl = self::MESSAGE_TTL)
    {
        $msg = $this->createAMQPMessage(
            $body,
            [
                'delivery_mode'  => 2,
                'priority'       => $priority,
                'expiration'     => 1000 * (int) $ttl
            ]
        );
        $channel->basic_publish($msg, $exchange, $routingKey);
    }

    /**
     * Отправка сообщения с получением результата (rpc)
     * @link http://www.rabbitmq.com/tutorials/tutorial-six-php.html Remote procedure call (RPC)
     * @param string $routingKey
     * @param string $body
     * @param int    $priority
     * @param int    $ttl
     * @return mixed Результат
     * @throws TimeoutException
     */
    public function addRpcMessage($routingKey, $body, $priority = self::PRIORITY_NORMAL, $ttl = self::MESSAGE_TTL)
    {
        $channel  = $this->createRpcChannel();
        $response = null;
        $callback = function ($result) use (&$response) {
            $response = $result;
        };
        $this->appendRpcMessage($channel, $routingKey, $body, $callback, $priority, $ttl);
        while (is_null($response)) {
            try {
                $channel->wait(null, false, $ttl);
            } catch (AMQPTimeoutException $e) {
                unset($e);
                $channel->close();
                throw new TimeoutException(
                    "Превышено максимальное время ($ttl сек.)"
                    . " выполнения RPC-задачи."
                );
            }
        }
        $channel->close();
        return $response;
    }

    /**
     * Добавление rpc-сообщения в канал
     * @param AMQPChannel $channel
     * @param string      $routingKey
     * @param string      $body
     * @param callback    $outerCallback
     * @param int         $priority
     * @param int         $ttl
     * @return void
     */
    public function appendRpcMessage($channel, $routingKey, $body, $outerCallback, $priority = self::PRIORITY_NORMAL, $ttl = self::MESSAGE_TTL)
    {
        $exchange = $this->appPrefix . 'rpc';
        $correlationId = $this->createUniqid();
        $callback = function ($msg) use ($correlationId, $outerCallback) {
            if ($msg->get('correlation_id') == $correlationId) {
                // Выполняем внешний callback
                $outerCallback(json_decode($msg->body, true));
                // Подтверждаем обработку результата
                $msg->delivery_info['channel']->basic_ack($msg->delivery_info['delivery_tag']);
                // Перестаём слушать канал, так как ответ получен
                $msg->delivery_info['channel']->basic_cancel($msg->delivery_info['consumer_tag']);
            }
        };
        $arguments = $this->createAMQPTable(['x-max-priority' => self::PRIORITY_HIGH]);
        list($callbackQueueName) = $channel->queue_declare('', false, true, true, true, false, $arguments);
        $channel->basic_consume($callbackQueueName, '', false, false, false, false, $callback);
        $this->publishRpcMessage($channel, $exchange, $routingKey, $body, $correlationId, $callbackQueueName, $priority, $ttl);
    }

    /**
     * Создает и отправляет rpc-сообщение
     * @param AMQPChannel $channel
     * @param string      $exchange
     * @param string      $routingKey
     * @param string      $body
     * @param string      $correlationId
     * @param string      $replyTo
     * @param int         $priority
     * @param int         $ttl
     * @return void
     */
    public function publishRpcMessage($channel, $exchange, $routingKey, $body, $correlationId, $replyTo, $priority = self::PRIORITY_NORMAL, $ttl = self::MESSAGE_TTL)
    {
        $msg = $this->createAMQPMessage(
            $body,
            [
                'delivery_mode'  => 2,
                'correlation_id' => $correlationId,
                'reply_to'       => $replyTo,
                'priority'       => $priority,
                'expiration'     => 1000 * (int) $ttl
            ]
        );
        $channel->basic_publish($msg, $exchange, $routingKey);
    }

    /**
     * Ожидание выполнения всех добавленных rpc-сообщений в канале
     * @param AMQPChannel $channel
     * @param int         $ttl
     * @return void
     * @throws TimeoutException
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
                unset($e);
                break;
            }
        }
        $tasksLeft = count($channel->callbacks);
        $channel->close();
        if ($tasksLeft > 0) {
            throw new TimeoutException(
                "Превышено максимальное время ($ttl сек.)"
                . " параллельного выполнения RPC-задач."
                . " Не выполнено: $tasksLeft шт."
            );
        }
    }

    /**
     * Создает очередь для отложенной обработки результатов RPC-задач
     * @param AMQPChannel $channel
     * @param string $routingKey
     * @param callback $outerCallback
     * @param bool $noAck
     * @return string Название созданной очереди
     */
    public function createCallbackQueue($channel, $routingKey, $outerCallback, $noAck = false) {
        $callback = function ($msg) use ($noAck, $outerCallback) {
            $outerCallback(json_decode($msg->body, true), $msg->get('correlation_id'));
            if (!$noAck) {
                $msg->delivery_info['channel']->basic_ack($msg->delivery_info['delivery_tag']);
            }
        };
        $exchangeName = $this->appPrefix . 'callback';
        $channel->exchange_declare($exchangeName, 'topic', false, false, false);
        $queueName = $this->appPrefix . 'callback_queue_' . $routingKey;
        $arguments = $this->createAMQPTable([
            'x-max-priority'         => self::PRIORITY_MAX,
            'x-message-ttl'          => self::MESSAGE_TTL * 1000
        ]);
        $channel->queue_declare($queueName, false, true, false, false, false, $arguments);
        $channel->queue_bind($queueName, $exchangeName, $routingKey);
        $channel->basic_consume($queueName, '', false, $noAck, false, false, $callback);
        return $queueName;
    }

    /**
     * Создаёт очередь для отложенного выполнения задач
     * @param string $exchange
     * @param string $routingKey
     * @param int $delay
     * @return string Название созданной очереди
     */
    public function createDelayedQueue($exchange, $routingKey, $delay) {
        $channel = $this->createChannel();
        $channel->exchange_declare($exchange, 'topic', false, false, false);
        $queueName = $this->appPrefix . "delayed_queue_$routingKey.$delay";
        $arguments = $this->createAMQPTable([
            'x-dead-letter-exchange'    => $exchange,
            'x-dead-letter-routing-key' => $routingKey,
            'x-max-priority'            => self::PRIORITY_MAX,
            'x-message-ttl'             => $delay * 1000,
        ]);
        $channel->queue_declare($queueName, false, true, false, false, false, $arguments);
        $channel->close();
        return $queueName;
    }

    /**
     * Отправка сообщения с подтверждением в отложенную очередь
     * @link https://www.rabbitmq.com/dlx.html Dead Letter Exchanges
     * @param string $exchange
     * @param string $routingKey
     * @param string $body
     * @param int    $startTime
     * @param int    $priority
     * @return void
     */
    public function addDelayedMessage($exchange, $routingKey, $body, $startTime, $priority = self::PRIORITY_NORMAL)
    {
        $channel   = $this->createChannel();
        $delay     = $startTime - time();
        $arguments = $this->createAMQPTable([
            'x-dead-letter-exchange'    => $exchange,
            'x-dead-letter-routing-key' => $routingKey,
            'x-max-priority'            => self::PRIORITY_MAX,
            'x-message-ttl'             => $delay * 1000,
            'x-expires'                 => ($delay + 60) * 1000
        ]);
        list($queue) = $channel->queue_declare('', false, true, false, true, false, $arguments);
        $msg = $this->createAMQPMessage($body, ['delivery_mode' => 2, 'priority' => $priority]);
        $channel->basic_publish($msg, '', $queue);
        $channel->close();
    }
}
