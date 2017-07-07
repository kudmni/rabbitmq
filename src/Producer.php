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

    const MESSAGE_TTL        = 300; // TTL сообщения в очереди, сек

    protected $connection;
    protected $prefix;

    /**
     * Конструктор класса
     * @param string $host
     * @param integer $port
     * @param string $user
     * @param string $password
     * @param string $prefix    Используется для формирования ID сообщений
     */
    public function __construct($host = '127.0.0.1', $port = 5672, $user = 'guest', $password = 'guest', $prefix = '')
    {
        $this->connection = $this->createConnection($host, $port, $user, $password);
        $this->prefix = $prefix;
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
        return uniqid($this->prefix, true);
    }

    /**
     * Отправка сообщения без подтверждения (отправили и забыли)
     * @link http://www.rabbitmq.com/tutorials/tutorial-one-php.html Simple message
     * @param string $routingKey
     * @param string $messageBody
     * @return void
     */
    public function addMessage($routingKey, $messageBody)
    {
        $channel = $this->connection->channel();
        $channel->basic_qos(null, 1, null);
        $msg = $this->createMessage(
            $messageBody,
            ['delivery_mode' => 2]
        );
        $channel->basic_publish($msg, 'bg', $routingKey);
        $channel->close();
    }

    /**
     * Отправка сообщения с подтверждением
     * @link http://www.rabbitmq.com/tutorials/tutorial-two-php.html Message acknowledgment
     * @param string $routingKey
     * @param string $messageBody
     * @param int $priority
     * @return void
     */
    public function addAckMessage($routingKey, $messageBody, $priority = self::PRIORITY_NORMAL)
    {
        $channel = $this->connection->channel();
        $channel->basic_qos(null, 1, null);
        $correlationId = $this->createUniqid();
        $msg = $this->createMessage(
            $messageBody,
            [
                'delivery_mode'  => 2,
                'correlation_id' => $correlationId,
                'priority'       => $priority
            ]
        );
        $channel->basic_publish($msg, 'ack', $routingKey);
        $channel->close();
    }

    /**
     * Создание канала для отправки сообщений
     * @return object channel
     */
    public function createChannel()
    {
        $channel = $this->connection->channel();
        $channel->basic_qos(null, 1, null);
        return $channel;
    }

    /**
     * Добавление ack-сообщения в канал
     * @param object $channel
     * @param string $routingKey
     * @param string $messageBody
     * @param int $priority
     * @return void
     */
    public function appendAckMessage($channel, $routingKey, $messageBody, $priority = self::PRIORITY_NORMAL)
    {
        $msg = $this->createMessage(
            $messageBody,
            [
                'delivery_mode'  => 2,
                'correlation_id' => $this->createUniqid(),
                'priority'       => $priority
            ]
        );
        $channel->basic_publish($msg, 'ack', $routingKey);
    }

    /**
     * Отправка сообщения с подтверждением в отложенную очередь
     * @link https://www.rabbitmq.com/dlx.html Dead Letter Exchanges
     * @param string $queueName
     * @param string $exchangeName
     * @param string $messageBody
     * @param int    $delay
     * @param int    $time
     */
    public function addDelayedMessage($queueName, $exchangeName, $messageBody, $delay, $time)
    {
        $channel = $this->connection->channel();
        // Обменник
        $channel->exchange_declare($exchangeName, 'topic', false, false, false);
        // Обменник dlx
        $exchangeDlx = $exchangeName . ".dlx";
        $channel->exchange_declare($exchangeDlx, 'topic', false, false, false);
        // Очередь dlx
        $endtime     = $time + $delay * 1000;
        $queueFull   = "$exchangeName.$queueName.$endtime";
        $arguments   = $this->createTable([
            "x-dead-letter-exchange"    => $exchangeName,
            "x-message-ttl"             => $delay * 1000,
            "x-expires"                 => $delay * 1000 + 10000
        ]);
        $channel->queue_declare($queueFull, false, true, false, true, false, $arguments);
        // Переплет очереди и обменника
        $routingKey = "$queueName.$endtime";
        $channel->queue_bind($queueFull, $exchangeDlx, $routingKey);
        // Публикуем сообщение
        $msg = $this->createMessage($messageBody, ['delivery_mode' => 2]);
        $channel->basic_qos(null, 1, null);
        $channel->basic_publish($msg, $exchangeDlx, $routingKey);
        $channel->close();
    }

    /**
     * Отправка сообщения с получением результата
     * @link http://www.rabbitmq.com/tutorials/tutorial-six-php.html Remote procedure call (RPC)
     * @param string $routingKey
     * @param string $messageBody
     * @param int $priority
     * @param int $timeLimit
     * @return string
     */
    public function addRpcMessage($routingKey, $messageBody, $priority = self::PRIORITY_NORMAL, $timeLimit = self::MESSAGE_TTL)
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
            $messageBody,
            [
                'correlation_id' => $correlationId,
                'reply_to'       => $callbackQueueName,
                'priority'       => $priority
            ]
        );
        $channel->basic_publish($msg, 'rpc', $routingKey);
        while ($response === null) {
            try {
                $channel->wait(null, false, $timeLimit);
            } catch (AMQPTimeoutException $e) {
                $channel->close();
                throw new TimeoutException(
                    __FUNCTION__ . " превышено максимальное время"
                    . " ($timeLimit сек.) выполнения RPC-задачи."
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
     * @param string $messageBody
     * @param callback $outerCallback
     * @param int $priority
     * @return void
     */
    public function appendRpcMessage($channel, $routingKey, $messageBody, $outerCallback, $priority = self::PRIORITY_NORMAL)
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
            $messageBody,
            [
                'correlation_id' => $correlationId,
                'reply_to'       => $callbackQueueName,
                'priority'       => $priority
            ]
        );
        $channel->basic_publish($msg, 'rpc', $routingKey);
    }

    /**
     * Ожидание выполнения всех добавленных rpc-сообщений в канале
     * @param object $channel
     * @param int $timeLimit
     * @return void
     */
    public function waitRpcCallbacks($channel, $timeLimit = self::MESSAGE_TTL)
    {
        $startTime = time();
        // Ждём выполнения всех задач
        while (count($channel->callbacks)) {
            $timeLeft = $startTime + $timeLimit - time();
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
                __FUNCTION__ . " превышено максимальное время ($timeLimit сек.)"
                . " параллельного выполнения RPC-задач."
                . " Не выполнено: " . $tasksLeft . " шт."
            );
        }
    }
}
