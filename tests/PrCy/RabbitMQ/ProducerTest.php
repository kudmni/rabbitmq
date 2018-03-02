<?php

namespace PrCY\RabbitMQ;

use \PhpAmqpLib\Exception\AMQPTimeoutException;

/**
 * Class ProducerTest
 * @package PrCy\RabbitMQ
 */
class ProducerTest extends \PHPUnit_Framework_TestCase
{
    protected function createProducerMock($methods = [])
    {
        return $this->getMockBuilder('\PrCy\RabbitMQ\Producer')
            ->disableOriginalConstructor()
            ->setMethods($methods)
            ->getMock();
    }

    public function testCreateChannel()
    {
        $exchangeName = 'exchangeNameMock';
        $channelMock = $this->getMockBuilder('\PhpAmqpLib\Channel')
            ->disableOriginalConstructor()
            ->setMethods(['basic_qos', 'exchange_declare'])
            ->getMock();
        $channelMock->expects($this->once())
            ->method('basic_qos');
        $channelMock->expects($this->once())
            ->method('exchange_declare');

        $connectionMock = $this->getMockBuilder('\PhpAmqpLib\Connection\AMQPConnection')
            ->disableOriginalConstructor()
            ->getMock();
        $connectionMock->expects($this->once())
            ->method('channel')
            ->willReturn($channelMock);

        $producer = $this->createProducerMock(['createConnection']);
        $producer->expects($this->once())
            ->method('createConnection')
            ->willReturn($connectionMock);

        $producer->__construct();
        $channel = $producer->createChannel(1, $exchangeName);
        $this->assertEquals($channelMock, $channel);
    }

    public function testCreateBgChannel()
    {
        $prefetchCount = 123;

        $channelMock = $this->getMockBuilder('\PhpAmqpLib\Channel')
            ->disableOriginalConstructor()
            ->getMock();

        $producer = $this->createProducerMock(['createChannel']);
        $producer->expects($this->once())
            ->method('createChannel')
            ->with(
                $this->equalTo($prefetchCount),
                $this->equalTo('bg')
            )
            ->willReturn($channelMock);

        $channel = $producer->createBgChannel($prefetchCount);
        $this->assertEquals($channelMock, $channel);
    }

    public function testCreateAckChannel()
    {
        $prefetchCount = 123;

        $channelMock = $this->getMockBuilder('\PhpAmqpLib\Channel')
            ->disableOriginalConstructor()
            ->getMock();

        $producer = $this->createProducerMock(['createChannel']);
        $producer->expects($this->once())
            ->method('createChannel')
            ->with(
                $this->equalTo($prefetchCount),
                $this->equalTo('ack')
            )
            ->willReturn($channelMock);

        $channel = $producer->createAckChannel($prefetchCount);
        $this->assertEquals($channelMock, $channel);
    }

    public function testCreateRpcChannel()
    {
        $prefetchCount = 123;

        $channelMock = $this->getMockBuilder('\PhpAmqpLib\Channel')
            ->disableOriginalConstructor()
            ->getMock();

        $producer = $this->createProducerMock(['createChannel']);
        $producer->expects($this->once())
            ->method('createChannel')
            ->with(
                $this->equalTo($prefetchCount),
                $this->equalTo('rpc')
            )
            ->willReturn($channelMock);

        $channel = $producer->createRpcChannel($prefetchCount);
        $this->assertEquals($channelMock, $channel);
    }

    public function testCreateCallbackQueue()
    {
        $queueName = 'queueNameMock';

        $channelMock = $this->getMockBuilder('\PhpAmqpLib\Channel')
            ->disableOriginalConstructor()
            ->setMethods(['queue_declare'])
            ->getMock();
        $channelMock->expects($this->once())
            ->method('queue_declare')
            ->willReturn([$queueName]);

        $argumentsMock = $this->getMock('\PhpAmqpLib\Wire\AMQPTable');

        $producer = $this->createProducerMock(['createChannel', 'createAMQPTable']);
        $producer->expects($this->once())
            ->method('createChannel')
            ->willReturn($channelMock);
        $producer->expects($this->once())
            ->method('createAMQPTable')
            ->willReturn($argumentsMock);

        list($resultQueueName) = $producer->createCallbackQueue($queueName);
        $this->assertEquals($queueName, $resultQueueName);
    }

    public function testAddMessage()
    {
        $routingKey = 'routingKeyMock';
        $body       = 'bodyMock';

        $channelMock = $this->getMockBuilder('\PhpAmqpLib\Channel')
            ->disableOriginalConstructor()
            ->setMethods(['basic_publish', 'close'])
            ->getMock();
        $channelMock->expects($this->once())
            ->method('basic_publish');
        $channelMock->expects($this->once())
            ->method('close');

        $messageMock = $this->getMock('\PhpAmqpLib\Message\AMQPMessage');

        $producer = $this->createProducerMock(['createBgChannel', 'createAMQPMessage']);
        $producer->expects($this->once())
            ->method('createBgChannel')
            ->willReturn($channelMock);
        $producer->expects($this->once())
            ->method('createAMQPMessage')
            ->willReturn($messageMock);
        $producer->addMessage($routingKey, $body);
    }

    public function testAddAckMessage()
    {
        $routingKey = 'routingKeyMock';
        $body       = 'bodyMock';

        $channelMock = $this->getMockBuilder('\PhpAmqpLib\Channel')
            ->disableOriginalConstructor()
            ->setMethods(['close'])
            ->getMock();
        $channelMock->expects($this->once())
            ->method('close');

        $producer = $this->createProducerMock(['createAckChannel', 'appendAckMessage']);
        $producer->expects($this->once())
            ->method('createAckChannel')
            ->willReturn($channelMock);
        $producer->expects($this->once())
            ->method('appendAckMessage');
        $producer->addAckMessage($routingKey, $body);
    }

    public function testAppendAckMessage()
    {
        $channelMock = $this->getMock('\PhpAmqpLib\Channel');
        $routingKey  = 'routingKeyMock';
        $body        = 'bodyMock';

        $producer = $this->createProducerMock(['publishAckMessage']);
        $producer->expects($this->once())
            ->method('publishAckMessage');
        $producer->appendAckMessage($channelMock, $routingKey, $body);
    }

    public function testPublishAckMessage()
    {
        $exchange    = 'exchangeMock';
        $routingKey  = 'routingKeyMock';
        $body        = 'bodyMock';
        $messageMock = $this->getMock('\PhpAmqpLib\Message\AMQPMessage');

        $channelMock = $this->getMockBuilder('\PhpAmqpLib\Channel')
            ->disableOriginalConstructor()
            ->setMethods(['basic_publish'])
            ->getMock();
        $channelMock->expects($this->once())
            ->method('basic_publish');

        $producer = $this->createProducerMock(['createAMQPMessage']);
        $producer->expects($this->once())
            ->method('createAMQPMessage')
            ->willReturn($messageMock);
        $producer->publishAckMessage($channelMock, $exchange, $routingKey, $body);
    }

    public function testAddRpcMessageSuccess()
    {
        $routingKey   = 'routingKeyMock';
        $body         = 'bodyMock';
        $callbackMock = null;
        $result       = 123;

        $channelMock = $this->getMockBuilder('\PhpAmqpLib\Channel')
            ->disableOriginalConstructor()
            ->setMethods(['close', 'wait'])
            ->getMock();
        $channelMock->expects($this->once())
            ->method('close');
        $channelMock->expects($this->once())
            ->method('wait')
            ->with($this->callback(function () use (&$callbackMock, $result) {
                $callbackMock($result);
                return true;
            }));

        $producer = $this->createProducerMock(['createRpcChannel', 'appendRpcMessage']);
        $producer->expects($this->once())
            ->method('createRpcChannel')
            ->willReturn($channelMock);
        $producer->expects($this->once())
            ->method('appendRpcMessage')
            ->with(
                $this->equalTo($channelMock),
                $this->equalTo($routingKey),
                $this->equalTo($body),
                $this->callback(function ($rpcCallback) use (&$callbackMock) {
                    $callbackMock = $rpcCallback;
                    return true;
                })
            );
        $producer->addRpcMessage($routingKey, $body);
    }

    /**
     * @expectedException \PrCy\RabbitMQ\Exception\TimeoutException
     */
    public function testAddRpcMessageTimeout()
    {
        $routingKey  = 'routingKeyMock';
        $body        = 'bodyMock';

        $channelMock = $this->getMockBuilder('\PhpAmqpLib\Channel')
            ->disableOriginalConstructor()
            ->setMethods(['close', 'wait'])
            ->getMock();
        $channelMock->expects($this->once())
            ->method('close');
        $channelMock->expects($this->once())
            ->method('wait')
            ->willThrowException(new AMQPTimeoutException('Exception message mock'));

        $producer = $this->createProducerMock(['createRpcChannel', 'appendRpcMessage']);
        $producer->expects($this->once())
            ->method('createRpcChannel')
            ->willReturn($channelMock);
        $producer->expects($this->once())
            ->method('appendRpcMessage');
        $producer->addRpcMessage($routingKey, $body);
    }

    public function testAppendRpcMessage()
    {
        $routingKey        = 'routingKeyMock';
        $body              = 'bodyMock';
        $correlationId     = 'correlationIdMock';
        $deliveryTag       = 'deliveryTagMock';
        $consumerTag       = 'consumerTagMock';
        $callbackQueueName = 'callbackQueueNameMock';
        $argumentsMock     = $this->getMock('\PhpAmqpLib\Wire\AMQPTable');
        $callbackMock      = null;
        $expectedResult    = 123;
        $actualResult      = null;
        $outerCallback     = function($data) use (&$actualResult) {
            $actualResult = $data;
        };

        $channelMock = $this->getMockBuilder('\PhpAmqpLib\Channel')
            ->disableOriginalConstructor()
            ->setMethods(['queue_declare', 'basic_consume', 'basic_ack', 'basic_cancel'])
            ->getMock();
        $channelMock->expects($this->once())
            ->method('queue_declare')
            ->willReturn([$callbackQueueName]);
        $channelMock->expects($this->once())
            ->method('basic_consume')
            ->with(
                $this->equalTo($callbackQueueName),
                $this->equalTo(''),
                $this->equalTo(false),
                $this->equalTo(false),
                $this->equalTo(false),
                $this->equalTo(false),
                $this->callback(function ($callback) use (&$callbackMock) {
                    $callbackMock = $callback;
                    return true;
                })
            );
        $channelMock->expects($this->once())
            ->method('basic_ack')
            ->with($this->equalTo($deliveryTag));
        $channelMock->expects($this->once())
            ->method('basic_cancel')
            ->with($this->equalTo($consumerTag));

        $messageMock = $this->getMockBuilder('\PhpAmqpLib\Message\AMQPMessage')
            ->disableOriginalConstructor()
            ->setMethods(['get'])
            ->getMock();
        $messageMock->body = json_encode($expectedResult);
        $messageMock->delivery_info = [
            'channel'      => $channelMock,
            'delivery_tag' => $deliveryTag,
            'consumer_tag' => $consumerTag,
        ];
        $messageMock->expects($this->once())
            ->method('get')
            ->with($this->equalTo('correlation_id'))
            ->willReturn($correlationId);

        $producer = $this->createProducerMock(['createUniqid', 'createAMQPTable', 'publishRpcMessage']);
        $producer->expects($this->once())
            ->method('createUniqid')
            ->willReturn($correlationId);
        $producer->expects($this->once())
            ->method('createAMQPTable')
            ->willReturn($argumentsMock);
        $producer->expects($this->once())
            ->method('publishRpcMessage');
        $producer->appendRpcMessage($channelMock, $routingKey, $body, $outerCallback);

        $callbackMock($messageMock);
        $this->assertEquals($expectedResult, $actualResult);
    }

    public function testPublishRpcMessage()
    {
        $exchange      = 'exchangeMock';
        $routingKey    = 'routingKeyMock';
        $body          = 'bodyMock';
        $correlationId = 'correlationIdMock';
        $replyTo       = 'replyToMock';
        $messageMock   = $this->getMock('\PhpAmqpLib\Message\AMQPMessage');

        $channelMock = $this->getMockBuilder('\PhpAmqpLib\Channel')
            ->disableOriginalConstructor()
            ->setMethods(['basic_publish'])
            ->getMock();
        $channelMock->expects($this->once())
            ->method('basic_publish');

        $producer = $this->createProducerMock(['createAMQPMessage']);
        $producer->expects($this->once())
            ->method('createAMQPMessage')
            ->willReturn($messageMock);
        $producer->publishRpcMessage($channelMock, $exchange, $routingKey, $body, $correlationId, $replyTo);
    }

    public function testWaitRpcCallbacksSuccess()
    {
        $channelMock = $this->getMockBuilder('\PhpAmqpLib\Channel')
            ->disableOriginalConstructor()
            ->setMethods(['close'])
            ->getMock();
        $channelMock->expects($this->once())
            ->method('close');
        $channelMock->callbacks = [];

        $producer = $this->createProducerMock(['createConnection']);
        $producer->waitRpcCallbacks($channelMock, 0);
    }

    /**
     * @expectedException \PrCy\RabbitMQ\Exception\TimeoutException
     */
    public function testWaitRpcCallbacksInternalTimeout()
    {
        $callbackMock = [$this, 'testWaitRpcCallbacks'];

        $channelMock = $this->getMockBuilder('\PhpAmqpLib\Channel')
            ->disableOriginalConstructor()
            ->setMethods(['close'])
            ->getMock();
        $channelMock->expects($this->once())
            ->method('close');
        $channelMock->callbacks = [$callbackMock];

        $producer = $this->createProducerMock(['createConnection']);
        $producer->waitRpcCallbacks($channelMock, 0);
    }

    /**
     * @expectedException \PrCy\RabbitMQ\Exception\TimeoutException
     */
    public function testWaitRpcCallbacksAMQPTimeout()
    {
        $callbackMock = [$this, 'testWaitRpcCallbacks'];

        $channelMock = $this->getMockBuilder('\PhpAmqpLib\Channel')
            ->disableOriginalConstructor()
            ->setMethods(['close', 'wait'])
            ->getMock();
        $channelMock->expects($this->once())
            ->method('close');
        $channelMock->expects($this->once())
            ->method('wait')
            ->willThrowException(new AMQPTimeoutException());
        $channelMock->callbacks = [$callbackMock];

        $producer = $this->createProducerMock(['createConnection']);
        $producer->waitRpcCallbacks($channelMock);
    }

    public function testAddDelayedMessage()
    {
        $exchange      = 'exchangeMock';
        $routingKey    = 'routingKeyMock';
        $body          = 'bodyMock';
        $startTime     = strtotime('+30 minutes');
        $argumentsMock = $this->getMock('\PhpAmqpLib\Wire\AMQPTable');
        $messageMock   = $this->getMock('\PhpAmqpLib\Message\AMQPMessage');

        $channelMock = $this->getMockBuilder('\PhpAmqpLib\Channel')
            ->disableOriginalConstructor()
            ->setMethods(['queue_declare', 'basic_publish', 'close'])
            ->getMock();
        $channelMock->expects($this->once())
            ->method('queue_declare');
        $channelMock->expects($this->once())
            ->method('basic_publish');
        $channelMock->expects($this->once())
            ->method('close');

        $producer = $this->createProducerMock(['createChannel', 'createAMQPTable', 'createAMQPMessage']);
        $producer->expects($this->once())
            ->method('createChannel')
            ->willReturn($channelMock);
        $producer->expects($this->once())
            ->method('createAMQPTable')
            ->willReturn($argumentsMock);
        $producer->expects($this->once())
            ->method('createAMQPMessage')
            ->willReturn($messageMock);
        $producer->addDelayedMessage($exchange, $routingKey, $body, $startTime);
    }
}
