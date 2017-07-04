<?php

namespace PrCY\RabbitMQ;

use PrCy\RabbitMQ\Producer;
use PhpAmqpLib\Exception\AMQPTimeoutException;

/**
 * Class ProducerTest
 * @package PrCy\RabbitMQ
 */
class ProducerTest extends \PHPUnit_Framework_TestCase {
    /**
     * @var Producer instance
     */
    protected $producer;

    public function setUp() {
        $this->producer = $this->getMockBuilder('\PrCy\RabbitMQ\Producer')
			->disableOriginalConstructor()
			->setMethods(['createConnection', 'createMessage', 'createTable', 'createUniqid'])
			->getMock();
	}

    public function testAddMessage() {
		$routingKeyMock	 = 'routing.key.mock';
		$messageBodyMock = 'messageBodyMock';
		$messageMock	 = $this->getMock('\PhpAmqpLib\Message\AMQPMessage');

		$channelMock = $this->getMockBuilder('\PhpAmqpLib\Channel')
			->disableOriginalConstructor()
			->setMethods(['basic_qos', 'basic_publish', 'close'])
			->getMock();
		$channelMock->expects($this->once())
			->method('basic_qos')
			->with(
				$this->equalTo(null),
				$this->equalTo(1),
				$this->equalTo(null)
			);
		$channelMock->expects($this->once())
			->method('basic_publish')
			->with(
				$this->equalTo($messageMock),
				$this->equalTo('bg'),
				$this->equalTo($routingKeyMock)
			);
		$channelMock->expects($this->once())
			->method('close');

		$connectionMock = $this->getMockBuilder('\PhpAmqpLib\Connection\AMQPConnection')
			->disableOriginalConstructor()
			->getMock();
		$connectionMock->expects($this->once())
			->method('channel')
			->willReturn($channelMock);

		$this->producer->expects($this->once())
			->method('createConnection')
			->willReturn($connectionMock);
		$this->producer->expects($this->once())
			->method('createMessage')
			->with(
				$this->equalTo($messageBodyMock),
				$this->equalTo(['delivery_mode' => 2])
			)
			->willReturn($messageMock);

		$this->producer->__construct();
		$this->producer->addMessage($routingKeyMock, $messageBodyMock);
    }

    public function testAddAckMessage() {
		$routingKeyMock		 = 'routing.key.mock';
		$messageBodyMock	 = 'messageBodyMock';
		$priority			 = Producer::PRIORITY_NORMAL;
		$messageMock		 = $this->getMock('\PhpAmqpLib\Message\AMQPMessage');
		$correlationIdMock	 = uniqid();

		$channelMock = $this->getMockBuilder('\PhpAmqpLib\Channel')
			->disableOriginalConstructor()
			->setMethods(['basic_qos', 'basic_publish', 'close'])
			->getMock();
		$channelMock->expects($this->once())
			->method('basic_qos')
			->with(
				$this->equalTo(null),
				$this->equalTo(1),
				$this->equalTo(null)
			);
		$channelMock->expects($this->once())
			->method('basic_publish')
			->with(
				$this->equalTo($messageMock),
				$this->equalTo('ack'),
				$this->equalTo($routingKeyMock)
			);
		$channelMock->expects($this->once())
			->method('close');

		$connectionMock = $this->getMockBuilder('\PhpAmqpLib\Connection\AMQPConnection')
			->disableOriginalConstructor()
			->getMock();
		$connectionMock->expects($this->once())
			->method('channel')
			->willReturn($channelMock);

		$this->producer->expects($this->once())
			->method('createConnection')
			->willReturn($connectionMock);
		$this->producer->expects($this->once())
			->method('createMessage')
			->with(
				$this->equalTo($messageBodyMock),
				$this->equalTo([
					'delivery_mode'	 => 2,
					'correlation_id' => $correlationIdMock,
					'priority'		 => $priority
				])
			)
			->willReturn($messageMock);
		$this->producer->expects($this->once())
			->method('createUniqid')
			->willReturn($correlationIdMock);

		$this->producer->__construct();
		$this->producer->addAckMessage($routingKeyMock, $messageBodyMock, $priority);
    }

    public function testCreateChannel() {
		$channelMock = $this->getMockBuilder('\PhpAmqpLib\Channel')
			->disableOriginalConstructor()
			->setMethods(['basic_qos'])
			->getMock();
		$channelMock->expects($this->once())
			->method('basic_qos')
			->with(
				$this->equalTo(null),
				$this->equalTo(1),
				$this->equalTo(null)
			);

		$connectionMock = $this->getMockBuilder('\PhpAmqpLib\Connection\AMQPConnection')
			->disableOriginalConstructor()
			->getMock();
		$connectionMock->expects($this->once())
			->method('channel')
			->willReturn($channelMock);

		$this->producer->expects($this->once())
			->method('createConnection')
			->willReturn($connectionMock);

		$this->producer->__construct();
		$channel = $this->producer->createChannel();
		$this->assertEquals($channelMock, $channel);
    }

    public function testAppendAckMessage() {
		$routingKeyMock		 = 'routing.key.mock';
		$messageBodyMock	 = 'messageBodyMock';
		$priority			 = Producer::PRIORITY_NORMAL;
		$correlationIdMock	 = uniqid();
		$messageMock		 = $this->getMock('\PhpAmqpLib\Message\AMQPMessage');

		$channelMock = $this->getMockBuilder('\PhpAmqpLib\Channel')
			->disableOriginalConstructor()
			->setMethods(['basic_publish'])
			->getMock();
		$channelMock->expects($this->once())
			->method('basic_publish')
			->with(
				$this->equalTo($messageMock),
				$this->equalTo('ack'),
				$this->equalTo($routingKeyMock)
			);

		$this->producer->expects($this->once())
			->method('createMessage')
			->with(
				$this->equalTo($messageBodyMock),
				$this->equalTo([
					'delivery_mode'	 => 2,
					'correlation_id' => $correlationIdMock,
					'priority'		 => $priority
				])
			)
			->willReturn($messageMock);
		$this->producer->expects($this->once())
			->method('createUniqid')
			->willReturn($correlationIdMock);

		$this->producer->__construct();
		$this->producer->appendAckMessage($channelMock, $routingKeyMock, $messageBodyMock, $priority);
    }

    public function testAddDelayedMessage() {
		$queueNameMock		 = 'queueNameMock';
		$exchangeNameMock	 = 'exchangeNameMock';
		$exchangeDlxMock	 = 'exchangeNameMock.dlx';
		$messageBodyMock	 = 'messageBodyMock';
		$delay				 = 100500;
		$time				 = time();
		$messageMock		 = $this->getMock('\PhpAmqpLib\Message\AMQPMessage');
		$argumentsMock		 = $this->getMock('\PhpAmqpLib\Wire\AMQPTable');

		$channelMock = $this->getMockBuilder('\PhpAmqpLib\Channel')
			->disableOriginalConstructor()
			->setMethods(['exchange_declare', 'queue_declare', 'queue_bind', 'basic_qos', 'basic_publish', 'close'])
			->getMock();
		$channelMock->expects($this->at(0))
			->method('exchange_declare')
			->with(
				$this->equalTo($exchangeNameMock),
				$this->equalTo('topic'),
				$this->equalTo(false),
				$this->equalTo(false),
				$this->equalTo(false));
		$channelMock->expects($this->at(1))
			->method('exchange_declare')
			->with(
				$this->equalTo($exchangeDlxMock),
				$this->equalTo('topic'),
				$this->equalTo(false),
				$this->equalTo(false),
				$this->equalTo(false)
			);
		$channelMock->expects($this->once())
			->method('queue_declare')
			->with(
				$this->stringStartsWith($exchangeNameMock),
				$this->equalTo(false),
				$this->equalTo(true),
				$this->equalTo(false),
				$this->equalTo(true),
				$this->equalTo(false),
				$this->equalTo($argumentsMock)
			);
		$channelMock->expects($this->once())
			->method('queue_bind')
			->with(
				$this->stringStartsWith($exchangeNameMock),
				$this->equalTo($exchangeDlxMock),
				$this->stringStartsWith($queueNameMock)
			);
		$channelMock->expects($this->once())
			->method('basic_qos')
			->with(
				$this->equalTo(null),
				$this->equalTo(1),
				$this->equalTo(null)
			);
		$channelMock->expects($this->once())
			->method('basic_publish')
			->with(
				$this->equalTo($messageMock),
				$this->equalTo($exchangeDlxMock),
				$this->stringStartsWith($queueNameMock)
			);
		$channelMock->expects($this->once())
			->method('close');

		$connectionMock = $this->getMockBuilder('\PhpAmqpLib\Connection\AMQPConnection')
			->disableOriginalConstructor()
			->getMock();
		$connectionMock->expects($this->once())
			->method('channel')
			->willReturn($channelMock);

		$this->producer->expects($this->once())
			->method('createConnection')
			->willReturn($connectionMock);
		$this->producer->expects($this->once())
			->method('createTable')
			->with(
				$this->equalTo([
					"x-dead-letter-exchange"	=> $exchangeNameMock,
					"x-message-ttl"				=> $delay * 1000,
					"x-expires"					=> $delay * 1000 + 10000
				])
			)
			->willReturn($argumentsMock);
		$this->producer->expects($this->once())
			->method('createMessage')
			->with(
				$this->equalTo($messageBodyMock),
				$this->equalTo(['delivery_mode' => 2])
			)
			->willReturn($messageMock);

		$this->producer->__construct();
		$this->producer->addDelayedMessage($queueNameMock, $exchangeNameMock, $messageBodyMock, $delay, $time);
    }

    protected function doAddRpcMessage($throwException = false) {
		$routingKeyMock			 = 'routing.key.mock';
		$messageBodyMock		 = 'messageBodyMock';
		$priority				 = Producer::PRIORITY_NORMAL;
		$timeLimit				 = 100500;
		$messageMock			 = $this->getMock('\PhpAmqpLib\Message\AMQPMessage');
		$argumentsMock			 = $this->getMock('\PhpAmqpLib\Wire\AMQPTable');
		$callbackQueueNameMock	 = 'callbackQueueNameMock';
		$correlationIdMock		 = uniqid();
		$callbackMock			 = null;
		$resultMock				 = 'resultMock';
		$consumerTagMock		 = 'consumerTagMock';

		$resultMessageMock = $this->getMockBuilder('\PhpAmqpLib\Message\AMQPMessage')
			->disableOriginalConstructor()
			->getMock();
		$resultMessageMock->expects($this->once())
			->method('get')
			->with($this->equalTo('correlation_id'))
			->willReturn($correlationIdMock);
		$resultMessageMock->body = json_encode($resultMock);
		$resultMessageMock->delivery_info['consumer_tag'] = $consumerTagMock;

		$channelMock = $this->getMockBuilder('\PhpAmqpLib\Channel')
			->disableOriginalConstructor()
			->setMethods(['basic_qos', 'queue_declare', 'basic_cancel', 'basic_consume' , 'basic_publish', 'wait', 'close'])
			->getMock();
		$channelMock->expects($this->once())
			->method('basic_qos')
			->with(
				$this->equalTo(null),
				$this->equalTo(1),
				$this->equalTo(null)
			);
		$channelMock->expects($this->once())
			->method('queue_declare')
			->with(
				$this->equalTo(''),
				$this->equalTo(false),
				$this->equalTo(true),
				$this->equalTo(true),
				$this->equalTo(true),
				$this->equalTo(false),
				$this->equalTo($argumentsMock)
				)
			->willReturn([$callbackQueueNameMock]);
		if (!$throwException) {
			$channelMock->expects($this->once())
				->method('basic_cancel')
				->with($this->equalTo($consumerTagMock));
		}
		$channelMock->expects($this->once())
			->method('basic_consume')
			->with(
				$this->equalTo($callbackQueueNameMock),
				$this->equalTo(''),
				$this->equalTo(false),
				$this->equalTo(false),
				$this->equalTo(false),
				$this->equalTo(false),
				$this->callback(function($rpcCallback) use (&$callbackMock) {
					$callbackMock = $rpcCallback;
					return true;
				})
			);
		$channelMock->expects($this->once())
			->method('basic_publish')
			->with(
				$this->equalTo($messageMock),
				$this->equalTo('rpc'),
				$this->equalTo($routingKeyMock)
			);
		if ($throwException) {
			$channelMock->expects($this->once())
				->method('wait')
				->with($this->callback(function() use (&$callbackMock, $resultMessageMock) {
					$callbackMock($resultMessageMock);
					return true;
				}))
				->willThrowException(new AMQPTimeoutException('Exception message mock'));
		} else {
			$channelMock->expects($this->once())
				->method('wait')
				->with($this->callback(function() use (&$callbackMock, $resultMessageMock) {
					$callbackMock($resultMessageMock);
					return true;
				}));
		}
		$channelMock->expects($this->once())
			->method('close');

		$connectionMock = $this->getMockBuilder('\PhpAmqpLib\Connection\AMQPConnection')
			->disableOriginalConstructor()
			->getMock();
		$connectionMock->expects($this->once())
			->method('channel')
			->willReturn($channelMock);

		$this->producer->expects($this->once())
			->method('createConnection')
			->willReturn($connectionMock);
		$this->producer->expects($this->once())
			->method('createTable')
			->with($this->equalTo(["x-max-priority" => Producer::PRIORITY_HIGH]))
			->willReturn($argumentsMock);
		$this->producer->expects($this->once())
			->method('createUniqid')
			->willReturn($correlationIdMock);
		$this->producer->expects($this->once())
			->method('createMessage')
			->with(
				$this->equalTo($messageBodyMock),
				$this->equalTo([
					'correlation_id' => $correlationIdMock,
					'reply_to'		 => $callbackQueueNameMock,
					'priority'		 => $priority
				])
			)
			->willReturn($messageMock);

		$this->producer->__construct();
		$result = $this->producer->addRpcMessage($routingKeyMock, $messageBodyMock, $priority, $timeLimit);
		if (!$throwException) {
			$this->assertEquals($resultMock, $result);
		}
	}

	public function testAddRpcMessage() {
		$this->doAddRpcMessage(false);
	}

	/**
	 * @expectedException \PrCy\RabbitMQ\Exception\TimeoutException
	 */
	public function testAddRpcMessageException() {
		$this->doAddRpcMessage(true);
	}

    public function testAppendRpcMessage() {
		$routingKeyMock			 = 'routing.key.mock';
		$messageBodyMock		 = 'messageBodyMock';
		$priority				 = Producer::PRIORITY_NORMAL;
		$correlationIdMock		 = uniqid();
		$callbackQueueNameMock	 = 'callbackQueueNameMock';
		$messageMock			 = $this->getMock('\PhpAmqpLib\Message\AMQPMessage');
		$argumentsMock			 = $this->getMock('\PhpAmqpLib\Wire\AMQPTable');
		$consumerTagMock		 = 'consumerTagMock';
		$resultMock				 = 'resultMock';

		$resultMessageMock = $this->getMockBuilder('\PhpAmqpLib\Message\AMQPMessage')
			->disableOriginalConstructor()
			->getMock();
		$resultMessageMock->expects($this->once())
			->method('get')
			->with($this->equalTo('correlation_id'))
			->willReturn($correlationIdMock);
		$resultMessageMock->body = json_encode($resultMock);
		$resultMessageMock->delivery_info['consumer_tag'] = $consumerTagMock;

		$channelMock = $this->getMockBuilder('\PhpAmqpLib\Channel')
			->disableOriginalConstructor()
			->setMethods(['basic_publish', 'basic_cancel', 'queue_declare', 'basic_consume'])
			->getMock();
		$channelMock->expects($this->once())
			->method('basic_publish')
			->with(
				$this->equalTo($messageMock),
				$this->equalTo('rpc'),
				$this->equalTo($routingKeyMock)
			);
		$channelMock->expects($this->once())
			->method('basic_cancel')
			->with($this->equalTo($consumerTagMock));
		$channelMock->expects($this->once())
			->method('queue_declare')
			->with(
				$this->equalTo(''),
				$this->equalTo(false),
				$this->equalTo(true),
				$this->equalTo(true),
				$this->equalTo(true),
				$this->equalTo(false),
				$this->equalTo($argumentsMock)
			)
			->willReturn([$callbackQueueNameMock]);
		$channelMock->expects($this->once())
			->method('basic_consume')
			->with(
				$this->equalTo($callbackQueueNameMock),
				$this->equalTo(''),
				$this->equalTo(false),
				$this->equalTo(false),
				$this->equalTo(false),
				$this->equalTo(false),
				$this->callback(function($rpcCallback) use ($resultMessageMock) {
					$rpcCallback($resultMessageMock);
					return true;
				})
			);

		$this->producer->expects($this->once())
			->method('createTable')
			->with($this->equalTo(["x-max-priority" => Producer::PRIORITY_HIGH]))
			->willReturn($argumentsMock);
		$this->producer->expects($this->once())
			->method('createMessage')
			->with(
				$this->equalTo($messageBodyMock),
				$this->equalTo([
					'correlation_id' => $correlationIdMock,
					'reply_to'		 => $callbackQueueNameMock,
					'priority'		 => $priority
				])
			)
			->willReturn($messageMock);
		$this->producer->expects($this->once())
			->method('createUniqid')
			->willReturn($correlationIdMock);

		$that = $this;
		$outerCallback = function($result) use($that, $resultMock) {
			$that->assertEquals($resultMock, $result);
		};
		$this->producer->__construct();
		$this->producer->appendRpcMessage(
			$channelMock,
			$routingKeyMock,
			$messageBodyMock,
			$outerCallback,
			$priority
		);
    }

	public function testWaitRpcCallbacksNoCallbacks() {
		$channelMock = $this->getMockBuilder('\PhpAmqpLib\Channel')
			->disableOriginalConstructor()
			->setMethods(['close'])
			->getMock();
		$channelMock->expects($this->once())
			->method('close');
		$channelMock->callbacks = [];
		$this->producer->__construct();
		$this->producer->waitRpcCallbacks($channelMock, 123);
	}

	/**
	 * @expectedException \PrCy\RabbitMQ\Exception\TimeoutException
	 */
	public function testWaitRpcCallbacksTimeoutException() {
		$channelMock = $this->getMockBuilder('\PhpAmqpLib\Channel')
			->disableOriginalConstructor()
			->setMethods(['close'])
			->getMock();
		$channelMock->expects($this->once())
			->method('close');
		$channelMock->callbacks = [1, 2, 3];
		$this->producer->__construct();
		$this->producer->waitRpcCallbacks($channelMock, 0);
	}

	/**
	 * @expectedException \PrCy\RabbitMQ\Exception\TimeoutException
	 */
	public function testWaitRpcCallbacksAMQPTimeoutException() {
		$channelMock = $this->getMockBuilder('\PhpAmqpLib\Channel')
			->disableOriginalConstructor()
			->setMethods(['close', 'wait'])
			->getMock();
		$channelMock->expects($this->at(0))
			->method('wait')
			->with(
				$this->equalTo(null),
				$this->equalTo(false),
				$this->greaterThan(0)
			);
		$channelMock->expects($this->at(1))
			->method('wait')
			->with(
				$this->equalTo(null),
				$this->equalTo(false),
				$this->greaterThan(0)
			)
			->willThrowException(new AMQPTimeoutException('Exception message mock'));
		$channelMock->expects($this->once())
			->method('close');
		$channelMock->callbacks = [1, 2, 3];
		$this->producer->__construct();
		$this->producer->waitRpcCallbacks($channelMock, 123);
	}
}
