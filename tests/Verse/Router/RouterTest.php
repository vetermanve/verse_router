<?php


namespace Router\Tests;

use Router\Router;
use Router\RouterConfig;
use Testing\TestBase;


include (__DIR__.'/../../Testing/bootstrap.php');

class RouterTest extends TestBase
{
    public function testRouterConstructDefaultServer () 
    {
        $r = new Router();
        $r->init();
        $r->getRegistry();
        $this->assertNotEmpty($r->getRegistry()->findServerForQueue('anyQueue', RouterConfig::SERVER_TAG_DEFAULT));
    }
    
    public function testRouterRegisterNonDefaultServer () 
    {
        $r = new Router();
        $r->init();
        
        $externalQueue = 'testing.external.Queue';
        
        $testServerHost = 'testing.server.com';
        $testServerPort = 1111;
        
        $clearedQueueName = $r->registerQueue($externalQueue, $testServerHost , $testServerPort);
        
        $server = $r->getRegistry()->findServerForQueue($clearedQueueName, '');
        
        $this->assertNotEmpty($server);
        $this->assertEquals($testServerHost, $server->getHost());
        $this->assertEquals($testServerPort, $server->getPort());
    }
    
//    public function testResultConsumerConstruct () 
//    {
//        $faceServiceQueue = 'fake.queue.consume';
//    
//        $r = new Router();
//        $r->init();
//        $consumer = $r->getConsumer($faceServiceQueue);
//        
//        $consumerQueue = $consumer->getQueue();
//        $this->assertNotEmpty($consumerQueue);
//        $this->assertNotEmpty($consumerQueue->getConnection());
//        $this->assertTrue($consumerQueue->getConnection()->isConnected());
//    }
    
    public function testFullPublishAndRead () 
    {
        $faceServiceQueue = 'fake.queue.full.test';
        
        $r = new Router();
        $r->init();
        
        $consumer = $r->getConsumer($faceServiceQueue, 0.01);
        
        while ($consumer->readOne()) {
            true;
        }
    
        $testData = ['test' => (string)microtime(1),];
        $r->publish($testData, $faceServiceQueue);
        $testData2 = ['test' =>  (string)microtime(1),];
        $r->publish($testData2, $faceServiceQueue);
        
        $res = $consumer->readOne();
        $res2 = $consumer->readOne();
        
        $this->assertEquals($testData2, $res2);
        $this->assertEquals($testData, $res);
    }
    
    public function testConsumeAndRead () 
    {
        $fakeQueue = 'fake.queue.consume.process';
        $fakeQueueRepublish = 'fake.queue.consume.republish';
        
        $r = new Router();
        $r->init();
    
        $consumer = $r->getConsumer($fakeQueue, 0.01);
        while ($consumer->readOne()) {
            true;
        }
        $consumerRepublish = $r->getConsumer($fakeQueueRepublish, 0.01);
        while ($consumerRepublish->readOne()) {
            true;
        }
        
        $data1 = ['test' => mt_rand(1, 1000000)];
        $data2 = ['test' => mt_rand(1, 1000000)];
        
        $r->publish($data1, $fakeQueue);
        $r->disconnectAll();
        $r->publish($data2, $fakeQueue);
        $r->disconnectAll();
        
        $consumer->consume(function (\AMQPEnvelope $envelope) use ($r, $fakeQueueRepublish) {
            
            $r->publish([
                're' => \json_decode($envelope->getBody(), 1),
            ], $fakeQueueRepublish);
        });
        
        $r->disconnectAll();
    
        $consumerRepublish = $r->getConsumer($fakeQueueRepublish, 0.01);
        
        $rep1 = $consumerRepublish->readOne();
        $r->disconnectAll();
        $rep2 = $consumerRepublish->readOne();
        
        $this->assertNotEmpty($rep1);
        $this->assertNotEmpty($rep2);
        $this->assertEquals($rep1['re'], $data1);
        $this->assertEquals($rep2['re'], $data2);
    }
    
    public function testConsumeAndReadFail ()
    {
        $fakeQueue = 'fake.queue.consume.process';
        $fakeQueueRepublish = 'fake.queue.consume.republish';
        
        $r = new Router();
        $r->init();
        
        $data1 = ['test' => mt_rand(1, 1000000)];
        $data2 = ['test' => mt_rand(1, 1000000)];
        
        $r->publish($data1, $fakeQueue);
        $r->publish($data2, $fakeQueue);
        
        $consumer = $r->getConsumer($fakeQueue, 0.01);
        $consumer->consume(function (\AMQPEnvelope $envelope) use ($r, $fakeQueueRepublish) {
            $r->publish([
                're' => \json_decode($envelope->getBody(), 1),
            ], $fakeQueueRepublish);
            
            return false;
        });
        
        $consumerRepublish = $r->getConsumer($fakeQueueRepublish, 0.01);
        
        $rep1 = $consumerRepublish->readOne();
        $r->disconnectAll();
        $rep2 = $consumerRepublish->readOne();
        
        $this->assertNull($rep2);
        
        $this->assertNotEmpty($rep1);
        $this->assertEquals($rep1['re'], $data1);
    
        $rep2Correct = $consumer->readOne();
        
        $this->assertEquals($rep2Correct, $data2);
    }
    
    public function testPublishAndReply () 
    {
        $fakeQueue = 'fake.queue.publish.reply';
        
        $r = new Router();
        $r->init();
    
        $consumer = $r->getConsumer($fakeQueue, 0.01);
        while ($consumer->readOne()) {
            true;
        }
        
        $testData = ['test' => (string)microtime(1)];
        $corrId = $r->publish($testData, $fakeQueue, true);
        
        $r->disconnectAll();
        
        $consumer->consume(function(\AMQPEnvelope $envelope) use ($r, $fakeQueue) {
            $r->reply(['rep' => \json_decode($envelope->getBody(), 1)], $envelope->getReplyTo(), $envelope->getCorrelationId());
            return false;
        });
        
        $result = $r->readResult($fakeQueue, $corrId, 0.01);
        
        $this->assertNotEmpty($result);
        $this->assertEquals($result['rep'], $testData);
    }
    
    public function testConsumerReconnects () 
    {
        $r = new Router();
        $r->init();
        
        $consumer = $r->getConsumer('fake.consumer.reconnect.test', 5, 1);
        
        $testClass = $this;
        $consumer->consume(function () {}, function ($_, \Exception $e) { throw $e;}, function($time) use ($testClass, $r) {
            static $tick;
            $tick++;
            $testClass->assertEquals($tick, round($time));
        });
    }
    
    public function testReplyReaderReconnect () 
    {
        $r = new Router();
        $r->init();
    
        $queue = 'fake.queue.publish.reply.reader';
        $replyQueue = $r->getReplyQueueName();
        
        $consumer = $r->getConsumer($queue, 0.1);
        $r->readResult($queue, 'make_queue_empty', 0.01);
        
        $thread = Router::THREAD_MAIN;
        
        $server = $r->getRegistry()->findServerForQueue($queue, RouterConfig::SERVER_TAG_DEFAULT);
        $testData = function () {
            return [
                date('c'), microtime(1)
            ];   
        };
    
        $replyReader = $server->getReplyReader($thread, $queue, RouterConfig::CONFIG_REPLY_CONSUMER);
        $corrId = $r->publish($testData(), $queue, 1);
        $op = $consumer->readOne();
        $this->assertNotEmpty($op);
        $r->reply($op, $replyQueue, $corrId);
        
        $data = $replyReader->read($corrId, 0.1);
        $this->assertNotEmpty($data);
    
        $corrId = $r->publish($testData(), $queue, 1);
        $r->disconnectAll();
        $replyReader->recovery();
        $r->disconnectAll();
        $op = $consumer->readOne();
        $this->assertNotEmpty($op);
        
        $replyReader->recovery();
        
        $res = $r->reply($op, $replyQueue, $corrId);
        $this->assertTrue($res);
        $data = $replyReader->read($corrId, 0.1);
        $this->assertNotEmpty($data);
    }
    
    public function testConsumerReconnectOnDisconnections () 
    {
        $r = new Router();
        $r->init();
    
        $queue = 'fake.queue.publish.reply.reconnect';
    
        $requests = [];
    
        $msgCount = 20;
    
        $c = 0;
        while ($c++ < $msgCount) {
            $value = $c*3;
        
            $requests[] = [
                'req' => $value,
                'rep' => $value/2,
            ];
        }
        
        $allValues = [];
    
        foreach ($requests as &$request) {
            $request['c'] = $r->publish($request['req'], $queue, true);
            $allValues[$request['c']] = $request['req'];
        } unset($request);
    
        $state = new \stdClass();
        $state->processed = 0;
        $state->records = [];
        $state->errors = [];
        
        $consumerFunction = function(\AMQPEnvelope $envelope) use ($r, $state) {
            rand(0, 1) == 0 && $r->disconnectAll();
            $state->records[$envelope->getCorrelationId()] = $envelope->getBody();
            $state->processed++;
            $r->reply($envelope->getBody()/2, $envelope->getReplyTo(), $envelope->getCorrelationId());
        };
        
        $errorFunction  = function (\AMQPEnvelope $envelope, \Exception $error)  use ($state) {
            $state->errors[] = [
                'error' => $error->getMessage().' at '. $error->getTraceAsString(),
                'envelope' => $envelope->getBody(),
            ];
        };
    
        $consumer = $r->getConsumer($queue, 0.002);
        $consumer->setConfig(RouterConfig::ACTOR_TASK_RETRY_COUNT, 200);
        $consumer->consume($consumerFunction, $errorFunction);
    
        $this->assertEmpty($state->errors);
        $this->assertEquals($allValues, $state->records, 'all records consumed');
    }
    
    public function testPublishAndReplyAsync ()
    {
        $r = new Router();
        $r->init();
        
        $queues = [
            'fake.queue.publish.reply.async1' => 1,
            'fake.queue.publish.reply.async2' => 2,
            'fake.queue.publish.reply.async3' => 3,
        ];
        
        $requests = [];
        
        $c = 0;
        $msgCount = 100;
        
        while ($c++ < $msgCount) {
            $value = mt_rand(1, 10000);
            
            $requests[] = [
                'q' => array_rand($queues),
                'req' => $value,
                'rep' => $value/2,
            ];
        }
        
        shuffle($requests);
        
        foreach ($requests as &$request) {
            $request['c'] = $r->publish($request['req'], $request['q'], true);
        } unset($request);
        
        foreach ($queues as $queue => $_) {
            $consumer = $r->getConsumer($queue, 0.002);
            $consumer->setConfig(RouterConfig::ACTOR_TASK_RETRY_COUNT, $msgCount);
    
            $consumer->consume(function(\AMQPEnvelope $envelope) use ($r) {
                $r->reply($envelope->getBody()/2, $envelope->getReplyTo(), $envelope->getCorrelationId());
                mt_rand(0, 5) === 5 && $r->disconnectAll();
            });
        }
    
        $r->disconnectAll();
        
        shuffle($requests);
    
        foreach ($requests as $id => &$request) {
            $res = $r->readResult($request['q'], $request['c'], 0.02);
            $this->assertEquals($request['rep'], $res, 'equal valuation on '.$id);
        } unset($request);
    }
    
}