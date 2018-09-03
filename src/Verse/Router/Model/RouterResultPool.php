<?php


namespace Verse\Router\Model;

use Verse\Router\RouterConfig;

class RouterResultPool extends RouterModuleProto
{
    /**
     * @var RouterQueue
     */
    private $routerQueue;
    
    /**
     * @var RouterConnection
     */
    private $routerConnection;
    
    /**
     * @var RouterChannel
     */
    private $routerChannel;
    
    public $results = [];
    
    private $readResultId;
    
    private $lastException;
    
    private $isQueueAlreadyProtected = false;
    
    private $queueName = '';
    
    private $autoAck = true;
    
    private $queueConfigurationId;
    
    /**
     * Запрос на установку соединения
     *
     * @return mixed
     */
    public function setup()
    {
        $this->loadDeps();
    }
    
    /**
     * Запрос на восстановление
     *
     * @return mixed
     */
    public function recovery()
    {
        $this->loadDeps();
        $this->routerConnection->recovery();
        $this->routerChannel->recovery();
        $this->routerQueue->recovery();
    }
    
    public function loadDeps()
    {
        $this->queueName            = $this->configuration[RouterConfig::COMMON_QUEUE_NAME];
        $this->autoAck              = $this->configuration[RouterConfig::REPLY_READER_AUTO_ACK] ?: true;
        $this->queueConfigurationId = $this->configuration[RouterConfig::COMMON_QUEUE_CONFIGURATION_ID];
        
        $this->routerQueue      = $this->server->getQueue($this->thread, $this->queueName, $this->configuration);
        $this->routerChannel    = $this->server->getChannel($this->thread);
        $this->routerConnection = $this->server->getConnection($this->thread);
    }
    
    public function read($correlationId, $timeout)
    {
        if (isset($this->results[$correlationId])) {
            return $this->results[$correlationId];
        }
        
        $this->readResultId = $correlationId;
        
        $tag = $this->queueName . ($this->isQueueAlreadyProtected ? '' : '.first');
        
        $this->routerConnection->amqpConnection->setReadTimeout($timeout);
        $this->routerChannel->amqpChannel->setPrefetchCount(0);
        
        try {
            $this->routerQueue->amqpQueue->consume([$this, 'consume'], $this->autoAck ? AMQP_AUTOACK : AMQP_NOPARAM, $tag);
        } catch (\AMQPException $e) {
            $this->lastException = $e;
        }
        
        if (!$this->isQueueAlreadyProtected) {
            $this->isQueueAlreadyProtected = true;
        } else {
            $this->routerQueue->amqpQueue->cancel($tag);
        }
        
        $res = isset($this->results[$correlationId]) ? $this->results[$correlationId] : null;
        
        unset($this->results[$correlationId]);
        
        $this->readResultId = null;
        
        return $res ? \json_decode($res, true) : null;
    }
    
    public function consume(\AMQPEnvelope $envelope)
    {
        $res = call_user_func();
            
        if (!$this->autoAck) {
            $this->routerQueue->amqpQueue->ack($envelope->getDeliveryTag());    
        }
        
        return $res;
    }
    
    /**
     * @return \AMQPException
     */
    public function getLastException()
    {
        return $this->lastException;
    }
}