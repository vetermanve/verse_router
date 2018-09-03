<?php


namespace Verse\Router\Actors;


use Verse\Router\Model\RouterConsumer;
use Verse\Router\RouterConfig;

class RouterReplyReader extends RouterActorProto
{
    const CORRELATION_ID = 'corr_id';
    const TIMEOUT        = 'timeout';
    
    const CONSUME_EXIT     = false;
    const CONSUME_CONTINUE = null;
    
    public $results = [];
    
    private $findCorrelationId = '';
    
    private $timeout = 0;
    
    /**
     * @var RouterConsumer
     */
    private $consumer;
    
    public function read($correlationId , $timeout)
    {
        $this->findCorrelationId = $correlationId;
        $this->timeout = $timeout;
        return $this->run();
    }
    
    protected function processTask()
    {
        $correlationId = $this->findCorrelationId;
        
        if (!isset($this->results[$correlationId])) {
            $this->consumer->read($this->timeout);
        }
        
        if (isset($this->results[$correlationId])) {
            $res = $this->results[$correlationId];
            unset($this->results[$correlationId]);
            
            $start = microtime(1);
            $data = \json_decode($res, true);
            
            if (is_array($data)) {
               $data['__unpack'] = round(microtime(1) - $start, 6); 
            }
            
            return $data;
        }
        
        return null;
    }
    
    /**
     * Запрос на установку соединения
     *
     * @return mixed
     */
    public function loadDeps()
    {
        $callback    = [$this, 'callback'];
        $queueName   = $this->getConfig(RouterConfig::COMMON_QUEUE_NAME);
        $queueConfig = $this->getConfig(RouterConfig::COMMON_QUEUE_CONFIGURATION_ID);
        
        $this->consumer = $this->server->getConsumer($this->thread, $queueName, $queueConfig, $callback);
    }
    
    public function callback(\AMQPEnvelope $envelope)
    {
        $correlationId = $envelope->getCorrelationId();
        
        $this->results[$correlationId] = $envelope->getBody();
        
        return ($correlationId === $this->findCorrelationId) ? self::CONSUME_EXIT : self::CONSUME_CONTINUE;
    }
    
    /**
     * Запрос на восстановление
     *
     * @return mixed
     */
    public function recovery()
    {
        $this->loadDeps();
        $this->consumer->recovery();
    }
}