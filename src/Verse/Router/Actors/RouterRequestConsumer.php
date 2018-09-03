<?php


namespace Verse\Router\Actors;


use Verse\Router\Model\RouterConsumer;
use Verse\Router\RouterConfig;
use Run\Util\Tracer;

class RouterRequestConsumer extends RouterActorProto
{
    const DEFAULT_REFRESH_INTERVAL = 3;
    
    private $refreshInterval = 3;
    
    /**
     * @var RouterConsumer
     */
    private $consumer;
    
    /**
     * @var callable
     */
    private $callback;
    
    /**
     * @var callable
     */
    private $errorCallback;
    
    /**
     * @var callable
     */
    private $refreshCallback;
    
    private $timeLimit;
    
    private $continueProcessing = true;
    
    public function readOne () 
    {
        $res = new \stdClass();
    
        $callable = function (\AMQPEnvelope $envelope) use ($res) {
            $res->res = \json_decode($envelope->getBody(), true);
        
            return false;
        };
    
        $this->consume($callable);
    
        return isset($res->res) ? $res->res : null;
    }
    
    public function consume($callback, $errorCallback = null, $refreshCallback = null)
    {
        $this->server->log(__METHOD__);
        $this->callback = $callback;
        $this->errorCallback = $errorCallback;
        $this->refreshCallback = $refreshCallback;
        
        return $this->run();
    }
    
    protected function processTask()
    {
        $this->continueProcessing = true;
        $startTime = microtime(1);
        
        $listenTime = $this->refreshInterval;
        
        if ($this->timeLimit && $this->timeLimit < $listenTime) {
            $listenTime = $this->timeLimit;
        }
        
        while ($this->continueProcessing) {
            $this->server->runtime(__METHOD__.' at '.round(microtime(1) - $startTime, 4));
            $this->consumer->read($listenTime);
    
            $processingTime = microtime(1) - $startTime;
            if ($this->timeLimit && $processingTime > $this->timeLimit) {
                break;
            }
         
            try {
                $this->refreshCallback && call_user_func($this->refreshCallback, $processingTime);    
            } catch (\Exception $exception) {
                $this->server->runtime(__METHOD__.' refreshCallback '.get_class($exception).': '.$exception->getMessage());
                $this->errorCallback && call_user_func($this->errorCallback, null, $exception);
            }
        }
    }
    
    public function loadDeps()
    {
        $this->timeLimit = $this->getConfig(RouterConfig::REQUEST_CONSUMER_TIME_LIMIT);
        $this->refreshInterval = $this->getConfig(RouterConfig::REQUEST_CONSUMER_REFRESH_INTERVAL, self::DEFAULT_REFRESH_INTERVAL);
        
        $callback    = [$this, 'callback'];
        $queueName   = $this->getConfig(RouterConfig::COMMON_QUEUE_NAME);
        $queueConfig = $this->getConfig(RouterConfig::COMMON_QUEUE_CONFIGURATION_ID);
    
        $this->server->log(__METHOD__.' built consumer', ['callable' => $callback,]);
        $this->consumer = $this->server->getConsumer($this->thread, $queueName, $queueConfig, $callback);
    }
    
    public function callback(\AMQPEnvelope $envelope)
    {
        $res = true;
        try {
            $res = call_user_func($this->callback, $envelope);    
        } catch (\Exception $exception) {
            $this->errorCallback && call_user_func($this->errorCallback, $envelope, $exception);               
        }
        
        $this->continueProcessing = $res !== false;
        
        return $res;
    }
    
    /**
     * Запрос на восстановление
     *
     * @return mixed
     */
    public function recovery()
    {
        $this->server->runtime(__METHOD__, ['trace' => (new Tracer)->getTrace(10, 2)]);
        $this->loadDeps();
        $this->consumer->recovery();
    }
}