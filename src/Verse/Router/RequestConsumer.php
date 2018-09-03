<?php


namespace Verse\Router;


use Run\Event\EventDispatcher;
use Run\Event\Object\RuntimeDispatch;

class RequestConsumer
{
    const REFRESH_INTERVAL = 3;
    
    /**
     * @var \AMQPQueue
     */
    private $queue;
    
    private $lastException;
    
    private $callable;
    
    private $continueProcessing = true;
    
    private $timeLimit;
    
    private $startTime;
    
    /**
     * @var EventDispatcher
     */
    private $eventDispatcher;
    
    /**
     *
     * @var bool
     */
    private $autoAck = true;
    
    public function readOne()
    {
        $res = new \stdClass();
        
        $callable = function (\AMQPEnvelope $envelope) use ($res) {
            $res->res = \json_decode($envelope->getBody(), true);
            
            return false;
        };
        
        try {
            $this->_startListen($callable, $this->timeLimit);
        } catch (\AMQPException $exception) {
            $this->lastException = $exception;
        }
        
        return isset($res->res) ? $res->res : null;
    }
    
    public function consume($callable)
    {
        $this->startTime = microtime(1);
        
        $this->callable           = $callable;
        $this->continueProcessing = true;
        $listenTime               = self::REFRESH_INTERVAL;
        
        if ($this->timeLimit && $this->timeLimit < $listenTime) {
            $listenTime = $this->timeLimit;
        }
        
        while ($this->continueProcessing) {
            $this->_startListen($callable, $listenTime);
            
            if (null === $this->lastException) {
                break;
            }
            
            if ($this->timeLimit && microtime(1) - $this->startTime > $this->timeLimit) {
                break;
            }
            
            if ($this->eventDispatcher) {
                $this->eventDispatcher->dispatch(new RuntimeDispatch($this));
            }
        }
    }
    
    private function _startListen($callable, $timeLimit)
    {
        $this->queue->getConnection()->setReadTimeout($timeLimit);
        
        $this->lastException = null;
        
        try {
            $this->queue->consume($callable, $this->autoAck ? AMQP_AUTOACK : AMQP_NOPARAM, $this->queue->getName());
        } catch (\AMQPException $exception) {
            $this->lastException = $exception;
        }
        
        $this->queue->cancel($this->queue->getName());
    }
    
    public function listen(\AMQPEnvelope $envelope)
    {
        $res                      = call_user_func($this->callable, $envelope);
        $this->continueProcessing = $res !== false;
        
        return $res;
    }
    
    /**
     * @return \AMQPQueue
     */
    public function getQueue()
    {
        return $this->queue;
    }
    
    /**
     * @param \AMQPQueue $queue
     */
    public function setQueue($queue)
    {
        $this->queue = $queue;
    }
    
    /**
     * @param mixed $callable
     */
    public function setCallable($callable)
    {
        $this->callable = $callable;
    }
    
    /**
     * @param mixed $timeLimit
     */
    public function setTimeLimit($timeLimit)
    {
        $this->timeLimit = $timeLimit;
    }
    
    /**
     * @param boolean $autoAck
     */
    public function setAutoAck($autoAck)
    {
        $this->autoAck = $autoAck;
    }
    
    /**
     * @param EventDispatcher $eventDispatcher
     */
    public function setEventDispatcher($eventDispatcher)
    {
        $this->eventDispatcher = $eventDispatcher;
    }
    
    /**
     * Получить название описывающее объект
     *
     * @return string
     */
    public function getName()
    {
        $name = 'Request consumer: ';
        if ($this->queue) {
            $name .= ' on queue: ' . $this->queue->getName();
            $name .= ', on host: ' 
                . $this->queue->getConnection()->getHost() . ':' . $this->queue->getConnection() ->getPort();
            $name .= ', ' . $this->queue->getConnection()->isConnected() ? 'connected' : 'disconnected';
        }
        
        return $name;
    }
    
    /**
     * Получить массив описания объекта
     *
     * @return array
     */
    public function getDescription()
    {
        if (!$this->queue) {
            return [
                'queue' => 'not set',
            ];
        }
        
        $connection = $this->queue->getConnection();
        
        return [
            'queue'     => $this->queue->getName(),
            'host'      => $connection->getHost(),
            'port'      => $connection->getPort(),
            'connected' => $connection->isConnected(),
            'class'     => __CLASS__,
        ];
    }
}