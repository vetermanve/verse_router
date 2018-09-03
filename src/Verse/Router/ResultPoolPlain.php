<?php


namespace Verse\Router;


class ResultPoolPlain 
{
    public $results;
    
    /**
     * @var \AMQPQueue
     */
    private $queue;
    
    private $readResultId;
    
    private $lastException;
    
    private $isQueueProtected = false;
    
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
    
    public function read($correlationId, $timeout)
    {
        if (!isset($this->results[$correlationId])) {
            $this->readResultId = $correlationId;
            $tag = $this->queue->getName().($this->isQueueProtected ? '' : '.first');
            $this->queue->getConnection()->setReadTimeout($timeout);
            $this->queue->getChannel()->setPrefetchCount(0);
            
            try {
                $this->queue->consume([$this, 'consume'], AMQP_AUTOACK, $tag);    
            } catch (\AMQPException $e) {
                $this->lastException = $e;
            }
            
            if (!$this->isQueueProtected) {
                $this->isQueueProtected = true;   
            } else {
                $this->queue->cancel($this->queue->getName());
            }
        }
        
        $res = isset($this->results[$correlationId]) ? $this->results[$correlationId] : null;
        
        unset($this->results[$correlationId]);
        $this->readResultId = null;
    
        return $res ? \json_decode($res, true) : null;
    }
    
    public function consume(\AMQPEnvelope $envelope) {
        $resultId = $envelope->getCorrelationId(); 
        $this->results[$resultId] = $envelope->getBody();
        
        if ($resultId === $this->readResultId) {
            return false;
        }
    }
    
    /**
     * @return \AMQPException
     */
    public function getLastException()
    {
        return $this->lastException;
    }
}