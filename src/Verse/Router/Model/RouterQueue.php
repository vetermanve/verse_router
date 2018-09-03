<?php


namespace Verse\Router\Model;


use Verse\Router\RouterConfig;

class RouterQueue extends RouterModuleProto
{
    /**
     * @var \AMQPQueue
     */
    public $amqpQueue;
    
    /**
     * @var RouterChannel
     */
    private $channel;
    
    public function setup()
    {
        $this->loadChannel();
        $this->loadQueue();
    }
    
    public function loadQueue () 
    {
        try {
            if (!$this->channel->amqpChannel) {
                throw new \Exception('Empty channel');
            }
            
            $queue = new \AMQPQueue($this->channel->amqpChannel);
            
            $queue->setName($this->getConfig(RouterConfig::QUEUE_NAME));
            
            if ($flags = $this->getConfig(RouterConfig::QUEUE_FLAGS)) {
                $queue->setFlags($flags);    
            }
    
            if ($args = $this->getConfig(RouterConfig::QUEUE_ARGUMENTS)) {
                $queue->setArguments($args);
            }
    
            $queue->declareQueue();
    
            if ($exchange = $this->getConfig(RouterConfig::QUEUE_EXCHANGE)) {
                $queue->bind($exchange);
            }
            
            $this->amqpQueue = $queue;
        } catch (\Exception $exception) {
            $this->server->reportProblem($this, $exception->getMessage());
        }
    }
    
    public function loadChannel () 
    {
        $this->channel = $this->server->getChannel($this->thread);
    }
    
    public function recovery()
    {
        $this->loadChannel();
        $this->channel->recovery();
        $this->loadQueue();
    }
}