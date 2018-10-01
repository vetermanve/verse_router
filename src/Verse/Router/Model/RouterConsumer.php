<?php


namespace Verse\Router\Model;

use Verse\Router\RouterConfig;
use Verse\Router\Helper\Uuid;

class RouterConsumer extends RouterModuleProto
{
    const PROTECTION_TAG_POSTFIX = '.protection.';
    
    const TIMEOUT_EXCEPTION_TEXT = 'Consumer timeout exceed';
    const TIMEOUT_EXCEPTION_OLD = '(unknown error)';
    
    const DEFAULT_PREFETCH_SIZE = 0;
    
    /**
     * @var callable
     */
    protected $internalConsumeCallback;
    
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
    
    private $queueNeedProtection = false;
    
    private $queueName = '';
    
    private $autoAck = true;
    
    private $queueConfigurationId;
    
    private $callback;
    
    /**
     * Запрос на установку соединения
     *
     * @return mixed
     */
    public function setup()
    {
        $this->loadDeps();
        
        $this->callback = $this->getConfig(RouterConfig::CONSUMER_CALLBACK);
    }
    
    /**
     * Запрос на восстановление
     *
     * @return mixed
     */
    public function recovery()
    {
        $this->loadDeps();
        $this->routerQueue->recovery();
    }
    
    public function loadDeps()
    {
        $this->queueName            = $this->getConfig(RouterConfig::CONSUMER_QUEUE_NAME);
        $this->autoAck              = $this->getConfig(RouterConfig::CONSUMER_AUTO_ACK, false);
        $this->queueConfigurationId = $this->getConfig(RouterConfig::CONSUMER_QUEUE_CONFIGURATION_ID);
        
        $this->internalConsumeCallback = [$this, 'consume'];
        
        $this->routerQueue      = $this->server->getQueue($this->thread, $this->queueName, $this->queueConfigurationId);
        $this->routerChannel    = $this->server->getChannel($this->thread);
        $this->routerConnection = $this->server->getConnection($this->thread);
        
        $this->queueNeedProtection = $this->routerQueue->getConfig(RouterConfig::QUEUE_AUTO_DELETE_PROTECT);
    }
    
    public function read($timeout)
    {
        $tag = $this->queueName;
        
        if ($this->queueNeedProtection) {
            $tag .= self::PROTECTION_TAG_POSTFIX.Uuid::v4();
        }
        
        $this->routerConnection->amqpConnection->setReadTimeout($timeout);
        
        try {
            $this->routerChannel->amqpChannel->setPrefetchCount(self::DEFAULT_PREFETCH_SIZE);
            
            $this->routerQueue->amqpQueue->consume(
                $this->internalConsumeCallback, 
                $this->autoAck ? AMQP_AUTOACK : AMQP_NOPARAM, 
                $tag
            );
        } catch (\AMQPConnectionException $e) {
            if ($e->getMessage() !== self::TIMEOUT_EXCEPTION_OLD) {
                throw $e;
            }
        } catch (\AMQPQueueException $e) {
            $isRegularTimeout = strpos($e->getMessage(), self::TIMEOUT_EXCEPTION_TEXT) === 0;
            
            if (!$isRegularTimeout) {
                $this->queueNeedProtection = true;
                try {
                    $this->routerQueue->amqpQueue->cancel($tag);    
                } catch (\Exception $_) {
                    
                }
                
                throw $e;  
            }
        }
        
        if (!$this->queueNeedProtection) {
            $this->routerQueue->amqpQueue->cancel($tag);
        }
    
        $this->queueNeedProtection = false;
    }
    
    public function consume(\AMQPEnvelope $envelope)
    {
        $result = call_user_func($this->callback, $envelope);
        
        if (!$this->autoAck) {
            $this->routerQueue->amqpQueue->ack($envelope->getDeliveryTag());    
        }
        
        return $result;
    }
}