<?php


namespace Verse\Router\Actors;


use Verse\Router\Model\RouterExchange;

class RouterPublisher extends RouterActorProto
{
    /**
     * @var RouterExchange
     */
    private $routerExchange;
    
    /**
     * Тело сообщения
     * @var string
     */
    private $message = '';
    
    /**
     * Целевая очередь/тэг
     *
     * @var string
     */
    private $targetQueue = '';
    
    /**
     * Параметры сообщения
     *
     * @var array
     */
    private $params = [];
    
    /**
     * Флаги amqp
     *
     * @var int
     */
    private $flags = 0;
    
    /**
     * Опубликовать сообщение
     * 
     * @param       $message
     * @param       $targetQueue
     * @param array $params
     * @param int   $flags
     *
     * @return null
     */
    public function publish($message, $targetQueue, $params = [], $flags = AMQP_NOPARAM)
    {
        $this->message     = $message;
        $this->targetQueue = $targetQueue;
        $this->params      = $params;
        $this->flags       = $flags;
        
        return $this->run();
    }
    
    public function processTask()
    {
        return $this->routerExchange->amqpExchange->publish($this->message, $this->targetQueue, $this->flags, $this->params);
    }
    
    /**
     * Запрос на установку соединения
     *
     * @return mixed
     */
    public function loadDeps()
    {
        $this->routerExchange = $this->server->getExchange($this->thread);
    }
    
    /**
     * Запрос на восстановление
     *
     * @return mixed
     */
    public function recovery()
    {
        $this->loadDeps();
        $this->routerExchange->recovery();
    }
    
}