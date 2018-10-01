<?php


namespace Verse\Router;


use Verse\Router\Helper\Uuid;
use Verse\Router\Actors\RouterRequestConsumer;
use Verse\Router\Exceptions\EmptyRouterMessage;
use Verse\Router\Exceptions\ServerNotConfigured;
use Verse\Router\Model\RouterServer;

class Router
{
    const THREAD_MAIN          = 'main';
    const REASON_CONSUME_REPLY = 'consumeReply';
    const THREAD_CONSUME       = 'consume:';
    
    private $nodeId = '';
    
    private $replyQueueName = '';
    
    /**
     * @var RouterRegistry
     */
    private $registry;
    
    /**
     * Router2 constructor.
     *
     * @param string $nodeId
     */
    public function __construct($nodeId = null)
    {
        $nodeId = $nodeId ?: gethostname() . ':' . getmypid().':'.Uuid::v4();
        
        $this->nodeId         = $this->_prepareQueueName($nodeId);
        $this->replyQueueName = RouterConfig::getReplyQueueName($this->nodeId);
        $this->registry       = new RouterRegistry();
    }
    
    /**
     * PUBLIC API
     */

    /**
     * Инициализация модуля
     *
     * @param array $config
     * 
     * @return $this|\Verse\Router\Router
     */
    public function init(array $config = []) : self
    {
        $host = $config[RouterConfig::ROUTER_CONNECTION_HOST] ?? RouterConfig::AMQP_RABBIT_DEFAULT_HOST;
        $port = $config[RouterConfig::ROUTER_CONNECTION_PORT] ?? RouterConfig::AMQP_RABBIT_DEFAULT_PORT;
        $login = $config[RouterConfig::ROUTER_CONNECTION_LOGIN] ?? RouterConfig::AMQP_RABBIT_DEFAULT_LOGIN;
        $password = $config[RouterConfig::ROUTER_CONNECTION_PASSWORD] ?? RouterConfig::AMQP_RABBIT_DEFAULT_PASSWORD;
        
        $defaultServer = new RouterServer(
            $host, 
            $port,
            $login, 
            $password,
            RouterConfig::SERVER_TAG_DEFAULT
        );
        
        $this->registry->registerServer($defaultServer);
        
        return $this;
    }
    
    /**
     * Зарегистрировать нахождение очереди на определенном сервере
     *
     * @param $queueName
     * @param $host
     * @param $port
     *
     * @return bool
     */
    public function registerQueue($queueName, $host, $port = RouterConfig::AMQP_RABBIT_DEFAULT_PORT)
    {
        $queueName = $this->_prepareQueueName($queueName);
        $server    = $this->registry->findServer($host, $port);
        
        if (!$server) {
            $server = new RouterServer($host, $port);
            $this->registry->registerServer($server);
        }
        
        $this->registry->registerQueueToServer($queueName, $server);
        
        return $queueName;
    }
    
    
    public function publish($messageData, $queueNameRaw, $needReply = false, $params = [])
    {
        $messageString = $this->_prepareMessage($messageData);
        
        if (!$messageString) {
            throw new EmptyRouterMessage();
        }
        
        $thread = self::THREAD_MAIN;
        
        $queueName = $this->_prepareQueueName($queueNameRaw);
        
        $server = $this->_getServerOrFail($queueName, RouterConfig::SERVER_TAG_DEFAULT);
        
        $publisher = $server->getPublisher($thread);
        
        $correlationId = true;
        
        if ($needReply) {
            // setup reply queue
            $server->getQueue($thread, $this->replyQueueName, RouterConfig::CONFIG_REPLY_CONSUMER);
            
            $correlationId = $this->_getCorrelationId();
            
            $params += [
                'correlation_id' => $correlationId, // Связующий ID
                'reply_to'       => $this->replyQueueName,
            ];
        }
        
        $res = $publisher->publish($messageString, $queueName, $params);
        
        return $res ? $correlationId : false;
    }
    
    public function reply($replyData, $replyQueue, $corrId)
    {
        $queueName = $this->_prepareQueueName($replyQueue);
        
        $messageString = $this->_prepareMessage($replyData);
        if ($messageString === null) {
            throw new EmptyRouterMessage('Message body is empty or cannot be encoded');
        }
        
        $server = $this->_getServerOrFail($queueName, RouterConfig::SERVER_TAG_DEFAULT);
        
        $publisher = $server->getPublisher(self::THREAD_MAIN);
        
        $params = [
            'correlation_id' => $corrId, // Связующий ID
        ];
        
        return $publisher->publish($messageString, $queueName, $params);
    }
    
    /**
     * @param      $queueName
     *
     * @param      $timeout
     *
     * @param null $refresh
     *
     * @return RouterRequestConsumer
     */
    public function getConsumer($queueName, $timeout = null, $refresh = null)
    {
        $queueName = $this->_prepareQueueName($queueName);
        $thread    = self::THREAD_CONSUME . $queueName.':'.$timeout;
        
        $server = $this->_getServerOrFail($queueName, RouterConfig::SERVER_TAG_DEFAULT);
        
        $config = [
            RouterConfig::REQUEST_CONSUMER_TIME_LIMIT => $timeout,
            RouterConfig::REQUEST_CONSUMER_REFRESH_INTERVAL => $refresh 
        ];
        
        $consumer = $server->getRequestConsumer($thread, $queueName, RouterConfig::CONFIG_PERSISTENT_CONSUMER, $config);
        
        return $consumer;
    }
    
    /**
     * @param $queueName
     *
     * @return Actors\RouterReplyReader
     */
    public function getRequestReader ($queueName) 
    {
        $forwardQueueName = $this->_prepareQueueName($queueName);
        
        $server = $this->_getServerOrFail($forwardQueueName, RouterConfig::SERVER_TAG_DEFAULT);
    
        return $server->getReplyReader(self::THREAD_MAIN, $this->replyQueueName, RouterConfig::CONFIG_REPLY_CONSUMER);
    }
    
    public function readResult($forwardQueueName, $correlationId, $timeout)
    {
        /* prepare read result */
        $forwardQueueName = $this->_prepareQueueName($forwardQueueName);
        
        $server = $this->_getServerOrFail($forwardQueueName, RouterConfig::SERVER_TAG_DEFAULT);
        
        $replyReader = $server->getReplyReader(self::THREAD_MAIN, $this->replyQueueName, RouterConfig::CONFIG_REPLY_CONSUMER);
        
        return $replyReader->read($correlationId, $timeout);
    }
    
    /**
     * INTERNAL FUNCTIONS
     */
    
    /**
     * @param $queueName
     *
     * @return string
     */
    private function _prepareQueueName($queueName)
    {
        return strtolower($queueName);
    }

    /**
     * @param $queueName
     * @param $serverTag
     * 
     * @return \Verse\Router\Model\RouterServer
     */
    private function _getServerOrFail($queueName, $serverTag) : RouterServer
    {
        $server = $this->registry->findServerForQueue($queueName, $serverTag);
        if (!$server) {
            throw new ServerNotConfigured('Server not found for queue: "' .$queueName. '" or tag: "'.$serverTag.'"');
        }
        
        return $server;
    }
    
    /**
     * @param $message
     *
     * @return null|string
     */
    private function _prepareMessage($message)
    {
        if (is_string($message) || is_numeric($message)) {
            return $message;
        }
        
        if (is_array($message)) {
            return json_encode($message, JSON_UNESCAPED_UNICODE);
        }
        
        return null;
    }
    
    
    /**
     * @return string
     */
    private function _getCorrelationId()
    {
        return Uuid::v4();
    }
    
    /**
     * @return RouterRegistry
     */
    public function getRegistry()
    {
        return $this->registry;
    }
    
    public function disconnectAll()
    {
        foreach ($this->registry->getServers() as $server) {
            $server->disconnectAll();
        }
    }
    
    function __destruct()
    {
        $this->disconnectAll();
    }
    
    /**
     * @return string
     */
    public function getReplyQueueName()
    {
        return $this->replyQueueName;
    }
}