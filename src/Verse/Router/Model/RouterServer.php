<?php


namespace Verse\Router\Model;

use Mu\Env;
use Verse\Router\Actors\RouterPublisher;
use Verse\Router\Actors\RouterReplyReader;
use Verse\Router\Actors\RouterRequestConsumer;
use Verse\Router\RouterConfig;
use Verse\Router\RouterProblemResolver;

class RouterServer
{
    /**
     * Хост
     *
     * @var string
     */
    private $host;
    
    /**
     * Порт
     *
     * @var integer
     */
    private $port;
    
    /**
     * Тэг сервера
     *
     * @see RouterConfig::SERVER_TAG_DEFAULT
     * @var string
     */
    private $tag;
    
    /**
     * Конекшны по тредам
     *
     * @var RouterConnection[]
     */
    private $connections = [];
    
    /**
     * Обменники
     *
     * @var RouterExchange[]
     */
    private $exchanges = [];
    
    /**
     * Каналы
     *
     * @var RouterChannel[]
     */
    private $channels = [];
    
    /**
     * Очереди
     *
     * @var RouterQueue[][]
     */
    private $queues = [];
    
    /**
     * Пул ответов единой ответной очереди
     *
     * @var RouterReplyReader[]
     */
    private $replyReaders = [];
    
    /**
     * @var RouterProblemResolver
     */
    private $problemResolver;
    
    /**
     * @var RouterPublisher[]
     */
    protected $publishers = [];
    
    /**
     * @var RouterRequestConsumer[]
     */
    protected $requestConsumers = [];

    /**
     * RouterServer constructor.
     *
     * @param string $host
     * @param int $port
     * @param string $user
     * @param string $password
     * @param string $tag
     */
    public function __construct(
        $host = RouterConfig::AMQP_RABBIT_DEFAULT_HOST, 
        $port = RouterConfig::AMQP_RABBIT_DEFAULT_PORT, 
        $user = RouterConfig::AMQP_RABBIT_DEFAULT_LOGIN,
        $password = RouterConfig::AMQP_RABBIT_DEFAULT_PASSWORD,
        $tag = null)
    {
        $this->host = $host;
        $this->port = $port;
        $this->user = $user;
        $this->password = $password;
        
        $this->tag  = $tag;
    }
    
    public function getPublisher($thread)
    {
        if (!isset($this->publishers[$thread])) {
            $pool = new RouterPublisher($this, $thread, []);
            $pool->setup();
            
            $this->publishers[$thread] = $pool;
        }
        
        return $this->publishers[$thread];
    }
    
    /**
     * @param       $thread
     * @param       $queueName
     * @param       $queueConfigId
     *
     * @param array $config
     *
     * @return RouterRequestConsumer
     * @internal param null $timeLimit
     *
     */
    public function getRequestConsumer($thread, $queueName, $queueConfigId, $config = [])
    {
        if (!isset($this->requestConsumers[$thread])) {
            $configuration = $config + [
                RouterConfig::COMMON_QUEUE_NAME             => $queueName,
                RouterConfig::COMMON_QUEUE_CONFIGURATION_ID => $queueConfigId,
            ];
            
            $requestConsumer = new RouterRequestConsumer($this, $thread, $configuration);
            $requestConsumer->setup();
            
            $this->requestConsumers[$thread] = $requestConsumer;
        }
        
        return $this->requestConsumers[$thread];
    }
    
    /**
     * @param $thread
     * @param $queueName
     * @param $queueConfigId
     *
     * @return RouterReplyReader
     */
    public function getReplyReader($thread, $queueName, $queueConfigId)
    {
        if (!isset($this->replyReaders[$thread])) {
            $configuration = [
                RouterConfig::REPLY_READER_AUTO_ACK         => false,
                RouterConfig::COMMON_QUEUE_NAME             => $queueName,
                RouterConfig::COMMON_QUEUE_CONFIGURATION_ID => $queueConfigId,
            ];
            
            $pool = new RouterReplyReader($this, $thread, $configuration);
            $pool->setup();
            
            $this->replyReaders[$thread] = $pool;
        }
        
        return $this->replyReaders[$thread];
    }
    
    public function getQueue($thread, $queueName, $configurationId)
    {
        if (!isset($this->queues[$thread][$queueName])) {
            $configuration                           = RouterConfig::getQueueConfiguration($configurationId);
            $configuration[RouterConfig::QUEUE_NAME] = $queueName;
            
            $queue = new RouterQueue($this, $thread, $configuration);
            $queue->setup();
            
            $this->queues[$thread][$queueName] = $queue;
        }
        
        return $this->queues[$thread][$queueName];
    }
    
    private $consumers = [];
    
    /**
     * @param $thread
     * @param $queueName
     * @param $queueConfigurationId
     * @param $callback
     *
     * @return RouterConsumer
     */
    public function getConsumer($thread, $queueName, $queueConfigurationId, $callback)
    {
        if (!isset($this->consumers[$thread][$queueName])) {
            $configuration = [
                RouterConfig::CONSUMER_CALLBACK               => $callback,
                RouterConfig::CONSUMER_QUEUE_CONFIGURATION_ID => $queueConfigurationId,
                RouterConfig::CONSUMER_QUEUE_NAME             => $queueName
            ];
            
            $consumer = new RouterConsumer($this, $thread, $configuration);
            $consumer->setup();
            
            $this->consumers[$thread][$queueName] = $consumer;
        }
        
        return $this->consumers[$thread][$queueName];
    }
    
    /**
     * @param $thread
     *
     * @return RouterExchange
     */
    public function getExchange($thread)
    {
        if (!isset($this->exchanges[$thread])) {
            $exchange = new RouterExchange($this, $thread);
            $exchange->setup();
            
            $this->exchanges[$thread] = $exchange;
        }
        
        return $this->exchanges[$thread];
    }
    
    /**
     * @param       $thread
     *
     * @param array $configuration
     *
     * @return RouterChannel
     */
    public function getChannel($thread, $configuration = [])
    {
        if (!isset($this->channels[$thread])) {
            $channel = new RouterChannel($this, $thread, $configuration);
            $channel->setup();
            
            $this->channels[$thread] = $channel;
        }
        
        return $this->channels[$thread];
    }
    
    /**
     * @param       $thread
     *
     * @param array $configuration
     *
     * @return RouterConnection
     */
    public function getConnection($thread, $configuration = [])
    {
        if (!isset($this->connections[$thread])) {
            $configuration = $configuration + [
                    RouterConfig::ROUTER_CONNECTION_HOST            => $this->host,
                    RouterConfig::ROUTER_CONNECTION_PORT            => $this->port,
                    RouterConfig::ROUTER_CONNECTION_READ_TIMEOUT    => RouterConfig::ROUTER_CONNECTION_DEFAULT_READ_TIMEOUT,
                    RouterConfig::ROUTER_CONNECTION_WRITE_TIMEOUT   => RouterConfig::ROUTER_CONNECTION_DEFAULT_WRITE_TIMEOUT,
                    RouterConfig::ROUTER_CONNECTION_CONNECT_TIMEOUT => RouterConfig::ROUTER_CONNECTION_DEFAULT_CONNECT_TIMEOUT,
                ];
            $connection    = new RouterConnection($this, $thread, $configuration);
            $connection->setup();
            
            $this->connections[$thread] = $connection;
        }
        
        return $this->connections[$thread];
    }
    
    public function reportProblem(RouterModuleProto $routerModule, $problem)
    {
        $startResolve = !$this->problemResolver;
        if ($startResolve) {
            $this->problemResolver = new RouterProblemResolver($problem);
        }
        
        if ($this->problemResolver->addReport($routerModule->getId(), $problem)) {
            $routerModule->recovery();
        }
        
        if ($startResolve) {
            $this->problemResolver = null;
        }
    }
    
    /**
     * @return mixed
     */
    public function getHost()
    {
        return $this->host;
    }
    
    /**
     * @param mixed $host
     */
    public function setHost($host)
    {
        $this->host = $host;
    }
    
    /**
     * @return mixed
     */
    public function getPort()
    {
        return $this->port;
    }
    
    /**
     * @param mixed $port
     */
    public function setPort($port)
    {
        $this->port = $port;
    }
    
    public function getId()
    {
        return $this->host . ':' . $this->port;
    }
    
    /**
     * @return mixed
     */
    public function getTag()
    {
        return $this->tag;
    }
    
    /**
     * @param mixed $tag
     */
    public function setTag($tag)
    {
        $this->tag = $tag;
    }
    
    public function log ($msg, $context = []) 
    {
        Env::isProfiling() && Env::getLogger()->debug($msg, $context);
    }
    
    public function runtime ($msg, $context = []) 
    {   
        false && Env::isDebugMode() && Env::getLogger()->debug($msg, $context);
    }
    
    public function disconnectAll()
    {
        foreach ($this->connections as $routerConnection) {
            $routerConnection->amqpConnection->disconnect();
        }
    }
}