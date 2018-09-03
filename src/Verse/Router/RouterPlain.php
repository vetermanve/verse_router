<?php


namespace Verse\Router;


use Mu\Env;
use Verse\Router\Exceptions\EmptyRouterMessage;
use Uuid\Uuid;

class RouterPlain
{
    const ALL_QUEUES = '*';
    
    const DEFAULT_EXCHANGE = '';
    
    const QUEUE_AUTO_DELETE_TTL = 3600; // 1h 
    const QUEUE_REDECLARE_TTL   = 2700; // 45m 
    
    const SERVER_HOST        = 'host';
    const SERVER_PORT        = 'port';
    const SERVER_CONNECTION  = 'connection';
    const SERVER_CHANNELS    = 'channels';
    const SERVER_EXCHANGES   = 'exchanges';
    const SERVER_QUEUES      = 'queues';
    const SERVER_RESULT_POOL = 'resultPool';
    const SERVER_Q_REDECLARE = 'queuesRedeclare';
    
    const REASON_MAIN          = 'main';
    const REASON_CONSUME_REPLY = 'consumeReply';
    const REASON_CONSUME       = 'consume:';
    
    private $registeredQueues = [];
    private $servers          = [];
    
    private $defaultServerId = '';
    private $nodeId          = '';
    
    /**
     * PUBLIC API 
     */
    
    /**
     * Инициализация модуля
     *
     * @param \Mu\Interfaces\ConfigInterface $config
     *
     * @return $this
     */
    public function init($config = null)
    {
        $config = $config ?: Env::getLegacyConfig();
        $host = $config->get('host', 'amqp', 'localhost');
        $port = $config->get('port', 'amqp', 5672);
        
        $this->defaultServerId = $host . ':' . $port;
        
        $this->servers[$this->defaultServerId] = [
            self::SERVER_HOST => $host,
            self::SERVER_PORT => $port
        ];
        
        $this->registeredQueues = [
            self::ALL_QUEUES => $this->defaultServerId
        ];
        
        $this->nodeId = $this->_prepareQueueName(gethostname() . ':' . getmypid());
        
        return $this;
    }
    
    /**
     * Зарегистрировать нахождение очереди на определенном сервере
     *
     * @param $queue
     * @param $host
     * @param $port
     *
     * @return bool
     */
    public function registerQueue($queue, $host, $port = 5672)
    {
        $hostId = $host . ':' . $port;
        if ($hostId === $this->defaultServerId) {
            return true;
        }
        
        $queue = $this->_prepareQueueName($queue);
        
        if (!isset($this->servers[$hostId])) {
            $this->servers[$hostId] = [
                self::SERVER_HOST => $host,
                self::SERVER_PORT => $port
            ];
        }
        
        $this->registeredQueues[$queue] = $hostId;
        
        return true;
    }
    
    
    public function publish($messageData, $queueNameRaw, $needReply = false, $params = [])
    {
        $queueName = $this->_prepareQueueName($queueNameRaw);
        $exchange  = $this->_ensureExchangeForQueue($queueName);
        
        $messageString = $this->_prepareMessage($messageData);
        
        if (!$messageString) {
            throw new EmptyRouterMessage();
        }
        
        $resultId = true;
        
        if ($needReply) {
            $resultId = $this->_getCorrelationId();
            
            $params += [
                'correlation_id' => $resultId, // Связующий ID
                'reply_to'       => $this->_getReplyQueue($queueName)->getName(),
            ];
        }
        
        $res = $exchange->publish($messageString, $queueName, AMQP_NOPARAM, $params);
        
        return $res ? $resultId : false;
    }
    
    public function reply($replyData, $replyQueue, $corrId)
    {
        $queueName = $this->_prepareQueueName($replyQueue);
        $exchange  = $this->_ensureExchangeForQueue($queueName);
        
        $messageString = $this->_prepareMessage($replyData);
        
        if (is_null($messageString)) {
            throw new EmptyRouterMessage();
        }
        
        $params = [
            'correlation_id' => $corrId, // Связующий ID
        ];
        
        return $exchange->publish($messageString, $queueName, AMQP_NOPARAM, $params);
    }
    
    /**
     * @param $queueName
     *
     * @param $timeout
     *
     * @return RequestConsumer
     */
    public function getConsumer($queueName, $timeout = null)
    {
        $queue = $this->_getConsumeQueue($queueName);
        
        $consumer = new RequestConsumer();
        $consumer->setQueue($queue);
        $consumer->setTimeLimit($timeout);
        
        return $consumer;
    }
    
    
    public function readResult($forwardQueueName, $correlationId, $timeout)
    {
        /* prepare read result */
        $forwardQueueName = $this->_prepareQueueName($forwardQueueName);
        
        $pool = $this->_getResultPull($forwardQueueName);
        
        return $pool->read($correlationId, $timeout);
    }
    
    /**
     * @return array
     */
    public function getServers()
    {
        return $this->servers;
    }
    
    /**
     * INTERNAL FUNCTIONS 
     */
    
    private function &_getReplyQueue($forwardQueueName)
    {
        $serverId = $this->_getQueueServerId($forwardQueueName);
        $server   = &$this->servers[$serverId];
        
        $reason = self::REASON_MAIN;
        
        if (isset($server[self::SERVER_QUEUES][$reason])) {
            // пересоздаем очереди каждые 45 минут.
            if ($server[self::SERVER_Q_REDECLARE][$reason] > time() - self::QUEUE_REDECLARE_TTL) {
                return $server[self::SERVER_QUEUES][$reason];
            }
        }
        
        $this->_serverEnsureConnection($server, $reason);
        $channel = $this->_serverEnsureChannel($server, $reason);
        
        $queueName = 'reply:' . $this->nodeId;
        
        $queue = new \AMQPQueue($channel);
        $queue->setFlags(AMQP_AUTODELETE);
        $queue->setName($queueName);
        $queue->setArgument('x-expires', self::QUEUE_AUTO_DELETE_TTL * 1000); // очередь самоудалится через час
        $queue->declareQueue();
        if (self::DEFAULT_EXCHANGE) {
            $queue->bind(self::DEFAULT_EXCHANGE, $queueName);
        }
        
        $server[self::SERVER_QUEUES][$reason]      = $queue;
        $server[self::SERVER_Q_REDECLARE][$reason] = time();
        
        return $server[self::SERVER_QUEUES][$reason];
    }
    
    private function _getConsumeQueue($queueName)
    {
        $serverId = $this->_getQueueServerId($queueName);
        $server   = &$this->servers[$serverId];
        
        $reason = self::REASON_CONSUME . $queueName;
        
        if (isset($server[self::SERVER_QUEUES][$reason])) {
            return $server[self::SERVER_QUEUES][$reason];
        }
        
        $this->_serverEnsureConnection($server, $reason);
        $channel  = $this->_serverEnsureChannel($server, $reason);
        $this->_ensureExchangeForQueue($queueName);
        
        $queue = new \AMQPQueue($channel);
        $queue->setFlags(AMQP_DURABLE);
        $queue->setName($queueName);
        $queue->declareQueue();
        if (self::DEFAULT_EXCHANGE) {
            $queue->bind(self::DEFAULT_EXCHANGE, $queueName);
        }
        
        $server[self::SERVER_QUEUES][$reason] = $queue;
        
        return $queue;
    }
    
    
    /**
     * @param $queue
     *
     * @return \AMQPExchange
     */
    private function _ensureExchangeForQueue($queue)
    {
        $serverId = $this->_getQueueServerId($queue);
        $server   = &$this->servers[$serverId];
        
        $reason       = self::REASON_MAIN;
        $exchangeName = self::DEFAULT_EXCHANGE;
        
        if (isset($server[self::SERVER_EXCHANGES][$reason][$exchangeName])) {
            return $server[self::SERVER_EXCHANGES][$reason][$exchangeName];
        }
        
        $this->_serverEnsureConnection($server, $reason);
        $this->_serverEnsureChannel($server, self::REASON_MAIN);
        $exchangeObj = $this->_setupExchange($server, self::REASON_MAIN, $exchangeName);
        
        $server[self::SERVER_EXCHANGES][$reason][$exchangeName] = $exchangeObj;
        
        return $exchangeObj;
    }
    
    private function _getQueueServerId($queue)
    {
        return isset($this->registeredQueues[$queue]) ? $this->registeredQueues[$queue] : $this->defaultServerId;
    }
    
    
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
     * @param $server
     *
     * @param $reason
     *
     * @return \AMQPConnection
     */
    private function _serverEnsureConnection(&$server, $reason)
    {
        if (isset($server[self::SERVER_CONNECTION][$reason])) {
            return $server[self::SERVER_CONNECTION][$reason];
        }
        
        $connection = $this->_setupConnection($server);
        
        if (!$connection->isConnected()) {
            if (!$connection->reconnect()) {
                $connection = $this->_setupConnection($server);
                
                unset($server[self::SERVER_CHANNELS]);
                unset($server[self::SERVER_EXCHANGES]);
                unset($server[self::SERVER_QUEUES]);
            }
        }
        
        $server[self::SERVER_CONNECTION][$reason] = $connection;
        
        return $connection;
    }
    
    /**
     * @param $server
     *
     * @return \AMQPConnection
     */
    private function _setupConnection($server)
    {
        $connectionData = [
            'host'            => $server[self::SERVER_HOST],
            'port'            => $server[self::SERVER_PORT],
            'read_timeout'    => 30,
            'write_timeout'   => 20,
            'connect_timeout' => 3,
            //     *      'login' => amqp.login The login name to use. Note: Max 128 characters.
            //     *      'password' => amqp.password Password. Note: Max 128 characters.
        ];
            
        $connection = new \AMQPConnection($connectionData);
        
        try {
            $connection->connect();
        } catch (\Exception $e) {
            Env::getLogger()->error($e->getMessage(), $connectionData);
            throw $e;
        }
        
        return $connection;
    }
    
    /**
     * @param $server
     * @param $reason
     *
     * @return \AMQPChannel
     */
    private function _serverEnsureChannel(&$server, $reason)
    {
        $channel = isset($server[self::SERVER_CHANNELS][$reason]) ? $server[self::SERVER_CHANNELS][$reason] : null;
        
        if (!$channel) {
            $channel = $this->_setupChannel($server[self::SERVER_CONNECTION][$reason]);
            
            $server[self::SERVER_CHANNELS][$reason] = $channel;
        }
        
        return $channel;
    }
    
    /**
     * @param \AMQPConnection $connection
     *
     * @return \AMQPChannel
     */
    private function _setupChannel(\AMQPConnection $connection)
    {
        return new \AMQPChannel($connection);
    }
    
    /**
     * @param $server
     * @param $reason
     * @param $exchangeName
     *
     * @return \AMQPExchange
     */
    private function _setupExchange($server, $reason, $exchangeName)
    {
        $channel  = $server[self::SERVER_CHANNELS][$reason];
        $exchange = new \AMQPExchange($channel);
        $exchange->setName($exchangeName);
        if ($exchangeName) {
            $exchange->setType(AMQP_EX_TYPE_DIRECT);
            $exchange->setFlags(AMQP_DURABLE);
            $exchange->declareExchange();
        }
        
        return $exchange;
    }
    
    /**
     * @return string
     */
    private function _getCorrelationId()
    {
        return Uuid::v4();
    }
    
    /**
     * @param $forwardQueueName
     *
     * @return ResultPoolPlain
     */
    private function _getResultPull($forwardQueueName)
    {
        $serverId = $this->_getQueueServerId($forwardQueueName);
        $server   = &$this->servers[$serverId];
        
        if (isset($server[self::SERVER_RESULT_POOL])) {
            return $server[self::SERVER_RESULT_POOL];
        }
        
        $queue = &$this->_getReplyQueue($forwardQueueName);
        
        $pool = new ResultPoolPlain();
        $pool->setQueue($queue);
        
        $server[self::SERVER_RESULT_POOL] = $pool;
        
        return $server[self::SERVER_RESULT_POOL];
    }
}