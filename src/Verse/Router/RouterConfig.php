<?php


namespace Verse\Router;


class RouterConfig
{
    const SERVER_TAG_DEFAULT = 'default';

    const AMQP_RABBIT_DEFAULT_PORT     = 5672;
    const AMQP_RABBIT_DEFAULT_HOST     = 'localhost';
    const AMQP_RABBIT_DEFAULT_LOGIN    = 'guest';
    const AMQP_RABBIT_DEFAULT_PASSWORD = 'guest';
    const AMQP_RABBIT_DEFAULT_VHOST = '/';
    
    const REASON_MAIN = 'main';
    
    const QUEUE_FLAGS     = 'flags';
    const QUEUE_NAME      = 'name';
    const QUEUE_ARGUMENTS = 'arguments';
    const QUEUE_EXCHANGE  = 'exchange';
    const QUEUE_REDECLARE = 'redeclare';
    
    const QUEUE_AUTO_DELETE_PROTECT = 'auto_delete';
    
    const QUEUE_AUTO_DELETE_TTL = 3600; // 1h 
    const QUEUE_REDECLARE_TTL   = 2700; // 45m
    
    const COMMON_QUEUE_NAME = 'read_queue_name';
    const COMMON_QUEUE_CONFIGURATION_ID = 'queue_config_id';
    
    const REPLY_READER_AUTO_ACK               = 'auto_ack';
    
    const REQUEST_CONSUMER_AUTO_ACK           = 'auto_ack';
    const REQUEST_CONSUMER_TIME_LIMIT         = 'time_limit';
    const REQUEST_CONSUMER_REFRESH_INTERVAL   = 'refresh_interval';
    
    /* connection config */
    const ROUTER_CONNECTION_HOST            = 'host';
    const ROUTER_CONNECTION_PORT            = 'port';
    const ROUTER_CONNECTION_LOGIN           = 'login';
    const ROUTER_CONNECTION_PASSWORD        = 'password';
    const ROUTER_CONNECTION_VHOST           = 'vhost';
    
    const ROUTER_CONNECTION_READ_TIMEOUT    = 'read_timeout';
    const ROUTER_CONNECTION_WRITE_TIMEOUT   = 'write_timeout';
    const ROUTER_CONNECTION_CONNECT_TIMEOUT = 'connect_timeout';
    
    // defaults
    const ROUTER_CONNECTION_DEFAULT_READ_TIMEOUT    = 30;
    const ROUTER_CONNECTION_DEFAULT_WRITE_TIMEOUT   = 20;
    const ROUTER_CONNECTION_DEFAULT_CONNECT_TIMEOUT = 3;
    
    /* channel config */
    const ROUTER_CHANNEL_PREFETCH_SIZE = 'prefetch_size';
    // defaults
    const ROUTER_CHANNEL_DEFAULT_PREFETCH_SIZE = 0;
    
    const CONSUMER_CALLBACK               = 'callback';
    const CONSUMER_AUTO_ACK               = 'auto_ack';
    const CONSUMER_QUEUE_NAME             = 'queue_name';
    const CONSUMER_QUEUE_CONFIGURATION_ID = 'queue_config_id';
    
    const PUBLISHER_MESSAGE      = 'msg';
    const PUBLISHER_TARGET_QUEUE = 'queue';
    const PUBLISHER_FLAGS        = 'flags';
    const PUBLISHER_PARAMS       = 'params';
    
    const ACTOR_TASK_RETRY_COUNT = 'retry';
    const ACTOR_TASK_RETRY_SLEEP = 'sleep';
    
    const ACTOR_DEFAULT_TASK_RETRY_COUNT = 20;
    const ACTOR_DEFAULT_TASK_RETRY_SLEEP = 50000; //mksec
    
    public static $queueConfig         = [];
    public static $consumerQueueConfig = [];
    
    const CONFIG_REPLY_CONSUMER      = 'reply';
    const CONFIG_PERSISTENT_CONSUMER = 'persist_consumer';
    const CONFIG_TEMPORARY_CONSUMER  = 'tmp_consumer';
    
    public static function getQueueConfiguration($configId)
    {
        !self::$queueConfig
        && self::$queueConfig = [
            self::CONFIG_REPLY_CONSUMER => [
                self::QUEUE_AUTO_DELETE_PROTECT => true,
                
                self::QUEUE_FLAGS     => AMQP_AUTODELETE,
                self::QUEUE_ARGUMENTS => [
                    'x-expires' => 3600 * 1000 // 1h (msec)
                ],
                self::QUEUE_REDECLARE => 2700 // 45m (sec)
            ],
            
            self::CONFIG_PERSISTENT_CONSUMER => [
                self::QUEUE_FLAGS => AMQP_DURABLE,
            ],
            
            self::CONFIG_TEMPORARY_CONSUMER => [
                self::QUEUE_FLAGS     => AMQP_AUTODELETE | AMQP_DURABLE,
                self::QUEUE_ARGUMENTS => [
                    'x-expires' => 3600 * 24 * 1000 // 24h (msec)
                ],
                self::QUEUE_REDECLARE => 3600 * 24 // 45m (sec)
            ]
        ];
        
        return isset(self::$queueConfig[$configId]) ? self::$queueConfig[$configId] : [];
    }
    
    public static function getConsumerQueueConfig()
    {
        return;
    }
    
    public static function getReplyQueueName($nodeId)
    {
        return 'reply:' . $nodeId;
    }
}