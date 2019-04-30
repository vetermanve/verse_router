<?php


namespace Verse\Router\Model;


use AMQPConnection;
use Verse\Router\RouterConfig;

class RouterConnection extends RouterModuleProto
{
    const HOST = 'host';
    const PORT = 'port';
    
    const READ_TIMEOUT    = 'read_timeout';
    const WRITE_TIMEOUT   = 'write_timeout';
    const CONNECT_TIMEOUT = 'connect_timeout';
    const LOGIN           = 'login';
    const PASSWORD        = 'password';
    const VHOST           = 'vhost';

    /**
     * @var AMQPConnection;
     */
    public $amqpConnection;
    
    public function setup()
    {
        $connectionData = [
            self::HOST            => $this->configuration[RouterConfig::ROUTER_CONNECTION_HOST],
            self::PORT            => $this->configuration[RouterConfig::ROUTER_CONNECTION_PORT],
            self::LOGIN           => $this->configuration[RouterConfig::ROUTER_CONNECTION_LOGIN],
            self::PASSWORD        => $this->configuration[RouterConfig::ROUTER_CONNECTION_PASSWORD],
            self::VHOST           => $this->configuration[RouterConfig::ROUTER_CONNECTION_VHOST],
            
            self::READ_TIMEOUT    => $this->configuration[RouterConfig::ROUTER_CONNECTION_READ_TIMEOUT],
            self::WRITE_TIMEOUT   => $this->configuration[RouterConfig::ROUTER_CONNECTION_WRITE_TIMEOUT],
            self::CONNECT_TIMEOUT => $this->configuration[RouterConfig::ROUTER_CONNECTION_CONNECT_TIMEOUT],
        ];
        
        $connection = new AMQPConnection($connectionData);
        
        try {
            $connection->connect();
            $this->amqpConnection = $connection;
        } catch (\Exception $exception) {
            $this->_reportProblem($exception->getMessage());
        }
    }
    
    public function recovery()
    {
        $connection = $this->amqpConnection;
        if (!$this->amqpConnection || !$this->amqpConnection->isConnected()) {
            $this->setup();
            try {
                $connection && $connection->disconnect();
            } catch (\Exception $exception ){
                $this->server->log(__METHOD__.': previous connection destroy exception: ' .$exception->getMessage());
            }
        }
    }
    
    public function getAmqpConnectionOrFail() : AMQPConnection
    {
        if (!$this->amqpConnection) {
            throw new \RuntimeException('Empty amqpConnection');
        }
        return $this->amqpConnection;
    }
}
