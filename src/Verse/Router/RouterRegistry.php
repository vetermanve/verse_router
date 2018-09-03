<?php


namespace Verse\Router;


use Verse\Router\Model\RouterServer;

class RouterRegistry
{
    /**
     * @var RouterServer[]
     */
    private $servers = [];
    
    /**
     * @var RouterServer[][]
     */
    private $serversByHosts = [];
    
    /**
     * @var RouterServer[]
     */
    private $serversByTags = [];
    
    /**
     * @var RouterServer[]
     */
    private $serversByQueues = [];
    
    /**
     * @param RouterServer $server
     */
    public function registerServer (RouterServer $server) 
    {
        $this->servers[$server->getId()] = $server;
        $this->serversByHosts[$server->getHost()][$server->getPort()] = $server;
        
        if ($tag = $server->getTag()) {
            $this->serversByTags[$tag] = $server;   
        }
    }
    
    /**
     * @param $host
     * @param $port
     *
     * @return null|RouterServer
     */
    public function findServer($host, $port)
    {
        if (isset($this->serversByHosts[$host][$port])) {
            return $this->serversByHosts[$host][$port];
        }
        
        return null;
    }
    
    /**
     * @param              $queueName
     * @param RouterServer $server
     */
    public function registerQueueToServer ($queueName, RouterServer $server) 
    {
        $this->serversByQueues[$queueName] = $server;   
    }
    
    /**
     * @param $queueName
     * @param $serverTag
     *
     * @return RouterServer|null
     */
    public function findServerForQueue ($queueName, $serverTag) 
    {
        if (isset($this->serversByQueues[$queueName])) {
            return $this->serversByQueues[$queueName];
        }
        
        if (isset($this->serversByTags[$serverTag])) {
            return $this->serversByTags[$serverTag];
        }
        
        return null;
    }
    
    /**
     * @return RouterServer[]
     */
    public function getServers()
    {
        return $this->servers;
    }
}