<?php


namespace Verse\Router\Actors;


use Verse\Router\Model\RouterModuleProto;
use Verse\Router\RouterConfig;

abstract class RouterActorProto extends RouterModuleProto
{
    protected $taskRetryCount;
    
    protected $recoveryRetrySleep;
    
    abstract protected function processTask();
    
    final public function setup()
    {
        $this->loadDeps();
        
        $this->configuration[RouterConfig::ACTOR_TASK_RETRY_COUNT] = 
            $this->getConfig(RouterConfig::ACTOR_TASK_RETRY_COUNT, RouterConfig::ACTOR_DEFAULT_TASK_RETRY_COUNT);
        
        $this->configuration[RouterConfig::ACTOR_TASK_RETRY_SLEEP] = 
            $this->getConfig(RouterConfig::ACTOR_TASK_RETRY_SLEEP, RouterConfig::ACTOR_DEFAULT_TASK_RETRY_SLEEP);
    }
    
    abstract public function loadDeps();
    
    final protected function run()
    {
        $maxRetryCount = $this->configuration[RouterConfig::ACTOR_TASK_RETRY_COUNT] + 1;
        do {
            try {
                return $this->processTask();
            } catch (\Exception $exception) {
                $this->server->runtime(__METHOD__ . ' exception', [ 
                        'msg' => $exception->getMessage(),
                        'line' => $exception->getLine(),
                        'file' => $exception->getFile(),
                    ]
                );
                usleep($this->configuration[RouterConfig::ACTOR_TASK_RETRY_SLEEP]);
                $this->server->reportProblem($this, $exception->getMessage());
            }
        } while ($maxRetryCount-- > 0);
        
        return null;
    }
}