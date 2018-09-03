<?php


namespace Verse\Router\Model;


abstract class RouterModuleProto
{
    /**
     * Айди модуля
     * 
     * @var string
     */
    private $id;
    
    /**
     * Сервер которому принадлежит модуль
     *
     * @var RouterServer
     */
    protected $server;
    
    /**
     * Поток которому принадлежит модуль
     *
     * @var string
     */
    protected $thread;
    
    /**
     * Конфигурация модуля
     * 
     * @var []
     */
    protected $configuration = [];
    
    /**
     * Время когда объект был создан
     * 
     * @var integer
     */
    protected $createdAt;
    
    /**
     * Айди текущей проблемы
     * 
     * @var
     */
    protected $currentProblem;
    
    /**
     * RouterServerConnection constructor.
     *
     * @param RouterServer $server
     * @param string       $thread
     * @param array        $configuration
     */
    public function __construct(RouterServer $server, $thread, $configuration = [])
    {
        $this->server = $server;
        $this->thread = $thread;
        $this->configuration = $configuration;
        $this->createdAt = time();
        
        $this->id = get_called_class().':'.$thread.':'.$server->getId();
    }
    
    /**
     * Запрос на установку соединения
     * 
     * @return mixed
     */
    abstract public function setup();
    
    /**
     * Запрос на восстановление
     * 
     * @return mixed
     */
    abstract public function recovery();
    
    protected function _reportProblem($problem) {
        !$this->currentProblem && $this->currentProblem = $problem;
        $this->server->reportProblem($this, $problem);
        $this->currentProblem = null;
    }
    
    
    /**
     * @return string
     */
    public function getId()
    {
        return $this->id;
    }
    
    /**
     * @return string
     */
    public function getThread()
    {
        return $this->thread;
    }
    
    /**
     * @return integer
     */
    public function getCreatedAt()
    {
        return $this->createdAt;
    }
    
    /**
     * @return array
     */
    public function getFullConfiguration()
    {
        return $this->configuration;
    }
    
    public function getConfig ($configField, $default = null) 
    {
        return isset($this->configuration[$configField]) ? $this->configuration[$configField] : $default;
    }
    
    public function setConfig ($configField, $value) 
    {
        $this->configuration[$configField] = $value;
    }
    
    public function setConfigArray (array $configValues)
    {
        $this->configuration = $configValues + $this->configuration;
    }
}