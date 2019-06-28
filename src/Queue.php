<?php

namespace HughCube\Yii\Queue\MNS;

use AliyunMNS\Client;
use AliyunMNS\Exception\MessageNotExistException;
use AliyunMNS\Exception\MnsException;
use AliyunMNS\Requests\SendMessageRequest;
use HughCube\Yii\Queue\MNS\Exceptions\Exception;
use yii\base\InvalidConfigException;

/**
 * Class Queue
 * @package xutl\queue
 */
class Queue extends \yii\queue\cli\Queue
{
    /**
     * @var string 阿里云的 access
     */
    public $access;

    /**
     * @var string 阿里云 secret
     */
    public $secret;

    /**
     * @var string 阿里云mns的地址
     */
    public $endPoint;

    /**
     * @var string 队列名字
     */
    public $queueName;

    /**
     * @var bool 是否使用base64编码
     */
    public $base64 = false;

    /**
     * @var \AliyunMNS\Queue
     */
    protected $_mnsQueue;

    /**
     * @var \AliyunMNS\Client 阿里云的mns的客户端
     */
    protected $_mnsClient;

    /**
     * @var string command class name
     */
    public $commandClass = Command::class;

    /**
     * @inheritdoc
     * @throws
     */
    public function init()
    {
        parent::init();

        if (empty ($this->endPoint)){
            throw new InvalidConfigException ('The "endPoint" property must be set.');
        }

        if (empty ($this->access)){
            throw new InvalidConfigException ('The "access" property must be set.');
        }

        if (empty ($this->secret)){
            throw new InvalidConfigException ('The "secret" property must be set.');
        }

        if (empty ($this->queueName)){
            throw new InvalidConfigException ('The "queueName" property must be set.');
        }
    }

    /**
     * @return \AliyunMNS\Queue
     */
    protected function getQueue()
    {
        if (null === $this->_mnsQueue){
            $this->_mnsQueue = $this->getClient()->getQueueRef($this->queueName);
        }

        return $this->_mnsQueue;
    }

    /**
     * @return \AliyunMNS\Client
     */
    protected function getClient()
    {
        if (null === $this->_mnsClient){
            $this->_mnsClient = new Client($this->endPoint, $this->access, $this->secret);
        }

        return $this->_mnsClient;
    }

    /**
     * Listens queue and runs each job.
     *
     * @param bool $repeat whether to continue listening when queue is empty.
     * @param int $timeout number of seconds to wait for next message.
     * @return null|int exit code.
     * @internal for worker command only.
     * @since 2.0.2
     */
    public function run($repeat, $waitSeconds = null)
    {
        return $this->runWorker(function (callable $canContinue) use ($repeat, $waitSeconds){
            while($canContinue()){
                if (($payload = $this->reserve($waitSeconds)) !== null){
                    list($id, $message, $ttr, $attempt, $receiptHandle) = $payload;
                    if ($this->handleMessage($id, $message, $ttr, $attempt)){
                        $this->delete($receiptHandle);
                    }
                }elseif (!$repeat){
                    break;
                }
            }
        });
    }

    /**
     * @inheritdoc
     * @throws \HughCube\Yii\Queue\MNS\Exceptions\Exception
     */
    public function status($id)
    {
        throw new Exception('Status is not supported in the driver.');
    }

    /**
     * 获取队列的属性
     *
     * @return array
     * @throws \HughCube\Yii\Queue\MNS\Exceptions\Exception
     */
    protected function getMnsQueueAttributes()
    {
        try{
            $response = $this->getQueue()->getAttribute();

            return $response->getQueueAttributes();
        }catch(MnsException $e){
            throw new Exception($e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * Deletes message by ID.
     *
     * @param int $receiptHandle of a message
     * @throws \HughCube\Yii\Queue\MNS\Exceptions\Exception
     */
    protected function delete($receiptHandle)
    {
        try{
            $response = $this->getQueue()->deleteMessage($receiptHandle);

            return $response->isSucceed();
        }catch(MnsException $e){
            throw new Exception($e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * @inheritdoc
     * @throws \HughCube\Yii\Queue\MNS\Exceptions\Exception
     */
    protected function pushMessage($message, $ttr, $delay, $priority)
    {
        try{
            $request = new SendMessageRequest($message, $delay, $priority, $this->base64);
            $res = $this->getQueue()->sendMessage($request);

            return $res->getMessageId();
        }catch(MnsException $e){
            throw new Exception($e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * @param int $timeout timeout
     * @return array|null payload
     * @throws \HughCube\Yii\Queue\MNS\Exceptions\Exception
     */
    protected function reserve($waitSeconds = null)
    {
        try{
            $response = $this->getQueue()->receiveMessage($waitSeconds);

            return [
                $response->getMessageId(),
                $response->getMessageBody(),
                0,
                round((((microtime(true) * 1000) - $response->getEnqueueTime()) / 1000), 3),
                $response->getReceiptHandle()
            ];
        }catch(MessageNotExistException $e){
            return null;
        }catch(MnsException $e){
            throw new Exception($e->getMessage(), $e->getCode(), $e);
        }
    }
}
