<?php
/**
 * Created by IntelliJ IDEA.
 * User: hugh.li
 * Date: 2019-06-27
 * Time: 18:36
 */

namespace HughCube\Yii\Queue\MNS;

use yii\queue\cli\Command as CliCommand;

/**
 * Class Command
 */
class Command extends CliCommand
{
    /**
     * @var Queue
     */
    public $queue;

    /**
     * Runs all jobs from beanstalk-queue.
     * It can be used as cron job.
     */
    public function actionRun($waitSeconds = null)
    {
        $this->queue->run(false, $waitSeconds);
    }

    /**
     * Listens beanstalk-queue and runs new jobs.
     * It can be used as demon process.
     */
    public function actionListen($waitSeconds = null)
    {
        $this->queue->run(true, $waitSeconds);
    }

    /**
     * @param string $actionID
     * @return bool
     * @since 2.0.2
     */
    protected function isWorkerAction($actionID)
    {
        return in_array($actionID, ['run', 'listen'], true);
    }
}
