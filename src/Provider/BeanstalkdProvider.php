<?php
namespace Uecode\Bundle\QPushBundle\Provider;

use Doctrine\Common\Cache\Cache;
use Monolog\Logger;
use Uecode\Bundle\QPushBundle\Event\MessageEvent;
use Uecode\Bundle\QPushBundle\Message\Message;
use Pheanstalk\Pheanstalk;

class BeanstalkdProvider extends AbstractProvider
{
    /**
     * @var Pheanstalk
     */
    protected $pheanstalk;

    public function __construct($name, array $options, $client, Cache $cache, Logger $logger) {
        $this->name     = $name;
        $this->options  = $options;
        $this->cache    = $cache;
        $this->logger   = $logger;
        $this->pheanstalk = $client;
    }

    public function getProvider()
    {
        return 'Beanstalkd';
    }

    public function create()
    {
        return true;
    }

    public function publish(array $message, array $options = [])
    {
        $publishStart = microtime(true);

        $id = $this->pheanstalk
            ->useTube($this->getNameWithPrefix())
            ->put(json_encode($message));

        $context = [
            'message_id'    => $id,
            'publish_time'  => microtime(true) - $publishStart
        ];
        $this->log(200, "Message has been published.", $context);

        return $id;
    }

    /**
     * @param array $options
     * @return Message[]
     */
    public function receive(array $options = [])
    {
        $job = $this->pheanstalk
            ->watch($this->getNameWithPrefix())
            ->ignore('default')
            ->reserve();

        $message = new Message($job->getId(), $job->getData(), []);
        $this->log(200, "Message has been received.", ['message_id' => $job->getId()]);

        $messages = [];
        $messages[] = $message;

        return $messages;
    }

    public function delete($id)
    {
        $job = $this->pheanstalk->peek($id);

        $this->pheanstalk->delete($job);

        $context = [
            'message_id'    => $id
        ];
        $this->log(200,"Message deleted from Beanstalkd", $context);

        return true;
    }

    public function destroy()
    {
        return true;
    }

    /**
     * Removes the message from queue after all other listeners have fired
     *
     * If an earlier listener has erred or stopped propagation, this method
     * will not fire and the Queued Message should become visible in queue again.
     *
     * Stops Event Propagation after removing the Message
     *
     * @param MessageEvent $event The SQS Message Event
     * @return bool|void
     */
    public function onMessageReceived(MessageEvent $event)
    {
        $id = $event->getMessage()->getId();
        $this->delete($id);
        $event->stopPropagation();
    }
}
