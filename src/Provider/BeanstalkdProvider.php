<?php
namespace Uecode\Bundle\QPushBundle\Provider;

use Doctrine\Common\Cache\Cache;
use Monolog\Logger;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Finder\Finder;
use Symfony\Component\Finder\SplFileInfo;
use Uecode\Bundle\QPushBundle\Event\MessageEvent;
use Uecode\Bundle\QPushBundle\Message\Message;
use Pheanstalk\Pheanstalk;

class BeanstalkdProvider extends AbstractProvider
{
    protected $filePointerList = [];
    protected $queuePath;
    /**
     * @var Pheanstalk
     */
    protected $pheanstalk;

    public function __construct($name, array $options, $client, Cache $cache, Logger $logger) {
        $this->name     = $name;
        /* md5 only contain numeric and A to F, so it is file system safe */
        $this->queuePath = $options['path'].DIRECTORY_SEPARATOR.str_replace('-', '', hash('md5', $name));
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
        $options = $this->mergeOptions($options);
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
        $options = $this->mergeOptions($options);

        $job = $this->pheanstalk->
            ->watch($this->getNameWithPrefix())
            ->ignore('default')
            ->reserve();

        echo $job->getData();

        $pheanstalk->delete($job);
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

    public function cleanUp()
    {

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
