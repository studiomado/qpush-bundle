<?php

/**
 * Copyright 2014 Underground Elephant
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * @package     qpush-bundle
 * @copyright   Underground Elephant 2014
 * @license     Apache License, Version 2.0
 */

namespace Uecode\Bundle\QPushBundle\Tests\Provider;

use Uecode\Bundle\QPushBundle\Provider\BeanstalkdProvider;

use Uecode\Bundle\QPushBundle\Event\MessageEvent;
use Uecode\Bundle\QPushBundle\Event\NotificationEvent;

use Uecode\Bundle\QPushBundle\Message\Message;
use Uecode\Bundle\QPushBundle\Message\Notification;

use Uecode\Bundle\QPushBundle\Tests\MockClient\BeanstalkdMockClient;

/**
 * @author Keith Kirk <kkirk@undergroundelephant.com>
 */
class BeanstalkdProviderTest extends \PHPUnit_Framework_TestCase
{
    /**
     * Mock Client
     *
     * @var BeanstalkdProvider
     */
    protected $provider;

    public function setUp()
    {
        $this->provider = $this->getBeanstalkdProvider();
    }

    public function tearDown()
    {
        $this->provider = null;
    }

    private function getBeanstalkdProvider(array $options = [])
    {
        $options = array_merge(
            [
                'logging_enabled'            => false,
                'push_notifications'         => true,
                'push_type'                  => 'multicast',
                'notification_retries'       => 3,
                'notification_retries_delay' => 60,
                'message_delay'              => 0,
                'message_timeout'            => 30,
                'message_expiration'         => 604800,
                'messages_to_receive'        => 1,
                'rate_limit'                 => -1,
                'receive_wait_time'          => 3,
                'subscribers'                => [
                    [ 'protocol' => 'http', 'endpoint' => 'http://fake.com' ]
                ]
            ],
            $options
        );

        $client = new BeanstalkdMockClient([
            'host'    => '123_this_is_a_host',
            'port'    => '123_this_is_a_port',
        ]);

        return new BeanstalkdProvider(
            'test',
            $options,
            $client,
            $this->getMock(
                'Doctrine\Common\Cache\PhpFileCache',
                [],
                ['/tmp', 'qpush.beanstalkd.test.php']
            ),
            $this->getMock(
                'Symfony\Bridge\Monolog\Logger',
                [],
                ['qpush.test']
            )
        );
    }

    public function testGetProvider()
    {
        $provider = $this->provider->getProvider();

        $this->assertEquals('Beanstalkd', $provider);
    }

    public function testCreate()
    {
        $this->assertTrue($this->provider->create());
    }

    public function testDestroy()
    {
        $this->assertTrue($this->provider->destroy());
    }

    public function testPublish()
    {
        $this->assertEquals(123, $this->provider->publish(['foo' => 'bar']));
    }

    public function testReceive()
    {
        $messages = $this->provider->receive();
        $this->assertInternalType('array', $messages);
        $this->assertEquals(['foo' => 'bar'], $messages[0]->getBody());
    }

    public function testDelete()
    {
        // First call returns true when the queue exists
        $this->assertTrue($this->provider->delete(123));

        // Second call catches exception and returns true when the queue
        // does not exists
        $this->assertTrue($this->provider->delete(456));

        // Last call throws an exception if there is an exception outside
        // of a HTTP 404
        $this->setExpectedException('Exception');
        $this->provider->delete(789);
    }

    public function testOnMessageReceived()
    {
        $this->provider->onMessageReceived(new MessageEvent(
            'test',
            new Message(123, ['foo' => 'bar'], [])
        ));
    }
}
