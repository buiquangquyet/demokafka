<?php

namespace App\Http\Controllers;

use Illuminate\Http\Request;
use RdKafka\Conf;
use RdKafka\Producer;


class ProducerController extends Controller
{
    /**
     * Handle the incoming request.
     *
     * @param  \Illuminate\Http\Request  $request
     * @return \Illuminate\Http\Response
     */
    public function __invoke(Request $request)
    {
        //
    }

    public function index(){
        echo __METHOD__;
        $conf = new Conf();
        $conf->set('log_level', (string) LOG_DEBUG);
        $conf->set('debug', 'all');
        $producer = new Producer($conf);
        $producer->addBrokers("127.0.0.1:9092");

        $timeout_ms = 10000;
        $kafkaPurgeFlag = (int)env('KAFKA_PURGE_FLAG', 0);
        $payload = 'hello kship toi co webhook ve';
        try {
            $topic = $producer->newTopic(env('KAFKA_QUEUE'));
            $topic->produce(RD_KAFKA_PARTITION_UA, env('KAFKA_PARTITION'), $payload);
            if ($kafkaPurgeFlag) {
                $producer->purge(RD_KAFKA_PURGE_F_QUEUE);
            }
            $producer->flush($timeout_ms);
            dd($producer);
        } catch (\Exception $e) {
            if ($kafkaPurgeFlag) {
                $producer->purge(RD_KAFKA_PURGE_F_QUEUE);
            }
            $producer->flush($timeout_ms);
            dd($e->getMessage());
        }

    }
}
