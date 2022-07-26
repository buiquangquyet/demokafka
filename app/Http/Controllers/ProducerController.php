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
        $rk = new Producer($conf);
        $rk->addBrokers("127.0.0.1:9092");

        $producer = new Producer($conf);
        $timeout_ms = 10000;
        $kafkaPurgeFlag = (int)env('KAFKA_PURGE_FLAG', 0);
        $payload = ['ptest producer 1'];
        try {
            $topic = $producer->newTopic(env('KAFKA_QUEUE'));
            $topic->produce(RD_KAFKA_PARTITION_UA, 0, json_encode($payload));
            if ($kafkaPurgeFlag) {
                $producer->purge(RD_KAFKA_PURGE_F_QUEUE);
            }
            //$producer->flush($timeout_ms);
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
