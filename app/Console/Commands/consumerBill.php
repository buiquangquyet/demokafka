<?php

namespace App\Console\Commands;
use RdKafka\Conf;
use RdKafka\Consumer;

use Illuminate\Console\Command;

class consumerBill extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'consumer:bill';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'Command description';

    /**
     * Create a new command instance.
     *
     * @return void
     */
    public function __construct()
    {
        parent::__construct();
    }

    /**
     * Execute the console command.
     *
     * @return int
     */
    public function handle()
    {
        $conf = new Conf();
        $conf->set('log_level', (string) LOG_DEBUG);
        $conf->set('debug', 'all');
        $rk = new Consumer($conf);
        $rk->addBrokers("127.0.0.1");

//        $topic = $rk->newTopic(env('KAFKA_QUEUE'));
//
//// The first argument is the partition to consume from.
//// The second argument is the offset at which to start consumption. Valid values
//// are: RD_KAFKA_OFFSET_BEGINNING, RD_KAFKA_OFFSET_END, RD_KAFKA_OFFSET_STORED.
//        $topic->consumeStart(0, RD_KAFKA_OFFSET_BEGINNING);
//        while (true) {
//            // The first argument is the partition (again).
//            // The second argument is the timeout.
//            $msg = $topic->consume(0, 1000);
//            if (null === $msg || $msg->err === RD_KAFKA_RESP_ERR__PARTITION_EOF) {
//                // Constant check required by librdkafka 0.11.6. Newer librdkafka versions will return NULL instead.
//                dd($msg);
//                echo '123'.PHP_EOL;
//                continue;
//            } elseif ($msg->err) {
//                echo $msg->errstr(), "\n";
//                break;
//            } else {
//                echo $msg->payload, "\n";
//            }
//        }
        $queue = $rk->newQueue();
        $topic1 = $rk->newTopic(env('KAFKA_QUEUE'));
        $topic1->consumeQueueStart(0, RD_KAFKA_OFFSET_BEGINNING, $queue);
        //$topic1->consumeQueueStart(1, RD_KAFKA_OFFSET_BEGINNING, $queue);
        while (true) {
            // The only argument is the timeout.
            $msg = $queue->consume(1000);
            if (null === $msg || $msg->err === RD_KAFKA_RESP_ERR__PARTITION_EOF) {
                // Constant check required by librdkafka 0.11.6. Newer librdkafka versions will return NULL instead.
                continue;
            } elseif ($msg->err) {
                echo $msg->errstr(), "\n";
                break;
            } else {
                echo $msg->payload, "\n";
            }
        }


    }
}
