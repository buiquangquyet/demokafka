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
        $queue = env('KAFKA_QUEUE');
        $conf = new \RdKafka\Conf();
        $conf->setRebalanceCb(function (\RdKafka\KafkaConsumer $kafka, $err, array $partitions = null) {
            switch ($err) {
                case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
                    echo "Assign: ";
                    $kafka->assign($partitions);
                    break;

                case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
                    echo "Revoke: ";
                    $kafka->assign(NULL);
                    break;

                default:
                    throw new Exception($err);
            }
        });
        $conf->set('group.id', env('KAFKA_CONSUMER_GROUP_ID', 'laravel_queue'));
        $conf->set('metadata.broker.list', env('KAFKA_BROKERS', 'localhost:9092'));
        $conf->set('auto.offset.reset', 'largest');
        $conf->set('enable.auto.commit', 'true');
//        $conf->set('auto.commit.interval.ms', '101');

        $consumer = new \RdKafka\KafkaConsumer($conf);
        $consumer->subscribe([$queue]);
        $this->line("START PULL");
        while (true) {
            $message = $consumer->consume(120 * 1000);
            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    $this->line(json_encode($message));
                    // check trong redis có offset của message chưa, nếu có rồi thì sẽ không xử lý
//                    try {
//                        $key = 'market-' . $message->partition . '-' . $message->offset;
//                        $rsps = Redis::setnx($key, $key);
//                        if ($rsps <= 0) {
//                            LogHelper::channel('kafka')->error('KAFKA-DUPLICATE: ' . json_encode($message));
//                        } else {
//                            Redis::expire($key, 60); // 10 minutes
//                            dispatch(new ProcessKDNQueueKafka((array) $message));
//                        }
//                    } catch (\Exception $e) { // nếu trường hợp lỗi redis thì sẽ push job bình thường - vì có cơ chế chặn
//                        LogHelper::channel('kafka')->error('KAFKA-PULL-ERROR: ' . $e->getMessage());
//                        dispatch(new ProcessKDNQueueKafka((array) $message));
//                    }
                    break;
                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                    $this->line("'[' . date('H:i:s') . \"][partition {$message->partition}] No more messages; will wait for more [key: '{$message->key}' offset: {$message->offset}]\n");
                    break;
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    $this->line('[' . date('H:i:s') . "] Timed out \n");
                    break;
                default:
                    throw new \Exception($message->errstr(), $message->err);
                    break;
            }
        }


    }
}
