package com.nano;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.nano.spouts.MessageSpout;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;

/**
 * Created by Administrator on 2016/3/22.
 */
public class Application {

    public static void main(String... args) throws AlreadyAliveException, InvalidTopologyException, InterruptedException {
        Config config = new Config();
        config.setMaxSpoutPending(20);

        if (args.length == 0) {
            config.setMaxTaskParallelism(5);

            LocalCluster localCluster = new LocalCluster();
            LocalDRPC localDRPC = new LocalDRPC();

            StormTopology stormTopology = buildTopology(localDRPC);

            localCluster.submitTopology("wordcount", config, stormTopology);

            for (int i = 0; i < 100; i++) {
                System.out.println("DRPC COST:" + localDRPC.execute("costs", "王五 abc000"));
//                System.out.println("DRPC SALE:" + localDRPC.execute("costs", "王五 abc001"));
                Thread.sleep(1000);
            }
        } else {
            config.setNumWorkers(3);

            StormTopology stormTopology = buildTopology(null);

            StormSubmitter.submitTopology(args[0], config, stormTopology);
        }
    }

    public static StormTopology buildTopology(LocalDRPC localDRPC) {
        TridentTopology topology = new TridentTopology();
        MessageSpout spout = new MessageSpout();
        TridentState costState = topology.newStream("costSpout", spout)
                .groupBy(new Fields("owner", "barcode"))
                .persistentAggregate(new MemoryMapState.Factory(), new Fields("cost"), new Sum(), new Fields("costs"));

        TridentState saleState = topology.newStream("saleSpout", spout)
                .groupBy(new Fields("owner", "barcode"))
                .persistentAggregate(new MemoryMapState.Factory(), new Fields("sale"), new Sum(), new Fields("sales"));

        topology.newDRPCStream("costs", localDRPC)
                .each(new Fields("args"), new NanoSplit(), new Fields("owner", "barcode"))
                .groupBy(new Fields("owner", "barcode"))
                .stateQuery(costState, new Fields("owner", "barcode"), new MapGet(), new Fields("costs"))
                .each(new Fields("costs"),new FilterNull())
                .localOrShuffle()
                .stateQuery(saleState, new Fields("owner", "barcode"), new MapGet(), new Fields("sales"))
                .each(new Fields("sales"),new FilterNull());

        return topology.build();
    }
}

class NanoSplit extends BaseFunction {
    /* (non-Javadoc)
     * @see storm.trident.operation.Function#execute(storm.trident.tuple.TridentTuple, storm.trident.operation.TridentCollector)
     */
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        String[] values = tuple.getString(0).split(" ");
        collector.emit(new Values(values));
    }

}