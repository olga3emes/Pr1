import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.zookeeper.ZooKeeper;


import java.io.IOException;

/**
 * Created by olga on 22/5/17.
 */

    public class HBaseConnectionWithJava {
        public static void main(String[] args) throws ServiceException, IOException {

            System.out.println("Trying to connect...");

            Configuration conf = HBaseConfiguration.create();
            conf.clear();
            //conf.addResource("hbase-site.xml");
            //conf.set("hbase.zookeeper.quorum","192.168.0.10,192.168.0.11");
            //conf.set("hbase.zookeeper.property.clientPort","2181");
            //conf.set("hbase.master", "localhost:60010");
            //conf.set("hbase.cluster.distributed", "true");

            HBaseAdmin.checkHBaseAvailable(conf);

            System.out.println("connected to hbase");
            HTable table = new HTable(conf, "bunty");
            Scan scan = new Scan();
        }
    }


