package producer;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class SimplePartitioner implements Partitioner {

  public SimplePartitioner (VerifiableProperties props) {

  }

  public int partition(Object key, int a_numPartitions) {
    Integer intKey = (Integer) key;
    int partition = intKey % a_numPartitions;
    System.out.println("partition: " + partition + " key: " + intKey);
    return partition;
  }

}
