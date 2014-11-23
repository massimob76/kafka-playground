package producer;

import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;

import java.nio.ByteBuffer;

public class IntegerEncoder implements Encoder<Integer> {

  public IntegerEncoder(VerifiableProperties verifiableProperties) {

  }

  @Override
  public byte[] toBytes(Integer value) {
    return ByteBuffer.allocate(4).putInt(value).array();
  }
}
