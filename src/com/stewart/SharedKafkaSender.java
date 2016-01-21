package com.stewart;

/**
 * @author mstewart 12/4/15
 */

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;


public class SharedKafkaSender {

  final private Logger logger = LoggerFactory.getLogger(this.getClass());

  final private KafkaProducer<String, byte[]> producer;
  final private String topic;
  final private Random rng = new Random();

  private List<Integer> partitionIds;

  public SharedKafkaSender(String topic) {

    Properties props = new Properties();

    try {
      props.load(getClass().getClassLoader().getResourceAsStream("config.properties"));
    } catch (IOException | NullPointerException e) {
      throw new IllegalStateException("Failed to load config.properties");
    }

    producer = new KafkaProducer<>(props);

    this.topic = topic;

    partitionIds = producer.partitionsFor(topic)
            .stream()
            .map(PartitionInfo::partition)
            .collect(ArrayList<Integer>::new, ArrayList::add, ArrayList::addAll);

    partitionIds.forEach(id -> logger.info("found partition id={}", id));
  }


  public boolean send(byte[] msg) {
    try {
      // spread messages across however many partitions are available
      Integer partition = partitionIds.get((rng.nextInt(partitionIds.size())));
      RecordMetadata recordMetadata = producer.send(new ProducerRecord<>(topic, partition.toString(), msg)).get();
      logger.debug("sent to {}:{}:{}", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
      return true;
    } catch (InterruptedException | ExecutionException e) {
      logger.error(e.getMessage(), e);
      return false;
    }
  }

}
