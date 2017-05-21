package com.upm.etsit.raquel.tfg;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraStreamingJavaUtil;

public class CosumeSpark {
	
	private CosumeSpark() {
    }
	
	public static void main(String args[]) throws InterruptedException{
		
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "localhost:9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", TweetDeserializer.class);
		kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);

		Collection<String> topics = Arrays.asList("twitterdata");
		
		// Create a local StreamingContext with two working thread and batch interval of 1 second
		SparkConf conf = new SparkConf().setMaster("local[5]").setAppName("TwitterApp")
				.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		
		
		conf.set("spark.cassandra.connection.host", "192.168.56.103");
		conf.set("spark.cassandra.connection.port", "9042");
		
		JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(5));

		
		
		//ConsumerRecord = A key/value pair to be received from Kafka
		
		JavaInputDStream<ConsumerRecord<String, Tweet>> stream = KafkaUtils.createDirectStream(
				 streamingContext, 
				 LocationStrategies.PreferConsistent(),
				 ConsumerStrategies.<String, Tweet>Subscribe(topics, kafkaParams)
				);
		 
		 
		
		JavaDStream<Tweet> tweetStream= stream.map(
				 new Function<ConsumerRecord<String, Tweet>, Tweet>() {
					 public Tweet call(ConsumerRecord<String, Tweet> record) {
						 return record.value();
					 }
		});
		
		//tweetStream.print();
		
		
		
		
		//Tweets que contengan la palabra 
		
		JavaDStream<Tweet> tweetStream2= tweetStream.filter(
				 new Function< Tweet, Boolean>() {
				      public Boolean call(Tweet tweet) throws Exception {
				    	  if(tweet.getText().contains(" ")){
				    		  return true;
				    	  }else return false;
	
					 }
		});
		
		//Imprimimos el texto del tweet y el número de retweets y la localizacion (ciudad)
		tweetStream2.foreachRDD( x-> {
	        x.collect().stream().forEach(
	        		n-> System.out.println(n.getText()+"\n"+ n.getRetweets()+"\n"+n.getCountry()));
	    });
		
		
		

		

		
		
		
		CassandraStreamingJavaUtil.javaFunctions(tweetStream).writerBuilder("twitterkeyspace", "tweets", CassandraJavaUtil.mapToRow(Tweet.class)).saveToCassandra();
		
	
		
		
		
		
		/*
		// you can create an RDD for a defined range of offsets
		
		OffsetRange[] offsetRanges = {
				  // topic, partition, inclusive starting offset, exclusive ending offset
				  OffsetRange.create("twitterdata", 0, 0, 100),
				  OffsetRange.create("twitterdata", 1, 0, 100)
				};

		JavaRDD<ConsumerRecord<String, String>> rdd = KafkaUtils.createRDD(
				  sc,
				  kafkaParams,
				  offsetRanges,
				  LocationStrategies.PreferConsistent()
				);
		
		
		// obtaining offsets
		 
	
		stream.foreachRDD(new VoidFunction<JavaRDD<ConsumerRecord<String, String>>>() {
			  @Override
			  public void call(JavaRDD<ConsumerRecord<String, String>> rdd) {
			    final OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
			    rdd.foreachPartition(new VoidFunction<Iterator<ConsumerRecord<String, String>>>() {
			      @Override
			      public void call(Iterator<ConsumerRecord<String, String>> consumerRecords) {
			        OffsetRange o = offsetRanges[TaskContext.get().partitionId()];
			        System.out.println(
			          o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset());
			      }
			    });
			  }
			});*/
		
		
		
		
		streamingContext.start();
		streamingContext.awaitTermination();
	}

}
