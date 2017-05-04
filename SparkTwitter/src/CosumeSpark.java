import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


import java.util.Set;


import org.apache.spark.api.java.function.Function;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
//import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;


import scala.Tuple2;

import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.KafkaUtils;


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
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
				.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		
		
		//conf.set("spark.cassandra.connection.host", "localhost");
		//conf.set("spark.cassandra.connection.port", "9042");
		
		JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(5));

		// Create a Java version of the Spark Context from the configuration
        //JavaSparkContext sc = new JavaSparkContext(conf);
	
		/*final JavaInputDStream<ConsumerRecord<String, String>> stream =
		  KafkaUtils.createDirectStream(
			streamingContext,
		    LocationStrategies.PreferConsistent(),
		    ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
		  );*/
		
	
		/*JavaPairDStream<String, String> streammap = stream.mapToPair(
		  new PairFunction<ConsumerRecord<String, String>, String, String>() {
		    @Override
		    public Tuple2<String, String> call(ConsumerRecord<String, String> record) {
		      return new Tuple2<>(record.key(), record.value());
		    }
		  });*/
		
		//streammap.print();
		
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
		tweetStream.print();
		//CassandraStreamingJavaUtil.javaFunctions(tweetStream).writerBuilder("dcos", "tweets", CassandraJavaUtil.mapToRow(Tweet.class)).saveToCassandra();
		
	
		
		
		
		
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
