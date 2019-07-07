package recommend; /**
 * Created by caoyihong on 2019/7/5.
 */

import java.util.HashMap;
import java.util.HashSet;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;


import org.apache.spark.SparkConf;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.Durations;
import kafka.serializer.StringDecoder;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import kafka.serializer.StringDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

public class KafkaToSpark {

    private static final Logger log = LoggerFactory.getLogger(KafkaToSpark.class);

    private static final Pattern SPACE = Pattern.compile(" ");

    private static final String KAFKA_ADDR = "localhost:9092";
    private static final String TOPIC = "recommend";
    private static final String HDFS_ADDR = "hdfs://localhost:9000";

    private static final String MODEL_PATH = "/spark-als/model";


    //	基于Hadoop、Flume、Kafka、spark-streaming、logback、商城系统的实时推荐系统DEMO
    //	Real time recommendation system DEMO based on Hadoop, Flume, Kafka, spark-streaming, logback and mall system
    //	商城系统采集的数据集格式 Data Format:
    //	用户ID，商品ID，用户行为评分，时间戳
    //	UserID,ItemId,Rating,TimeStamp
    //	53,1286513,9,1508221762

    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "zhangni"); // 设置权限用户

        // 连接spark
        SparkConf sparkConf = new SparkConf().setAppName("JavaKafkaDirectWordCount").setMaster("local[*]");

        final JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(6));

        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list", KAFKA_ADDR);
        HashSet<String> topicsSet = new HashSet<String>();
        topicsSet.add(TOPIC); // 指定操作的topic

        // spark direct方式直连 kafka
        JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(jssc, String.class, String.class,
                StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);

        JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
            public String call(Tuple2<String, String> tuple2) {
                return tuple2._2();
            }
        });
        // kafka数据格式  用户ID,商品ID,评分
        JavaDStream<Rating> ratingsStream = lines.map(new Function<String, Rating>() {
            public Rating call(String s) {
                String[] sarray = StringUtils.split(StringUtils.trim(s), ",");
                return new Rating(Integer.parseInt(sarray[0]), Integer.parseInt(sarray[1]),
                        Double.parseDouble(sarray[2]));
            }
        });

        // 进行流推荐计算
        ratingsStream.foreachRDD(new VoidFunction<JavaRDD<Rating>>() {

            public void call(JavaRDD<Rating> ratings) throws Exception {
                //  获取到原始的数据集
                SparkContext sc = ratings.context();

                RDD<String> textFileRDD = sc.textFile(HDFS_ADDR + "/flume/logs", 3); // 读取原始数据集文件
                JavaRDD<String> originalTextFile = textFileRDD.toJavaRDD();

                final JavaRDD<Rating> originaldatas = originalTextFile.map(new Function<String, Rating>() {
                    public Rating call(String s) {
                        String[] sarray = StringUtils.split(StringUtils.trim(s), ",");
                        return new Rating(Integer.parseInt(sarray[0]), Integer.parseInt(sarray[1]),
                                Double.parseDouble(sarray[2]));
                    }
                });
                log.info("========================================");
                log.info("Original TextFile Count:{}", originalTextFile.count()); // HDFS中已经存储的原始用户行为日志数据
                log.info("========================================");

                //  将原始数据集和新的用户行为数据进行合并
                JavaRDD<Rating> calculations = originaldatas.union(ratings);

                log.info("Calc Count:{}", calculations.count());

                // Build the recommendation model using ALS
                int rank = 10; // 模型中隐语义因子的个数
                int numIterations = 6; // 训练次数

                // 得到训练模型
                if (!ratings.isEmpty()) { // 如果有用户行为数据
                    MatrixFactorizationModel model = ALS.train(JavaRDD.toRDD(calculations), rank, numIterations, 0.01);
                    //  判断文件是否存在,如果存在 删除文件目录
                    Configuration hadoopConfiguration = sc.hadoopConfiguration();
                    hadoopConfiguration.set("fs.defaultFS", HDFS_ADDR);
                    FileSystem fs = FileSystem.get(hadoopConfiguration);
                    Path outpath = new Path(MODEL_PATH);
                    if (fs.exists(outpath)) {
                        log.info("########### 删除" + outpath.getName() + " ###########");
                        fs.delete(outpath, true);
                    }

                    // 保存model
                    model.save(sc, HDFS_ADDR + MODEL_PATH);

                    //  读取model
                    MatrixFactorizationModel modelLoad = MatrixFactorizationModel.load(sc, HDFS_ADDR + MODEL_PATH);
                    // 为指定用户推荐10个商品(电影)
                    for(int userId=0;userId<30;userId++){ // streaming_sample_movielens_ratings.txt
                        Rating[] recommendProducts = modelLoad.recommendProducts(userId, 10);
//                        log.info("get recommend result:{}", Arrays.toString(recommendProducts));
                        System.out.println("get recommend result:");
                        System.out.println(Arrays.toString(recommendProducts));
                    }
                }

            }
        });

        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
