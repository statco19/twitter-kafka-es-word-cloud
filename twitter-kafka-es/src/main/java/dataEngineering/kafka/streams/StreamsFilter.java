package dataEngineering.kafka.streams;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StreamsFilter {

    private static String APPLICATION_NAME = "streams-filter-application";
    private static String BOOTSTRAP_SERVERS = "localhost:9092";
    private static String STREAM_TWEET = "stream_tweet";
    private static String STREAM_TWEET_NOT_TRUNCATED = "stream_tweet_not_truncated";
    private static String STREAM_TWEET_TRUNCATED = "stream_tweet_truncated";

    public static void main(String[] args) {

        Gson gson = new GsonBuilder().setPrettyPrinting().create();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> streamTweet = builder.stream(STREAM_TWEET);
        KStream<String, String> filterStream_truncated = streamTweet.filter(
                (key, value) -> gson.fromJson(value, JsonElement.class)
                        .getAsJsonObject()
                        .get("truncated")
                        .getAsString()
                        .equals("true")
        );

        KStream<String, String> filteredStream_not_truncated = streamTweet.filter(
                (key, value) -> gson.fromJson(value, JsonElement.class)
                        .getAsJsonObject()
                        .get("truncated")
                        .getAsString()
                        .equals("false")
        );
        filteredStream_not_truncated.to(STREAM_TWEET_NOT_TRUNCATED);
        filterStream_truncated.to(STREAM_TWEET_TRUNCATED);

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }
}
