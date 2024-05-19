package spendreport;

import java.util.ArrayList;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.walkthrough.common.sink.AlertSink;
import org.apache.flink.walkthrough.common.entity.Alert;

/**
 * Skeleton code for the datastream walkthrough
 */
public class FraudDetectionJob {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		ArrayList<String> gettrans = dataList.getArrayList();



		DataStream<String> transactions = env
				.fromCollection( gettrans )
				.name("transactions");

		DataStream<Alert> alerts = transactions
				.keyBy( (String x) -> { return x.split(" ")[0]; } )
				.process(new FraudDetector())
				.name("fraud-detector");

		alerts
				.addSink(new AlertSink())
				.name("send-alerts");

		env.execute("Fraud Detection");
	}
}