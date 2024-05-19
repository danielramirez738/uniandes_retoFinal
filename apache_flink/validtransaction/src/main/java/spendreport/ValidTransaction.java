/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package spendreport;

import java.math.BigDecimal;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;
/**
 * Skeleton code for implementing a fraud detector.
 */
public class FraudDetector extends KeyedProcessFunction<String, String, Alert> {
	private static final long serialVersionUID = 1L;
	private transient ValueState<BigDecimal> importState;

	@Override
	public void open(Configuration parameters) {
		System.out.println("\n\nAbrir Validacion de transacciones\n=====================================================");

		ValueStateDescriptor<BigDecimal> importDescriptor = new ValueStateDescriptor<>( "flag", Types.BIG_DEC);

		importState = getRuntimeContext().getState(importDescriptor);
	}

	@Override
	public void processElement( String transaction, Context context, Collector<Alert> collector) throws Exception {
		String[] tr=transaction.split(" ");

		System.out.println("Transaction ID TC: "+transaction+" valor anterior: $ "+ importState.value()+" ");

		/* Crear flag */
		importState.update( new BigDecimal( tr[1] ) );

		/* Alarmas */
		Alert alert = new Alert();
		alert.setId( Long.parseLong( tr[0] ) );

		collector.collect(alert);

	}
}
