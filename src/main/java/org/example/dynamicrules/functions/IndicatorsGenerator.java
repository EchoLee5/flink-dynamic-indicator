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

package org.example.dynamicrules.functions;

import org.example.dynamicrules.Transaction;
import org.example.sources.BaseGenerator;

import java.util.HashMap;
import java.util.Map;
import java.util.SplittableRandom;

public class IndicatorsGenerator extends BaseGenerator<String> {

    private static long MAX_PAYEE_ID = 100000;
    private static long MAX_BENEFICIARY_ID = 100000;

    private static double MIN_PAYMENT_AMOUNT = 5d;
    private static double MAX_PAYMENT_AMOUNT = 20d;

    public IndicatorsGenerator(int maxRecordsPerSecond) {
        super(maxRecordsPerSecond);
    }

    @Override
    public String randomEvent(SplittableRandom rnd, long id) {
        //String indicatorString = "esb_as_ProcBizFuncLibs,func_caption=AS_融资融券申报回报_参数获取,func_no=2275001,host=18.18.1.113,monitorKey=86cc74d02e0542a8a7528fd8a9f5c43a ave_time=0.0,createtime=1623285118247.0,cur_avgExcCount=0.0,cur_avgTime=0.5,cur_excCount=20.0,cur_excTime=10.0,cur_que_ave_time=\"0\",curr_queue_ave_time=\"0\",max_time=3.0,min_time=0.0,time_see=370.0,total=913.0 1623285118549000009";

        String[] ips = new String[]{"18.18.1.113", "19.18.1.113", "20.18.1.113",
                "21.18.1.113", "22.18.1.113", "23.18.1.113"};

        String[] monitorKeys = new String[]{"86cc74d02e0542a8a7528fd8a9f5c43a",
                "86cc74d02e0542a8a7528fd8a9f5c43a", "87877d8dfe0542a8a7528fd8a9f5c43a",
                "656gfg302e0542a8a7528fd8a9f5c43a", "54f7f8fd2e0542a8a7528fd8a9f5c43a"};
        long transactionId = rnd.nextLong(Long.MAX_VALUE);
//    long payeeId = rnd.nextLong(MAX_PAYEE_ID);
//    long beneficiaryId = rnd.nextLong(MAX_BENEFICIARY_ID);
        String measurement = "measurement" + rnd.nextInt(3);
        String host = ips[rnd.nextInt(5)];
        String monitorKey = monitorKeys[rnd.nextInt(4)];

        int cpu = rnd.nextInt(10);
        long memory = rnd.nextLong(MAX_BENEFICIARY_ID);
//        double paymentAmountDouble =
//                ThreadLocalRandom.current().nextDouble(MIN_PAYMENT_AMOUNT, MAX_PAYMENT_AMOUNT);
//        paymentAmountDouble = Math.floor(paymentAmountDouble * 100) / 100;
//        BigDecimal paymentAmount = BigDecimal.valueOf(paymentAmountDouble);
        Map<String, String> tags = new HashMap<>();
        Map<String, String> fields = new HashMap<>();

        tags.put("host", host);
        tags.put("monitorKey", monitorKey);
        fields.put("curCpu", String.valueOf(cpu));
        fields.put("curMem", String.valueOf(memory));


        StringBuilder builder = new StringBuilder();
        builder.append(measurement)
                .append(",");
        tags.forEach((k, v) -> {
            builder.append(k);
            builder.append("=");
            builder.append(v);
            builder.append(",");
        });
        builder.deleteCharAt(builder.length() - 1);
        builder.append(" ");

        fields.forEach((k, v) -> {
            builder.append(k);
            builder.append("=");
            builder.append(v);
            builder.append(",");
        });
        builder.deleteCharAt(builder.length() - 1);
        builder.append(" ");

        builder.append(System.currentTimeMillis() * 1000 * 1000);


        return builder.toString();
    }

    private Transaction.PaymentType paymentType(long id) {
        int name = (int) (id % 2);
        switch (name) {
            case 0:
                return Transaction.PaymentType.CRD;
            case 1:
                return Transaction.PaymentType.CSH;
            default:
                throw new IllegalStateException("");
        }
    }
}
