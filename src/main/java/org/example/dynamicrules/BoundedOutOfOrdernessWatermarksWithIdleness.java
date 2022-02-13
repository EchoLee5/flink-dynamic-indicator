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

package org.example.dynamicrules;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.SystemClock;

import java.time.Duration;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * BoundedOutOfOrdernessWatermarks + WatermarksWithIdleness
 * 当标记为idle后发出一个更大的水印到下游, 解决onTimer一直不触发的问题
 */
@Public
@Slf4j
public class BoundedOutOfOrdernessWatermarksWithIdleness<T> implements WatermarkGenerator<T> {

    private final IdlenessTimer idlenessTimer;
    private long maxTimestamp;
    private final long outOfOrdernessMillis;

    public BoundedOutOfOrdernessWatermarksWithIdleness(Duration maxOutOfOrderness, Duration idleTimeout) {
        this(maxOutOfOrderness, idleTimeout, SystemClock.getInstance());
    }

    @VisibleForTesting
    BoundedOutOfOrdernessWatermarksWithIdleness(Duration maxOutOfOrderness, Duration idleTimeout, Clock clock) {
        checkNotNull(idleTimeout, "idleTimeout");
        checkNotNull(maxOutOfOrderness, "maxOutOfOrderness");
        checkArgument(!maxOutOfOrderness.isNegative(), "maxOutOfOrderness cannot be negative");
        checkArgument(
                !(idleTimeout.isZero() || idleTimeout.isNegative()),
                "idleTimeout must be greater than zero");
        this.idlenessTimer = new IdlenessTimer(clock, idleTimeout);
        this.outOfOrdernessMillis = maxOutOfOrderness.toMillis();

        // start so that our lowest watermark would be Long.MIN_VALUE.
        this.maxTimestamp = Long.MIN_VALUE + outOfOrdernessMillis + 1;
    }

    @Override
    public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
        maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
        idlenessTimer.activity();
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        if (idlenessTimer.checkIfIdle()) {
            log.debug("Mark idle, and emit greater watermark.");
            output.emitWatermark(new Watermark(maxTimestamp - outOfOrdernessMillis));
        } else {
            output.emitWatermark(new Watermark(maxTimestamp - outOfOrdernessMillis - 1));
        }
    }

    // ------------------------------------------------------------------------

    @VisibleForTesting
    static final class IdlenessTimer {

        /** The clock used to measure elapsed time. */
        private final Clock clock;

        /** Counter to detect change. No problem if it overflows. */
        private long counter;

        /** The value of the counter at the last activity check. */
        private long lastCounter;

        /**
         * The first time (relative to {@link Clock#relativeTimeNanos()}) when the activity check
         * found that no activity happened since the last check. Special value: 0 = no timer.
         */
        private long startOfInactivityNanos;

        /** The duration before the output is marked as idle. */
        private final long maxIdleTimeNanos;

        IdlenessTimer(Clock clock, Duration idleTimeout) {
            this.clock = clock;

            long idleNanos;
            try {
                idleNanos = idleTimeout.toNanos();
            } catch (ArithmeticException ignored) {
                // long integer overflow
                idleNanos = Long.MAX_VALUE;
            }

            this.maxIdleTimeNanos = idleNanos;
        }

        public void activity() {
            counter++;
        }

        public boolean checkIfIdle() {
            if (counter != lastCounter) {
                // activity since the last check. we reset the timer
                lastCounter = counter;
                startOfInactivityNanos = 0L;
                return false;
            } else // timer started but has not yet reached idle timeout
            if (startOfInactivityNanos == 0L) {
                // first time that we see no activity since the last periodic probe
                // begin the timer
                startOfInactivityNanos = clock.relativeTimeNanos();
                return false;
            } else {
                return clock.relativeTimeNanos() - startOfInactivityNanos > maxIdleTimeNanos;
            }
        }
    }
}
