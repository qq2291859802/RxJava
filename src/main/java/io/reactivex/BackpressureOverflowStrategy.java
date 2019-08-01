/**
 * Copyright (c) 2016-present, RxJava Contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.reactivex;

/**
 * Options to deal with buffer overflow when using onBackpressureBuffer.
 * 参考：  https://blog.csdn.net/jdsjlzx/article/details/52717636
 * 所谓背压，即生产者的速度大于消费者的速度带来的问题
 */
public enum BackpressureOverflowStrategy {
    /** Signal a MissingBackpressureException and terminate the sequence. */
    ERROR,
    /** Drop the oldest value from the buffer. */
    DROP_OLDEST,
    /** Drop the latest value from the buffer. */
    DROP_LATEST
}
