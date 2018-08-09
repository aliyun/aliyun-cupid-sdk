/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.odps.cupid.utils;

import java.io.Serializable;

public abstract class JTuple implements Serializable {

    private static final long serialVersionUID = -7917870896539663758L;

    public static <A, B> JTuple2<A, B> tuple(A a, B b)
    {
        return new JTuple2<A, B>(a, b);
    }

    public static final class JTuple2<A, B> extends JTuple implements Serializable
    {
        private static final long serialVersionUID = 3110504014584721231L;
        private A a;
        private B b;

        private JTuple2(A a, B b)
        {
            this.a = a;
            this.b = b;
        }

        public A _1()
        {
            return this.a;
        }

        public B _2()
        {
            return this.b;
        }
    }
}
