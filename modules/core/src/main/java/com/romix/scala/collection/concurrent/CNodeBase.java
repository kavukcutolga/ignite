/*                     __                                               *\
**     ________ ___   / /  ___     Scala API                            **
**    / __/ __// _ | / /  / _ |    (c) 2003-2012, LAMP/EPFL             **
**  __\ \/ /__/ __ |/ /__/ __ |    http://scala-lang.org/               **
** /____/\___/_/ |_/____/_/ | |                                         **
**                          |/                                          **
\*                                                                      */

/*
 Copyright (C) Roman Levenstein. All Rights Reserved.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package com.romix.scala.collection.concurrent;


import java.util.concurrent.atomic.*;


abstract class CNodeBase<K, V> extends MainNode<K, V> {

    public static final AtomicIntegerFieldUpdater<CNodeBase> updater = AtomicIntegerFieldUpdater.newUpdater(CNodeBase.class, "csize");

    public volatile int csize = -1;

    public boolean CAS_SIZE(int oldval, int nval) {
        return updater.compareAndSet(this, oldval, nval);
    }

    public void WRITE_SIZE(int nval) {
        updater.set(this, nval);
    }

    public int READ_SIZE() {
        return updater.get(this);
    }

}