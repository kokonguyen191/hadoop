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
package org.apache.hadoop.hdfs.server.namenode.fgl;

import org.apache.hadoop.hdfs.server.namenode.NameNodeUtils;
import org.apache.hadoop.util.GSet;
import org.apache.hadoop.util.LightWeightGSet;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TestThreadSafeGSet {

  public static class DummyBlock implements LightWeightGSet.LinkedElement, Comparable<DummyBlock> {
    private LightWeightGSet.LinkedElement next;
    final int blockId;
    final int value;

    DummyBlock(int blockId, int value) {
      this.blockId = blockId;
      this.value = value;
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof DummyBlock && blockId == ((DummyBlock) obj).blockId;
    }

    // Hash directly with block ID
    @Override
    public int hashCode() {
      return blockId;
    }

    @Override
    public int compareTo(DummyBlock that) {
      return value - that.value;
    }

    @Override
    public String toString() {
      return blockId + ":" + value;
    }

    @Override
    public LightWeightGSet.LinkedElement getNext() {
      return next;
    }

    @Override
    public void setNext(LightWeightGSet.LinkedElement e) {
      next = e;
    }
  }

  @Test
  public void testAddWithRandomRemove() throws Exception {
    int K = 32;
    int THREADS = 1000;
    int N = 100000;
    ExecutorService executor = Executors.newFixedThreadPool(THREADS);

    List<GSet<DummyBlock, DummyBlock>> gsets = new ArrayList<>();
    gsets.add(new ThreadSafeLightWeightGSet<>(K, K / 4));

    for (GSet<DummyBlock, DummyBlock> gset : gsets) {
      // Test with 1/32/1024 different blocks for under/full/over-utilization
      for (int z = 0; z <= 10; z += 5) {
        // 0/25/50/75% removal rate
        for (int i = 0; i < 100; i += 25) {
          gset.clear();
          // 1% chance to clear entire set
          testAddWithRandomRemoveInternal(gset, executor, 1 << z, N, (double) i / 100, true);
          gset.clear();
          testAddWithRandomRemoveInternal(gset, executor, 1 << z, N, (double) i / 100, false);
        }
      }
    }
  }

  private static void testAddWithRandomRemoveInternal(final GSet<DummyBlock, DummyBlock> gset,
      final ExecutorService executor, final int dataSize, final int nTests,
      final double removeProbability, final boolean randomClear)
      throws InterruptedException, ExecutionException {
    Random ran = new Random();
    ReentrantReadWriteLock masterLock = new ReentrantReadWriteLock();

    AtomicInteger reportedSize = new AtomicInteger(0);

    DummyBlock[] testData = new DummyBlock[dataSize];
    for (int i = 0; i < dataSize; i++) {
      testData[i] = new DummyBlock(i, i);
    }
    ArrayList<Callable<Void>> threads = new ArrayList<>();

    for (int i = 0; i < nTests; i++) {
      int finalI = i % dataSize;
      threads.add(() -> {
        if (randomClear && ran.nextDouble() < 0.01) {
          masterLock.writeLock().lock();
          // Has to lock `clear()` to comply with thread-safe GSet requirements
          gset.clear();
          reportedSize.set(0);
          masterLock.writeLock().unlock();
        } else {
          masterLock.readLock().lock();
          if (ran.nextDouble() < removeProbability) {
            if (gset.remove(testData[ran.nextInt(dataSize)]) != null) {
              reportedSize.getAndDecrement();
            }
          } else {
            if (gset.put(testData[finalI]) == null) {
              reportedSize.getAndIncrement();
            }
          }
          masterLock.readLock().unlock();
        }
        return null;
      });
    }

    Collections.shuffle(threads);

    List<Future<Void>> futures = executor.invokeAll(threads);
    for (Future<Void> f : futures) {
      f.get();
    }

    int gsetSize = gset.size();
    Assert.assertEquals(reportedSize.get(), gsetSize);
    if (gset.getClass().isInstance(LightWeightGSet.class)) {
      Assert.assertEquals(reportedSize.get(), ((LightWeightGSet<?, ?>) gset).realSize());
      Assert.assertTrue(gsetSize >= 0 && gsetSize <= dataSize);
    }
    System.out.printf("Removal rate %2d%% with%s random clear, final set size: %s/%s%n",
        (int) (removeProbability * 100), randomClear ? "" : "out", gsetSize, dataSize);
  }

  @Test
  public void testClassConstructors() {
    GSet<DummyBlock, DummyBlock> gset =
        NameNodeUtils.newGSetMap(ThreadSafeLightWeightGSet.class, 10);
    gset.put(new DummyBlock(1, 1));
  }

  @Test
  public void testThreadSafeLightWeightGSetCapacity() {
    // Initial cap 40 => true cap 64
    Assert.assertEquals(1, new ThreadSafeLightWeightGSet(40, -2).totalBins());
    Assert.assertEquals(8, new ThreadSafeLightWeightGSet(40, 7).totalBins());
    Assert.assertEquals(8, new ThreadSafeLightWeightGSet(40, 8).totalBins());
    Assert.assertEquals(64, new ThreadSafeLightWeightGSet(40, 33).totalBins());
    Assert.assertEquals(64, new ThreadSafeLightWeightGSet(40, 50).totalBins());
  }
}
