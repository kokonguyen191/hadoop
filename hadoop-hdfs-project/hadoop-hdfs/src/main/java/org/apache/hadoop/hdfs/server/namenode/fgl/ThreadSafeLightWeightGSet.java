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

import java.util.AbstractCollection;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.concurrent.atomic.LongAdder;

import com.google.common.math.LongMath;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.NameNodeUtils;
import org.apache.hadoop.util.GSet;
import org.apache.hadoop.util.LightWeightGSet;

import static java.math.RoundingMode.UNNECESSARY;

/**
 * A (semi) thread-safe implementation of {@link GSet} based on {@link LightWeightGSet}.
 * <p>
 * Locks are binned. Access to set elements in the same bin has to go through that bin's lock.
 * A set of adjacent elements go in one bin, sharing one lock. Bins are always created
 * in powers of 2 to facilitate easier binning using bit-wise operations.
 * For example, with 14 elements and 4 bins:
 * <ol>
 *   <li>1 2 3 4</li>
 *   <li>5 6 7 8</li>
 *   <li>9 10 11 12</li>
 *   <li>13 14</li>
 * </ol>
 * {@link #remove(Object)} and {@link #put(Object)} are protected by locks.
 * {@link #clear()} and {@link #values()} are not protected
 * and the responsibility of thread-safety must be handled by callers of these methods.
 */
public class ThreadSafeLightWeightGSet<K, E extends K> extends LightWeightGSet<K, E> {
  final private Object[] locks;
  final private LongAdder size = new LongAdder();
  final private LongAdder modification = new LongAdder();
  final private int lockOffset;

  /**
   * Initializer to be used with {@link NameNodeUtils#newGSetMap(Class, int)}.
   *
   * @param capacity set capacity.
   */
  public ThreadSafeLightWeightGSet(int capacity) {
    this(capacity,
        new Configuration().getInt(DFSConfigKeys.DFS_NAMENODE_FINE_GRAINED_LOCK_GSET_BINS_KEY,
            DFSConfigKeys.DFS_NAMENODE_FINE_GRAINED_LOCK_GSET_BINS_DEFAULT));
  }

  /**
   * Initialize the GSet with partially binned locks.
   *
   * @param suggestedLockBins suggested number of locks, will round up to the nearest power of 2.
   */
  public ThreadSafeLightWeightGSet(int capacity, int suggestedLockBins) {
    super(capacity);
    int lockBins = Math.min(entries.length, actualArrayLength(suggestedLockBins));
    lockOffset = LongMath.log2(entries.length / lockBins, UNNECESSARY);
    locks = new Object[lockBins];
    for (int i = 0; i < lockBins; i++) {
      locks[i] = new Object();
    }
  }

  @Override
  public int size() {
    return size.intValue();
  }

  @Override
  public Iterator<E> iterator() {
    return new ThreadSafeSetIterator();
  }

  /**
   * Gets the lock of a bin associated with an index.
   */
  private Object getLock(int index) {
    return locks[index >> lockOffset];
  }

  @Override
  public E put(final E element) {
    // validate element
    if (element == null) {
      throw new NullPointerException("Null element is not supported.");
    }
    LinkedElement e = null;
    try {
      e = (LinkedElement) element;
    } catch (ClassCastException ex) {
      throw new HadoopIllegalArgumentException(
          "!(element instanceof LinkedElement), element.getClass()=" + element.getClass());
    }

    // find index
    final int index = getIndex(element);

    // remove if it already exists
    synchronized (getLock(index)) {
      final E existing = remove(index, element);

      // insert the element to the head of the linked list
      incrementModification();
      incrementSize();
      e.setNext(entries[index]);
      entries[index] = e;

      return existing;
    }
  }

  @Override
  public E remove(final K key) {
    //validate key
    if (key == null) {
      throw new NullPointerException("key == null");
    }
    int index = getIndex(key);
    synchronized (getLock(index)) {
      return remove(index, key);
    }
  }

  public class ThreadSafeSetIterator extends SetIterator {
    private int iterModification = modification.intValue();
    private int index = -1;
    private LinkedElement cur = null;
    private LinkedElement next = nextNonemptyEntry();
    private boolean trackModification = true;

    private LinkedElement nextNonemptyEntry() {
      for (index++; index < entries.length && entries[index] == null; index++) {
      }
      return index < entries.length ? entries[index] : null;
    }

    private void ensureNext() {
      if (trackModification && modification.intValue() != iterModification) {
        throw new ConcurrentModificationException(
            "modification=" + modification + " != iterModification = " + iterModification);
      }
      if (next != null) {
        return;
      }
      if (cur == null) {
        return;
      }
      next = cur.getNext();
      if (next == null) {
        next = nextNonemptyEntry();
      }
    }

    @Override
    public boolean hasNext() {
      ensureNext();
      return next != null;
    }

    @Override
    public E next() {
      ensureNext();
      if (next == null) {
        throw new IllegalStateException("There are no more elements");
      }
      cur = next;
      next = null;
      return convert(cur);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void remove() {
      ensureNext();
      if (cur == null) {
        throw new IllegalStateException("There is no current element " + "to remove");
      }
      ThreadSafeLightWeightGSet.this.remove((K) cur);
      iterModification++;
      cur = null;
    }

    public void setTrackModification(boolean trackModification) {
      this.trackModification = trackModification;
    }
  }

  protected final class MyValues extends AbstractCollection<E> {

    @Override
    public Iterator<E> iterator() {
      return ThreadSafeLightWeightGSet.this.iterator();
    }

    @Override
    public int size() {
      return size.intValue();
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean contains(Object o) {
      return ThreadSafeLightWeightGSet.this.contains((K) o);
    }

    @Override
    public void clear() {
      ThreadSafeLightWeightGSet.this.clear();
    }
  }

  @Override
  public Collection<E> values() {
    if (values == null) {
      values = new MyValues();
    }
    return values;
  }

  @Override
  public void clear() {
    super.clear();
    size.reset();
  }

  @Override
  protected void incrementModification() {
    modification.increment();
  }

  @Override
  protected void incrementSize() {
    size.increment();
  }

  @Override
  protected void decrementSize() {
    size.decrement();
  }

  @VisibleForTesting
  public int totalBins() {
    return locks.length;
  }
}
