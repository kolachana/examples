package io.confluent.examples.streams

import com.twitter.algebird.{CMSHasher, TopCMS, TopPctCMS}
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.streams.processor.{ProcessorContext, StateStore}
import org.apache.kafka.streams.state.StateSerdes
import org.apache.kafka.streams.state.internals.AbstractStoreSupplier


/**
  * An in-memory store based on Algebird's Count-Min Sketch.
  */
class CMSStoreSupplier[T: CMSHasher](name: String,
                                     val serde: Serde[T],
                                     time: Time,
                                     logged: Boolean,
                                     logConfig: java.util.Map[String, String])
    extends AbstractStoreSupplier[T, Long, CmsStore[T]](name, serde, Serdes.Long().asInstanceOf[Serde[Long]], time, logged, logConfig) {

  def this(name: String, serde: Serde[T]) {
    this(name, serde, null, true, new java.util.HashMap[String, String])
  }

  def this(name: String, serde: Serde[T], logged: Boolean) {
    this(name, serde, null, logged, new java.util.HashMap[String, String])
  }

  def this(name: String, serde: Serde[T], logged: Boolean, logConfig: java.util.Map[String, String]) {
    this(name, serde, null, logged, logConfig)
  }

  override def get(): CmsStore[T] = new CmsStore[T](name, serde)

}

// TODO: Support changelogging via `StoreChangeLogger` (cf. Kafka's InMemoryKeyValueLoggedStore)
class CmsStore[T: CMSHasher](val name: String, val serde: Serde[T]) extends StateStore {

  // TODO: Make the CMS parameters configurable
  private val cmsMonoid = {
    val delta = 1E-10
    val eps = 0.001
    val seed = 1
    val heavyHittersPct = 0.01
    TopPctCMS.monoid[T](eps, delta, seed, heavyHittersPct)
  }

  private var cms: TopCMS[T] = _

  @volatile private var open: Boolean = false

  private var serdes: StateSerdes[T, Long] = _

  def init(context: ProcessorContext, root: StateStore) {
    this.serdes = new StateSerdes[T, Long](
      name,
      if (serde == null) context.keySerde.asInstanceOf[Serde[T]] else serde,
      Serdes.Long().asInstanceOf[Serde[Long]])

    cms = cmsMonoid.zero
    // TODO: Implement the state store restoration function (which would restore the `cms` instance)
    context.register(root, true, (key, value) => {
    })

    open = true
  }

  def get(item: T): Long = cms.frequency(item).estimate

  def put(item: T): Unit = cms = cms + item

  def heavyHitters: Set[T] = cms.heavyHitters

  def totalCount: Long = cms.totalCount

  override val persistent: Boolean = false

  override def isOpen: Boolean = open

  override def flush() {
    // do-nothing since this store is in-memory
  }

  override def close() {
    open = false
  }

}