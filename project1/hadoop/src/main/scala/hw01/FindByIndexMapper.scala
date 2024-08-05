package hw01

import org.apache.hadoop.io.{LongWritable, NullWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

import scala.collection.mutable.{PriorityQueue => MutablePriorityQueue}

class FindByIndexMapper extends Mapper[LongWritable, Text, LongWritable, LongWritable] {
  private var heap: MutablePriorityQueue[Long] = _
  private var heapSize: Int = _

  override def setup(context: Mapper[LongWritable, Text, LongWritable, LongWritable]#Context): Unit = {
    super.setup(context)
    heapSize = context.getConfiguration.getInt("index", 0)
    heap = new MutablePriorityQueue[Long]()(Ordering[Long].reverse)
  }

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, LongWritable, LongWritable]#Context): Unit = {
    val item = value.toString.toLong

    if (heap.size < heapSize) {
      heap.enqueue(item)
    } else if (item > heap.head) {
      heap.dequeue()
      heap.enqueue(item)
    }
  }

  override def cleanup(context: Mapper[LongWritable, Text, LongWritable, LongWritable]#Context): Unit = {
    heap.foreach { num =>
      context.write(new LongWritable(-num), new LongWritable(num))
    }
  }
}
