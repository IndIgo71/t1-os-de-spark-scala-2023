package hw01

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.Reducer

import scala.collection.JavaConverters.iterableAsScalaIterableConverter

class FindByIndexCombiner extends Reducer[LongWritable, LongWritable, LongWritable, LongWritable] {
  private var size: Int = _

  override def setup(context: Reducer[LongWritable, LongWritable, LongWritable, LongWritable]#Context): Unit = {
    super.setup(context)
    size = context.getConfiguration.getInt("index", 0)
  }

  override def reduce(key: LongWritable,
                      values: java.lang.Iterable[LongWritable],
                      context: Reducer[LongWritable, LongWritable, LongWritable, LongWritable]#Context): Unit = {
    val sortedNumbers = values.asScala.map(_.get).toSeq.sorted(Ordering[Long].reverse).take(size)
    sortedNumbers.foreach { number =>
      context.write(key, new LongWritable(number))
    }
  }
}
