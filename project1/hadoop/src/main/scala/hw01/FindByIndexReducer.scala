package hw01

import org.apache.hadoop.io.{LongWritable, NullWritable}
import org.apache.hadoop.mapreduce.Reducer

class FindByIndexReducer extends Reducer[LongWritable, LongWritable, NullWritable, LongWritable] {
  private var targetIndex = 0
  private var currentIndex = 0

  override def setup(context: Reducer[LongWritable, LongWritable, NullWritable, LongWritable]#Context): Unit = {
    targetIndex = context.getConfiguration.getInt("index", -1)
  }

  override def reduce(key: LongWritable, values: java.lang.Iterable[LongWritable], context: Reducer[LongWritable, LongWritable, NullWritable, LongWritable]#Context): Unit = {
    values.forEach { value =>
      currentIndex += 1
      if (currentIndex == targetIndex) {
        context.write(NullWritable.get, new LongWritable(value.get()))
        return
      }
    }
  }
}