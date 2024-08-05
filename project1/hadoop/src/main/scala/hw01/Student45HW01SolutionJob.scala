package hw01

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

object Student45HW01SolutionJob {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println("Usage: Student45HW01SolutionJob <input_path> <output_path> <index>")
      System.exit(1)
    }

    val inputPath = args(0)
    val outputPath = args(1)
    val index = args(2).toInt + 10

    val conf = new org.apache.hadoop.conf.Configuration()
    conf.setLong("index", index)

    val job = Job.getInstance(conf, "student45-hw01-mapreduce")
    job.setJarByClass(this.getClass)

    job.setMapperClass(classOf[FindByIndexMapper])
    job.setCombinerClass(classOf[FindByIndexCombiner])
    job.setReducerClass(classOf[FindByIndexReducer])

    job.setOutputKeyClass(classOf[LongWritable])
    job.setOutputValueClass(classOf[LongWritable])

    FileInputFormat.addInputPath(job, new Path(inputPath))
    FileOutputFormat.setOutputPath(job, new Path(outputPath))

    val fileSystem = FileSystem.get(conf)
    if (fileSystem.exists(new Path(outputPath))) {
      fileSystem.delete(new Path(outputPath), true)
    }

    System.exit(if (job.waitForCompletion(true)) 0 else 1)
  }
}