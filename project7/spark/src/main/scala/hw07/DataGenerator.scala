package hw07

import net.datafaker.Faker
import org.joda.time.{DateTime, Days}

import java.util.UUID
import java.util.concurrent.ThreadLocalRandom


object DataGenerator {
  private val faker = new Faker()
  private val random = scala.util.Random

  def generateUniqueFruits(): Seq[String] = {
    var fruits = Set[String]()
    val numFruits = faker.number().numberBetween(20, 31)

    while (fruits.size < numFruits) {
      val fruit = faker.food().fruit()
      fruits += fruit
    }
    fruits.toSeq
  }

  private def generateRandomDateTime(startDate: DateTime, endDate: DateTime): String = {
    val days = Days.daysBetween(startDate, endDate).getDays()
    val daysCount = random.nextInt(days) + 1
    val randomDateTime = startDate.plusDays(daysCount).withMillisOfDay(random.nextInt(86400000))
    randomDateTime.toString("yyyy-MM-dd HH:mm:ss")
  }


  def generateReviews(numReviews: Int, fruits: Seq[String]): Seq[FruitReview] = {
    val startDate = new DateTime(2023, 1, 1, 0, 0, 0)
    val endDate = startDate.plusYears(1)

    val popularFruits = random.shuffle(fruits).take(2)
	// формиуруем перекос
    val popularFruitsReviewCount = (numReviews * faker.number().numberBetween(80, 90) / 100).toInt

    (1 to popularFruitsReviewCount).map { _ =>
      val uuid = UUID.randomUUID().toString
      val name = faker.options().option(popularFruits: _*)
      val reviewDatetime = generateRandomDateTime(startDate, endDate)
      val rating = faker.number().numberBetween(3, 6)
      val comment = faker.lorem().sentence()

      FruitReview(uuid, name, reviewDatetime, rating, comment)
    } ++
      (1 to numReviews - popularFruitsReviewCount).map { _ =>

        val uuid = UUID.randomUUID().toString
        val name = faker.options().option(fruits: _*)
        val reviewDatetime = generateRandomDateTime(startDate, endDate)
        val rating = faker.number().numberBetween(1, 5)
        val comment = faker.lorem().sentence()

        FruitReview(uuid, name, reviewDatetime, rating, comment)
      }
  }


  def generateSales(numSales: Int, fruits: Seq[String]): Seq[FruitSale] = {
    val startDate = new DateTime(2023, 1, 1, 0, 0, 0)
    val endDate = startDate.plusYears(1)

    val popularFruits = random.shuffle(fruits).take(2)
	// формиуруем перекос
    val popularFruitsSalesCount = (numSales * faker.number().numberBetween(85, 95) / 100).toInt

    (1 to popularFruitsSalesCount).map { _ =>
      val uuid = UUID.randomUUID().toString
      val name = faker.options().option(popularFruits: _*)
      val saleDatetime = generateRandomDateTime(startDate, endDate)
      val quantity = faker.number().numberBetween(5, 50)
      val price = faker.number().numberBetween(25, 5000)

      FruitSale(uuid, name, saleDatetime, quantity, price)
    } ++
      (1 to numSales - popularFruitsSalesCount).map { _ =>

        val uuid = UUID.randomUUID().toString
        val name = faker.options().option(fruits: _*)
        val saleDatetime = generateRandomDateTime(startDate, endDate)
        val quantity = faker.number().numberBetween(1, 10)
        val price = faker.number().numberBetween(10, 5000)

        FruitSale(uuid, name, saleDatetime, quantity, price)
      }
  }

}

