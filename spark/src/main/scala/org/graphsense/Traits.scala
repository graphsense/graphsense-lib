package org.graphsense

trait Job {

  def run(from: Option[Int], to: Option[Int]): Unit

}
