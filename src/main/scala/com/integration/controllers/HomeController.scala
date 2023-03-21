package com.integration.controllers

import javax.servlet.http.HttpServletResponse
import org.apache.spark.sql.SparkSession
import javax.validation.Valid
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.{GetMapping, PostMapping, RequestBody, RequestMapping, ResponseBody, RestController}

@RequestMapping(path = Array("/users"))
@RestController
class HomeController {
  @GetMapping(path = Array("/"))
  def demo: String = {
    "Hola!"
  }

  def init() : Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExample")
      .getOrCreate();

    println("First SparkContext:")
    println("APP Name :" + spark.sparkContext.appName);
    println("Deploy Mode :" + spark.sparkContext.deployMode);
    println("Master :" + spark.sparkContext.master);

  }

  @GetMapping(path = Array("/make-table"))
  @ResponseBody
  def makeTable(@Valid @RequestBody tableName: String, response: HttpServletResponse): Unit = {
    /*
      Code to integrate spark with this project
    */
    response.getWriter.println("make table API")
    response.getWriter.flush()
    response.getWriter.close()
  }

  @GetMapping(path = Array("/run-query"))
  def runQuery(@Valid @RequestBody query: String, response: HttpServletResponse): Unit = {
    /*
      Code to integrate spark with this project
    */
    response.setStatus(HttpStatus.OK.value())
    response.getWriter.println("Query Run successfully!")
    response.getWriter.flush()
    response.getWriter.close()
  }

}
