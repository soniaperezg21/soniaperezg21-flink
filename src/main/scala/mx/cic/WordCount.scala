package mx.cic
//import java.io.File
import java.io._
import scala.collection.mutable.ListBuffer

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.scala._

/**
 * Implements the "WordCount" program that computes a simple word occurrence histogram
 * over some sample data
 *
 * This example shows how to:
 *
 *   - write a simple Flink program.
 *   - use Tuple data types.
 *   - write and use user-defined functions.
 */
object WordCount {
  def main(args: Array[String]) {

    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // get input data
    //val text = env.fromElements("To be, or not to be,--that is the question:--",
    //  "Whether 'tis nobler in the mind to suffer", "The slings and arrows of outrageous fortune",
    //  "Or to take arms against a sea of troubles,")


    //Poner en una variable el path (argumento 0)
    val path = args(0)
    //print(path)

    //Archivo de Salida
    val writer =new PrintWriter(new File("salida/"  + args(1) ))

    //Obtener el path del argumento
    val files = getListOfFiles(path)
    //print(files)

    // Variables de trabajo
    var ListArchivos = new ListBuffer[String]() // Guardar el nombre de los archivos
    var ListTotales = new ListBuffer[Int]()  //Lista donde voy acumular el total por cada archivo
    var totalFinal = 0  //Variable para guardar el total de todos los archivos

    // 1) Procesar todos los archivos de un directorio para generar archivo con totales
    //Entrada: Directorio de donde se tomarán los archivos (argumento 0)
    //Salida: Generar archivo de salida (argumento1)
    //  - Primer  renglón el total de cada archivo ej 50-60-30
    //  - Segundo renglon el total general de todos. ej 140
    //*********************************************************************************************
    for (i <- 0 until files.length) {
      val archivo = files(i)
      val ruta  = archivo.toString.split("/")  // Sin el subdirectorio
      val nomArchivo = ruta(1)
      //println(archivo)
      //Obtengo el total de palabras por cada archivo quedando en un DataSet
      val text = env.readTextFile(archivo.getPath)
      val counts2 = text.flatMap { _.toLowerCase.split("\\W+") }
        .map { (_, 1) }
        .sum(1)
      //println( counts2.getClass())

      //Convierto el dataset a una colección y obtengo la suma de palabras del archivo
      val lista = counts2.collect()
      var suma = 0
      for ((k,v) <- lista)  suma = v
      //Acumula la suma a la lista y a un campo que contiene el total general
      ListArchivos += nomArchivo
      ListTotales += suma
      totalFinal += suma
      //println(valor)
    }

    //Escribo en archivo de salida Totales
    writer.write(ListArchivos.mkString(" - ")+ "\n" )  //Separa cada elemento por guión
    writer.write("*****************************************************" + "\n")
    writer.write(ListTotales.mkString(" - ")+ "\n" )  //Separa cada elemento por guión
    writer.write("*****************************************************" + "\n")
    writer.write(totalFinal.toString + "\n")
    writer.write("*****************************************************" + "\n")

    //println(ListTotales)  //Lista con los totales
    //println(totalFinal) //Total Final

    // 2) Calcular los totales de palabras por Archivo por Llave-valor
    //*********************************************************************************************
    for (i <- 0 until files.length) {

      val archivo = files(i)
      val ruta  = archivo.toString.split("/")  // Sin el subdirectorio
      val nomArchivo = ruta(1)
      val text = env.readTextFile(archivo.getPath)
      val countsW = text.flatMap { _.toLowerCase.split("\\W+") }
        .map { (_, 1) }
        .groupBy(0)
        .sum(1)
      val lista = countsW.collect()
      //imprime
      writer.write("\n"  + nomArchivo + "\n")
      for ((k,v) <- lista)  writer.write(s"$k : $v \n")
    }



    // Cerrar el archivo de salida
    writer.close()
  }

  //Función que regresa la lista de archivos en el subdirectorio
  def getListOfFiles(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

}
