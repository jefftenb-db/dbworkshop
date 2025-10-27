# Databricks notebook source
# Helper class that captures the execution context

import unicodedata
import re

class CloudLakehouseLabsContext:
  def __init__(self, useCase: str, catalog: str):
    self.__useCase = useCase
    #self.__cloud = spark.conf.get("spark.databricks.clusterUsageTags.cloudProvider").lower()
    self.__cloud = 'aws'
    self.__user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
    text = self.__user
    try: text = unicode(text, 'utf-8')
    except (TypeError, NameError): pass
    text = unicodedata.normalize('NFD', text)
    text = text.encode('ascii', 'ignore').decode("utf-8").lower()
    self.__user_id = re.sub("[^a-zA-Z0-9]", "_", text)
    self.__volumeName = useCase
    self.__catalog = catalog

    # Create the working schema
    catalogName = self.__catalog
    databaseName = self.__user_id # + '_' + self.__useCase
    volumeName = self.__volumeName
    try:
      spark.sql("create database if not exists " + catalogName + "." + databaseName)
      spark.sql("CREATE VOLUME IF NOT EXISTS " + catalogName + "." + databaseName + "." + volumeName)
      spark.sql("CREATE VOLUME IF NOT EXISTS " + catalogName + "." + databaseName + ".home")
    except Exception as e:
      pass
    if catalogName is None: raise Exception("No catalog found with CREATE SCHEMA privileges for user '" + self.__user + "'")
    self.__schema = databaseName
    spark.sql('use database ' + self.__schema)
    spark.sql('use catalog ' + self.__catalog)

    # Create the working directory under UC volume
    self.__workingDirectory = '/Volumes/' + catalogName + '/' + databaseName + '/home_' + self.__useCase

  def cloud(self): return self.__cloud

  def user(self): return self.__user

  def schema(self): return self.__schema

  def volumeName(self): return self.volumeName

  def catalog(self): return self.__catalog

  def catalogAndSchema(self): return self.__catalog + '.' + self.__schema

  def workingDirectory(self): return self.__workingDirectory

  def workingVolumeDirectory(self): return "/Volumes/" + self.__catalog + "/"+self.__schema+"/"+self.__volumeName

  def useCase(self): return self.__useCase

  def userId(self): return self.__user_id

  def dropAllDataAndSchema(self):
    try:
      spark.sql('DROP DATABASE IF EXISTS ' + self.catalogAndSchema() + ' CASCADE')
    except Exception as e:
      print(str(e))
    try:
      dbutils.fs.rm(self.__workingDirectory, recurse=True)
    except Exception as e:
      print(str(e))
