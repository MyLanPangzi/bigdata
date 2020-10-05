[Parent](../README.md)

# Ranger

## 目的

* 允许用户使用UI或REST API对所有和安全相关的任务进行集中化的管理
* 允许用户使用一个管理工具对操作Hadoop体系中的组件和工具的行为进行细粒度的授权
* 支持Hadoop体系中各个组件的授权认证标准
* 增强了对不同业务场景需求的授权方法支持，例如基于角色的授权或基于属性的授权
* 支持对Hadoop组件所有涉及安全的审计行为的集中化管理

## 架构

* Rest API/UI
* Admin
    * DB
    * Solr
* Plugin
    * Hadoop
    * Hive
    * Kafka
    * Sqoop

## 支持框架

* Apache Hadoop
* Apache Hive
* Apache HBase
* Apache Storm
* Apache Knox
* Apache Solr
* Apache Kafka
* YARN
* NIFI

## 安装

* admin
* usersync
* create service
* enable hive plugin
