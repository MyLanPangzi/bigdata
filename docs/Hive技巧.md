[Parent](../README.md)
<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->

- [Hive疑难杂症](#hive%E7%96%91%E9%9A%BE%E6%9D%82%E7%97%87)
  - [累积型事务表的数据处理](#%E7%B4%AF%E7%A7%AF%E5%9E%8B%E4%BA%8B%E5%8A%A1%E8%A1%A8%E7%9A%84%E6%95%B0%E6%8D%AE%E5%A4%84%E7%90%86)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->


# Hive疑难杂症

## 累积型事务表的数据处理

* 先从ods层查出新数据
* 查出历史数据，过滤出在分区在新数据中的旧数据，（根据具体分区字段含义过滤）
* 使用full join关联新旧数据集，查询时使用if(新数据 is null, 旧数据, 新数据)

## str_to_map的巧用

* 搭配collect_set，concat，concat_ws使用，可变成一个map，再结合if使用，可灵活判断数据是否存在

## 拉链表

* 首次手动导入原始数据
* 每日导入ods变化数据
* 合并原始数据以及ods变化数据至临时表
    * ods的变化数据新增2列，起始时间（导入日期），结束时间（9999-99-99）
    * 查询出dwd的拉链表，left join ods变化数据，将结束时间=9999-99-99那条数据的结束时间改成（导入日期-1）
        * 需注意重复导入的情况，条件要加上导入日期大于结束时间，才能修改结束时间字段
    * union两个查询的结果
* 临时表再覆盖dwd拉链表