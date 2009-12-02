(ns org.rathore.amit.filter
  (:import [org.apache.hadoop.hbase.filter BinaryComparator CompareFilter$CompareOp RowFilter]))

(defn binary-comparator-for [str]
  (BinaryComparator. (.getBytes str)))

(defn filter-for-row-id-after-inclusive [the-comparator]
  "Includes things greater than or equal to the comparator."
  (RowFilter. CompareFilter$CompareOp/GREATER_OR_EQUAL the-comparator))
