(ns org.rathore.amit.filter
  (:import [org.apache.hadoop.hbase.filter BinaryComparator CompareFilter$CompareOp RowFilter]))

(defn binary-comparator-for [str]
  (BinaryComparator. (.getBytes str)))

;; Includes things equal to comparator
;; (which is useful in a wrapper, like the WhileMatchFilter)
(defn filter-for-equal-to [the-comparator]
  (RowFilter. CompareFilter$CompareOp/EQUAL the-comparator))

;; Includes things greater than or equal to the comparator
(defn filter-for-greater-than-or-equal-to [the-comparator]
  (RowFilter. CompareFilter$CompareOp/GREATER_OR_EQUAL the-comparator))
