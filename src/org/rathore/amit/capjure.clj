(ns org.rathore.amit.capjure)

(import '(org.apache.hadoop.hbase HBaseConfiguration)
	'(org.apache.hadoop.hbase.client HTable Scanner)
	'(org.apache.hadoop.hbase.io BatchUpdate Cell))


(def *mock-mode* false)
(def *hbase-master* "localhost:60000")
(def *qualifier-config* {:inserts :merchant_product_id})

(defn test-save [data-string]
  (let [table (HTable. (HBaseConfiguration.) "amit_development_consumer_events")
       batch-update (BatchUpdate. "myRow")]
    (.put batch-update "api:", (.getBytes data-string))
    (.commit table batch-update)))

(declare flatten add-to-insert-batch capjure-insert)
(defn try-to-insert [object hbase-table]
  (try
   (capjure-insert object hbase-table)
   (catch Exception _ (println "exception!!"))))

(defn capjure-insert [object-to-save hbase-table-name]
  (let [flattened (flatten object-to-save)
	table (HTable. (HBaseConfiguration.) "amit_development_consumer_events")
	batch-update (BatchUpdate. (str (System/currentTimeMillis)))]
    (dorun (add-to-insert-batch batch-update flattened))
    (.commit table batch-update)))

(defn add-to-insert-batch [batch-update flattened]
;  (loop [column (first
    (.put batch-update column (.getBytes (str value)))))

(defn symbol-name [prefix]
  (cond
   (keyword? prefix) (name prefix)
   :else (str prefix)))

(defn new-key [part1 separator part2]
  (str (symbol-name part1) separator (symbol-name part2)))

(defn prepend-to-keys [prefix separator hash-map]
  (let [all-keys (to-array (keys hash-map))]
    (areduce all-keys idx ret {} 
	     (assoc ret 
	       (new-key prefix separator (aget all-keys idx))
	       (hash-map (aget all-keys idx))))))

(defn postpend-to-keys [postfix separator hash-map]
  (let [all-keys (to-array (keys hash-map))]
    (areduce all-keys idx ret {} 
	     (assoc ret 
	       (new-key (aget all-keys idx) separator postfix)
	       (hash-map (aget all-keys idx))))))

(declare process-multiple process-maps process-map process-strings)
(defn process-key-value [key value]
  (cond
   (map? value) (prepend-to-keys key ":" value)
   (vector? value) (process-multiple key value)
   :else {(new-key key ":" "") value}))

(defn process-multiple [key values]
  (let [all (seq values)]
    (cond
     (map? (first all)) (process-maps key all)
     :else (process-strings key (to-array all)))))

(defn process-maps [key maps]
  (let [qualifier (*qualifier-config* key)]
    (apply merge (map 
		  (fn [single-map]
		    (process-map (symbol-name key) (single-map qualifier) (dissoc single-map qualifier)))
		  maps))))

(defn process-map [initial-prefix final-prefix single-map]
  (let [all-keys (to-array (keys single-map))]
    (areduce all-keys idx ret {}
	     (assoc ret
		   (str initial-prefix "_" (symbol-name (aget all-keys idx)) ":" final-prefix)
		   (single-map (aget all-keys idx))))))

(defn process-strings [key strings] 
  (areduce strings idx ret {}
	   (assoc ret (new-key key ":" (aget strings idx)) (aget strings idx))))

(defn flatten [bloated_object]
  (apply merge (map 
		(fn [pair] 
		  (process-key-value (first pair) (last pair)))
		(seq bloated_object))))

(defn hbase_table [object_name table_name column_information]
     ())
