(ns org.rathore.amit.capjure)

(import '(org.apache.hadoop.hbase HBaseConfiguration)
	'(org.apache.hadoop.hbase.client HTable Scanner)
	'(org.apache.hadoop.hbase.io BatchUpdate Cell))


(def *mock-mode* false)
(def *hbase-master* "localhost:60000")
(def *primary-keys-config* {})

(declare flatten add-to-insert-batch capjure-insert hbase-table)
(defn capjure-insert [object-to-save hbase-table-name row-id]
  (let [table (hbase-table hbase-table-name)
	batch-update (BatchUpdate. (str row-id))
	flattened (flatten object-to-save)]
    (add-to-insert-batch batch-update flattened)
    (.commit table batch-update)))

(defn add-to-insert-batch [batch-update flattened-list]
  (loop [flattened-pairs flattened-list]
    (if (empty? flattened-pairs)
      :done
      (let [first-pair (first flattened-pairs)
	    column (first first-pair)
	    value (last first-pair)]
	(.put batch-update column (.getBytes (str value)))
	(recur (rest flattened-pairs))))))

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
  (let [qualifier (*primary-keys-config* key)]
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

(defn read-row [hbase-table-name row-id]
  (let [table (hbase-table hbase-table-name)]
    (.getRow table (.getBytes row-id))))

(defn read-cell [hbase-table-name row-id column-name]
  (let [row (read-row hbase-table-name row-id)]
    (String. (.getValue (.get row (.getBytes column-name))))))
	
(defn rowcount [hbase-table-name & columns]
  (let [table (hbase-table hbase-table-name)
	row-results (iterator-seq (.iterator (.getScanner table (into-array columns))))]
    (count row-results)))  

(defn delete-all [hbase-table-name column-name]
  (let [table (hbase-table hbase-table-name)]
    (.deleteAll table column-name)))

(defn hbase-table [hbase-table-name]
  (let [h-config (HBaseConfiguration.) 	
	_ (.set h-config "hbase.master", *hbase-master*)]
    (HTable. h-config hbase-table-name)))
  