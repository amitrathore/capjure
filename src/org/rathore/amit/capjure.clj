(ns org.rathore.amit.capjure)

(import '(org.apache.hadoop.hbase HBaseConfiguration HColumnDescriptor HTableDescriptor)
	'(org.apache.hadoop.hbase.client HTable Scanner HBaseAdmin)
	'(org.apache.hadoop.hbase.io BatchUpdate Cell)
	'(org.apache.hadoop.hbase.filter InclusiveStopRowFilter))

(def *hbase-master* "localhost:60000")
(def *primary-keys-config* {})

(declare symbol-name)

(defn encoding-keys []
  (*primary-keys-config* :encode))

(defn decoding-keys []
  (*primary-keys-config* :decode))

(defn qualifier-for [key-name]
  (((encoding-keys) (keyword key-name)) :qualifier))

(defn encoding-functor-for [key-name]
  (((encoding-keys) (keyword key-name)) :functor))

(defn all-primary-keys []
  (map #(symbol-name %) (keys (encoding-keys))))

(defn primary-key [column-family]
  (first (filter #(.startsWith column-family (str %)) (all-primary-keys))))

(defn decoding-functor-for [key-name]
  (((decoding-keys) (keyword key-name)) :functor))

(defn decode-with-key [key-name value]
  ((decoding-functor-for key-name) value))

(declare flatten add-to-insert-batch capjure-insert hbase-table read-row read-cell)

(defn capjure-insert [object-to-save hbase-table-name row-id]
  (let [table (hbase-table hbase-table-name)
	batch-update (BatchUpdate. (str row-id))
	flattened (flatten object-to-save)]
    (add-to-insert-batch batch-update flattened)
    (.commit table batch-update)))

(defn add-to-insert-batch [batch-update flattened-list]
  (loop [flattened-pairs flattened-list]
    (if (not (empty? flattened-pairs))
      (let [first-pair (first flattened-pairs)
	    column (first first-pair)
	    value (last first-pair)]
	(.put batch-update column (.getBytes (str value)))
	(recur (rest flattened-pairs))))))

(defn symbol-name [prefix]
  (if
   (keyword? prefix) 
     (name prefix)
     (str prefix)))

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
    (if
     (map? (first all)) 
     (process-maps key all)
     (process-strings key (to-array all)))))

(defn process-maps [key maps]
  (let [qualifier (qualifier-for key)
	encoding-functor (encoding-functor-for key)]
    (apply merge (map 
		  (fn [single-map]
		    (process-map (symbol-name key) (encoding-functor single-map) (dissoc single-map qualifier)))
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

(declare read-as-hash cell-value-as-string hydrate-pair has-many-strings-hydration has-many-objects-hydration has-one-string-hydration has-one-object-hydration collapse-for-hydration)
(defn is-from-primary-keys [key-name]
  (let [key-name-str (symbol-name key-name)]
    (some #(.startsWith key-name-str %) (all-primary-keys))))

(defn column-name-empty? [key-name]
  (= 1 (count (.split key-name ":"))))

(defn collapse-for-hydration [mostly-hydrated]
  (let [primary-keys (to-array (all-primary-keys))]
    (areduce primary-keys idx ret mostly-hydrated
	     (let [primary-key (symbol-name (aget primary-keys idx))
		   inner-map (ret primary-key)
		   inner-values (apply vector (vals inner-map))]
	       (if (empty? inner-values) 
		 ret
		(assoc ret primary-key inner-values))))))

(defn hydrate [flattened-object]
  (let [flat-keys (to-array (keys flattened-object))]
    (collapse-for-hydration (areduce flat-keys idx ret {}
				     (hydrate-pair (aget flat-keys idx) flattened-object ret)))))

(defn hydrate-pair [key-name flattened hydrated]
  (let [value (.trim (str (flattened key-name)))
	key-tokens (seq (.split key-name ":"))
	column-family (first key-tokens)
	column-name (last key-tokens)]
    (cond
     (= column-name value) (has-many-strings-hydration hydrated column-family value)
     (is-from-primary-keys column-family) (has-many-objects-hydration hydrated column-family column-name value)
     (column-name-empty? key-name) (has-one-string-hydration hydrated column-family value)
     :else (has-one-object-hydration hydrated column-family column-name value))))

(defn has-one-string-hydration [hydrated column-family value]
  (assoc hydrated column-family value))

(defn has-one-object-hydration [hydrated column-family column-name value]
  (let [value-map (or (hydrated column-family) {})]
    (assoc hydrated column-family
	   (assoc value-map column-name value))))

(defn has-many-strings-hydration [hydrated column-family value]
  (let [old-value (hydrated column-family)]
    (if (nil? old-value) 
      (assoc hydrated column-family [value])
      (assoc hydrated column-family (apply vector (seq (cons value old-value)))))))

(defn has-many-objects-hydration [hydrated column-family column-name value]
  (let [outer-key (primary-key column-family)
	inner-key (.substring column-family (+ 1 (count outer-key)) (count column-family))
	primary-key-name (qualifier-for (keyword outer-key))
	inner-map (or (hydrated outer-key) {})
	inner-object (or (inner-map column-name) {(symbol-name primary-key-name) (decode-with-key outer-key column-name)})]
    (assoc hydrated outer-key 
	   (assoc inner-map column-name 
		  (assoc inner-object inner-key value)))))

(defn columns-from-hbase-row-result [hbase-row]
  (map #(String. %) (seq (.keySet hbase-row))))

(defn hbase-object-as-hash [hbase-row]
  (let [keyset (columns-from-hbase-row-result hbase-row)
	columns-and-values (map (fn [column-name]
				  {column-name (cell-value-as-string hbase-row column-name)})
				keyset)]
    (apply merge columns-and-values)))  


(defn all-versions-of-data-from-hbase-row [hbase-row]
  (let [available-columns (keys (.entrySet hbase-row))
	pair-creator (fn [col cell] {col cell})
	col-cell-pairs (map #(pair-creator % (.get hbase-row %)) available-columns)
	cell-values-stringify (fn [c] (map #(String. (.getValue %)) (iterator-seq (.iterator c))))]
    (map #(pair-creator (first %) (cell-values-stringify (last %))) col-cell-pairs)))

(defn read-as-hash [hbase-table-name row-id]
  (let [row (read-row hbase-table-name row-id)]
    (hbase-object-as-hash row)))

(defn read-as-hydrated [hbase-table-name row-id]
  (let [as-hash (read-as-hash hbase-table-name row-id)]
    (hydrate as-hash)))	

(defn row-exists? [hbase-table-name row-id-string]
  (let [table (hbase-table hbase-table-name)]
    (.exists table (.getBytes row-id-string))))	

(defn cell-value-as-string [row column-name]
  (String. (.getValue (.get row (.getBytes column-name)))))

(defn read-row [hbase-table-name row-id]
  (let [table (hbase-table hbase-table-name)]
    (.getRow table (.getBytes row-id))))

(defn read-rows [hbase-table-name row-id-list]
  (let [table (hbase-table hbase-table-name)]
    (map #(.getRow table %) row-id-list)))

(defn read-rows-between [hbase-table-name columns start-row-id end-row-id]
  (let [scanner (table-scanner hbase-table-name columns (.getBytes start-row-id) (InclusiveStopRowFilter. (.getBytes end-row-id)))]
    (iterator-seq (.iterator scanner))))



;(defn read-all-versions-between [hbase-table-name column-family-as-string start-row-id end-row-id]
;  (let [rows-between (read-rows-between hbase-table-name [column-family-as-string] start-row-id end-row-id)
;	row-ids (map #(.getRow %) rows-between)
;	rows (map #(read-all-versions hbase-table-name % 100000) row-ids)]
    
    

;    (map #(read-all-versions-as-strings hbase-table-name % 100000 column-family-as-string) row-ids)))

(defn read-all-versions [hbase-table-name row-id-string number-of-versions]
  (let [table (hbase-table hbase-table-name)]
    (.getRow table row-id-string number-of-versions)))	

(defn read-all-versions-as-strings [hbase-table-name row-id-string number-of-versions column-family-as-string]
  (let [table (hbase-table hbase-table-name)
	all-versions (.getRow table row-id-string number-of-versions)]
    (map #(String. (.getValue %)) (iterator-seq (.iterator (.get all-versions column-family-as-string))))))

(defn read-cell [hbase-table-name row-id column-name]
  (let [row (read-row hbase-table-name row-id)]
    (String. (.getValue (.get row (.getBytes column-name))))))

(defn table-iterator [hbase-table-name columns]
  (let [table (hbase-table hbase-table-name)]
    (iterator-seq (.iterator (.getScanner table (into-array columns))))))

(defn table-scanner
  ([hbase-table-name columns]
     (let [table (hbase-table hbase-table-name)]
       (.getScanner table (into-array columns))))
  ([hbase-table-name columns start-row-string]
     (let [table (hbase-table hbase-table-name)]
       (.getScanner table (into-array columns) start-row-string)))
  ([hbase-table-name columns start-row-string row-filter]
     (let [table (hbase-table hbase-table-name)
	   columns-to-scan (into-array (map #(.getBytes %) columns))]
       (.getScanner table columns-to-scan start-row-string row-filter))))

(defn rowcount [hbase-table-name & columns]
  (count (table-iterator hbase-table-name columns)))

(defn delete-all [hbase-table-name & row-ids-as-strings]
  (let [table (hbase-table hbase-table-name)]
    (doseq [row-id row-ids-as-strings]
      (.deleteAll table row-id))))

(defn hbase-config []
  (let [h-config (HBaseConfiguration.) 	
	_ (.set h-config "hbase.master", *hbase-master*)]
    h-config))

(defn create-hbase-table [table-name & column-families]
  (let [desc (HTableDescriptor. table-name)
	_ (doall (map #(.addFamily desc (HColumnDescriptor. %)) column-families))
	admin (HBaseAdmin. (hbase-config))]
    (.createTable admin desc)))

(defn drop-hbase-table [hbase-table-name]
  (let [admin (HBaseAdmin. (hbase-config))]
    (.deleteTable hbase-table-name)))

(defn hbase-table [hbase-table-name]
  (let [h-config (hbase-config)]
    (HTable. h-config hbase-table-name)))