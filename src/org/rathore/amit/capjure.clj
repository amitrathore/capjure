(ns org.rathore.amit.capjure)

(def *mock-mode* false)
(def *hbase-master* "tank.cinchcorp.com")
(def *qualifier-config* {:inserts :merchant_product_id})

(defn save [object_to_save]
  ())

(defn symbol-name [prefix]
  (cond
   (keyword? prefix) (name prefix)
   (string? prefix) prefix))

(defn postfixed-key [key separator postfix]
  (str (symbol-name key) separator (symbol-name postfix)))

(defn prefixed-key [key separator prefix]
  (str (symbol-name prefix) separator (symbol-name key)))

(defn prepend-to-keys [prefix separator hash-map]
  (let [all-keys (to-array (keys hash-map))]
    (areduce all-keys idx ret {} 
	     (assoc ret 
	       (prefixed-key (aget all-keys idx) separator prefix)
	       (hash-map (aget all-keys idx))))))

(defn postpend-to-keys [postfix separator hash-map]
  (let [all-keys (to-array (keys hash-map))]
    (areduce all-keys idx ret {} 
	     (assoc ret 
	       (postfixed-key (aget all-keys idx) separator postfix)
	       (hash-map (aget all-keys idx))))))

(declare process-multiple process-maps process-strings)
(defn process-key-value [key value]
  (cond
   (not (vector? value)) (prepend-to-keys key ":" value)
   :else (process-multiple key value)))

(defn process-multiple [key values]
  (let [all (seq values)]
    (cond
     (map? (first all)) (process-maps key all)
     :else (process-strings key all))))

(defn process-maps [key maps]
  (let [qualifier (*qualifier-config* key)]
    (println "qualifier is " qualifier)
    (apply merge (map 
		  (fn [single-map]
		    (let [prefix (prefixed-key key "_" (single-map qualifier))]
		      (println "prefix is " prefix)
		      (prepend-to-keys prefix "" (dissoc single-map qualifier))))
		  maps))))

(defn process-strings [key strings] ())

(defn flatten [bloated_object]
  (apply merge (map 
		(fn [pair] 
		  (process-key-value (first pair) (last pair)))
		(seq bloated_object))))

(defn hbase_table [object_name table_name column_information]
     ())
