(ns org.rathore.amit.capjure)

(use 'org.rathore.amit.utils)
(import '(java.util Set)
	'(org.apache.hadoop.hbase HBaseConfiguration HColumnDescriptor HTableDescriptor)
	'(org.apache.hadoop.hbase.client Get HBaseAdmin HTable Scan Scanner)
	'(org.apache.hadoop.hbase.io BatchUpdate Cell)
	'(org.apache.hadoop.hbase.filter Filter InclusiveStopFilter RegExpRowFilter StopRowFilter RowFilterInterface))

(def *hbase-master*)
(def *single-column-family?*) 
(def *hbase-single-column-family*)
(def *primary-keys-config*)

(defn COLUMN-NAME-DELIMITER []
  (if *single-column-family?* "__" ":"))

(defn single-column-prefix []
  (str *hbase-single-column-family* ":"))

(declare symbol-name)

(defmemoized symbolize [a-string]
  (keyword a-string))

(defn encoding-keys []
  (*primary-keys-config* :encode))

(defn decoding-keys []
  (*primary-keys-config* :decode))

(defmemoized qualifier-for [key-name]
  (((encoding-keys) (keyword key-name)) :qualifier))

(defmemoized encoding-functor-for [key-name]
  (((encoding-keys) (keyword key-name)) :functor))

(defmemoized all-primary-keys []
  (map #(symbol-name %) (keys (encoding-keys))))

(defmemoized primary-key [column-family]
  (first (filter #(.startsWith column-family (str %)) (all-primary-keys))))

(defmemoized decoding-functor-for [key-name]
  (((decoding-keys) (keyword key-name)) :functor))

(defmemoized decode-with-key [key-name value]
  ((decoding-functor-for key-name) value))

(declare flatten add-to-insert-batch capjure-insert hbase-table read-row read-cell)

(defn capjure-insert [object-to-save hbase-table-name row-id]
  (let [#^HTable table (hbase-table hbase-table-name)
        batch-update (BatchUpdate. (str row-id))
        flattened (flatten object-to-save)]
    (add-to-insert-batch batch-update flattened)
    (.commit table batch-update)))

(defn add-to-insert-batch [#^BatchUpdate batch-update flattened-list]
  (loop [flattened-pairs flattened-list]
    (if (not (empty? flattened-pairs))
      (let [first-pair (first flattened-pairs)
            column (first first-pair)
            value (last first-pair)]
        (.put batch-update column (.getBytes (str value)))
        (recur (rest flattened-pairs))))))

(defmemoized symbol-name [prefix]
  (if (keyword? prefix) 
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
    (map? value) (prepend-to-keys key (COLUMN-NAME-DELIMITER) value)
    (vector? value) (process-multiple key value)
    :else {(new-key key (COLUMN-NAME-DELIMITER) "") value}))

(defn process-multiple [key values]
  (let [all (seq values)]
    (if (map? (first all)) 
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
        (str initial-prefix "_" (symbol-name (aget all-keys idx)) (COLUMN-NAME-DELIMITER) final-prefix)
        (single-map (aget all-keys idx))))))

(defn process-strings [key strings] 
  (areduce strings idx ret {}
	  (assoc ret (new-key key (COLUMN-NAME-DELIMITER) (aget strings idx)) (aget strings idx))))

(defn prepend-keys-for-single-column-family [flattened]
  (if-not *single-column-family?*
    flattened
    (let [prefix (single-column-prefix)
          key-prepender (fn [[key value]] 
                          {(str prefix key) value})]
      (apply merge (map key-prepender flattened)))))
  
(defn flatten [bloated_object]
  (let [f (apply merge (map 
			(fn [pair] 
			  (process-key-value (first pair) (last pair)))
			(seq bloated_object)))]
    (prepend-keys-for-single-column-family f)))
    
(declare read-as-hash cell-value-as-string hydrate-pair has-many-strings-hydration has-many-objects-hydration has-one-string-hydration has-one-object-hydration collapse-for-hydration)

(defmemoized is-from-primary-keys [key-name]
  (let [key-name-str (symbol-name key-name)]
    (some #(.startsWith key-name-str %) (all-primary-keys))))

(defmemoized column-name-empty? [key-name]
  (= 1 (count (.split key-name (COLUMN-NAME-DELIMITER)))))

(defn strip-prefixes [flattened-and-prepended]
  (if-not *single-column-family?*
    flattened-and-prepended
    (let [prefix-length (count (single-column-prefix))
          prefix-stripper (fn [[key value]]
                            {(.substring key prefix-length) value})]
      (apply merge (map prefix-stripper flattened-and-prepended)))))

(defn collapse-for-hydration [mostly-hydrated]
  (let [primary-keys (to-array (all-primary-keys))]
    (areduce primary-keys idx ret mostly-hydrated
	     (let [primary-key (symbol-name (aget primary-keys idx))
             inner-map (ret primary-key)
             inner-values (apply vector (vals inner-map))]
	       (if (empty? inner-values) 
           ret
           (assoc ret primary-key inner-values))))))

(defn hydrate [flattened-and-prepended]
  (let [flattened-object (strip-prefixes flattened-and-prepended)
        flat-keys (to-array (keys flattened-object))
        mostly-hydrated (areduce flat-keys idx ret {}
                          (hydrate-pair (aget flat-keys idx) flattened-object ret))
        pair-symbolizer (fn [[key value]] {(symbolize key) value})]
    (apply merge (map pair-symbolizer (collapse-for-hydration mostly-hydrated)))))

(defn hydrate-pair [#^String key-name flattened hydrated]
  (let [#^String value (.trim (str (flattened key-name)))
        key-tokens (seq (.split key-name (COLUMN-NAME-DELIMITER)))
        #^String column-family (first key-tokens)
        #^String column-name (last key-tokens)]
    (cond
      (= column-name value) (has-many-strings-hydration hydrated column-family value)
      (is-from-primary-keys column-family) (has-many-objects-hydration hydrated column-family column-name value)
      (column-name-empty? key-name) (has-one-string-hydration hydrated column-family value)
      :else (has-one-object-hydration hydrated column-family column-name value))))

(defn has-one-string-hydration [hydrated #^String column-family #^String value]
  (assoc hydrated (symbolize column-family) value))

(defn has-one-object-hydration [hydrated #^String column-family #^String column-name #^String value]
  (let [value-map (or (hydrated column-family) {})]
    (assoc hydrated column-family
	   (assoc value-map (symbolize column-name) value))))

(defn has-many-strings-hydration [hydrated #^String column-family #^String value]
  (let [old-value (hydrated column-family)]
    (if (nil? old-value) 
      (assoc hydrated (symbolize column-family) [value])
      (assoc hydrated (symbolize column-family) (apply vector (seq (cons value old-value)))))))

(defn has-many-objects-hydration [hydrated #^String column-family #^String column-name #^String value]
  (let [#^String outer-key (primary-key column-family)
        #^String inner-key (.substring column-family (+ 1 (count outer-key)) (count column-family))
        primary-key-name (qualifier-for outer-key)
        inner-map (or (hydrated outer-key) {})
        inner-object (or (inner-map column-name) {(symbolize (symbol-name primary-key-name)) (decode-with-key outer-key column-name)})]
    (assoc hydrated outer-key 
	    (assoc inner-map column-name
		    (assoc inner-object (symbolize inner-key) value)))))

(defn columns-from-hbase-row-result [hbase-row-result]
  (let [#^Set<byte[]> key-set (.keySet hbase-row-result)]
    (map (fn [#^bytes k] (String. k)) (seq key-set))))

(defn hbase-object-as-hash [hbase-result]
  (let [extractor (fn [kv]
                    {(String. (.getColumn kv)) (String. (.getValue kv))})
        key-values-objects (.list hbase-result)
        key-values (map extractor key-values-objects)]
    (apply merge key-values)))

(defn hydrate-hbase-row [hbase-row]
  (hydrate (hbase-object-as-hash hbase-row)))

(defn to-strings [array-of-byte-arrays]
  (map #(String. %) array-of-byte-arrays))

(defn column-name-from [column-family-colon-column-name]
  (last (to-strings (.split (String. column-family-colon-column-name) (COLUMN-NAME-DELIMITER)))))

(defn read-as-hash [hbase-table-name row-id]
  (let [row (read-row hbase-table-name row-id)]
    (hbase-object-as-hash row)))

(defn read-as-hydrated [hbase-table-name row-id]
  (let [as-hash (read-as-hash hbase-table-name row-id)]
    (hydrate as-hash)))	

(defn row-exists? [hbase-table-name row-id-string]
  (let [#^HTable table (hbase-table hbase-table-name)]
    (.exists table (.getBytes row-id-string))))	

(defn cell-value-as-string [row #^String column-name]
  (let [value (.getValue row (.getBytes column-name))]
    (if-not value ""
      (String. value))))
  
(defn get-result-for [hbase-table-name #^String row-id]
  (let [#^HTable table (hbase-table hbase-table-name)
        hbase-get-row-id (Get. (.getBytes row-id))]
    (.get table hbase-get-row-id)))

(defn read-row [hbase-table-name row-id]
  (get-result-for hbase-table-name row-id))

(defn read-rows [hbase-table-name row-id-list]
  (map #(get-result-for hbase-table-name %) row-id-list))

(declare table-scanner)
(defn read-rows-between [hbase-table-name columns start-row-id end-row-id]
  (let [row-filter (InclusiveStopFilter. (.getBytes end-row-id))
        #^Scanner scanner (table-scanner hbase-table-name columns (.getBytes start-row-id) row-filter)]
    (iterator-seq (.iterator scanner))))

(defn read-rows-up-to [hbase-table-name columns start-row-id end-row-id]
  ;; NOTE: not inclusive of the end row
  (let [#^Scanner scanner (table-scanner hbase-table-name columns (.getBytes start-row-id) (StopRowFilter. (.getBytes end-row-id)))]
    (iterator-seq (.iterator scanner))))

(defn row-id-of-row [ hbase-row]
  (String. (.getRow hbase-row)))

(defn first-row-id [hbase-table-name column-name]
  (let [#^Scanner first-row-scanner (table-scanner hbase-table-name [column-name])]
    (row-id-of-row (first (iterator-seq (.iterator first-row-scanner))))))

(defn read-rows-like [hbase-table-name columns start-row-id-string row-id-regex]
  (let [#^Scanner scanner (table-scanner hbase-table-name columns (.getBytes start-row-id-string) (RegExpRowFilter. row-id-regex))]
    (iterator-seq (.iterator scanner))))

(defn read-all-versions 
  ([hbase-table-name row-id-string number-of-versions]
     (let [#^HTable table (hbase-table hbase-table-name)]
       (.getRow table row-id-string number-of-versions)))
  ([hbase-table-name row-id-string column-family-as-string number-of-versions]
     (let [#^HTable table (hbase-table hbase-table-name)]
       (.getRow table row-id-string (into-array [column-family-as-string]) number-of-versions))))

(defn all-versions-as-hash [hbase-table-name row-id-string column-family-as-string number-of-versions]
  (let [hbase-row (read-all-versions hbase-table-name row-id-string column-family-as-string number-of-versions)
        available-columns (keys (.entrySet hbase-row))
        cell-versions-collector (fn [col-name] { 
                                                (column-name-from col-name) 
                                                (map #(String. (.getValue %)) (iterator-seq (.iterator (.get hbase-row col-name)))) 
                                                })]
    (apply merge (map cell-versions-collector available-columns))))

(defn read-all-versions-between [hbase-table-name column-family-as-string start-row-id end-row-id]
  (let [rows-between (read-rows-between hbase-table-name [column-family-as-string] start-row-id end-row-id)
        row-ids (map (fn [ rr] (row-id-of-row rr)) rows-between)]
    (apply merge (map (fn[row-id] {row-id (all-versions-as-hash hbase-table-name row-id column-family-as-string 100000)}) row-ids))))

(defn read-cell [hbase-table-name row-id column-name]
  (let [row (read-row hbase-table-name row-id)]
    (cell-value-as-string row column-name)))

(defn table-iterator [#^String hbase-table-name columns]
    (iterator-seq (.iterator (table-scanner hbase-table-name columns))))

(defn add-columns-to-scan [#^Scan scan columns]
  (doseq [#^String col columns]
    (.addColumn scan (.getBytes col))))

(defn scan-for-all [columns]
  (let [scan (Scan.)]
    (add-columns-to-scan scan columns)
    scan))

(defn scan-for-start [columns #^bytes start-row-bytes]
  (let [scan (Scan. start-row-bytes)]
    (add-columns-to-scan scan columns)
    scan))

(defn scan-for-start-and-filter [columns #^bytes start-row-bytes #^Filter filter]
  (let [scan (Scan. start-row-bytes filter)]
    (add-columns-to-scan scan columns)
    scan))

(defn table-scanner
  ([#^String hbase-table-name columns]
     (let [table (hbase-table hbase-table-name)]
       (.getScanner table (scan-for-all columns))))
  ([#^String hbase-table-name columns #^String start-row-string]
     (let [table (hbase-table hbase-table-name)]
       (.getScanner table (scan-for-start columns (.getBytes start-row-string)))))
  ([#^String hbase-table-name columns #^bytes start-row-bytes #^RowFilterInterface row-filter]
     (let [table (hbase-table hbase-table-name)]
       (.getScanner table (scan-for-start-and-filter columns start-row-bytes row-filter)))))

(defn hbase-row-seq [scanner]
  (let [first-row (.next scanner)]
    (if-not first-row
      nil
      (lazy-seq 
        (cons first-row (hbase-row-seq scanner))))))

(defn next-row-id [#^String hbase-table-name column-to-use row-id]
  (let [scanner (table-scanner hbase-table-name [column-to-use] row-id)
        _ (.next scanner)]
    (row-id-of-row (.next scanner))))

(defn rowcount [#^String hbase-table-name & columns]
  (count (table-iterator hbase-table-name columns)))

(defn delete-all [#^String hbase-table-name & row-ids-as-strings]
  (let [table (hbase-table hbase-table-name)]
    (doseq [row-id row-ids-as-strings]
      (.deleteAll table row-id))))

(defmemoized column-families-for [hbase-table-name]
  (let [table (hbase-table hbase-table-name)
        table-descriptor (.getTableDescriptor table)]
    (map #(String. (.getNameWithColon %)) (.getFamilies table-descriptor))))

(defn column-names-as-strings [ result-row]
  (map #(String. %) (.keySet result-row)))

(defmemoized hbase-config []
  (let [h-config (HBaseConfiguration.) 	
        _ (.set h-config "hbase.master", *hbase-master*)]
    h-config))

(defn create-hbase-table [#^String table-name max-versions & column-families]
  (let [desc (HTableDescriptor. table-name)
        col-desc (fn [col-family-name]
                   (let [hcdesc (HColumnDescriptor. col-family-name)]
                     (.setMaxVersions hcdesc max-versions)
                     (.addFamily desc hcdesc)))
        _ (doall (map col-desc column-families))
        admin (HBaseAdmin. (hbase-config))]
    (.createTableAsync admin desc)))

(defn add-hbase-columns [table-name column-family-names versions]
  (if-not (empty? column-family-names)
	  (let [admin (HBaseAdmin. (hbase-config))
          col-desc (fn [col-name] (let [desc (HColumnDescriptor. col-name)]
                                    (.setMaxVersions desc versions)
                                    desc))]
	    (.disableTable admin (.getBytes table-name))
	    (doall (map #(.addColumn admin table-name (col-desc %)) column-family-names))
	    (.enableTable admin (.getBytes table-name)))))	

(defn clone-table [#^String new-hbase-table-name #^String from-hbase-table-name max-versions]
  (apply create-hbase-table new-hbase-table-name max-versions (column-families-for from-hbase-table-name)))

(defn disable-table [#^String table-name]
  (let [admin (HBaseAdmin. (hbase-config))]
    (.disableTable admin (.getBytes table-name))))

(defn enable-table [#^String table-name]
  (let [admin (HBaseAdmin. (hbase-config))]
    (.enableTable admin (.getBytes table-name))))	

(defn drop-hbase-table [#^String hbase-table-name]
  (let [admin (HBaseAdmin. (hbase-config))]
    (.deleteTable hbase-table-name)))

(defn hbase-table [#^String hbase-table-name]
  (let [h-config (hbase-config)
        table (HTable. h-config hbase-table-name)]
    (.setScannerCaching table 1000)
    table))

(defmacro with-hbase-table [[table hbase-table-name] & exprs]
  `(let [~table (hbase-table ~hbase-table-name)]
     (do ~@exprs
       (.close ~table))))

(defmacro with-scanner [[scanner] & exprs]
  `(do ~@exprs
     (.close ~scanner)))