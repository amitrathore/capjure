(ns org.rathore.amit.capjure
  (:use org.rathore.amit.capjure-utils)
  (:import (java.util Set)
           (org.apache.hadoop.hbase HBaseConfiguration HColumnDescriptor HTableDescriptor)
           (org.apache.hadoop.hbase.client Delete Get HBaseAdmin HTable Put Scan Scanner)
           (org.apache.hadoop.hbase.io BatchUpdate Cell)
           (org.apache.hadoop.hbase.util Bytes)
           (org.apache.hadoop.hbase.filter Filter InclusiveStopFilter RegExpRowFilter StopRowFilter RowFilterInterface)))

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

(defn create-put [row-id version-timestamp]
  (let [put (Put. (Bytes/toBytes row-id))]
    (if version-timestamp
      (.setTimeStamp put version-timestamp))
    put))

(defn insert-with-put [object-to-save hbase-table-name put]
  (let [#^HTable table (hbase-table hbase-table-name)
        flattened (flatten object-to-save)]
    (add-to-insert-batch put flattened)
    (.put table put)))

(defn capjure-insert
  ([object-to-save hbase-table-name row-id]
     (let [put (create-put row-id nil)]
       (insert-with-put object-to-save hbase-table-name put)))
  ([object-to-save hbase-table-name row-id version-timestamp]
     (let [put (create-put row-id version-timestamp)]
       (insert-with-put object-to-save hbase-table-name put))))

(defn to-bytes [value]
  (Bytes/toBytes (str (or value ""))))

(defn add-to-insert-batch [put flattened-list]
  (doseq [[column value] flattened-list]
    (let [[family qualifier] (.split column ":")]
      (.add put (Bytes/toBytes family) (Bytes/toBytes (or  qualifier "")) (to-bytes value))
      )))

(defmemoized symbol-name [prefix]
  (if (keyword? prefix) 
    (name prefix)
    (str prefix)))

(defn new-key [part1 separator part2]
  (str (symbol-name part1) separator (symbol-name part2)))

(defn prepend-to-keys [prefix separator hash-map]
  (reduce (fn [ret key] 
            (assoc ret 
              (new-key prefix separator key)
              (hash-map key)))
          {} (keys hash-map)))

(defn postpend-to-keys [postfix separator hash-map]
  (reduce (fn [ret key]
            (assoc ret 
              (new-key key separator postfix)
              (hash-map key))) 
          {} (keys hash-map)))

(declare process-multiple process-maps process-map process-strings)
(defn process-key-value [key value]
  (cond
    (map? value) (prepend-to-keys key (COLUMN-NAME-DELIMITER) value)
    (vector? value) (process-multiple key value)
    :else {(new-key key (COLUMN-NAME-DELIMITER) "") value}))

(defn process-multiple [key values]
  (if (map? (first values)) 
    (process-maps key values)
    (process-strings key values)))

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
  (reduce (fn [ret the-string] 
            (assoc ret (new-key key (COLUMN-NAME-DELIMITER) the-string) the-string))  
          {} strings))

(defn prepend-keys-for-single-column-family [flattened]
  (if-not *single-column-family?*
    flattened
    (let [prefix (single-column-prefix)
          key-prepender (fn [[key value]] 
                          {(str prefix key) value})]
      (apply merge (map key-prepender flattened)))))
  
(defn flatten [bloated-object]
  (let [f (apply merge (map 
			(fn [[k v]] 
			  (process-key-value k v))
			bloated-object))]
    (prepend-keys-for-single-column-family f)))
    
(declare read-as-hash cell-value-as-string hydrate-pair has-many-strings-hydration has-many-objects-hydration has-one-string-hydration has-one-object-hydration collapse-for-hydration)

(defmemoized is-from-primary-keys? [key-name]
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

(defn tokenize-column-name [full-column-name]
  (seq (.split full-column-name (COLUMN-NAME-DELIMITER))))

(defn collapse-for-hydration [mostly-hydrated]
  (reduce (fn [ret key]
            (let [primary-key (symbol-name key)
                 inner-map (ret primary-key)
                 inner-values (apply vector (vals inner-map))]
              (assoc ret primary-key inner-values)))
          mostly-hydrated (filter is-from-primary-keys? (keys mostly-hydrated))
          ))

(defn hydrate [flattened-and-prepended]
  (let [flattened-object (strip-prefixes flattened-and-prepended)
        flat-keys (keys flattened-object)
        mostly-hydrated (reduce (fn [ret key]
                                   (hydrate-pair key flattened-object ret))
                                {} flat-keys)
        pair-symbolizer (fn [[key value]] 
                          {(symbolize key) value})]
    (apply merge (map pair-symbolizer (collapse-for-hydration mostly-hydrated)))))

(defn hydrate-pair [#^String key-name flattened hydrated]
  (let [#^String value (.trim (str (flattened key-name)))
        [#^String column-family #^String column-name] (tokenize-column-name key-name)]
    (cond
      (= column-name value) (has-many-strings-hydration hydrated column-family value)
      (is-from-primary-keys? column-family) (has-many-objects-hydration hydrated column-family column-name value)
      (column-name-empty? key-name) (has-one-string-hydration hydrated column-family value)
      :else (has-one-object-hydration hydrated column-family column-name value))))

(defn has-one-string-hydration [hydrated #^String column-family #^String value]
  (assoc hydrated (symbolize column-family) value))

(defn has-one-object-hydration [hydrated #^String column-family #^String column-name #^String value]
  (let [value-map (or (hydrated column-family) {})]
    (assoc-in hydrated [column-family (symbolize column-name)] value)))

(defn has-many-strings-hydration [hydrated #^String column-family #^String value]
  (let [old-value (hydrated (symbolize column-family))]
    (if (nil? old-value) 
      (assoc hydrated (symbolize column-family) [value])
      (assoc hydrated (symbolize column-family) (conj old-value value)))))

(defn has-many-objects-hydration [hydrated #^String column-family #^String column-name #^String value]
  (let [#^String outer-key (primary-key column-family)
        #^String inner-key (.substring column-family (+ 1 (count outer-key)) (count column-family))
        primary-key-name (qualifier-for outer-key)
        inner-map (or (hydrated outer-key) {})
        inner-object (or (inner-map column-name) 
                         {(symbolize (symbol-name primary-key-name)) (decode-with-key outer-key column-name)})]
    (assoc hydrated outer-key 
	    (assoc inner-map column-name
		    (assoc inner-object (symbolize inner-key) value)))))

(defn columns-from-hbase-row-result [hbase-row-result]
  (let [#^Set<byte[]> key-set (.keySet hbase-row-result)]
    (map (fn [#^bytes k] (String. k)) (seq key-set))))

(defn hbase-object-as-hash [hbase-result]
  (let [extractor (fn [kv]
                    {(String. (.getColumn kv)) (String. (.getValue kv))})
        key-values-objects (.list hbase-result)]
    (apply merge (map extractor key-values-objects))))

(defn hydrate-hbase-row [hbase-row]
  (hydrate (hbase-object-as-hash hbase-row)))

(defn to-strings [array-of-byte-arrays]
  (map #(String. %) array-of-byte-arrays))

(defn column-name-from [column-family-colon-column-name]
  (last (tokenize-column-name column-family-colon-column-name)))

(defn read-as-hash [hbase-table-name row-id]
  (hbase-object-as-hash (read-row hbase-table-name row-id)))

(defn read-as-hydrated [hbase-table-name row-id]
  (hydrate (read-as-hash hbase-table-name row-id)))	

(defn row-exists? [hbase-table-name row-id-string]
  (let [#^HTable table (hbase-table hbase-table-name)]
    (.exists table (.getBytes row-id-string))))	

(defn cell-value-as-string [row #^String column-name]
  (let [value (.getValue row (.getBytes column-name))]
    (if-not value ""
      (String. value))))

(defn create-get
  ([row-id]
     (Get. (.getBytes row-id)))
  ([row-id number-of-versions]
     (let [the-get (create-get row-id)]
       (.setMaxVersions the-get number-of-versions)
       the-get))
  ([row-id columns number-of-versions]
     (let [the-get (create-get row-id number-of-versions)]
       (.addColumns the-get (into-array (map #(.getBytes %) columns)))
       the-get)))

(defn get-result-for [hbase-table-name #^String row-id]
  (let [#^HTable table (hbase-table hbase-table-name)
        hbase-get-row-id (create-get row-id)]
    (.get table hbase-get-row-id)))

(defn read-row [hbase-table-name row-id]
  (get-result-for hbase-table-name row-id))

(defn read-rows [hbase-table-name row-id-list]
  (map #(get-result-for hbase-table-name %) row-id-list))

(declare table-scanner)
(defn read-rows-between
  "Returns rows from start to end IDs provided.  Does NOT include stop-row-id.
   Use InclusiveStopRow Filter if you'd like to include the stop row."
  [hbase-table-name columns start-row-id-string end-row-id-string]
  (let [#^Scanner scanner (table-scanner hbase-table-name columns start-row-id-string end-row-id-string)]
    (iterator-seq (.iterator scanner))))

(defn row-id-of-row [hbase-row]
  (String. (.getRow hbase-row)))

(defn first-row-id [hbase-table-name column-name]
  (let [#^Scanner first-row-scanner (table-scanner hbase-table-name [column-name])]
    (row-id-of-row (first (iterator-seq (.iterator first-row-scanner))))))

(defn remove-single-column-family [all-versions-map]
  (let [smaller-map (fn [[row-id v]]
                      {row-id (v *hbase-single-column-family*)})]
    (doall
     (apply merge (map smaller-map all-versions-map)))))

(defn collect-by-split-key [compound-keys-map]
  (reduce (fn [bucket [compound-key vals-map]]
            (let [split-keys (seq (.split compound-key (COLUMN-NAME-DELIMITER)))
                  key1 (first split-keys)
                  key2 (second split-keys)]
              (assoc-in bucket [key1 key2] vals-map))) {} compound-keys-map))

(defn collect-compound-keys-by-row [bucket [row-id vals-map]]
  (assoc bucket row-id (collect-by-split-key vals-map)))

(defn expand-single-col-versions [all-versions-map]
  (let [smaller-map (remove-single-column-family all-versions-map)]
    (reduce collect-compound-keys-by-row {} smaller-map)))

(defn read-all-versions 
  ([hbase-table-name row-id-string number-of-versions]
     (let [#^HTable table (hbase-table hbase-table-name)]
       (stringify-nav-map (.getMap (.get table (create-get row-id-string number-of-versions))))))
  ([hbase-table-name row-id-string column-family-as-string number-of-versions]
     (let [#^HTable table (hbase-table hbase-table-name)]
       (stringify-nav-map (.getMap (.get table (create-get row-id-string [column-family-as-string] number-of-versions)))))))

(defn read-all-multi-col-versions-between [hbase-table-name column-family-as-string start-row-id end-row-id]
  (let [rows-between (read-rows-between hbase-table-name [column-family-as-string] start-row-id end-row-id)
        row-ids (map row-id-of-row rows-between)]
    (apply merge (map (fn [row-id] {row-id (read-all-versions hbase-table-name row-id column-family-as-string 100000)}) row-ids))))

(defn read-all-versions-between
  ([hbase-table-name column-family-as-string start-row-id end-row-id]
     (read-all-multi-col-versions-between hbase-table-name column-family-as-string start-row-id end-row-id))
  ([hbase-table-name start-row-id end-row-id]
     (expand-single-col-versions
      (read-all-versions-between hbase-table-name *hbase-single-column-family* start-row-id end-row-id))))

(defn read-cell [hbase-table-name row-id column-name]
  (let [row (read-row hbase-table-name row-id)]
    (cell-value-as-string row column-name)))

(defn table-iterator
  ([#^String hbase-table-name columns]
     (iterator-seq (.iterator (table-scanner hbase-table-name columns))))
  ([#^String hbase-table-name columns start-row-string]
     (iterator-seq (.iterator (table-scanner hbase-table-name columns start-row-string)))))

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

(defn scan-for-start-to-end [columns #^bytes start-row-bytes #^bytes end-row-bytes]
  (let [scan (Scan. start-row-bytes end-row-bytes)]
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
  ([#^String hbase-table-name columns #^String start-row-string #^String end-row-string]
     (let [table (hbase-table hbase-table-name)]
       (.getScanner table (scan-for-start-to-end columns (.getBytes start-row-string) (.getBytes end-row-string))))))
(defn hbase-row-seq [scanner]
  (let [first-row (.next scanner)]
    (if-not first-row
      nil
      (lazy-seq 
        (cons first-row (hbase-row-seq scanner))))))

(defn next-row-id [#^String hbase-table-name column-to-use row-id]
  (let [scanner (table-scanner hbase-table-name [column-to-use] row-id)
        _ (.next scanner)
        next-result (.next scanner)]
    (if next-result
      (row-id-of-row next-result))))

(defn rowcount [#^String hbase-table-name & columns]
  (count (table-iterator hbase-table-name columns)))

(defn delete-row-col-at [hbase-table-name row-id family qualifier timestamp]
  (let [table (hbase-table hbase-table-name)
        delete (Delete. (.getBytes row-id))]
    (if timestamp
      (.deleteColumn delete (.getBytes family) (.getBytes qualifier) timestamp)
      (.deleteColumn delete (.getBytes family) (.getBytes qualifier)))
    (.delete table delete)))

(defn delete-row-col-latest [hbase-table-name row-id family qualifier]
  (delete-row-col-at hbase-table-name row-id family qualifier nil))

(defn delete-all-versions-for [table-name row-id]
  (let [all-versions (read-all-versions table-name row-id 10000)
        families (keys all-versions)
        del-column (fn [family qualifier]
                     (let [timestamps (keys (get-in all-versions [family qualifier]))
                           num-timestamps (count timestamps)]
                       (dotimes [n num-timestamps]
                         (delete-row-col-latest table-name row-id family qualifier)
                         )))
        del-family (fn [family]
                     (let [qualifiers (keys (all-versions family))]
                       (dorun (map #(del-column family %) qualifiers))))]
    (if all-versions
      (do
        (dorun (map del-family families))
        ;; Versions could be higher than max-versions, but we don't
        ;; know that from all-versions (unfortunately)... hence the
        ;; recur step.
        (recur table-name row-id))
      )
    )
  )

(defn delete-all-rows-versions [table-name row-ids]
  (dorun
   (map #(delete-all-versions-for table-name %) row-ids)))

(defn delete-all [#^String hbase-table-name & row-ids-as-strings]
  (delete-all-rows-versions hbase-table-name row-ids-as-strings))

(defmemoized column-families-for [hbase-table-name]
  (let [table (hbase-table hbase-table-name)
        table-descriptor (.getTableDescriptor table)]
    (map #(String. (.getNameWithColon %)) (.getFamilies table-descriptor))))

(defn simple-delete-row [hbase-table-name row-id]
  (let [table (hbase-table hbase-table-name)
        delete (Delete. (.getBytes row-id))]
    (.delete table delete)
    ))

(defn column-names-as-strings [result-row]
  (map #(String. %) (.keySet result-row)))

(defmemoized hbase-config []
  (HBaseConfiguration.))

(defn hbase-admin []
  (HBaseAdmin. (hbase-config)))

(defn create-hbase-table [#^String table-name max-versions & column-families]
  (let [desc (HTableDescriptor. table-name)
        col-desc (fn [col-family-name]
                   (let [hcdesc (HColumnDescriptor. col-family-name)]
                     (.setMaxVersions hcdesc max-versions)
                     (.addFamily desc hcdesc)))]
    (doall (map col-desc column-families))
    (.createTableAsync (hbase-admin) desc)))

(defn add-hbase-columns [table-name column-family-names versions]
  (if-not (empty? column-family-names)
	  (let [admin (hbase-admin)
                col-desc (fn [col-name] 
                           (let [desc (HColumnDescriptor. col-name)]
                             (.setMaxVersions desc versions)
                             desc))]
	    (.disableTable admin (.getBytes table-name))
	    (doall (map #(.addColumn admin table-name (col-desc %)) column-family-names))
	    (.enableTable admin (.getBytes table-name)))))

(defn clone-table [#^String new-hbase-table-name #^String from-hbase-table-name max-versions]
  (apply create-hbase-table new-hbase-table-name max-versions (column-families-for from-hbase-table-name)))

(defn disable-table [#^String table-name]
  (.disableTable (hbase-admin) (.getBytes table-name)))

(defn enable-table [#^String table-name]
  (.enableTable (hbase-admin) (.getBytes table-name)))

(defn drop-hbase-table [#^String hbase-table-name]
  (.deleteTable (hbase-admin) hbase-table-name))

(defn hbase-table [#^String hbase-table-name]
  (let [table (HTable. (hbase-config) hbase-table-name)]
    (.setScannerCaching table 1000)
    table))

(defn table-exists?
  ([table-name]
     (table-exists? table-name (hbase-admin)))
  ([table-name hadmin]
     (.tableExists hadmin table-name)))

(defmacro with-hbase-table [[table hbase-table-name] & exprs]
  `(let [~table (hbase-table ~hbase-table-name)]
     (do ~@exprs
       (.close ~table))))

(defmacro with-scanner [[scanner] & exprs]
  `(do ~@exprs
       (.close ~scanner)))
