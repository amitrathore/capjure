(ns org.rathore.amit.capjure)

(def *mock-mode* false)
(def *hbase-master* "tank.cinchcorp.com")

(defn save [object_to_save]
  ())

(defn flattened [bloated_object]
  ())

(defn prepend-to-keys [prefix hash-map]
  (let [all-keys (to-array (keys hash-map))]
    (areduce all-keys idx ret {} 
	     (assoc ret (str prefix (aget all-keys idx)) (hash-map (aget all-keys idx))))))

  
(defn hbase_table [object_name table_name column_information]
     ())
