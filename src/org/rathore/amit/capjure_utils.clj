(ns org.rathore.amit.capjure-utils)

(import '(java.util Map$Entry))

(defmacro defmemoized [fn-name args & body]
  `(def ~fn-name (memoize (fn ~args 
			    (do
			      ~@body)))))

(defn- stringify-version-entry [v-entry]
  {(.getKey v-entry) (String. (.getValue v-entry))})

(defn- stringify-column-entry [c-entry]
  {(String. (.getKey c-entry)) (apply merge (map stringify-version-entry (.getValue c-entry)))})

(defn- stringify-qualifier-entry [q-entry]
  {(String. (.getKey q-entry)) (apply merge (map stringify-column-entry (.getValue q-entry)))})

(defn stringify-nav-map [nmap]
  (apply merge (map stringify-qualifier-entry nmap)))

(defn all-versions-from-map [qualifer-column c-map]
  (let [[qualifier column] (.split qualifer-column ":")
	column (or column "")]
    (vals ((c-map qualifier) column))))

(defn cell-value-from-map [qualifer-column c-map]
  (let [[qualifier column] (.split qualifer-column ":")
	column (or column "")]
    ((c-map qualifier) column)))

(defn- column-data-without-timestamps [column-entry]
  {(.getKey column-entry)  (vals (.getValue column-entry))})

(defn- qualifier-data-without-timestamps [qualifier-entry]
  {(.getKey qualifier-entry) (apply merge (map column-data-without-timestamps (.getValue qualifier-entry)))})

(defn all-versions-without-timestamps [with-timestamps]
  (apply merge (map qualifier-data-without-timestamps with-timestamps)))
