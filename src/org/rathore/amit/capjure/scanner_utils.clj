(ns org.rathore.amit.capjure.scanner-utils
  (:import org.apache.hadoop.hbase.client.ScannerTimeoutException)
  (:use [clojure.contrib.seq-utils :only (partition-all)]
        [com.cinch.furtive.consumer-event.utils
         :only [timestamp-from-event-id]]
        org.rathore.amit.capjure))

(def HYPOTHETICAL-LAST-ROW-ID "zzzzzzzzzzzzzzz")

(defn get-first-family-name [table-name]
  (-> (hbase-table table-name)
      (.getTableDescriptor)
      (.getColumnFamilies)
      (first)
      (.getNameAsString)))

(defn row-id-or-before
  "Retrieves the row-id, or row-id just before. Returns nil if no rows found."
  [table-name stop-row-id]
  (let [family-name (get-first-family-name table-name)
        result (.getRowOrBefore (hbase-table table-name)
                                (.getBytes stop-row-id)
                                (.getBytes family-name))]
    (and result (String. (.getRow result)))))

(defn last-row-id-in-table
  "Retrieves the last row-id in a table. Returns nil if no rows found."
  [table-name]
  (row-id-or-before table-name HYPOTHETICAL-LAST-ROW-ID))

(defn should-scan-more? [expected-id last-row-id]
  (let [is-empty-table? (nil? expected-id)
        is-first-check? (nil? last-row-id)]
    (and (not is-first-check?)
         (not is-empty-table?)
         (neg? (.compareTo last-row-id expected-id)))))

(defn row-id-ts-minus-1 [row-id]
  (let [ts (Long/parseLong (timestamp-from-event-id row-id))]
    (str (- ts 1))))

(defn desired-last-row-id [table-name stop-row-id]
  (let [table-last-row-id (last-row-id-in-table table-name)]
    (if (and stop-row-id
             (not (pos? (compare stop-row-id table-last-row-id))))
      (row-id-or-before table-name (row-id-ts-minus-1 stop-row-id))
      table-last-row-id)))

(defn exception-causes [ex]
  (take-while #(not (nil? %)) (iterate #(.getCause %) ex)))

(defn caused-by-scanner-timeout? [ex]
  (some #(isa? (type %) org.apache.hadoop.hbase.client.ScannerTimeoutException)
        (exception-causes ex)))

(defn next-result-scanner
  "Returns vector of next result and whether exception occurred in retrieval."
  [scanner]
  (try
   (let [next-result (.next scanner)]
     [next-result false])
   (catch Exception e
     (if (caused-by-scanner-timeout? e)
       [nil true]
       (throw e)))))

(defn safe-scan-seq [args-map]
  (let [{:keys [scanner scanner-fn start-row-id last-good-row-id
                expected-last-row-id]}
        args-map
        [next-result scanner-timeout?] (next-result-scanner scanner)]
    (cond
     next-result
     (lazy-seq
      (cons next-result
            (safe-scan-seq (assoc args-map :last-good-row-id
                                  (row-id-of-row next-result)))))

     (or scanner-timeout?
         (should-scan-more? expected-last-row-id last-good-row-id))
     (drop 1
           (lazy-seq
            (let [restart-row-id (or last-good-row-id start-row-id)
                  args-map1 (assoc args-map :start-row-id restart-row-id)
                  new-args-map (assoc args-map1
                                 :scanner (scanner-fn args-map1))]
              (.close scanner)
              (safe-scan-seq new-args-map))))

     :else (.close scanner))))

(defn wrapped-table-scanner
  [{:keys [table-name columns start-row-id stop-row-id]}]
  (let [scanner-fn (partial table-scanner table-name columns)]
    (cond
     (and start-row-id stop-row-id) (scanner-fn start-row-id stop-row-id)
     start-row-id (scanner-fn start-row-id)
     :else (scanner-fn))))

(defn scan-seq
  "Automatically closes the scanner AFTER YOU CONSUME THE ENTIRE SEQUENCE."
  ([table-name columns]
     (scan-seq table-name columns nil nil))
  ([table-name columns start-row-id]
     (scan-seq table-name columns start-row-id nil))
  ([table-name columns start-row-id stop-row-id]
     (let [expected-id (desired-last-row-id table-name stop-row-id)
           args-map {:table-name table-name
                     :columns columns
                     :start-row-id start-row-id
                     :stop-row-id stop-row-id
                     :expected-last-row-id expected-id
                     :scanner-fn wrapped-table-scanner}
           args-map (assoc args-map :scanner (wrapped-table-scanner args-map))]
       (safe-scan-seq args-map))))