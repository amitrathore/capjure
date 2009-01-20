(ns capjure-spec)

(use 'clojure.contrib.test-is)
(require '(org.danlarkin [json :as json]))
(use 'org.rathore.amit.capjure)

(defn is-same-sequence [seqa seqb]
     (is (= (sort seqa) (sort seqb))))

(def message-string "{\"merchant\": {\"id\": 11, \"name\": \"portable chairs\"}, \"active_campaigns\": [3, 4, 7, 10, 11, 13], \"consumer\": {\"kind\": \"visitor\", \"id\": 103, \"email_address\": \"f80a2173-5923-264f-e2d5-cb0f96220220@visitor.cinchcorp.com\"}, \"session\": {\"uber_session_id\": \"a96ec02e-0fd3-b030-c14a-1761ffe7d45b\", \"merchant_session_id\": \"3c0e276524dc843debaebfd9506138ee\", \"cinch_session_id\": \"42e00dc0eae2706a75eee1c1964d4d43\"}, \"api\": \"0.0.1.0\", \"inserts\": [{\"cinch_unit_price\": 36.95, \"campaign_id\": -1, \"html_id\": \"cinch_id_1231729409882\", \"merchant_product_id\": \"SS-REG\", \"merchant_unit_price\": 36.95, \"insert_type\": \"campaign\"}]}")

(def hash-object 
     (json/decode-from-str message-string))

(deftest test-prepend-to-keys
  (let [to-prepend (hash-object :consumer)
	prepended (prepend-to-keys "con" ":" to-prepend)]
    (is-same-sequence (keys prepended) (list "con:kind" "con:id" "con:email_address"))
    (is (= (count (vals prepended)) (count (vals to-prepend))))
    (is (= (prepended "con:id") (to-prepend :id)))
    (is (= (prepended "con:kind") (to-prepend :kind)))
    (is (= (prepended "con:email_address") (to-prepend :email_address)))))

(deftest test-postpend-to-keys
  (let [to-postpend (hash-object :consumer)
	postpended (postpend-to-keys "abc" ":" to-postpend)]
    (is-same-sequence (keys postpended) (list "kind:abc" "id:abc" "email_address:abc"))
    (is (= (count (vals postpended)) (count (vals to-postpend))))
    (is (= (postpended "id:abc") (to-postpend :id)))
    (is (= (postpended "kind:abc") (to-postpend :kind)))
    (is (= (postpended "email_address:abc") (to-postpend :email_address)))))

(deftest test-process-key-value-simple
  (let [to-process {:consumer {:kind "visitor" :id "110" :email_address "amitrathore@gmail.com"}}
	processed (process-key-value :consumer (to-process :consumer))]
    (is (= (count (keys processed)) 3))
    (is-same-sequence (keys processed) '("consumer:kind" "consumer:id" "consumer:email_address"))))

(deftest test-flatten-simple
  (let [to-flatten {:one {:a 1 :b 2} :two {:c 3 :d 4}}
	flattened (flatten to-flatten)]
    (is (= (count (keys flattened)) 4))
    (is (= (flattened "one:a") 1))
    (is (= (flattened "one:b") 2))
    (is (= (flattened "two:c") 3))
    (is (= (flattened "two:d") 4))))

(deftest test-flatten-value-configured
  (let [to-process {:inserts [{:merchant_product_id "kel-10-ab" :merchant_price "11.00" :cinch_price "9.95"}
			      {:merchant_product_id "sut-91-xy" :merchant_price "8.00" :cinch_price "6.55"}]}
	processed (flatten to-process)]
    (is (= (count (keys processed)) 4))
    (is-same-sequence (keys processed) '("inserts_merchant_price:kel-10-ab" "inserts_cinch_price:kel-10-ab" "inserts_merchant_price:sut-91-xy" "inserts_cinch_price:sut-91-xy"))
    (is (= (processed "inserts_merchant_price:kel-10-ab") "11.00"))
    (is (= (processed "inserts_merchant_price:sut-91-xy") "8.00"))
    (is (= (processed "inserts_cinch_price:kel-10-ab") "9.95"))
    (is (= (processed "inserts_cinch_price:sut-91-xy") "6.55"))))

(deftest test-flatten-array-of-strings
  (let [to-flatten {:active_campaigns [3 4 7 10 11 13]}
	flattened (flatten to-flatten)]
    (is (= (count (keys flattened)) 6))
    (is (= (flattened "active_campaigns:3") 3))
    (is (= (flattened "active_campaigns:4") 4))
    (is (= (flattened "active_campaigns:7") 7))
    (is (= (flattened "active_campaigns:10") 10))
    (is (= (flattened "active_campaigns:11") 11))
    (is (= (flattened "active_campaigns:13") 13))))

(deftest test-flatten-large-object
  (let [flattened (flatten hash-object)
	all-keys (keys flattened)]
    (is (= (count all-keys) 20))
    (is (= (flattened "api:") "0.0.1.0"))))

(defn run-all []
  (binding [*hbase-master* "tank.cinchcorp.com:60000"
            *primary-keys-config* {:inserts :merchant_product_id}]
    (run-tests)))
