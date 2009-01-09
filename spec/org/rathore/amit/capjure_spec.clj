(ns capjure-spec)

(use 'clojure.contrib.test-is)
(require '(org.danlarkin [json :as json]))
(use 'org.rathore.amit.capjure)

(def message-string "{\"consumer\": {\"kind\": \"visitor\", \"id\": 114, \"email_address\": \"2d2bbc2b-5719-eb56-1cef-b0c275315198@visitor.cinchcorp.com\"}, \"session\": {\"uber_session_id\": \"4531fe59-3aff-e570-0233-24f6d2bb3ca0\", \"merchant_session_id\": \"03e65d5456e26669f2e9ae7d13cf9a57\", \"cinch_session_id\": \"7e39738f50dfefd8a00ffcd38f7d557a\"}, \"api\": \"0.0.1.0\", \"inserts\": [{\"cinch_unit_price\": 36.95, \"active_campaigns\": [3, 4, 7], \"campaign_id\": -1, \"html_id\": \"cinch_id_1231474450408\", \"merchant_unit_price\": 36.95, \"insert_type\": null}]}")

(def hash-object 
     (json/decode-from-str message-string))

(deftest test-prepend-to-keys
  (let [to-prepend (hash-object :consumer)
	prepended (prepend-to-keys "con" to-prepend)]
    (is (= (keys prepended) (list "con:kind" "con:id" "con:email_address")))))