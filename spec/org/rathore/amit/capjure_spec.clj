(ns capjure-spec)

(use 'clojure.test)
(require '(org.danlarkin [json :as json]))
(use 'org.rathore.amit.capjure)

(defn stringified [aseq]
  (map #(str %) aseq))

(defn is-same-sequence [seqa seqb]
     (is (= (sort (stringified seqa)) (sort (stringified seqb)))))

(def message-string "{\"merchant\": {\"id\": 11, \"name\": \"portable chairs\"}, \"active_campaigns\": [3, 4, 7, 10, 11, 13], \"consumer\": {\"kind\": \"visitor\", \"id\": 103, \"email_address\": \"f80a2173-5923-264f-e2d5-cb0f96220220@visitor.cinchcorp.com\"}, \"session\": {\"uber_session_id\": \"a96ec02e-0fd3-b030-c14a-1761ffe7d45b\", \"merchant_session_id\": \"3c0e276524dc843debaebfd9506138ee\", \"cinch_session_id\": \"42e00dc0eae2706a75eee1c1964d4d43\"}, \"api\": \"0.0.1.0\", \"inserts\": [{\"cinch_unit_price\": 36.95, \"campaign_id\": -1, \"html_id\": \"cinch_id_1231729409882\", \"merchant_product_id\": \"SS-REG\", \"merchant_unit_price\": 36.95, \"insert_type\": \"campaign\"}]}")
(def hash-object (json/decode-from-str message-string))

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
  (let [to-process {:inserts [{:merchant_product_id "kel-10-ab" :merchant_price "11.00" :cinch_price "9.95" :insert_type "typeA"}
			      {:merchant_product_id "sut-91-xy" :merchant_price "8.00" :cinch_price "6.55" :insert_type "typeB"}]}
	processed (flatten to-process)]
    (println processed)
    (is (= (count (keys processed)) 6))
    (is-same-sequence (keys processed) '("inserts_merchant_price:kel-10-ab@typeA"
                                         "inserts_cinch_price:kel-10-ab@typeA"
                                         "inserts_insert_type:kel-10-ab@typeA"
                                         "inserts_insert_type:sut-91-xy@typeB"
                                         "inserts_merchant_price:sut-91-xy@typeB"
                                         "inserts_cinch_price:sut-91-xy@typeB"))
    (is (= (processed "inserts_merchant_price:kel-10-ab@typeA") "11.00"))
    (is (= (processed "inserts_merchant_price:sut-91-xy@typeB") "8.00"))
    (is (= (processed "inserts_cinch_price:kel-10-ab@typeA") "9.95"))
    (is (= (processed "inserts_cinch_price:sut-91-xy@typeB") "6.55"))))

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

(deftest test-hydrate
  (let [flattened (flatten hash-object)
	hydrated (hydrate flattened)
	consumer (hydrated :consumer)
	merchant (hydrated :merchant)
	inserts (hydrated :inserts)
	insert (first inserts)]
    (is-same-sequence (hydrated :active_campaigns) [3 4 7 10 11 13])
    (is (= (hydrated :api) "0.0.1.0"))
    (is (= (count inserts) 1))
    (is (= (insert :campaign_id) "-1"))
    (is (= (insert :merchant_product_id) "SS-REG"))
    (is (= (insert :cinch_unit_price) "36.95"))
    (is (= (consumer :kind) "visitor"))
    (is (= (consumer :id) "103"))
    (is (= (merchant :name) "portable chairs"))))

(def cart-string 
     "{\"event_type\": \"cart\", \"api\": \"0.0.1.0\", \"session\": {\"cinch_session_id\": \"1948c2cef48e58a6c1f215a545512077\", \"merchant_session_id\": \"b768e9911893b91b279e08ac8efde20f\", \"uber_session_id\": \"ee2bcc43-dae9-e0f0-40e8-325f96b5f114\"}, \"cart_items\": [{\"campaign_id\": -1, \"merchant_product_id\": \"EZ-30DC-HD\", \"merchant_unit_price\": 109.95, \"options\": null, \"name\": null, \"cinch_unit_price\": 109.95, \"sku\": \"EZ-30DC-HD (Yellow) XX\", \"quantity\": 1, \"description\": null}, {\"campaign_id\": -1, \"merchant_product_id\": \"OB-BW-LNGR\", \"merchant_unit_price\": 229.95, \"options\": null, \"name\": null, \"cinch_unit_price\": 229.95, \"sku\": \"OB-BW-LNGR (RED) LL\", \"quantity\": 1, \"description\": null}], \"page\": {\"merchant_template_name\": \"index\", \"referrer\": \"http://chairs.vasanta.hq.cinchcorp.com/shopping_cart.html?number_of_uploads=0\", \"request_url\": \"http://chairs.vasanta.hq.cinchcorp.com/hanging-chairs-c-23.html\"}, \"cart\": {\"checkout_state\": 1}, \"merchant\": {\"name\": \"Portable Folding Chairs\", \"id\": 14}, \"http_client\": {\"browser_version\": \"3\", \"operating_system\": \"Macintosh\", \"operating_system_version\": \"OS X\", \"ip_address\": \"192.168.10.10\", \"browser\": \"Safari\"}, \"active_campaigns\": [3, 4, 7, 10, 11, 13], \"consumer\": {\"email_address\": \"afd4cb68-bb3b-e51f-ac2a-cb6eefce528f@visitor.cinchcorp.com\", \"kind\": \"visitor\", \"id\": 103}}")
(def cart-object (json/decode-from-str cart-string))

(deftest test-flatten-with-sku 
  (let [flattened (flatten cart-object)
        hydrated (hydrate flattened)
        cart-items (hydrated :cart_items)
        cart-item (first cart-items)]
    (is (= (count cart-items) 2))
    (is (= (cart-item :sku) "OB-BW-LNGR (RED) LL"))
    (is (= (cart-item :merchant_product_id) "OB-BW-LNGR"))))
	     

