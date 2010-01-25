(ns new-capjure-spec
  (:use clojure.test)
  (:use org.rathore.amit.capjure)
  (:use org.rathore.amit.capjure-init))

(def consumer-event
     {:merchant {:id 14, :name "Portable Folding Chairs"} 
      :active_campaigns [3 4 7 10 11 13] 
      :event_type "inserts" 
      :inserts [
                {:insert_type "viewed_aggregate" :html_id "cinch_id_1233794973977" :action_id 2001 :cinch_unit_price 98.95 :merchant_unit_price 109.95 :merchant_product_id "EZ-30DC-HD" :campaign_id 10} 
                {:insert_type "viewed_product_detail", :html_id "cinch_id_1233794973978" :action_id 2002 :cinch_unit_price 98.95 :merchant_unit_price 109.95 :merchant_product_id "EZ-30DC-HD" :campaign_id 10}]
      :http_client {:browser "Firefox" :ip_address "192.168.10.10" :operating_system_version "OS X" :operating_system "Macintosh" :browser_version "3.0"}       
      :page {:request_url "http://chairs.vasanta.hq.cinchcorp.com/heavy-duty-3034-aluminum-frame-directors-chair-p-86.html" :referrer "http://chairs.vasanta.hq.cinchcorp.com/directors-chairs-c-1.html" :merchant_template_name "productinfo"}
      :consumer {:id "36799" :kind "interested" :email_address "6846821c-c6af-3052-cbf7-7a465c27aa69@interested.cinchcorp.com"}
      :api "0.0.1.0"})

(defn assert-equal-in-hashes [hash1 hash2 & ks]
  (is (= (get-in hash1 ks) (get-in hash2 ks))))

(deftest test-flatten-simple-elements
  (let [flattened (flatten consumer-event)]
    (is (= (flattened "meta:api__") "0.0.1.0"))
    (is (= (flattened "meta:event_type__") "inserts"))
    (is (= (flattened "meta:merchant__name") "Portable Folding Chairs"))
    (is (= (flattened "meta:http_client__browser") "Firefox"))
    (is (= (flattened "meta:page__merchant_template_name") "productinfo"))
    (is (= (flattened "meta:consumer__id") "36799"))))

(deftest test-flatten-has-many-hashes
  (let [flattened (flatten consumer-event)]
    (is (= (flattened "meta:inserts_campaign_id__EZ-30DC-HD@viewed_product_detail") 10))
    (is (= (flattened "meta:inserts_merchant_unit_price__EZ-30DC-HD@viewed_aggregate") 109.95))
    (is (= (flattened "meta:inserts_action_id__EZ-30DC-HD@viewed_aggregate") 2001))
    (is (= (flattened "meta:inserts_html_id__EZ-30DC-HD@viewed_product_detail") "cinch_id_1233794973978"))
    (is (= (flattened "meta:inserts_insert_type__EZ-30DC-HD@viewed_aggregate") "viewed_aggregate"))
    (is (= (flattened "meta:inserts_cinch_unit_price__EZ-30DC-HD@viewed_product_detail") 98.95))
    (is (= (flattened "meta:inserts_cinch_unit_price__EZ-30DC-HD@viewed_aggregate") 98.95))
    (is (= (flattened "meta:inserts_insert_type__EZ-30DC-HD@viewed_product_detail") "viewed_product_detail"))
    (is (= (flattened "meta:inserts_html_id__EZ-30DC-HD@viewed_aggregate") "cinch_id_1233794973977"))
    (is (= (flattened "meta:inserts_action_id__EZ-30DC-HD@viewed_product_detail") 2002))
    (is (= (flattened "meta:inserts_merchant_unit_price__EZ-30DC-HD@viewed_product_detail") 109.95))
    (is (= (flattened "meta:inserts_campaign_id__EZ-30DC-HD@viewed_aggregate") 10))))

(deftest test-hydration
  (let [flattened (flatten consumer-event)
        hydrated (hydrate flattened)
        inserts (hydrated :inserts)]
    (is (= (get-in hydrated [:merchant :name]) "Portable Folding Chairs"))
    (is (= (get-in hydrated [:merchant :id]) "14"))

    (is (= (get-in hydrated [:active_campaigns]) ["7"]))
    (is (= (get-in hydrated [:event_type]) "inserts"))
    (is (= (get-in hydrated [:api]) "0.0.1.0"))

    (is (= (get-in hydrated [:consumer :email_address]) "6846821c-c6af-3052-cbf7-7a465c27aa69@interested.cinchcorp.com"))
    (is (= (get-in hydrated [:consumer :id]) "36799"))
    (is (= (get-in hydrated [:consumer :kind]) "interested"))

    (is (= (get-in hydrated [:page :referrer]) "http://chairs.vasanta.hq.cinchcorp.com/directors-chairs-c-1.html"))
    (is (= (get-in hydrated [:page :request_url]) "http://chairs.vasanta.hq.cinchcorp.com/heavy-duty-3034-aluminum-frame-directors-chair-p-86.html"))
    (is (= (get-in hydrated [:page :merchant_template_name]) "productinfo"))

    (is (= (:html_id (first inserts)) "cinch_id_1233794973978"))
    (is (= (:cinch_unit_price (first inserts)) "98.95"))
    (is (= (:campaign_id (first inserts)) "10"))
    (is (= (:action_id (first inserts)) "2002"))
    (is (= (:insert_type (first inserts)) "viewed_product_detail"))
    (is (= (:merchant_product_id (first inserts)) "EZ-30DC-HD"))

    (is (= (:html_id (second inserts)) "cinch_id_1233794973977"))
    (is (= (:cinch_unit_price (second inserts)) "98.95"))
    (is (= (:campaign_id (second inserts)) "10"))
    (is (= (:action_id (second inserts)) "2001"))
    (is (= (:insert_type (second inserts)) "viewed_aggregate"))
    (is (= (:merchant_product_id (second inserts)) "EZ-30DC-HD"))))

(deftest test-single-column-based-persistence
  (delete-all "capjure_test" "capjure_test_row_id")
  (is (.isEmpty (read-row "capjure_test" "capjure_test_row_id")))
  (capjure-insert consumer-event "capjure_test" "capjure_test_row_id")
  (let [from-db (read-as-hydrated "capjure_test" "capjure_test_row_id")]
    (assert-equal-in-hashes consumer-event from-db [:merchant :name])
    (assert-equal-in-hashes consumer-event from-db [:api])
    (assert-equal-in-hashes consumer-event from-db [:active_campaigns])
    (assert-equal-in-hashes (first (:inserts consumer-event)) (first (:inserts from-db)) [:html_id])    
    (assert-equal-in-hashes (first (:inserts consumer-event)) (first (:inserts from-db)) [:insert_type])    
    (assert-equal-in-hashes (second (:inserts consumer-event)) (second (:inserts from-db)) [:html_id])    
    (assert-equal-in-hashes (second (:inserts consumer-event)) (second (:inserts from-db)) [:insert_type])))

(def encoders (config-keys
  (config-for :inserts 
              :merchant_product_id
              (fn [i-map]
                (str (i-map :merchant_product_id) "@" (i-map :insert_type))))))

(def decoders (config-keys
  (config-for :inserts 
              :merchant_product_id
              (fn [value]
                (first (.split value "@"))))))

(defn run-capjure-tests []
  (binding [*primary-keys-config* {:encode encoders :decode decoders} 
            *single-column-family?* true
            *hbase-single-column-family* "meta"]
    (run-tests)))