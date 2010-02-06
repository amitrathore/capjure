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

(defn is-same-sequence [seqa seqb]
  (is (= (sort seqa) (sort seqb))))

(deftest test-flatten-single-family-and-simple-elements
  (let [flattened (flatten consumer-event)]
    (is (= (flattened "meta:api__") "0.0.1.0"))
    (is (= (flattened "meta:event_type__") "inserts"))
    (is (= (flattened "meta:merchant__name") "Portable Folding Chairs"))
    (is (= (flattened "meta:http_client__browser") "Firefox"))
    (is (= (flattened "meta:page__merchant_template_name") "productinfo"))
    (is (= (flattened "meta:consumer__id") "36799"))))

(deftest test-flatten-multiple-family-and-simple-elements
  (binding [*single-column-family?* false]
    (let [flattened (flatten consumer-event)]
      (is (= (flattened "api:") "0.0.1.0"))
      (is (= (flattened "event_type:") "inserts"))
      (is (= (flattened "merchant:name") "Portable Folding Chairs"))
      (is (= (flattened "http_client:browser") "Firefox"))
      (is (= (flattened "page:merchant_template_name") "productinfo"))
      (is (= (flattened "consumer:id") "36799")))))

(deftest test-flatten-single-family-and-has-many-hashes
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

(deftest test-flatten-multiple-family-and-has-many-hashes
  (binding [*single-column-family?* false]
    (let [flattened (flatten consumer-event)]
      (is (= (flattened "inserts_campaign_id:EZ-30DC-HD@viewed_product_detail") 10))
      (is (= (flattened "inserts_merchant_unit_price:EZ-30DC-HD@viewed_aggregate") 109.95))
      (is (= (flattened "inserts_action_id:EZ-30DC-HD@viewed_aggregate") 2001))
      (is (= (flattened "inserts_html_id:EZ-30DC-HD@viewed_product_detail") "cinch_id_1233794973978"))
      (is (= (flattened "inserts_insert_type:EZ-30DC-HD@viewed_aggregate") "viewed_aggregate"))
      (is (= (flattened "inserts_cinch_unit_price:EZ-30DC-HD@viewed_product_detail") 98.95))
      (is (= (flattened "inserts_cinch_unit_price:EZ-30DC-HD@viewed_aggregate") 98.95))
      (is (= (flattened "inserts_insert_type:EZ-30DC-HD@viewed_product_detail") "viewed_product_detail"))
      (is (= (flattened "inserts_html_id:EZ-30DC-HD@viewed_aggregate") "cinch_id_1233794973977"))
      (is (= (flattened "inserts_action_id:EZ-30DC-HD@viewed_product_detail") 2002))
      (is (= (flattened "inserts_merchant_unit_price:EZ-30DC-HD@viewed_product_detail") 109.95))
      (is (= (flattened "inserts_campaign_id:EZ-30DC-HD@viewed_aggregate") 10)))))

(deftest test-hydration
  (let [flattened (flatten consumer-event)
        hydrated (hydrate flattened)
        inserts (hydrated :inserts)]
    (is (= (get-in hydrated [:merchant :name]) "Portable Folding Chairs"))
    (is (= (get-in hydrated [:merchant :id]) "14"))

    (is-same-sequence (get-in hydrated [:active_campaigns]) ["3" "4" "7" "10" "11" "13"])
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

(deftest delete-all-test
  "row-id must be something unique, otherwise you will get problems with timestamp deletions overlapping with other tests"
  (let [table-name "capjure_test"
        row-id "capjure_test_row_id_x"
        qualifier "delete_all_test"]
    (testing "does a normal delete"
      (testing "starting empty"
       (delete-all table-name row-id)
       (is (= 0 (count (get-in (read-all-versions table-name row-id 10000) ["meta" (str qualifier "__")]))) "was not initially empty!"))      
      
      (testing "create and delete a rec"
        (capjure-insert {qualifier "val1"} table-name row-id)
        (is (= 1 (count (get-in (read-all-versions table-name row-id 10000) ["meta" (str qualifier "__")]))) "was not able to insert a record!")
        (delete-all table-name row-id)
        (is (= 0 (count (get-in (read-all-versions table-name row-id 10000) ["meta" (str qualifier "__")]))) "didn't delete all recs!"))

      (testing "create and delete two recs"
        (capjure-insert {"test" "val1"} table-name row-id)
        (capjure-insert {"test" "val2"} table-name row-id)
        (is (= 2 (count (get-in (read-all-versions table-name row-id 10000) ["meta" "test__"]))))
        (delete-all table-name row-id)
        (is (= 0 (count (get-in (read-all-versions table-name row-id 10000) ["meta" "test__"])))))
      )))

(deftest delete-row-col-at-test
  (let [table-name "capjure_test"
        row-id "capjure_test_row_id"]
    (testing "does delete with specific timestamp"
      (testing "is initially empty"
        (delete-all-versions-for table-name row-id)
        (delete-all-versions-for table-name "other_row_id")
        (is (nil? (get-in (read-all-versions table-name row-id 10000) ["meta" "test__"])) "first col shouldn't exist!")
        (is (nil? (get-in (read-all-versions table-name "other_row_id" 10000) ["meta" "other__"])) "other test col shouldn't exist!")
        )

      (let [ts1 (System/currentTimeMillis)
            ts2 (inc ts1)]

        (testing "three recs addeed"
          (capjure-insert {"other" "other-val"} table-name "other_row_id" ts1)
          (is (= 1 (count (get-in (read-all-versions table-name "other_row_id" 10000) ["meta" "other__"]))))
          (capjure-insert {"test" "val1"} table-name row-id ts1)
          (capjure-insert {"test" "val2"} table-name row-id ts2)
          (is (= 2 (count (get-in (read-all-versions table-name row-id 10000) ["meta" "test__"]))))
          )
        
        (testing "only first one is deleted"
          (delete-row-col-at table-name row-id "meta" "test__" ts1)
          (let [versions-hash (get-in (read-all-versions table-name row-id 10000) ["meta" "test__"])]
            (is (= 1 (count versions-hash)))
            (is (= "val2" (first (vals versions-hash)))))
          (testing "other rec is left alone"
            (is (= 1 (count (get-in (read-all-versions table-name "other_row_id" 10000) ["meta" "other__"]))))))
        ))
    ))

(deftest test-single-column-based-persistence
  (delete-all-versions-for "capjure_test" "capjure_test_row_id")
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

(deftest capjure-insert-with-timestamps-test
  (let [table-name "capjure_test"
        row-id "capjure_test_row_id"
        ts1 (System/currentTimeMillis)]
    (testing "can insert with a custom timestamp"
      (delete-all-versions-for table-name row-id)
      (is (.isEmpty (read-row table-name row-id)))
      (capjure-insert consumer-event table-name row-id ts1)
      (is (= "14" (get-in (read-all-versions table-name row-id 10000) ["meta" "merchant__id" ts1])))
      )
    ))
  
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