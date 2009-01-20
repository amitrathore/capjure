(use 'clojure.contrib.test-is)
(load-file "spec/org/rathore/amit/capjure_spec.clj")

(in-ns 'capjure-spec)
(println "**************** running tests ******************")

(def keys-config 
     {:inserts :merchant_product_id
      :cart_items :merchant_product_id
      :latest_consumer :merchant_id})

(def test-table-config
     {:consumer-events "amit_test_consumer_events"
      :latest-consumers "amit_test_latest_consumers"})

(binding [*hbase-master* "tank.cinchcorp.com:60000"
	  *primary-keys-config* keys-config]
  (run-tests)
  (shutdown-agents))
