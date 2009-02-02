(use 'clojure.contrib.test-is)
(import '(java.net URLEncoder))
(load-file "spec/org/rathore/amit/capjure_spec.clj")


(in-ns 'capjure-spec)
(println "**************** running tests ******************")

;(def keys-config 
;     {:inserts :merchant_product_id
;      :cart_items :merchant_product_id
;      :latest_consumer :merchant_id})

(binding [*hbase-master* "tank.cinchcorp.com:60000"
	  *primary-keys-config* keys-config]
  (run-tests))
  (shutdown-agents))

(def keys-config {
    :inserts (fn [insert-map]
	     (insert-map :merchant_product_id))
    :latest_consumer (fn [consumer-map]
	      (consumer-map :merchant_id))
    :cart_items (fn [cart-item-map]
		  (str (cart-item-map :merchant_product_id) "@" (URLEncoder/encode (cart-item-map :sku))))})

;create struct to track primary key, and function associated with it