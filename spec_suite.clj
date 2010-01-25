(use 'clojure.test)
(load-file "/Users/amit/workspace/capjure/spec/org/rathore/amit/capjure_spec.clj")

(in-ns 'capjure-spec)
(use 'org.rathore.amit.capjure-init)
(import '(java.net URLEncoder URLDecoder))

(def encoders (config-keys
  (config-for :inserts :merchant_product_id  (fn [insert-map]
					       (insert-map :merchant_product_id)))
  (config-for :latest_consumer :merchant_id (fn [consumer-map]
					      (consumer-map :merchant_id)))
  (config-for :cart_items  :merchant_product_id (fn [cart-item-map]
						  (let [sku (cart-item-map :sku)
							m-pid (cart-item-map :merchant_product_id)]
						    (cond
						     (nil? sku) m-pid
						     :else (str m-pid "@" (URLEncoder/encode sku "UTF-8"))))))))

(def decoders (config-keys
  (config-for :inserts :merchant_product_id 
	      (fn [value] value))
  (config-for :latest_consumer :merchant_id 
	      (fn [value] value))
  (config-for :cart_items :merchant_product_id 
	      (fn [value]
		(cond
		 (.contains value "@") (first (.split (URLDecoder/decode value "UTF-8") "@"))
		 :else value)))))

(def keys-config {:encode encoders :decode decoders})

(defn run-suite []
  (binding [*hbase-master* "tank.cinchcorp.com:60000"
            *primary-keys-config* keys-config]
    (run-tests)))
