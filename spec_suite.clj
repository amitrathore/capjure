(use 'clojure.contrib.test-is)
(load-file "spec/org/rathore/amit/capjure_spec.clj")

(in-ns 'capjure-spec)
(import '(java.net URLEncoder URLDecoder))
(println "**************** running tests ******************")

(defstruct key-config :qualifier :functor)
(defn add-key-config [keys-config name qualifier functor]
  (assoc keys-config name (struct key-config qualifier functor)))
(defn encode-keys []
  (let [keys-config {}
	keys-config (add-key-config keys-config 
				    :inserts :merchant_product_id 
				    (fn [insert-map]
				      (insert-map :merchant_product_id)))
	keys-config (add-key-config keys-config 
				    :latest_consumer :merchant_id 
				    (fn [consumer-map]
				      (consumer-map :merchant_id)))
	keys-config (add-key-config keys-config 
				    :cart_items 
				    :merchant_product_id (fn [cart-item-map]
							   (let [sku (cart-item-map :sku)
								 m-pid (cart-item-map :merchant_product_id)]
							     (cond
							      (nil? sku) m-pid
							      :else (str m-pid "@" (URLEncoder/encode sku "UTF-8"))))))]
    keys-config))

(defn decode-keys []
  (let [keys-config {}
	keys-config (add-key-config keys-config 
				    :inserts :merchant_product_id 
				    (fn [value] value))
	keys-config (add-key-config keys-config 
				    :latest_consumer :merchant_id 
				    (fn [value] value))
	keys-config (add-key-config keys-config 
				    :cart_items 
				    :merchant_product_id (fn [value]
							   (cond
							      (.contains value "@") (first (.split (URLDecoder/decode value "UTF-8") "@"))
							      :else value)))]
    keys-config))

(def keys-config {:encode (encode-keys) :decode (decode-keys)})

(binding [*hbase-master* "tank.cinchcorp.com:60000"
	  *primary-keys-config* keys-config]
  (run-tests)
  (shutdown-agents))
