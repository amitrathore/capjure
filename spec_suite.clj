(use 'clojure.test)
(load-file "spec/org/rathore/amit/capjure_spec.clj")
(load-file "spec/org/rathore/amit/new_spec.clj")

(in-ns 'capjure-spec)
(use 'org.rathore.amit.capjure-init)
(import '(java.net URLEncoder URLDecoder))

;; (def encoders (config-keys
;;   (config-for :inserts :merchant_product_id  (fn [insert-map]
;; 					       (insert-map :merchant_product_id)))
;;   (config-for :latest_consumer :merchant_id (fn [consumer-map]
;; 					      (consumer-map :merchant_id)))
;;   (config-for :cart_items  :merchant_product_id (fn [cart-item-map]
;; 						  (let [sku (cart-item-map :sku)
;; 							m-pid (cart-item-map :merchant_product_id)]
;; 						    (cond
;; 						     (nil? sku) m-pid
;; 						     :else (str m-pid "@" (URLEncoder/encode sku "UTF-8"))))))))

;; (def decoders (config-keys
;;   (config-for :inserts :merchant_product_id 
;; 	      (fn [value] value))
;;   (config-for :latest_consumer :merchant_id 
;; 	      (fn [value] value))
;;   (config-for :cart_items :merchant_product_id 
;; 	      (fn [value]
;; 		(cond
;; 		 (.contains value "@") (first (.split (URLDecoder/decode value "UTF-8") "@"))
;; 		 :else value)))))

(def encoders (config-keys
  (config-for :inserts :merchant_product_id  (fn [insert-map]	
					       (str (insert-map :merchant_product_id) "@" (insert-map :insert_type))))
  (config-for :latest_consumer :merchant_id (fn [consumer-map]
					      (consumer-map :merchant_id)))
  (config-for :cart_items  :merchant_product_id (fn [cart-item-map]
						  (let [sku (cart-item-map :sku)
							m-pid (cart-item-map :merchant_product_id)]
						    (cond
						     (nil? sku) m-pid
						     :else (str m-pid "@" (URLEncoder/encode sku "UTF-8"))))))
  (config-for :jobs :session_id (fn [status-map]
				  (status-map :session_id)))))
(def decoders (config-keys
  (config-for :inserts :merchant_product_id 
	      (fn [value] (first (.split value "@"))))
  (config-for :latest_consumer :merchant_id 
	      (fn [value] value))
  (config-for :cart_items :merchant_product_id 
	      (fn [value]
		(cond
		 (.contains value "@") (first (.split (URLDecoder/decode value "UTF-8") "@"))
		 :else value)))
  (config-for :jobs :session_id (fn [value] value))))

(def keys-config {:encode encoders :decode decoders})

(defn run-capjure-spec-suite []
  (binding [*hbase-master* "localhost:60000"
            *single-column-family?* false
            *primary-keys-config* keys-config]
    (run-tests 'capjure-spec)))

(defn run-new-spec-suite []
  (binding [*hbase-master* "localhost:60000"
            *single-column-family?* true
            *hbase-single-column-family* "meta"
            *primary-keys-config* keys-config]
    (run-tests 'new-capjure-spec)))

(run-capjure-spec-suite)
(run-new-spec-suite)
