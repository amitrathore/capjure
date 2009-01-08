(ns capjure-spec)

(use 'clojure.contrib.test-is)
(use 'org.rathore.amit.capjure)

(def hash_object 
     {
      "inserts": [{
		   "cinch_unit_price": 33.26, 
		   "html_id": "cinch_id_1231449290449", 
		   "active_campaigns": [3, 4, 7, 10, 11, 13], 
		   "merchant_unit_price": 36.95, 
		   "campaign_id": 13, 
		   "insert_type": "display_type"}], 
      "session": {
		  "merchant_session_id": "cc0bd4007dd531c031b22a4c18224e85", 
		  "cinch_session_id": "b2392df2aa5765a332208f5cbf49c13d", 
		  "uber_session_id": "e5ac21a0-8eea-e6f6-3434-369cd7909355"
		  }, 
      "api": "0.0.1.0", 
      "consumer": {
		   "email_address": "8fe6865c-6450-5a3b-2497-7e3a2734b7f4@visitor.cinchcorp.com", 
		   "kind": "visitor", 
		   "id": 30137
		   }
      })

(deftest test-trial
  (is (= 1 1)))