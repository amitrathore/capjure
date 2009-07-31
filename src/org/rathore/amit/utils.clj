(ns org.rathore.amit.utils)

(defmacro defmemoized [fn-name args & body]
  `(def ~fn-name (memoize (fn ~args 
			    (do
			      ~@body)))))
