(defstruct key-config :qualifier :functor)

(defn config-for [key-name qualifier functor]
  {key-name (struct key-config qualifier functor)})

(defn config-keys [& encoders]
  (apply merge encoders))
   