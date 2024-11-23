(ns datahike-gcs.core
  (:require [datahike.store :refer [empty-store delete-store connect-store default-config config-spec release-store store-identity]]
            [datahike.config :refer [map-from-env]]
            [konserve-gcs.core :as k]
            [clojure.spec.alpha :as s]))

(defmethod store-identity :gcs [store-config]
  [:gcs (:location store-config) (:bucket store-config) (:store-id store-config)])

(defmethod empty-store :gcs [store-config]
  (k/connect-store store-config))

(defmethod delete-store :gcs [store-config]
  (k/delete-store store-config))

(defmethod connect-store :gcs [store-config]
  (k/connect-store store-config))

(defmethod default-config :gcs [config]
  (merge
    (map-from-env :datahike-store-config {:bucket "datahike" :store-id "datahike"})
    config))

(s/def :datahike.store.gcs/backend #{:gcs})
(s/def :datahike.store.gcs/bucket string?)
(s/def :datahike.store.gcs/location string?)
(s/def :datahike.store.gcs/access-key string?)
(s/def :datahike.store.gcs/secret string?)
(s/def ::gcs (s/keys :req-un [:datahike.store.gcs/backend]
                     :opt-un [:datahike.store.gcs/location
                              :datahike.store.gcs/bucket
                              :datahike.store.gcs/access-key
                              :datahike.store.gcs/secret]))

(defmethod config-spec :gcs [_] ::gcs)

(defmethod release-store :gcs [_ store]
  (k/release store {:sync? true}))
