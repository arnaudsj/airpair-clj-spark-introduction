(ns airpair-clj-spark-introduction.mine
  (:use
   [twitter.oauth]
   [twitter.callbacks]
   [twitter.callbacks.handlers]
   [twitter.api.restful]
   [cheshire.core :refer :all]
   [environ.core :refer [env]])
  (:import
   (twitter.callbacks.protocols SyncSingleCallback)))

(def ^:dynamic *app-consumer-key* (env :app-consumer-key))
(def ^:dynamic *app-consumer-secret* (env :app-consumer-secret))
(def ^:dynamic *user-access-token* (env :user-access-token))
(def ^:dynamic *user-access-token-secret* (env :user-access-token-secret))

(def my-creds (make-oauth-creds *app-consumer-key*
                                *app-consumer-secret*
                                *user-access-token*
                                *user-access-token-secret*))

(defn get-tweets [max-id]
  (Thread/sleep 3000)
  (statuses-user-timeline :oauth-creds my-creds
                          :params (into {:screen-name "airpair" :count 100} (if (< 0 max-id) {:max-id max-id} {}))))

(defn tweets-text [tweets]
  (vec (map #(select-keys % [:text :id :created_at]) (second (first (drop 2 tweets))))))

(defn harvest-tweet
  ([max-id]
    (let [a (tweets-text (get-tweets max-id))
            min-id (if (< 0 (count a)) (dec (reduce min (map :id a))) 0)]
     (lazy-cat a (lazy-seq (harvest-tweet min-id))))))

(defn -main []
  (spit "data/airpair-tweets.json" (generate-string (take 3200 (harvest-tweet 0)))))


