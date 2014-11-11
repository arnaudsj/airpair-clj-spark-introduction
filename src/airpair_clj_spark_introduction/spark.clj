(ns airpair-clj-spark-introduction.spark
  (:require  [flambo.api :as f]
             [flambo.conf :as conf]
             [cheshire.core :refer :all]
             [clj-time.core :as time]
             [clj-time.coerce :as tc]
             [clj-time.format :as t]
             [clj-http.client :as client]
             [environ.core :refer [env]]
             [clojure.java.browse])
  (:import [org.apache.log4j Level Logger])
  (:gen-class))

;; We don't need to see everything
;; (.setLevel (Logger/getRootLogger) Level/WARN)


;; Spark setup
;; ===========
(def c (-> (conf/spark-conf)
           (conf/master "local[*]")
           (conf/app-name "airpair_harvest")))

(def sc (f/spark-context c))



;; Get the data into Spark
;; =======================

(def airpair-tweets (cheshire.core/parse-string (slurp "data/airpair-tweets.json") true))
(def data (f/parallelize sc airpair-tweets))



;; Duckboard helper
;; ================

(defn duck-push [widget-id data]
    "Duck board helper"
    (client/post (clojure.string/join ["https://push.ducksboard.com/v/" widget-id])
                 {:basic-auth [(env :duck-auth-token) "unused"] :form-params data :content-type :json}))

;; example
;; (duck-push 539867 {:value {:board [{:name "test" :values [1 2 3]}]}})
;; (duck-push 540898 [{:value 500 :timestamp (tc/to-long (time/minus (time/now) (time/days 5)))}
;;                    {:value 100 :timestamp (tc/to-long (time/minus (time/now) (time/days 1)))}
;;                    ])



;; Refinery Functions
;; ==================

(defn extract-group [n] (fn [group] (group n)))
(def hourly-rate (f/fn [tweet]
                       (read-string (first (map (extract-group 2) (re-seq #"(\$)([\d]+)(\/hr)" (:text tweet)))))))
(def tags (f/fn [tweet]
                (map clojure.string/lower-case (re-seq #"\#[\d\w\.]+" (:text tweet)))))
(def parse-date (f/fn [tweet]
                      (t/parse (t/formatter "E MMM dd HH:mm:ss Z YYYY") (:created_at tweet))))




;; Scratch pad to figure out data transformations
;; ==============================================

;; (def sample-tweet {:text "Get paid $170/hr to help w #Jasmine and #Backbone.js over video chat =&gt; http://t.co/Q7MjNrdCUm"
;;                    :created_at "Tue Oct 21 03:24:44 +0000 2014"})
;; (hourly-rate sample-tweet)
;; (tags sample-tweet)
;; (.contains "asasf" "$")
;; (iterate (fn[x] x) ["a" "b" "c"])
;; (partition 2 (interleave ["a" "b" "c"] (repeat 3)))

;; ===============
;; End scratch pad


;; basics
;;(f/take tag-data-rdd 1)

;; out-of-spark version
;; (reduce max (map :rate (f/collect tag-data-rdd)))

;; Sparkling version!
;; (-> tag-data-rdd
;;     (f/map :rate)
;;     (f/reduce min))



(defn truncate-day [dt]
  "Helper to round up the datetime -> date"
   (time/to-time-zone
   (apply time/date-time
          (map #(% dt) [time/year time/month time/day]))
   (time/default-time-zone)))



;; Insight #1 √
;; Display a sample of the data we have

(def tweet-data (-> data
                 (f/map (f/fn[x] {:timestamp (/ (tc/to-long (truncate-day (parse-date x))) 1000) :value {:content (:text x)}}))
                 (f/take 30)))

(map (partial duck-push 541705) tweet-data)




;; Insight #2 √
;; Basics

(def tag-data-rdd (-> data
                 (f/filter (f/fn[x]((comp not nil?) (re-find #"(\$)([\d]+)(\/hr)" (:text x))))) ;; make sure we filter tweets that include a dollar amount
                 (f/map (f/fn[x] {:created_at (parse-date x)
                                  :tags (vec (tags x))
                                  :rate (hourly-rate x)}))))


;; Show how many records we harvested
(duck-push 541698 {:value (f/count tag-data-rdd)})

;; Find out the highest rate offered
(duck-push 541699 {:value (-> tag-data-rdd
                                (f/map :rate)
                                 (f/reduce max))})

(-> tag-data-rdd (f/map :rate) (f/reduce max))


;; Find out the lowest rate offered
(duck-push 541700 {:value (-> tag-data-rdd
                                (f/map :rate)
                                 (f/reduce min))})


; (duck-push 541701 {:value (-> tag-data-rdd
;                                 (f/map :rate)
;                                 (f/collect)
;                                 (stats/mean))})




;; Insight #3 √
;; Load up the volume of postings we have over time
;; Widget-id: 540863


(def tag-data-serie
   "Transform data into time serie with volume per day"
              (-> data
                 (f/filter (f/fn[x]((comp not nil?) (re-find #"(\$)([\d]+)(\/hr)" (:text x))))) ;; make sure we filter tweets that include a dollar amount
                  (f/map (f/fn[x] {:timestamp (/ (tc/to-long (truncate-day (parse-date x))) 1000)}))
                  (f/count-by-value)
                 ))


(duck-push 540898 (map (fn[e] (into (first e) {:value (last e)})) tag-data-serie))




;; Insight #4 √
;; Leaderboard of the top skills
;; Widget-id: 541608

(def tag-data (-> tag-data-rdd
                 (f/flat-map (f/fn[x] (partition 2 (interleave (:tags x) (repeat (:rate x))))))))


(def tag-data-count (-> tag-data
                       (f/count-by-key)
                       (#(sort-by val > %))))


(duck-push 541608 {:value {:board (map (fn[e]{:name (first e) :values [(last e)]}) tag-data-count)}})


(defn -main[]
  (clojure.java.browse/browse-url "https://public.ducksboard.com/J1YLJIHjy7KU7SNOqIzm"))
