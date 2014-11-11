(ns airpair-clj-spark-introduction.core
  (:require  [flambo.api :as f]
             [flambo.conf :as conf]
             [cheshire.core :refer :all]
             [clj-time.core :as time]
             [clj-time.coerce :as tc]
             [clj-time.format :as t]
             [clj-http.client :as client])
  (:import [org.apache.log4j Level Logger])
  (:gen-class))



