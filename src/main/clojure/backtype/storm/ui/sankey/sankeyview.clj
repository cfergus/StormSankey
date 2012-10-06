;; java -cp target\;C:\Apps\storm-0.8.1\storm-0.8.1.jar;C:\Apps\storm-0.8.1\lib\*;. clojure.main -i backtype/storm/ui/sankey/sankeyview.clj -e (backtype.storm.ui.sankey/-main)

(ns backtype.storm.ui.sankey
  (:use compojure.core)
  (:use [hiccup core page-helpers])
  (:use [backtype.storm config util])
  (:use [backtype.storm.ui helpers])
  (:use [backtype.storm.daemon [common :only [ACKER-COMPONENT-ID system-id?]]])
  (:use [ring.adapter.jetty :only [run-jetty]])
  (:use [clojure.string :only [trim]])
  (:import [backtype.storm.generated ExecutorSpecificStats
            ExecutorStats ExecutorSummary TopologyInfo SpoutStats BoltStats
            ErrorInfo ClusterSummary SupervisorSummary TopologySummary
            Nimbus$Client StormTopology GlobalStreamId])
  (:require [compojure.route :as route]
            [compojure.handler :as handler]
            [backtype.storm [thrift :as thrift]]
            [backtype.storm.ui.core :as storm-ui])
  (:gen-class))

(defn sankey-ui-template [body]
  (html
   [:head
    [:title "Storm UI"]
    (include-css "/css/bootstrap-1.1.0.css")
    (include-css "/css/topology-sankey.css")
    (include-js "/js/jquery-1.6.2.min.js")
    (include-js "/js/d3.v2.js")
    (include-js "/js/sankey.js")
    ]
    [:body
    [:h1 (link-to "/" "Storm UI")]
    (seq body)
     (include-js "/js/topology-sankey.js")
    ]))

; TODO : include sys stats conditionally

(defn topology-sankey-page1 [topology-id window include-sys?]
  (storm-ui/with-nimbus nimbus
    (let [window (if window window ":all-time")
          summ (.getTopologyInfo ^Nimbus$Client nimbus topology-id)
          topology (.getTopology ^Nimbus$Client nimbus topology-id)
          spout-summs (filter (partial storm-ui/spout-summary? topology) (.get_executors summ))
          bolt-summs (filter (partial storm-ui/bolt-summary? topology) (.get_executors summ))
          spout-comp-summs (storm-ui/group-by-comp spout-summs)
          bolt-comp-summs (storm-ui/group-by-comp bolt-summs)]
      (concat
       [[:h2 "Bolts (unknown duration)"]]
       (storm-ui/bolt-comp-table topology-id bolt-comp-summs (.get_errors summ) window include-sys?)

       ; get json of topo stats

       [[:h2 "Sankey"]
         [:p {:id "chart"}]
         ]

       ))
    )
)



; TODO : Post filter on streamId based on desired stream to view
(defn bolt-input-stats [window ^TopologyInfo topology-info component executors include-sys?]
  (let [stats (storm-ui/get-filled-stats executors)
        stream-summary (-> stats (storm-ui/aggregate-bolt-stats include-sys?))
        all-inputs-to-bolt (get (:acked stream-summary) window  )
        ]

        #_;(str all-inputs-to-bolt)
        ; Filter out those counts not part of the stream of interest
        (filter
            #(= "default" (.get_streamId (first %)))
            all-inputs-to-bolt
        )

    )
  )




(defn topology-sankey-page [topology-id window include-sys?]
  (storm-ui/with-nimbus nimbus
  (let [
        window (if window window ":all-time")
        summ (.getTopologyInfo ^Nimbus$Client nimbus topology-id)
        topology (.getTopology ^Nimbus$Client nimbus topology-id)
        bolt-summs (filter (partial storm-ui/bolt-summary? topology) (.get_executors summ))
        bolt-comp-summs (storm-ui/group-by-comp bolt-summs)
        ]
    (concat
     [[:h2 "Components" ]]
      [[:div

        (for [[bolt-id summs] bolt-comp-summs]
          [:p bolt-id " - " (count summs) ]
          ; (str summs)
          )

      ]]

      [[:div
        (for [[bolt-id summs] bolt-comp-summs]

          (let [input-stats (bolt-input-stats window summ bolt-id
                 (storm-ui/component-task-summs summ topology bolt-id)
                 include-sys?)]
            (concat

              (for [[global-stream-id count] input-stats]
                  #_[:p "{ \"source\" : " (.get_componentId global-stream-id)
                   ", \"target\": " bolt-id
                   ", \"value\": " count
                   "},"]
                   [:script "var energy =
                    {\"nodes\":[
                    {\"name\":\"Agricultural 'waste'\"},
                    {\"name\":\"Bio-conversion\"},
                    {\"name\":\"Liquid\"},
                    {\"name\":\"Losses\"},
                    {\"name\":\"Solid\"},
                    {\"name\":\"Gas\"}
                    ],
                    \"links\":[
                    {\"source\":0,\"target\":1,\"value\":124.729},
                    {\"source\":1,\"target\":2,\"value\":0.597},
                    {\"source\":1,\"target\":3,\"value\":26.862},
                    {\"source\":1,\"target\":4,\"value\":280.322},
                    {\"source\":1,\"target\":5,\"value\":81.144}
                    ]};
                    "
                   ]
                )

              )
          )
          )
        ]]

       [[:h2 "Sankey"]
         [:p {:id "chart"}]
         ]
     )))
  )



(defroutes main-routes
  (GET "/sankey/topology/:id" [:as {cookies :cookies} id & m]
    (let [include-sys? (storm-ui/get-include-sys? cookies)]
       (-> (topology-sankey-page id (:window m) include-sys?)
           sankey-ui-template)))

  (route/resources "/")
  (route/not-found "Page not found"))

(def app
  (handler/site main-routes)
 )

(defn -main []
  (run-jetty app {:port 8080}))