(ns org.clojars.pgdad.throttle.core
  (:gen-class))

(defprotocol Throttled
  "Item to feed to Throttle."
  (fire [this])
  )

(defn simple-throttled
  [f]
  (reify Throttled
    (fire [this] f)))

(defn throttle
  [intervalMs maxWait]
  (ref {:intervalMs intervalMs
        :maxWait maxWait
        :lastFired 0
        :items []
        :running false
        :runner (agent nil)}))

(defn- fire
  [throttle]
  (let [items (:items @throttle)
        n-items (count items)]
    (if-let [first-item (first items)]
      (dosync
       (alter throttle update-in [:items] rest)
       (alter throttle update-in [:lastFired] (fn [ & args] (System/currentTimeMillis)))
       (send (:runner @throttle) #(.fire first-item))))))

(defn- throttlerer
  [throttle]
  (let [intervalMs (:intervalMs @throttle)
        running (:running @throttle)
        lastFired (:lastFired @throttle)
        currentms (System/currentTimeMillis)]
    (if running
      (if (and (< lastFired (- currentms lastFired)))
        (fire throttle)
        (recur throttle)))))

(defn start
  [throttle]
  (dosync (alter throttle update-in [:running] (fn [& args] true)))
  (-> (Thread. #(throttlerer throttle)) .start ))

(defn stop
  [throttle]
  (dosync
   (alter throttle update-in [:running] (fn [& args] false))))

(defn add
  [throttle throttled]
  (let [items (:items @throttle)
        n-items (count items)
        running (:running @throttle)]
    (dosync
     (alter throttle update-in [:items] conj throttled))
    (if (and running (= 0 n-items))
      (start throttle))))
