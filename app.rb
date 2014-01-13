require 'rubygems'
require 'couchrest'
require 'logger'
require 'json'
require 'redis'

redis = Redis.new :driver => :hiredis

databases = redis.smembers 'speedwing:databases'

logger = Logger.new 'log/log.txt', 'daily'

logger.info "[INIT]"

def listen_for_changes(database, redis, logger)
  db = CouchRest.database("http://localhost:5984/#{database}")
  since = redis.get "shortcircuit:#{database}:since" || 'now'
  opts = {
    :since => since,
    :feed => 'continuous',
    :heartbeat => true,
    :include_docs => true,
    :filter => 'app/lines'
  }
  db.changes opts do |payload|
    pretty_payload = JSON.pretty_generate(payload)
    #TODO: code that saves the payload to a data store
    logger.info "[EVENT] #{database} : \n#{pretty_payload}"
    redis.set "shortcircuit:#{database}:since", payload['seq']
  end
end

threads = []

databases.each do |db|
  thread = Thread.new {listen_for_changes(db, redis, logger)}
  threads.push thread
end

logger.info "[STARTING THREADS]"

threads.map!(&:join)
