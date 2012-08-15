require 'store'
require 'em-synchrony/em-mongo'

class Store
  class Mongodb
    def initialize opts
      database = opts[:database]
      collection = opts[:collection] 

      @db = EM::Mongo::Connection.new.db(database)
      @co = @db.collection(collection)
    end

    def create data
      id = @co.insert(data)
    end

    def load_by_url url
      entry = @co.find({'url' => url}).first
      entry
    end

    def all
      @co.find.map{|i| i}
    end

    def update_by_url data
      @co.update({url: data['url']}, data)
    end
  end
end
