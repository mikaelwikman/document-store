require 'em-synchrony/em-mongo'

class Store
  class Mongodb
    def initialize database_name
      @database_name = database_name
    end

    def close
      @db.close
      @db=nil
    end

    def create table, entry
      collection(table).insert(entry)
    end

    def update table, id, entry
      collection(table).update({ _id: id }, entry)
    end

    def each table, &block
      collection(table).find &block
    end

    def reset table
      collection(table).remove({})
    end

    def find table, filters
      real_filters = {}
      filters.inject(real_filters) do |hash,f|
        f.add_filter(hash)
      end

      collection(table).find(real_filters)
    end

    # filter factories
    def create_equal_filter field, name
      EqualFilter.new(field, name)
    end

    private

    def collection name
      db.collection(name)
    end

    def db
      @db ||= EM::Mongo::Connection.new.db(@database_name)
    end

  end

  class EqualFilter
    def initialize(field, value)
      @field = field; @value = value
    end

    def add_filter(hash)
      hash[@field] = @value
    end
  end
end
