require 'em-synchrony/em-mongo'

class Store
  class Mongodb
    attr_writer :timestamper
    def timestamper
      @timestamper ||= lambda { Time.new }
    end

    def initialize database_name
      @database_name = database_name
    end

    def close
      @db.close
      @db=nil
    end

    def create table, entry
      entry['created_at'] = entry['updated_at'] = timestamper.call
      collection(table).insert(entry)
    end

    def update table, id, entry
      if entry.keys.any?{|key| key.kind_of?(Symbol) }
        raise "MongoDb can't handle symbols, use only string keys!"
      end
      filter = id.kind_of?(Hash) ? id : { _id: id }

      old_entry = collection(table).find(filter).first

      if old_entry
        entry = old_entry.merge(entry)
        entry['updated_at'] = timestamper.call
        collection(table).update(filter, entry)
        entry
      else
        id = create(table, entry)
        collection(table).find('_id' => id).first
      end
    end

    def all table
      each(table).map{|i|i}
    end

    def each table, &block
      collection(table).find &block
    end

    def reset table
      collection(table).remove({})
    end

    def find table, filters, opts={}
      real_filters = {}
      filters.inject(real_filters) do |hash,f|
        f.add_filter(hash)
        hash
      end

      if opts[:sort]
        fields = opts.delete(:sort).split(',')
        opts[:sort] = []
        fields.each do |field|
          sort = field.split('=')
          name = sort[0]
          order = (sort[1] || '1') == '1' ? :asc : :desc
          opts[:sort] << [name,order]
        end
      end

      collection(table).find(real_filters, opts)
    end

    def collate table, filters, opts={}
      # need to get all items, or else we can't calculate facets
      limit = opts.delete(:limit)
      facets = opts.delete(:facets)

      result = {
        items: find(table, filters, opts)
      }

      if facets
        result[:facets] = calculate_facets(facets, result[:items])
      end

      result[:count] = result[:items].count

      if limit
        result[:items].pop while result[:items].count > limit
      end

      result
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

    def calculate_facets facets, records
      result = {}
      facets.each do |facet| 
        facet = facet.to_s
        temp = {}

        records.each do |record|
          name = record[facet] || 'unknown'
          name = name.to_s
          name = 'unknown' if name.strip == '' 

          temp[name] ||= 0
          temp[name] += 1
        end

        facet_entries = temp.map do |name, value|
          { name: name.to_s, value: value }
        end

        facet_entries.sort! {|e1, e2| e2[:value] <=> e1[:value] }
        result[facet] = facet_entries
      end
      result
    end

  end

  class EqualFilter
    def initialize(field, value)
      @field = field; @value = value
    end

    def add_filter(hash)
      if @value == 'unknown' 
        hash[@field] = nil
      elsif @value.kind_of?(BSON::ObjectId)
        hash[@field] = @value
      else
        hash[@field] = @value
      end
    end
  end
end
