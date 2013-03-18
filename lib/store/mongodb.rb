require 'em-mongo'

class Store
  class Mongodb
    attr_writer :timestamper
    def timestamper
      @timestamper ||= lambda { Time.new }
    end

    def initialize database_name
      @database_name = database_name
      @free_connections ||= []
    end

    def close
      @db.close
      @db=nil
    end

    def create table, entry
      connect do |db|
        entry['created_at'] = entry['updated_at'] = timestamper.call

        resp = db.collection(table).safe_insert(entry)

        f = Fiber.current
        resp.callback{|doc| f.resume(doc)}
        resp.errback{|err| f.resume(:err, err)}

        result, error = Fiber.yield

        if result == :err
          raise error.inspect
        else
          result
        end
      end
    end

    def update table, id, entry
      if entry.keys.any?{|key| key.kind_of?(Symbol) }
        raise "MongoDb can't handle symbols, use only string keys!"
      end
      matcher = []
      filter = id.kind_of?(Hash) ? id : { _id: id }

      filter.each do |k,v|
        matcher << create_equal_filter(k,v)
      end

      connect do |db|
        old_entry = find(table, matcher).first

        if old_entry
          entry = old_entry.merge(entry)
          entry['updated_at'] = timestamper.call

          f = Fiber.current
          resp = db.collection(table).safe_update(filter, entry)
          resp.errback{|err| exit -1}
          resp.callback{|doc| f.resume doc}
          Fiber.yield
          entry
        else
          id = create(table, entry)
          find(table, matcher).first
        end
      end
    end

    def all table
      find(table,{})
    end

    def count table
      connect do |db|
        resp = db.collection(table).count
        f = Fiber.current
        resp.callback {|count| f.resume count }
        resp.errback {|err| raise err }

        Fiber.yield
      end
    end

    def reset table
      connect do |db|
        db.collection(table).remove()
      end
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

      if opts[:start]
        start = opts.delete(:start)
        opts[:skip] = start
      end

      connect do |db|
        f = Fiber.current
        docs = []
        resp = db.collection(table).find(real_filters, opts).each do |doc|
          if doc
            docs << doc
          else
            f.resume if f.alive?
          end
        end
        Fiber.yield
        docs
      end
    end

    def collate table, filters, opts={}
      # need to get all items, or else we can't calculate facets
      start = opts.delete(:start)
      limit = opts.delete(:limit)
      facets = opts.delete(:facets)
      facetlimit = opts.delete(:facetlimit)

      result = {
        items: find(table, filters, opts)
      }

      if facets
        result[:facets] = calculate_facets(facets, result[:items])

        if facetlimit
          result[:facets].each do |k,v|
            v.pop while v.count > facetlimit
          end
        end
      end

      result[:count] = result[:items].count

      if start
        start.times do |i|
          result[:items].shift
        end
      end

      if limit
        result[:items].pop while result[:items].count > limit
      end

      result
    end

    # filter factories
    def create_equal_filter field, name
      EqualFilter.new(field, name)
    end
    def create_lt_filter field, name
      LTFilter.new(field, name)
    end
    def create_gt_filter field, name
      GTFilter.new(field, name)
    end
    def create_gte_filter field, name
      GTEFilter.new(field, name)
    end

    private

    def connect
      # some simple connection pooling to avoid conflicts..

      con = if @free_connections.length > 0
        @free_connections.pop
      else
        EM::Mongo::Connection.new(slave_ok: true).db(@database_name)
      end

      result = yield(con)
      @free_connections << con
      result
    end

    def calculate_facets facets, records
      result = {}
      facets.each do |facet| 
        facet = facet.to_s
        temp = {}

        records.each do |record|
          record_value = record[facet] || 'unknown'

          r = record_value.kind_of?(Array) ? record_value : [record_value]

          r.each do |value|
            value = value.to_s
            value = 'unknown' if value.strip == '' 

            temp[value] ||= 0
            temp[value] += 1
          end
        end

        facet_entries = temp.map do |name, value|
          [name.to_s, value]
        end

        facet_entries.sort! {|e1, e2| e2[1] <=> e1[1] }
        result[facet] = facet_entries
      end
      result
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

    class LTFilter
      def initialize(field, value)
        @field = field; @value = value
      end

      def add_filter(hash)
        h = hash[@field] ||= {}

        if @value != 'unknown'
          h['$lt'] = @value
        end
      end
    end

    class GTFilter
      def initialize(field, value)
        @field = field; @value = value
      end

      def add_filter(hash)
        h = hash[@field] ||= {}

        if @value != 'unknown'
          h['$gt'] = @value
        end
      end
    end

    class GTEFilter
      def initialize(field, value)
        @field = field; @value = value
      end

      def add_filter(hash)
        h = hash[@field] ||= {}

        if @value != 'unknown'
          h['$gte'] = @value
        end
      end
    end
  end
end
