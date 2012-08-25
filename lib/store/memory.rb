class Store
  class Memory
    attr_writer :timestamper
    def timestamper
      @timestamper || lambda {Time.new}
    end

    def initialize database_name
      @collections = {}
      @id = 0
    end

    def create table, entry
      @id += 1
      entry.keys.each do |k|
        entry[k.to_s] = entry.delete(k)
      end
      entry['updated_at'] = entry['created_at'] = timestamper.call
      collection(table)[@id] = entry
      @id
    end

    def all table
      collection(table).values
    end

    def each table, &block
      collection(table).values.each &block
    end

    def reset table
      @collections.delete(table)
    end

    def find table, filters, opts={}
      values = collection(table).values

      filters.each do |filter|
        values = filter.filter(values)
      end

      if opts[:sort]
        fields = opts[:sort].split(',')
        fields.map! do |field|
          sort = field.split('=')
          [sort[0], (sort[1] || 1).to_i]
        end

        values.sort! do |e1,e2| 
          order = 0

          fields.each do |field|
            name = field[0]
            asc = field[1]
            f1 = e1[name]
            f2 = e2[name]

            f1 = 1 if f1 == true
            f1 = 0 if f1 == false
            f2 = 1 if f2 == true
            f2 = 0 if f2 == false

            order = asc * ((f1 <=> f2) || 0)
            break if order != 0
          end

          order
        end
      end

      if opts[:limit]
        values.pop while values.count > opts[:limit]
      end

      values
    end

    def collate table, filters, opts={}
      # need to get all items, or else we can't calculate facets
      limit = opts.delete(:limit)

      result = {
        items: find(table, filters, opts)
      }

      if opts[:facets]
        result[:facets] = calculate_facets(opts[:facets], result[:items])
      end

      result[:count] = result[:items].count

      if limit
        result[:items].pop while result[:items].count > limit
      end

      result
    end

    def update table, id, entry
      old_entry=nil

      if id.kind_of?(Hash)
        collection(table).each do |orig_k,orig_v|
          if id.all?{|k,v| orig_v[k.to_s] == v}
            old_entry = orig_v
            id = orig_k
            break;
          end
        end
      else
        old_entry = collection(table)[id]
      end

      if not old_entry
        create table, entry
        return
      end

      entry.keys.each do |key|
        entry[key.to_s] = entry.delete(key)
      end

      entry = old_entry.merge(entry)
      entry['updated_at'] = timestamper.call

      collection(table)[id] = entry
      nil
    end

    # filters
    def create_equal_filter field, value
      EqualFilter.new(field,value)
    end

    private 

    def collection table
      @collections[table] ||= {}
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
        result[facet.to_s] = facet_entries
      end
      result
    end

    class EqualFilter
      def initialize field, value
        @field = field.to_s

        if value == 'unknown'
          @value = ""
        else
          @value = value.to_s
        end
      end

      def filter entries
        entries.find_all do |entry|
          entry[@field].to_s == @value
        end
      end
    end
  end
end
