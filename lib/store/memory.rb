class Store
  class Memory
    def initialize database_name
      @collections = {}
      @id = 0
    end

    def create table, entry
      @id += 1
      entry.keys.each do |k|
        entry[k.to_s] = entry.delete(k)
      end
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

    def find table, filters
      values = collection(table).values

      filters.each do |filter|
        values = filter.filter(values)
      end

      values
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

    class EqualFilter
      def initialize field, value
        @field = field
        @value = value
      end

      def filter entries
        entries.find_all do |entry|
          entry[@field.to_s] == @value
        end
      end
    end
  end
end
