
class Store
  class Cache
    attr_reader :backend
    
    def initialize backend_store
      @backend = backend_store
      @cache = {}
    end

    def timestamper= ts
      backend.timestamper = ts
    end

    def timestamper
      backend.timestamper
    end

    def close
      invalidate_cache
      backend.close
    end

    def create *args
      invalidate_cache
      backend.create *args
    end

    def update *args
      invalidate_cache
      backend.update *args
    end

    def all table
      each(table).map{|i|i}
    end

    def count table
      backend.count(table)
    end

    def each table
      if data=cache_load(table)
        data
      else
        data = backend.all(table)
        cache_save table, data
        data
      end
    end

    def reset *args
      invalidate_cache
      backend.reset(*args)
    end

    def find *args
      key = Marshal.dump(args)
      if data=cache_load(key)
        data
      else
        data = backend.find(*args)
        cache_save key, data
        data
      end
    end

    def collate *args
      key = Marshal.dump(args)
      if data=cache_load(key)
        data
      else
        data = backend.collate(*args)
        cache_save(key,data)
        data
      end
    end

    def create_equal_filter *args
      backend.create_equal_filter(*args)
    end

    private

    def invalidate_cache
      @cache = {}
    end

    def cache_load key
      data = @cache[key]
      if data
        Marshal.load(data)
      end
    end

    def cache_save key, data
      @cache[key] = Marshal.dump(data)
    end

  end
end
