require 'store/caches/in_memory'
require 'store/caches/memcached'

class Store
  class Cache
    attr_reader :backend
    
    def initialize backend_store, args={}
      @backend = backend_store
      if args[:memcached]
        @cache = Caches::Memcached.new
      else
        @cache = Caches::InMemory.new
      end
    end

    def timestamper= ts
      backend.timestamper = ts
    end

    def timestamper
      backend.timestamper
    end

    def close
      @cache.invalidate
      backend.close
    end

    def create *args
      @cache.invalidate
      backend.create *args
    end

    def update *args
      @cache.invalidate
      backend.update *args
    end

    def all table
      each(table).map{|i|i}
    end

    def count table
      backend.count(table)
    end

    def each table
      if data=@cache.load(table)
        data
      else
        data = backend.all(table)
        @cache.save table, data
        data
      end
    end

    def reset *args
      @cache.invalidate
      backend.reset(*args)
    end

    def find *args
      key = Marshal.dump(args)
      if data=@cache.load(key)
        data
      else
        data = backend.find(*args)
        @cache.save key, data
        data
      end
    end

    def collate *args
      key = Marshal.dump(args)
      if data=@cache.load(key)
        data
      else
        data = backend.collate(*args)
        @cache.save(key,data)
        data
      end
    end

    def create_equal_filter *args
      backend.create_equal_filter(*args)
    end

  end
end
