
module Caches
  class InMemory
    def initialize
      @cache = {}
    end

    def invalidate
      @cache = {}
    end

    def load(key)
      data = @cache[key]
      if data
        Marshal.load(data)
      end
    end

    def save key, data
      @cache[key] = Marshal.dump(data)
    end
  end
end
