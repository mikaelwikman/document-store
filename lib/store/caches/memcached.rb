#
# THIS IS UNTESTED, AND UNUSED, DON'T USE!!
#

require 'em-synchrony/em-memcache'

module Caches
  class Memcached
    @@max_size = 1000000
    def initialize
      @cache = EM::P::Memcache.connect
    end

    def invalidate
      set 'invalidated_at', Time.new
    end

    def load(key)
      key_date = "#{key}_date"

      time_set = get(key_date) 
      invalidated_at = get('invalidated_at')

      if time_set && (!invalidated_at || time_set > invalidated_at)
        data = get(key)
        data
      end
    end

    def save key, data
      key_date = "#{key}_date"

      set(key, data)
      set(key_date, Time.new)
    end

    private

    def clean! key
      key.gsub! /[^a-zA-Z_]/, ''
    end

    def get key
      clean!(key)
      data = ""
      i = 0
      puts "READ #{key}_#{i}"
      while new_data=@cache.get("#{key}_#{i}")
        data << new_data
        puts "Got #{new_data.length} characters"
        i+=1
        puts "READ #{key}_#{i}"
      end
      if data.length > 0 
        Marshal.load(data)
      end
    end

    def set key, data
      clean!(key)
      data = Marshal.dump(data)
      i = 0
      while i*@@max_size < data.length
        tkey = "#{key}_#{i}"
        minidata = data[i*@@max_size, @@max_size]
        puts "Write #{tkey}, block of #{minidata.length}"
        @cache.set(tkey, minidata)

        i+=1
      end
    end
  end
end
