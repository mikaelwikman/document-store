require 'store'

class Store
  class Memory
    def initialize
      @store = {}
      @id = 0
    end

    def create data
      @id += 1
      save(@id, data)
      @id
    end

    def load id
      @store[id]
    end

    def save data
      @store[data['id']] = data
    end
  end
end
