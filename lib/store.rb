
class Store

  def initialize time_provider = ->() { Time.new }
    @id = 0
    @store = MemoryStore.new
    @time_provider = time_provider
  end
  
  def save data
    given_id = data['id']
    entry = @store.load(given_id)

    if entry
      entry['updated_at'] = @time_provider.call
      entry.merge!(data)
      @store.save(given_id, entry)
      given_id
    else
      @id+=1
      data['created_at'] = @time_provider.call
      @store.create(@id, data)
      @id
    end
  end

  def load id
    @store.load(id)
  end

  private

  class MemoryStore
    def initialize
      @store = {}
    end

    def create id, data
      save(id, data)
    end

    def load id
      @store[id]
    end

    def save id, data
      @store[id] = data
    end
  end
end
