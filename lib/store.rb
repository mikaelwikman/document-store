
class Store
  attr_accessor :backend

  def initialize time_provider = ->() { Time.new }
    @time_provider = time_provider
  end
  
  def save data
    url = data['url']
    entry = backend.load_by_url(url)

    if entry
      entry.merge!(data)
      data['updated_at'] = entry['updated_at'] = @time_provider.call
      backend.update_by_url(entry)
    else
      data['created_at'] = @time_provider.call
      id = backend.create(data)
    end
  end

  def load_by_url url
    backend.load_by_url(url)
  end
end
