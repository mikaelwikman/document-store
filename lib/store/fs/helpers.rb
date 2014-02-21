class Store
  class FS
    def collection_path name
      "#{@db_path}/#{name}".tap do |path|
        unless File.directory?(path)
          Dir.mkdir(path)
          Dir.mkdir("#{path}/data")
          Dir.mkdir("#{path}/index")
        end
      end
    end

    def entry_path collection_path, id
      "#{collection_path}/data/#{id}"
    end

    def data_path collection_path
      "#{collection_path}/data"
    end

    def index_path collection_path
      "#{collection_path}/index"
    end
  end
end
