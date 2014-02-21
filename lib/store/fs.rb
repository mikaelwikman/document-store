require 'fileutils'
require 'securerandom'
require 'json'

require 'store/fs/filters'
require 'store/fs/helpers'
require 'store/fs/index'

class Store
  class FS

    attr_writer :timestamper

    def timestamper
      @timestamper || lambda {Time.new}
    end

    def initialize db_name
      @folder = 'fsdb'
      @db_name = db_name
      @db_path = "#{@folder}/#{@db_name}"

      Dir.mkdir(@folder) unless File.directory?(@folder)
      Dir.mkdir(@db_path) unless File.directory?(@db_path)
    end

    def reset collection_name
      path = collection_path(collection_name)
      FileUtils.rm_rf(path)
    end

    def create collection_name, doc
      path = collection_path(collection_name)

      id = doc['_id']

      unless id
        id = SecureRandom.uuid 
        while File.exists?(entry_path(path, id))
          id = SecureRandom.uuid 
        end
      end

      doc['_id'] = id
      doc['created_at'] = doc['updated_at'] = timestamper.call

      serialized = JSON.pretty_generate(doc)

      File.write(entry_path(path, id), serialized)

      add_to_index(path, doc)

      id
    end

    def update collection_name, id_or_hash, changes
      path = collection_path(collection_name)

      id_is_hash = id_or_hash.kind_of?(Hash)
      
      # use id to find doc
      if !id_is_hash || id_or_hash.length == 1 && id_or_hash.key?(:_id)
        id = id_or_hash

        doc_path = entry_path(path, id)
        old_doc = nil

        if File.exists?(doc_path)
          old_doc = JSON.parse(File.read(doc_path))
        else
          old_doc = {}
        end

        new_doc = old_doc.merge(changes)
        new_doc['updated_at'] = timestamper.call

        File.write(doc_path, JSON.pretty_generate(new_doc))

        update_index path, old_doc, new_doc

        return new_doc
      else
        # can't use id to find event, use #find
        hash = id_or_hash

        filters = hash.map { |k,v| EqualFilter.new(k, v) }

        find(collection_name, filters, limit: 1).each do |old_doc|
          match = true
          hash.each do |k,v|
            if old_doc[k] != v
              match = false
              break
            end
          end

          if match
            new_doc = old_doc.merge(changes)
            new_doc['updated_at'] = timestamper.call

            doc_path = entry_path(path, new_doc['_id'])
            File.write(doc_path, JSON.pretty_generate(new_doc))

            update_index path, old_doc, new_doc

            return new_doc
          end
        end

        # if we got here, it means we didn't match anything, so lets create it
        doc = changes
        create(collection_name, doc)
        doc
      end
    end

    def count collection_name
      path = collection_path(collection_name)

      line_count = `ls -f '#{data_path(path)}' | wc -l`
      line_count.to_i - 2 # exclude . and ..
    end

    def all collection_name
      result = []
      each(collection_name) { |doc| result << doc }
      result
    end

    def each collection_name
      path = collection_path(collection_name)

      result = []
      Dir["#{data_path(path)}/*"].each do |entry_path|
        yield JSON.parse(File.read(entry_path))
      end

      result
    end

    def find collection_name, filters, opts={}
      found = []

      each(collection_name) do |doc|
        if filters.all?{|f| f.match?(doc)}
          found << doc
        end
      end

      if opts[:sort]
        fields = opts[:sort].split(',')
        fields.map! do |field|
          sort = field.split('=')
          [sort[0], (sort[1] || 1).to_i]
        end

        found.sort! do |e1,e2| 
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

      if opts[:start]
        opts[:start].times do |i|
          found.shift
        end
      end

      if opts[:limit]
        found.pop while found.count > opts[:limit]
      end

      found
    end

    def collate collection_name, filters, opts={}
      # need to get all items, or else we can't calculate facets
      start = opts.delete(:start)
      limit = opts.delete(:limit)
      facetlimit = opts.delete(:facetlimit)

      result = {
        items: find(collection_name, filters, opts)
      }

      if opts[:facets]
        result[:facets] = calculate_facets(opts[:facets], result[:items])

        if facetlimit
          result[:facets].each do |k,v|
            v.pop while v.count > facetlimit
          end
        end
      end

      result[:count] = result[:items].count

      if start
        start.times do |i|
          result[:items].shift
        end
      end

      if limit
        result[:items].pop while result[:items].count > limit
      end

      result
    end

    # filter factories
    def create_equal_filter field, name
      EqualFilter.new(field, name)
    end
    def create_lt_filter field, name
      LTFilter.new(field, name)
    end
    def create_gt_filter field, name
      GTFilter.new(field, name)
    end
    def create_gte_filter field, name
      GTEFilter.new(field, name)
    end

    private

    def calculate_facets facets, records
      result = {}
      facets.each do |facet| 
        facet = facet.to_s
        temp = {}

        records.each do |record|
          record_value = record[facet] || 'unknown'

          r = record_value.kind_of?(Array) ? record_value : [record_value]

          r.each do |value|
            value = value.to_s
            value = 'unknown' if value.strip == '' 

            temp[value] ||= 0
            temp[value] += 1
          end
        end

        facet_entries = temp.map do |name, value|
          [name.to_s, value]
        end

        facet_entries.sort! {|e1, e2| e2[1] <=> e1[1] }
        result[facet.to_s] = facet_entries
      end
      result
    end
  end
end
