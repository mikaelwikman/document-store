class Store
  class FS

    def create_index collection_name, index_name
      path = collection_path(collection_name)
      path_index = index_path(path)

      branch_path = "#{path_index}/#{index_name}"
      FileUtils.mkdir branch_path unless File.directory?(branch_path)

      each(collection_name) do |doc|
        add_to_index path, doc
      end
    end

    private

    def add_to_index path, doc
      path = index_path(path)

      # the task is to add this document to each index available

      get_indices(path).each do |index|
        branch_name = get_branch_name(doc[index])

        branch_path = "#{path}/#{index}/#{branch_name}"
        unless File.exists?(branch_path)
          FileUtils.mkdir(branch_path)
        end

        symlink_path = "#{branch_path}/#{doc['_id']}"
        unless File.symlink?(symlink_path)
          file_path = "../../../data/#{doc['_id']}"
          File.symlink file_path, symlink_path
        end

      end
    end

    def update_index path, old_doc, new_doc
      path = index_path(path)

      get_indices(path).each do |index|
        if old_doc[index] != new_doc[index]
          old_branch_name = get_branch_name(old_doc[index])
          File.delete "#{path}/#{index}/#{old_branch_name}/#{old_doc['_id']}"


          new_branch_name = get_branch_name(new_doc[index])
          new_branch_path = "#{path}/#{index}/#{new_branch_name}"
          unless File.exists?(new_branch_path)
            FileUtils.mkdir(new_branch_path)
          end
          File.symlink "../../../data/#{new_doc['_id']}", "#{new_branch_path}/#{new_doc['_id']}"
        end
      end
    end


    private 
    def get_indices path
      Dir.glob("#{path}/*/").map{|d| d.chop!; d[(d.rindex('/')+1)..-1]}
    end

    def get_branch_leaves path, name, value
      r = Dir["#{path}/#{name}/#{get_branch_name(value)}/*"]
      r.map{|d| d[(d.rindex('/')+1)..-1]}
    end

    def get_branch_name val
      branch_name = val.to_s
      branch_name.gsub! '/', '_'
      branch_name = '__empty__' if branch_name.empty?
      branch_name
    end
  end
end
