require 'test_helper'
require 'store/fs'

class FsTest < TestCase

  context 'select database' do
    should 'create folder' do
      FileUtils.rm_rf '/tmp/testdb'
      @it = Store::FS.new('index_test_db', folder: '/tmp/testdb')
      assert File.directory?('/tmp/testdb'), "didn't create folder"
    end
  end

  context 'fs' do

    setup do
      @it = Store::FS.new('index_test_db')
      @it.reset('collection')
    end

    context 'create index' do

      setup do
        @it.create_index('collection', 'name')
      end

      should 'create index directory' do
        assert File.directory?('fsdb/index_test_db/collection/index/name'), "didn't create index folder"
      end

      context 'adding a document' do
        setup do
          @data = { 'data' => 'stuff', 'name' => 'mikael' }
          @id = @it.create('collection', @data)
        end

        should 'add symlink to newly added document' do
          file = "fsdb/index_test_db/collection/index/name/mikael/#{@id}"
          assert File.exists?(file), "didn't create index file"
          assert File.symlink?(file), "File is not a symlink"

          data = JSON.parse(File.read(file))
          assert_equal @data['data'], data['data']
          assert_equal @data['name'], data['name']
        end

        should ', update index upon update record' do
          @it.update('collection', @id, { 'name' => 'pelle' })

          old_index_file = "fsdb/index_test_db/collection/index/name/mikael/#{@id}"
          new_index_file = "fsdb/index_test_db/collection/index/name/pelle/#{@id}"
          assert !File.exists?(old_index_file), "index should be removed"
          assert File.symlink?(new_index_file), "index should have been added"

          data = JSON.parse(File.read(new_index_file))
          assert_equal @data['data'], data['data']
          assert_equal 'pelle', data['name']
        end

        should 'allow and transcode slash' do
          id = @it.create('collection', { 'name' => 'mika/the/cool' })
          file = "fsdb/index_test_db/collection/index/name/mika_the_cool/#{id}"
          assert File.symlink?(file)
        end

        should 'not error on adding same index again' do
          @it.create_index('collection', 'name')
        end

        context 'adding a second index' do

          setup do
            @it.create_index('collection', 'data')
          end

          should 'index existing documents' do
            file = "fsdb/index_test_db/collection/index/data/stuff/#{@id}"
            assert File.symlink?(file), 'expected existing document to be indexed'
          end

          should 'use __empty__ for empty strings and nil' do
            data ={ 'data' => '', 'name' => nil}
            id = @it.create('collection', data)

            data_index_file = "fsdb/index_test_db/collection/index/data/__empty__/#{id}"
            name_index_file = "fsdb/index_test_db/collection/index/name/__empty__/#{id}"
            assert File.symlink?(data_index_file), 'index file not found'
            assert File.symlink?(name_index_file), 'index file not found'
          end

        end

      end

    end
  end
end
