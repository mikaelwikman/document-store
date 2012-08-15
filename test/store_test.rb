require 'test_helper'
require 'store/mongodb.rb'
require 'store/memory.rb'
require 'em-synchrony'

[Store::Mongodb, Store::Memory].each do |store|
  Class.new(TestCase).class_eval do 

    context "Testing #{store.class}" do
      setup do
        @it = store.new('testDb')
      end

      should "create and retrieve entry" do
        EM.synchrony do
          @it.reset('test_table')
          id = @it.create('test_table', { duck: 'monkey' })

          assert id.kind_of?(BSON::ObjectId) || id.to_i > 0

          result = @it.each('test_table').map {|e| e}
          assert_equal 1, result.count
          assert_equal 'monkey', result.first['duck']

          EM.stop
        end
      end

      should 'update entry' do
        EM.synchrony do
          @it.reset('test_table')
          id = @it.create('test_table', { duck: 'monkey' })

          entry = { duck: 'history' }

          @it.update('test_table', id, entry)

          entries = @it.each('test_table').map{|i| i}

          assert_equal 1, entries.count
          assert_equal 'history', entries.first['duck']

          EM.stop
        end
      end

      should 'find entries by filter' do
        EM.synchrony do
          @it.reset('test_table')
          @it.create('test_table', { duck: 'horse' })
          @it.create('test_table', { duck: 'monkey' })
          @it.create('test_table', { duck: 'donkey' })

          filters = [@it.create_equal_filter(:duck, 'monkey')]
          result = @it.find('test_table', filters).map {|e| e}
          assert_equal 1, result.count
          assert_equal 'monkey', result.first['duck']

          EM.stop
        end
      end
    end
  end
end
