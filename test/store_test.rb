require 'test_helper'
require 'store/mongodb.rb'
require 'store/memory.rb'
require 'em-synchrony'

[Store::Mongodb, Store::Memory].each do |store|
  Class.new(TestCase).class_eval do 

    context "Testing #{store.name}" do
      setup do
        @it = store.new('testDb')
        @it.reset('test_table')
      end

      should '#all aggregate all results' do
        id = @it.create('test_table', { duck: 'monkey' })

        result = @it.all('test_table')

        assert_equal 1, result.count
        assert_equal 'monkey', result[0]['duck']
      end

      should "create and retrieve entry" do
        id = @it.create('test_table', { duck: 'monkey' })

        assert id.kind_of?(BSON::ObjectId) || id.to_i > 0

        result = @it.all('test_table')
        assert_equal 1, result.count
        assert_equal 'monkey', result.first['duck']
      end

      should 'update entry' do
        id = @it.create('test_table', { duck: 'monkey' })

        entry = { duck: 'history' }

        @it.update('test_table', id, entry)

        entries = @it.all('test_table')

        assert_equal 1, entries.count
        assert_equal 'history', entries.first['duck']
      end

      should 'update entry by matcher' do
        @it.create('test_table', { duck: 'monkey' })
        @it.create('test_table', { duck: 'donkey' })
        @it.create('test_table', { duck: 'congo' })

        @it.update('test_table', 
                   { duck: 'donkey'}, 
                   { duck: 'history'})

        entries = @it.all('test_table')

        assert_equal 3, entries.count
        assert_equal 'monkey', entries[0]['duck']
        assert_equal 'history', entries[1]['duck']
        assert_equal 'congo', entries[2]['duck']
      end

      should 'update should create if not exist' do
        r = @it.update('test_table', {duck: 'donkey'}, { duck: 'donkey'})

        entries = @it.all('test_table')
        assert_equal 1, entries.count
        assert_equal 'donkey', entries[0]['duck']
      end

      context '#find' do
        setup do
          @it.create('test_table', { duck: 'horse' })
          @it.create('test_table', { duck: 'monkey' })
          @it.create('test_table', { duck: 'donkey' })
        end

        should 'find entries by filter' do
          filters = [@it.create_equal_filter(:duck, 'monkey')]
          result = @it.find('test_table', filters).map {|e| e}
          assert_equal 1, result.count
          assert_equal 'monkey', result.first['duck']
        end

        should 'limit response size' do
          result = @it.find('test_table', [], limit: 1).map{|i|i}
          assert_equal 1, result.count
        end
      end


    end
  end
end
