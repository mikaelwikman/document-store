require 'test_helper'
require 'store/mongodb.rb'
require 'store/memory.rb'
require 'em-synchrony'

[Store::Mongodb, Store::Memory].each do |store|
  Class.new(TestCase).class_eval do 

    context "Testing #{store.name}" do
      setup do
        @it = store.new('testDb')
      end

      should '#all aggregate all results' do
        EM.synchrony do
          @it.reset('test_table')
          id = @it.create('test_table', { duck: 'monkey' })

          result = @it.all('test_table')

          assert_equal 1, result.count
          assert_equal 'monkey', result[0]['duck']

          EM.stop
        end
      end

      should "create and retrieve entry" do
        EM.synchrony do
          @it.reset('test_table')
          id = @it.create('test_table', { duck: 'monkey' })

          assert id.kind_of?(BSON::ObjectId) || id.to_i > 0

          result = @it.all('test_table')
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

          entries = @it.all('test_table')

          assert_equal 1, entries.count
          assert_equal 'history', entries.first['duck']

          EM.stop
        end
      end

      should 'update entry by matcher' do
        EM.synchrony do
          @it.reset('test_table')
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

          EM.stop
        end
      end

      should 'update should create if not exist' do
        EM.synchrony do
          @it.reset('test_table')
          r = @it.update('test_table', {duck: 'donkey'}, { duck: 'donkey'})

          entries = @it.all('test_table')
          assert_equal 1, entries.count
          assert_equal 'donkey', entries[0]['duck']

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
