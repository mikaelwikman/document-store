require 'test_helper'
require 'store/mongodb'
require 'store/memory'
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
          @it.create('test_table', { 
            duck: 'horse',
            has_duck: true
          })
          @it.create('test_table', { 
            duck: 'MoNkeY',
            has_duck: true
          })
          @it.create('test_table', {
            duck: 'donkey',
            has_duck: true
          })
          @it.create('test_table', {
            noduckie: 'here',
            has_duck: false
          })
        end

        should 'find entries case insensitive by filter' do
          filters = [@it.create_equal_filter(:duck, 'monkey')]
          result = @it.find('test_table', filters).map {|e| e}
          assert_equal 1, result.count
          assert_equal 'MoNkeY', result.first['duck']
        end

        should 'limit response size' do
          result = @it.find('test_table', [], limit: 1).map{|i|i}
          assert_equal 1, result.count
        end

        should 'treat \'unknown\' as nil or empty' do
          filters = [@it.create_equal_filter(:duck, 'unknown')]
          result = @it.find('test_table', filters).map {|e| e}
          assert_equal 1, result.count
          assert_equal 'here', result.first['noduckie']
        end

        should 'sort asc' do
          result = @it.find('test_table', [], sort: 'has_duck=-1,duck=1').map {|e| e}
          assert_equal 4, result.count
          assert_equal 'MoNkeY', result[0]['duck']
          assert_equal 'donkey', result[1]['duck']
          assert_equal 'horse', result[2]['duck']
          assert_equal 'here', result[3]['noduckie']
        end

        should 'sort desc' do
          result = @it.find('test_table', [], sort: 'has_duck=-1,duck=-1').map {|e| e}
          assert_equal 4, result.count
          assert_equal 'horse', result[0]['duck']
          assert_equal 'donkey', result[1]['duck']
          assert_equal 'MoNkeY', result[2]['duck']
          assert_equal 'here', result[3]['noduckie']
        end
      end

      context '#collate' do
        setup do
          @it.create('test_table', { duck: 1990 })
          @it.create('test_table', { duck: nil })
          @it.create('test_table', { duck: "" })
          @it.create('test_table', { duck: 'monkey' })
          @it.create('test_table', { duck: 'donkey' })
          @it.create('test_table', { duck: 'donkey' })
        end

        should 'find entries by filter' do
          filters = [@it.create_equal_filter(:duck, 'monkey')]
          result = @it.collate('test_table', filters)
          assert_equal 1, result[:items].count
          assert_equal 'monkey', result[:items].first['duck']
        end

        should 'limit response size' do
          result = @it.collate('test_table', [], limit: 1)
          assert_equal 1, result[:items].count
        end

        should 'give information of total item count' do
          result = @it.collate('test_table', [], limit: 1)
          assert_equal 1, result[:items].count
          assert_equal 6, result[:count]
        end


        should 'include facets if given' do
          result = @it.collate('test_table', [], facets: [:duck])
          assert_equal 6, result[:items].count
          assert_equal 1, result[:facets].count

          entries = result[:facets]['duck']
          assert entries, "Expected facets to include 'duck'"
          assert_equal 4, entries.count
          assert_equal({ name: 'donkey', value: 2 } , entries[0])
          assert_equal({ name: 'unknown', value: 2 } , entries[1])
          assert_equal({ name: 'monkey', value: 1 } , entries[2])
          assert_equal({ name: '1990', value: 1 } , entries[3])
        end
      end

    end
  end
end
