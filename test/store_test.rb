require 'test_helper'
require 'store/mongodb'
require 'store/memory'
require 'store/cache'
require 'em-synchrony'

# The cache store differs in initializer from the others, so we'll
# create a fake one to initialize it properly
class InMemoryCacheStore < Store::Cache
  def initialize database
    super(Store::Memory.new(database))
  end
end
#class MemcachedStore < Store::Cache
#  def initialize database
#    super(Store::Memory.new(database), memcached: true)
#  end
#end

[
  Store::Mongodb,
  Store::Memory,
  InMemoryCacheStore
].each do |store|
  Class.new(TestCase).class_eval do 

    should store.name+ ' use current Time as default time stamper' do
      val = store.new('hubo').timestamper.call 
      assert val.kind_of?(Time)
    end

    should store.name+ 'allow setting time stamper' do
      s = store.new('hubo')
      s.timestamper = lambda { 4 }
      assert_equal 4, s.timestamper.call
    end

    context "Testing #{store.name}" do
      setup do
        @it = store.new('testDb')
        @it.reset('test_table')
        @it.reset('testosteron_table')
        timestamp = 0
        @it.timestamper = lambda { timestamp+=1 }
      end

      should '#count' do 
        id = @it.create('test_table', { duck: 'monkey' })
        id = @it.create('test_table', { duck: 'monkey' })
        id = @it.create('testosteron_table', { duck: 'monkey' })

        assert_equal 2, @it.count('test_table')
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
        assert_equal 1, result.first['created_at']
      end

      context '#update' do
        should 'update given fields entry' do
          id = @it.create('test_table', { duck: 'monkey' })

          entry = { 'duck' => 'history' }

          @it.update('test_table', id, entry)

          entries = @it.all('test_table')

          assert_equal 1, entries.count
          assert_equal 'history', entries.first['duck']
          assert_equal 2, entries.first['updated_at']
        end

        should 'update entry by matcher' do
          @it.create('test_table', { duck: 'monkey' })
          @it.create('test_table', { duck: 'donkey' })
          @it.create('test_table', { duck: 'congo' })

          @it.update('test_table', 
                     { 'duck' => 'donkey'}, 
                     { 'duck' => 'history'})

          entries = @it.all('test_table')

          assert_equal 3, entries.count
          assert_equal 'monkey', entries[0]['duck']
          assert_equal 'history', entries[1]['duck']
          assert_equal 'congo', entries[2]['duck']
        end

        should 'update should create if not exist' do
          r = @it.update('test_table', {'duck' => 'donkey'}, { 'duck' => 'donkey'})

          entries = @it.all('test_table')
          assert_equal 1, entries.count
          assert_equal 'donkey', entries[0]['duck']
          assert_equal 1, entries[0]['updated_at']
          assert_equal 1, entries[0]['created_at']
        end

        should 'return the resulting entry while updating' do
          id = @it.create('test_table', { duck: 'monkey', paid_taxes: true })
          entry = @it.update('test_table', id, 'duck' => 'history')

          assert_equal 'history', entry['duck']
          assert_equal true, entry['paid_taxes']
        end

        should 'return the resulting entry after created' do
          entry = @it.update('test_table', { 'duck' => 'history' }, 'duck' => 'history')

          assert_equal 'history', entry['duck']
          assert entry['_id'], 'ID should be set'
        end
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

        should 'treat "unknown" as empty string as unexisting' do
          @it.create('test_table', {
            verify: true
          })
          filters = [@it.create_equal_filter(:duck, 'unknown')]
          r = @it.find('test_table', filters)
          assert_equal 2, r.count
          assert_equal 'here', r[0]['noduckie']
          assert_equal true, r[1]['verify']
        end

        should 'find entries case sensitive by filter' do
          filters = [@it.create_equal_filter(:duck, 'monkey')]
          result = @it.find('test_table', filters).map {|e| e}
          assert_equal 0, result.count

          filters = [@it.create_equal_filter(:duck, 'MoNkeY')]
          result = @it.find('test_table', filters).map {|e| e}
          assert_equal 1, result.count
          assert_equal 'MoNkeY', result.first['duck']
        end

        should 'handle boolean filters' do
          filters = [@it.create_equal_filter(:has_duck, false)]
          result = @it.find('test_table', filters).map {|e| e}
          assert_equal 1, result.count
          assert_equal 'here', result.first['noduckie']
        end

        should 'limit response size' do
          result = @it.find('test_table', [], limit: 1).map{|i|i}
          assert_equal 1, result.count
        end

        should 'set zero-based start index' do
          result = @it.find('test_table', [], start: 2).map{|i|i}
          assert_equal 2, result.count
          assert_equal 'donkey', result[0]['duck']
          assert_equal 'here', result[1]['noduckie']
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

        should 'set zero-based start index' do
          result = @it.collate('test_table', [], start: 3, limit: 2)
          assert_equal 2, result[:items].count
          assert_equal 'monkey', result[:items][0]['duck']
          assert_equal 'donkey', result[:items][1]['duck']
        end


        context 'total count' do
          should 'not be affected by limit' do
            result = @it.collate('test_table', [], limit: 1)
            assert_equal 6, result[:count]
          end

          should 'not be affected by start' do
            result = @it.collate('test_table', [], start: 3)
            assert_equal 6, result[:count]
          end
        end

        should 'include facets if given' do
          @it.create('test_table', { duck: ['donkey', 'muppet'] })

          result = @it.collate('test_table', [], facets: [:duck])
          assert_equal 7, result[:items].count
          assert_equal 1, result[:facets].count

          entries = result[:facets]['duck']
          assert entries, "Expected facets to include 'duck'"
          assert_equal 5, entries.count
          assert_equal [
            ['donkey'  , 3],
            ['unknown' , 2],
            ['monkey'  , 1],
            ['1990'    , 1],
            ['muppet'  , 1],
          ], entries
        end

        should 'limit facet entries count, cutting lesser important' do
          result = @it.collate('test_table', [], facets: [:duck], facetlimit: 2)
          entries = result[:facets]['duck']
          assert_equal 2, entries.count
          assert_equal(['donkey', 2], entries[0])
        end
      end

    end
  end
end
