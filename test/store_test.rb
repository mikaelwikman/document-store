require 'test_helper'
require 'store/mongodb'
require 'store/memory'
require 'store/cache'
require 'store/fs'

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
  InMemoryCacheStore,
  Store::FS
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
        assert id.kind_of?(BSON::ObjectId) || id.to_i > 0 || id.length > 20

        result = @it.all('test_table')
        assert_equal 1, result.count
        assert_equal 'monkey', result.first['duck']
        assert_equal 1, result.first['created_at']
      end

      should 'be allowed to choose id' do
        id = @it.create('test_table', { '_id' => 'monkey' })

        assert_equal 'monkey', id
        assert_equal 'monkey', @it.all('test_table').first['_id']
      end

      context '#update' do
        should 'handle many concurrent updates' do
          id = @it.create('test_table', { duck: 'monkey' })

          100.times do
            f = Fiber.new do
              entry = { 'duck' => 'history' }
              @it.update('test_table', id, entry)
            end

            EM.next_tick { f.resume }
          end

          EM::Synchrony.sleep(0.1)
        end

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
          monkey_id = @it.create('test_table', { duck: 'monkey' })
          donkey_id = @it.create('test_table', { duck: 'donkey' })
          congo_id = @it.create('test_table', { duck: 'congo' })

          @it.update('test_table', 
                     { 'duck' => 'donkey'}, 
                     { 'duck' => 'history'})

          entries = {}
          @it.all('test_table').each do |d|
            entries[d['_id']] = d
          end

          assert_equal 3, entries.count
          assert_equal 'monkey', entries[monkey_id]['duck']
          assert_equal 'history', entries[donkey_id]['duck']
          assert_equal 'congo', entries[congo_id]['duck']
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

        should 'make partial updates' do
          entry = @it.create('test_table', { 'duck' => 'history', paid_taxes: true })
          entry = @it.update('test_table', { 'duck' => 'history' }, 'duck' => 'monkey')

          entries = @it.all('test_table')
          assert_equal 1, entries.count
          assert_equal 'monkey', entries[0]['duck']
          assert_equal true, entries[0]['paid_taxes']
        end
      end

      context '#find' do
        setup do
          @horse = @it.create('test_table', { 
            duck: 'horse',
            has_duck: true,
            array_test: [1, 2, 3],
            number: 1
          })
          @monkey = @it.create('test_table', { 
            duck: 'MoNkeY',
            has_duck: true,
            number: 2
          })
          @donkey = @it.create('test_table', {
            duck: 'donkey',
            has_duck: true,
            number: 3
          })
          @here = @it.create('test_table', {
            noduckie: 'here',
            has_duck: false,
            number: 4
          })
        end

        should 'treat "unknown" as empty string as unexisting' do
          id = @it.create('test_table', {
            verify: true
          })
          filters = [@it.create_equal_filter(:duck, 'unknown')]
          result = @it.find('test_table', filters)
          assert_equal 2, result.count

          r = {}
          result.each{|d| r[d['_id']] = d}

          assert_equal 'here', r[@here]['noduckie']
          assert_equal true, r[id]['verify']
        end

        should 'treat several values as OR' do
          filters = [@it.create_equal_filter('array_test', 1)]
          result = @it.find('test_table', filters)
          assert_equal 1, result.count
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

        context 'filters' do

          should 'equal' do
            filters = [@it.create_equal_filter(:has_duck, false)]
            result = @it.find('test_table', filters).map {|e| e}
            assert_equal 1, result.count
            assert_equal 'here', result.first['noduckie']
          end
          
          should 'less-than' do
            filters = [@it.create_lt_filter(:number, 3)]
            result = @it.find('test_table', filters).map {|e| e}
            r = {} ; result.each{|i| r[i['_id']] = i }
            assert_equal 2, result.count
            assert_equal 1, r[@horse]['number']
            assert_equal 2, r[@monkey]['number']
          end

          should 'greater-than' do
            filters = [@it.create_gt_filter(:number, 3)]
            result = @it.find('test_table', filters).map {|e| e}
            assert_equal 1, result.count
            assert_equal 4, result[0]['number']
          end

          should 'greater-or-equal' do
            filters = [@it.create_gte_filter(:number, 3)]
            result = @it.find('test_table', filters).map {|e| e}
            assert_equal 2, result.count
            r = {} ; result.each{|i| r[i['_id']] = i }

            assert_equal 3, r[@donkey]['number']
            assert_equal 4, r[@here]['number']
          end
        end

        should 'limit response size' do
          result = @it.find('test_table', [], limit: 1).map{|i|i}
          assert_equal 1, result.count
        end

        should 'set zero-based start index' do
          result = @it.find('test_table', [], start: 2, sort: 'number=1').map{|i|i}
          assert_equal 2, result.count
          r = {} ; result.each{|i| r[i['_id']] = i }

          assert_equal 'donkey', r[@donkey]['duck']
          assert_equal 'here', r[@here]['noduckie']
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
          @_1990   = @it.create('test_table', { n: 1, duck: 1990 })
          @nil     = @it.create('test_table', { n: 2, duck: nil })
          @empty   = @it.create('test_table', { n: 3, duck: "" })
          @monkey1 = @it.create('test_table', { n: 4, duck: 'monkey' })
          @monkey2 = @it.create('test_table', { n: 5, duck: 'donkey' })
          @monkey3 = @it.create('test_table', { n: 6, duck: 'donkey' })
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
          result = @it.collate('test_table', [], start: 3, limit: 2, sort: 'n=1')
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

          r = {} ; entries.each { |k,v| r[k] = v }

          assert_equal 3, r['donkey']
          assert_equal 2, r['unknown']
          assert_equal 1, r['monkey']
          assert_equal 1, r['1990']
          assert_equal 1, r['muppet']
        end

        should 'limit facet entries count, cutting lesser important' do
          result = @it.collate('test_table', [], facets: [:duck], facetlimit: 2)
          entries = result[:facets]['duck']
          assert_equal 2, entries.count

          r = {} ; entries.each { |k,v| r[k] = v }

          assert_equal 2, r['donkey']
          assert_equal 2, r['unknown']
        end
      end

    end
  end
end
