class Store
  class FS
    class Filter
      def initialize(field, value)
        @field = field.to_s
        @value = value
        @value = "" if value == 'unknown' || value == nil
      end
    end

    class EqualFilter < Filter
      def match?(doc)
        value2 = doc[@field]
        value2 = '' if value2 == nil
        value2 == @value
      end
    end

    class LTFilter < Filter
      def match?(doc)
        value2 = doc[@field]
        value2 = '' if value2 == nil
        value2 < @value
      end
    end

    class GTFilter < Filter
      def match?(doc)
        value2 = doc[@field]
        value2 = '' if value2 == nil
        value2 > @value
      end
    end

    class GTEFilter < Filter
      def match?(doc)
        value2 = doc[@field]
        value2 = '' if value2 == nil
        value2 >= @value
      end
    end
  end
end
