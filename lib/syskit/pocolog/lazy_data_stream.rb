module Syskit::Pocolog
    # Placeholder for Pocolog::DataStream that does not load any actual data /
    # index from the stream.
    #
    # It is used to manipulate the streams in the infrastructure/modelling phase
    # while only loading when actually needed
    #
    # To simplify the data management, it requires the stream to be bound to a
    # single file, which is done with 'syskit pocolog normalize'
    class LazyDataStream
        # The path to the streams' backing file
        #
        # @return [Pathname]
        attr_reader :path

        # The path to the index directory
        #
        # @return [Pathname]
        attr_reader :index_dir

        # The stream name
        #
        # @return [String]
        attr_reader :name

        # The stream type
        #
        # @return [Typelib::Type]
        attr_reader :type

        # The stream metadata
        #
        # @return [Hash]
        attr_reader :metadata

        # The size, in samples, of the stream
        #
        # @return [Integer]
        attr_reader :size

        # The realtime interval
        attr_reader :interval_rt

        # The logical-time interval
        attr_reader :interval_lg

        def initialize(path, index_dir, name, type, metadata, interval_rt, interval_lg, size)
            @path = path
            @index_dir = index_dir
            @name = name
            @type = type
            @metadata = metadata
            @interval_rt = interval_rt
            @interval_lg = interval_lg
            @size = size
        end

        # True if the size of this stream is zero
        def empty?
            size == 0
        end

        # The underlying typelib registry
        def registry
            type.registry
        end

        # Method used when the stream's data is actually needed
        #
        # @return [Pocolog::DataStream]
        def syskit_eager_load
            file = ::Pocolog::Logfiles.open(path, index_dir: index_dir)
            file.streams.first
        end

        def duration_lg
            if interval_lg.empty?
                0
            else
                interval_lg[1] - interval_lg[0]
            end
        end
    end
end
