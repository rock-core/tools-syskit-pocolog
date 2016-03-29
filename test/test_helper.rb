$LOAD_PATH.unshift File.expand_path('../../lib', __FILE__)
require 'syskit/test/self'
require 'syskit/pocolog'
require 'minitest/autorun'

module Syskit::Pocolog
    module Test
        attr_reader :created_log_dir
        attr_reader :created_log_file

        def setup
            @pocolog_log_level = ::Pocolog.logger.level
            ::Pocolog.logger.level = Logger::WARN
            super
        end

        def teardown
            if @pocolog_log_level
                ::Pocolog.logger.level = @pocolog_log_level
            end
            if created_log_dir
                created_log_dir.rmtree
            end
            super
        end

        # Create a temporary directory to be used by the other log-related
        # helpers
        def create_log_dir
            @created_log_dir ||= Pathname.new(Dir.mktmpdir('syskit-pocolog-test'))
        end

        # Create a log file in a temporary directory
        #
        # @return [Pathname,Pocolog::Logfiles] the path to the file and the
        #   file object
        def create_log_file(filename, *typenames)
            create_log_dir
            path = created_log_dir + filename

            registry = Typelib::Registry.new

            @created_log_file = ::Pocolog::Logfiles.create(path.to_s, registry)
            return path.sub_ext(".0.log"), created_log_file
        end

        # Write all pending changes done to {#created_log_file} on disk
        def flush_log_file
            created_log_file.io.each(&:flush)
        end

        # Create a log stream on the last file created with
        # {#create_log_file}
        def create_log_stream(name, typename, metadata = Hash.new)
            registry = Typelib::Registry.new
            type = registry.create_null typename
            created_log_file.create_stream name, type, metadata
        end
    end
end
Minitest::Test.include Syskit::Pocolog::Test
